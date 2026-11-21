//! Parallel scan and hash dedup for the high-volume path.
//!
//! Each worker reads a byte range, parses into a string arena, and appends
//! compact rows sharded by id hash. Dedup fingerprints a shard with a
//! saturating counter and emits ids that occur once. Only real duplicates
//! and fingerprint collisions go through a small open-address table.

use crate::{
    bucket, count_nl, field, find_nl, hash_bytes, list_csv, parse_links, parse_price, parse_ts,
    push_text, avx2_on, Clk, Compact, FxMap, BUY, EMPTY, LATE_MS, OK,
};
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const EVENT: u8 = 0;
const CLICK: u8 = 1;
/// When set, every SIMD line is checked against `split_unquoted`. A mismatch
/// falls back to the scalar splitter, so the output stays exact.
const VERIFY_SPANS: bool = false;

static SPAN_MISMATCH: AtomicU64 = AtomicU64::new(0);

struct Range {
    path: PathBuf,
    start: u64,
    end: u64,
    kind: u8,
}

struct EHit {
    id_hash: u64,
    user_hash: u64,
    ts: i64,
    id_off: u32,
    ing_off: u32,
    user_off: u32,
    id_len: u32,
    ing_len: u32,
    user_len: u32,
    cat: u32,
    price: u32,
    flags: u8,
}

struct CHit {
    id_hash: u64,
    user_hash: u64,
    ts: i64,
    id_off: u32,
    user_off: u32,
    id_len: u32,
    user_len: u32,
    camp: u32,
    flags: u8,
}

struct ThreadOut {
    arena: Vec<u8>,
    events: Vec<Vec<EHit>>,
    clicks: Vec<Vec<CHit>>,
}

struct ShardEmit {
    events: Vec<Compact>,
    clicks: Vec<Clk>,
    text: Vec<u8>,
}

/// High 26 bits of the id hash. 2^26 bytes is 64 MiB per shard.
const FINGER_N: usize = 1 << 26;
const FINGER_MASK: usize = FINGER_N - 1;
const HUGE_PAGE: usize = 2 << 20;
const EMPTY_SRC: u32 = u32::MAX;
/// Prefetch this many hits ahead so the counter miss is already in flight.
const PREFETCH_DIST: usize = 16;

#[derive(Clone, Copy)]
struct Loc {
    hash: u64,
    src: u32,
    row: u32,
}

struct FingerCounts {
    raw: *mut u8,
    raw_len: usize,
    ptr: *mut u8,
}

impl FingerCounts {
    fn new() -> Self {
        let raw_len = FINGER_N + HUGE_PAGE;
        let raw = mmap_anon(raw_len);
        let addr = raw as usize;
        let aligned = (addr + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        let ptr = aligned as *mut u8;
        madvise_huge(ptr, FINGER_N);
        // Fault 2 MiB pages up front. mmap memory is already zero.
        let mut off = 0usize;
        while off < FINGER_N {
            unsafe { *ptr.add(off) = 0; }
            off += HUGE_PAGE;
        }
        Self { raw, raw_len, ptr }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, FINGER_N) }
    }

    fn clear(&mut self) {
        self.as_mut().fill(0);
    }
}

impl Drop for FingerCounts {
    fn drop(&mut self) {
        munmap_anon(self.raw, self.raw_len);
    }
}

pub fn ingest(
    data: &Path,
    products: &FxMap<Vec<u8>, u32>,
    unknown: u32,
    campaigns: &FxMap<Vec<u8>, u32>,
    win_start: i64,
    win_end: i64,
    click_lo: i64,
    nthreads: usize,
    verbose: bool,
) -> io::Result<(Vec<Compact>, Vec<Clk>, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let nthreads = nthreads.max(1);
    let avx = avx2_on();
    let prod_arr = id_array(products, b"p");
    let camp_arr = id_array(campaigns, b"c_");
    let mut total = 0u64;
    let event_files = list_csv(&data.join("events"));
    let click_files = list_csv(&data.join("clicks"));
    for path in event_files.iter().chain(click_files.iter()) {
        total += fs_len(path);
    }
    let chunk = (total / nthreads as u64 / 2).max(8 << 20);
    let mut ranges = Vec::new();
    for path in &event_files {
        ranges.extend(split_file_chunk(path, EVENT, chunk)?);
    }
    for path in &click_files {
        ranges.extend(split_file_chunk(path, CLICK, chunk)?);
    }
    let link_files = list_csv(&data.join("links"));
    let queue = AtomicUsize::new(0);
    let started = Instant::now();
    let (mut threads, links) = std::thread::scope(|scope| -> io::Result<_> {
        let link_job = scope.spawn(|| parse_links(&link_files, avx));
        let mut guards = Vec::with_capacity(nthreads);
        for _ in 0..nthreads {
            let ranges = &ranges;
            let queue = &queue;
            let prod_arr = &prod_arr;
            let camp_arr = &camp_arr;
            let products = products;
            let campaigns = campaigns;
            guards.push(scope.spawn(move || {
                scan_worker(
                    ranges, queue, products, &prod_arr, unknown, campaigns, &camp_arr, win_start,
                    win_end, click_lo, nthreads, avx,
                )
            }));
        }
        let mut threads = Vec::with_capacity(guards.len());
        let mut error = None;
        for guard in guards {
            match guard.join().expect("scan thread") {
                Ok(out) => threads.push(out),
                Err(err) => error = Some(err),
            }
        }
        let links = link_job.join().expect("links thread")?;
        if let Some(err) = error {
            return Err(err);
        }
        Ok((threads, links))
    })?;
    if verbose {
        eprintln!("scan: {:.3}s", started.elapsed().as_secs_f64());
        let mismatched = SPAN_MISMATCH.swap(0, Ordering::Relaxed);
        if mismatched > 0 {
            eprintln!("span mismatches {mismatched}");
        }
    }
    let mark = Instant::now();
    let parts = dedup_parallel(&threads, nthreads);
    drop(threads);
    let (events, clicks, text) = concat_parts(parts);
    if verbose {
        eprintln!("kept events {} clicks {}", events.len(), clicks.len());
        eprintln!("dedup: {:.3}s", mark.elapsed().as_secs_f64());
    }
    Ok((events, clicks, text, links))
}

fn fs_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn split_file_chunk(path: &Path, kind: u8, chunk: u64) -> io::Result<Vec<Range>> {
    let len = fs_len(path);
    if len == 0 {
        return Ok(Vec::new());
    }
    if len <= chunk {
        return Ok(vec![Range {
            path: path.to_path_buf(),
            start: 0,
            end: len,
            kind,
        }]);
    }
    let file = File::open(path)?;
    let mut cuts = vec![0u64];
    let mut pos = chunk;
    while pos < len {
        let aligned = align_nl(&file, pos, len)?;
        if aligned <= *cuts.last().unwrap() {
            break;
        }
        if aligned >= len {
            break;
        }
        cuts.push(aligned);
        pos = aligned.saturating_add(chunk);
    }
    cuts.push(len);
    let mut out = Vec::new();
    for w in cuts.windows(2) {
        if w[1] > w[0] {
            out.push(Range {
                path: path.to_path_buf(),
                start: w[0],
                end: w[1],
                kind,
            });
        }
    }
    Ok(out)
}

fn align_nl(file: &File, pos: u64, len: u64) -> io::Result<u64> {
    let mut at = pos;
    let mut buf = [0u8; 8192];
    while at < len {
        let n = file.read_at(&mut buf, at)?;
        if n == 0 {
            return Ok(len);
        }
        for i in 0..n {
            if buf[i] == b'\n' {
                return Ok(at + i as u64 + 1);
            }
        }
        at += n as u64;
    }
    Ok(len)
}

fn scan_worker(
    ranges: &[Range],
    queue: &AtomicUsize,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    campaigns: &FxMap<Vec<u8>, u32>,
    camp_arr: &[u32],
    win_start: i64,
    win_end: i64,
    click_lo: i64,
    nthreads: usize,
    avx: bool,
) -> io::Result<ThreadOut> {
    let mut out = ThreadOut {
        arena: Vec::new(),
        events: (0..nthreads).map(|_| Vec::new()).collect(),
        clicks: (0..nthreads).map(|_| Vec::new()).collect(),
    };
    loop {
        let i = queue.fetch_add(1, Ordering::Relaxed);
        if i >= ranges.len() {
            break;
        }
        let range = &ranges[i];
        let bytes = read_range(&range.path, range.start, range.end)?;
        let skip = range.start == 0;
        if range.kind == EVENT {
            let rows = count_nl(&bytes, avx);
            out.arena.reserve(rows.saturating_mul(40));
            for part in &mut out.events {
                part.reserve(rows / nthreads + 8);
            }
            parse_events(
                &bytes, skip, &mut out, products, prod_arr, unknown, win_start, win_end, avx,
            );
        } else {
            parse_clicks(
                &bytes, skip, &mut out, campaigns, camp_arr, click_lo, win_end, avx,
            );
        }
    }
    Ok(out)
}

fn read_range(path: &Path, start: u64, end: u64) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let n = (end - start) as usize;
    let mut buf = vec![0u8; n];
    file.read_exact_at(&mut buf, start)?;
    Ok(buf)
}

fn parse_events(
    bytes: &[u8],
    skip_header: bool,
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
    avx: bool,
) {
    let mut i = 0usize;
    if skip_header {
        if bytes.is_empty() {
            return;
        }
        let header = find_nl(bytes, 0, avx);
        if header >= bytes.len() {
            return;
        }
        i = header + 1;
    }
    if avx {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            scan_events_avx2(
                bytes, i, out, products, prod_arr, unknown, win_start, win_end,
            );
        }
        #[cfg(not(target_arch = "x86_64"))]
        parse_events_scalar(
            bytes, i, out, products, prod_arr, unknown, win_start, win_end, avx,
        );
    } else {
        parse_events_scalar(
            bytes, i, out, products, prod_arr, unknown, win_start, win_end, avx,
        );
    }
}

fn parse_events_scalar(
    bytes: &[u8],
    mut i: usize,
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
    avx: bool,
) {
    let nshards = out.events.len();
    while i < bytes.len() {
        let j = find_nl(bytes, i, avx);
        if j > i {
            push_event(
                &bytes[i..j],
                out,
                products,
                prod_arr,
                unknown,
                win_start,
                win_end,
                nshards,
            );
        }
        if j >= bytes.len() {
            break;
        }
        i = j + 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_events_avx2(
    bytes: &[u8],
    mut i: usize,
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
) {
    use std::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    let ptr = bytes.as_ptr();
    let len = bytes.len();
    let comma_v = _mm256_set1_epi8(b',' as i8);
    let quote_v = _mm256_set1_epi8(b'"' as i8);
    let nl_v = _mm256_set1_epi8(b'\n' as i8);
    let nshards = out.events.len();
    let mut field = 0usize;
    let mut start = i;
    let mut quoted = false;
    let mut bad = false;
    let mut saw_quote = false;
    let mut line_start = i;
    let mut spans = [(0u32, 0u32); 7];
    while i + 32 <= len {
        let v = unsafe { _mm256_loadu_si256(ptr.add(i) as *const _) };
        let cmask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, comma_v)) } as u32;
        let qmask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, quote_v)) } as u32;
        let nmask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl_v)) } as u32;
        let mut bits = cmask | qmask | nmask;
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let pos = i + bit;
            let b = unsafe { *ptr.add(pos) };
            if b == b'\n' {
                // A comma closes only fields 0..5. The seventh field must end
                // on this newline. An earlier extra comma already set `bad`.
                if !quoted && !bad && field < 7 {
                    if close_span(bytes, &mut spans, field, start, pos, saw_quote) {
                        field += 1;
                    } else {
                        bad = true;
                    }
                } else if quoted {
                    bad = true;
                }
                if pos > line_start {
                    let line = &bytes[line_start..pos];
                    if !bad && field == 7 && (!VERIFY_SPANS || spans_agree(line, line_start, &spans))
                    {
                        push_spans(
                            bytes, &spans, out, products, prod_arr, unknown, win_start, win_end,
                            nshards,
                        );
                    } else {
                        push_event(
                            line, out, products, prod_arr, unknown, win_start, win_end, nshards,
                        );
                    }
                }
                field = 0;
                bad = false;
                quoted = false;
                saw_quote = false;
                line_start = pos + 1;
                start = line_start;
            } else if b == b'"' {
                if !quoted {
                    if pos != start {
                        bad = true;
                    } else {
                        quoted = true;
                        saw_quote = true;
                        start = pos + 1;
                    }
                } else {
                    quoted = false;
                }
            } else if !quoted {
                // Seven fields means six commas. A comma while on the last
                // field (or past it) is an extra column, which the scalar
                // splitter rejects.
                if field >= 6 || bad {
                    bad = true;
                } else if close_span(bytes, &mut spans, field, start, pos, saw_quote) {
                    field += 1;
                } else {
                    bad = true;
                }
                saw_quote = false;
                start = pos + 1;
            }
        }
        i += 32;
    }
    parse_events_scalar(
        bytes, line_start, out, products, prod_arr, unknown, win_start, win_end, true,
    );
}

fn close_span(
    bytes: &[u8],
    spans: &mut [(u32, u32); 7],
    field: usize,
    start: usize,
    pos: usize,
    saw_quote: bool,
) -> bool {
    let end = if saw_quote { pos.wrapping_sub(1) } else { pos };
    if end < start || end > bytes.len() {
        return false;
    }
    if saw_quote && (end == bytes.len() || bytes[end] != b'"') {
        return false;
    }
    spans[field] = (start as u32, end as u32);
    true
}

fn spans_agree(line: &[u8], line_start: usize, spans: &[(u32, u32); 7]) -> bool {
    let mut rel = [0u32; 7];
    let mut len = [0u32; 7];
    if !crate::split_unquoted(line, 7, &mut rel, &mut len) {
        SPAN_MISMATCH.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    for f in 0..7 {
        let a = line_start + rel[f] as usize;
        let b = a + len[f] as usize;
        if spans[f] != (a as u32, b as u32) {
            SPAN_MISMATCH.fetch_add(1, Ordering::Relaxed);
            return false;
        }
    }
    true
}

fn push_spans(
    bytes: &[u8],
    spans: &[(u32, u32); 7],
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
    nshards: usize,
) {
    let mut parts = [&b""[..]; 7];
    for i in 0..7 {
        let a = spans[i].0 as usize;
        let b = spans[i].1 as usize;
        if a > b || b > bytes.len() {
            return;
        }
        parts[i] = &bytes[a..b];
    }
    if parts[0].is_empty() {
        return;
    }
    accept_event(
        out, products, prod_arr, unknown, win_start, win_end, nshards, parts,
    );
}

fn push_event(
    line: &[u8],
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
    nshards: usize,
) {
    let mut rel = [0u32; 7];
    let mut len = [0u32; 7];
    if !crate::split_unquoted(line, 7, &mut rel, &mut len) || len[0] == 0 {
        return;
    }
    let parts = [
        field(line, rel[0], len[0]),
        field(line, rel[1], len[1]),
        field(line, rel[2], len[2]),
        field(line, rel[3], len[3]),
        field(line, rel[4], len[4]),
        field(line, rel[5], len[5]),
        field(line, rel[6], len[6]),
    ];
    accept_event(
        out, products, prod_arr, unknown, win_start, win_end, nshards, parts,
    );
}

fn accept_event(
    out: &mut ThreadOut,
    products: &FxMap<Vec<u8>, u32>,
    prod_arr: &[u32],
    unknown: u32,
    win_start: i64,
    win_end: i64,
    nshards: usize,
    parts: [&[u8]; 7],
) {
    let id = parts[0];
    let user = parts[1];
    let ts_b = parts[2];
    let ing_b = parts[3];
    let et = parts[4];
    let product = parts[5];
    let price_b = parts[6];
    let purchase = et == b"purchase";
    let type_ok = purchase || et == b"view" || et == b"cart";
    let ts = parse_ts(ts_b);
    let ingest = parse_ts(ing_b);
    let mut price = 0u32;
    let price_ok = if purchase {
        match parse_price(price_b) {
            Some(p) => {
                price = p;
                true
            }
            None => false,
        }
    } else {
        true
    };
    let ts_ok = match ts {
        Some(t) => t >= win_start && t < win_end,
        None => false,
    };
    let late_ok = match (ingest, ts) {
        (Some(i), Some(t)) => (i as i128) - (t as i128) <= LATE_MS as i128,
        _ => false,
    };
    let mut flags = 0u8;
    if !user.is_empty() && !product.is_empty() && type_ok && ts_ok && late_ok && price_ok {
        flags |= OK;
    }
    if purchase {
        flags |= BUY;
    }
    // Top category sums purchase price only. Views and carts still need a
    // non-empty product id (checked above) but their category is unused.
    let cat = if !purchase {
        0
    } else if product.is_empty() {
        unknown
    } else {
        lookup(product, b"p", prod_arr, products).unwrap_or(unknown)
    };
    // Short ASCII ids are the pack key itself. `len == 0` means that hash is
    // the key, so dedup and intern never hash or copy those bytes. Ingest
    // stays as raw text: the smallest ingest string wins, before validation.
    let (id_hash, id_off, id_len) = stash_key(&mut out.arena, id);
    let ing_off = push_raw(&mut out.arena, ing_b);
    let (user_hash, user_off, user_len) = stash_key(&mut out.arena, user);
    let shard = bucket(id_hash, nshards);
    out.events[shard].push(EHit {
        id_hash,
        user_hash,
        ts: ts.unwrap_or(0),
        id_off,
        ing_off,
        user_off,
        id_len,
        ing_len: ing_b.len() as u32,
        user_len,
        cat,
        price,
        flags,
    });
}

fn parse_clicks(
    bytes: &[u8],
    skip_header: bool,
    out: &mut ThreadOut,
    campaigns: &FxMap<Vec<u8>, u32>,
    camp_arr: &[u32],
    click_lo: i64,
    win_end: i64,
    avx: bool,
) {
    let nshards = out.clicks.len();
    let mut i = 0usize;
    if skip_header {
        if bytes.is_empty() {
            return;
        }
        let header = find_nl(bytes, 0, avx);
        if header >= bytes.len() {
            return;
        }
        i = header + 1;
    }
    while i < bytes.len() {
        let j = find_nl(bytes, i, avx);
        if j > i {
            push_click(&bytes[i..j], out, campaigns, camp_arr, click_lo, win_end, nshards);
        }
        if j >= bytes.len() {
            break;
        }
        i = j + 1;
    }
}

fn push_click(
    line: &[u8],
    out: &mut ThreadOut,
    campaigns: &FxMap<Vec<u8>, u32>,
    camp_arr: &[u32],
    click_lo: i64,
    win_end: i64,
    nshards: usize,
) {
    let mut rel = [0u32; 4];
    let mut len = [0u32; 4];
    if !crate::split_unquoted(line, 4, &mut rel, &mut len) || len[0] == 0 {
        return;
    }
    let id = field(line, rel[0], len[0]);
    let user = field(line, rel[1], len[1]);
    let camp_b = field(line, rel[2], len[2]);
    let ts_b = field(line, rel[3], len[3]);
    let ts = parse_ts(ts_b);
    let camp = lookup(camp_b, b"c_", camp_arr, campaigns);
    let mut flags = 0u8;
    if !user.is_empty() && camp.is_some() && matches!(ts, Some(t) if t >= click_lo && t < win_end)
    {
        flags |= OK;
    }
    let (id_hash, id_off, id_len) = stash_key(&mut out.arena, id);
    let (user_hash, user_off, user_len) = stash_key(&mut out.arena, user);
    let shard = bucket(id_hash, nshards);
    out.clicks[shard].push(CHit {
        id_hash,
        user_hash,
        ts: ts.unwrap_or(0),
        id_off,
        user_off,
        id_len,
        user_len,
        camp: camp.unwrap_or(0),
        flags,
    });
}

/// Pack 1..=9 ASCII bytes into the hash and skip the arena. Anything else is
/// hashed and copied. A zero length is a packed key only when the hash is the
/// key; empty input does not pack, so it is stored as a real (empty) copy.
fn stash_key(arena: &mut Vec<u8>, bytes: &[u8]) -> (u64, u32, u32) {
    let key = crate::pack_id(bytes);
    if key != 0 {
        (key, 0, 0)
    } else {
        (hash_bytes(bytes), push_raw(arena, bytes), bytes.len() as u32)
    }
}

fn push_raw(arena: &mut Vec<u8>, bytes: &[u8]) -> u32 {
    let off = arena.len();
    assert!(off <= u32::MAX as usize, "arena exceeded 4 GiB");
    arena.extend_from_slice(bytes);
    off as u32
}

fn slice<'a>(arena: &'a [u8], off: u32, len: u32) -> &'a [u8] {
    let s = off as usize;
    let n = len as usize;
    &arena[s..s + n]
}

fn dedup_parallel(threads: &[ThreadOut], nthreads: usize) -> Vec<ShardEmit> {
    std::thread::scope(|scope| {
        let mut guards = Vec::with_capacity(nthreads);
        for shard in 0..nthreads {
            let threads = threads;
            guards.push(scope.spawn(move || dedup_shard(threads, shard)));
        }
        let mut out = Vec::with_capacity(guards.len());
        for guard in guards {
            out.push(guard.join().expect("dedup thread"));
        }
        out
    })
}

fn dedup_shard(threads: &[ThreadOut], shard: usize) -> ShardEmit {
    let mut emit = ShardEmit {
        events: Vec::new(),
        clicks: Vec::new(),
        text: Vec::new(),
    };
    let mut counts = FingerCounts::new();
    dedup_events(threads, shard, &mut emit, counts.as_mut());
    counts.clear();
    dedup_clicks(threads, shard, &mut emit, counts.as_mut());
    emit
}

fn dedup_events(threads: &[ThreadOut], shard: usize, emit: &mut ShardEmit, count: &mut [u8]) {
    let mut n = 0usize;
    for thread in threads {
        let hits = &thread.events[shard];
        n += hits.len();
        bump_event_counts(hits, count);
    }
    if n == 0 {
        return;
    }
    emit.events.reserve(n);
    emit.text.reserve(n.saturating_mul(16));
    let mut suspects = Vec::new();
    let ptr = count.as_ptr();
    for (ti, thread) in threads.iter().enumerate() {
        let hits = &thread.events[shard];
        let mut k = 0usize;
        while k < hits.len() {
            if k + PREFETCH_DIST < hits.len() {
                prefetch_count(ptr, finger(hits[k + PREFETCH_DIST].id_hash));
            }
            let hit = &hits[k];
            let c = unsafe { *ptr.add(finger(hit.id_hash)) };
            if c == 1 {
                emit_event_hit(thread, hit, emit);
            } else {
                suspects.push(Loc {
                    hash: hit.id_hash,
                    src: ti as u32,
                    row: k as u32,
                });
            }
            k += 1;
        }
    }
    resolve_events(threads, shard, &suspects, emit);
}

fn dedup_clicks(threads: &[ThreadOut], shard: usize, emit: &mut ShardEmit, count: &mut [u8]) {
    let mut n = 0usize;
    for thread in threads {
        let hits = &thread.clicks[shard];
        n += hits.len();
        bump_click_counts(hits, count);
    }
    if n == 0 {
        return;
    }
    emit.clicks.reserve(n);
    emit.text.reserve(n.saturating_mul(32));
    let mut suspects = Vec::new();
    let ptr = count.as_ptr();
    for (ti, thread) in threads.iter().enumerate() {
        let hits = &thread.clicks[shard];
        let mut k = 0usize;
        while k < hits.len() {
            if k + PREFETCH_DIST < hits.len() {
                prefetch_count(ptr, finger(hits[k + PREFETCH_DIST].id_hash));
            }
            let hit = &hits[k];
            let c = unsafe { *ptr.add(finger(hit.id_hash)) };
            if c == 1 {
                emit_click_hit(thread, hit, emit);
            } else {
                suspects.push(Loc {
                    hash: hit.id_hash,
                    src: ti as u32,
                    row: k as u32,
                });
            }
            k += 1;
        }
    }
    resolve_clicks(threads, shard, &suspects, emit);
}

#[inline(always)]
fn finger(hash: u64) -> usize {
    // Pack keys are not uniform in the high bits. Mix before the 26-bit
    // counter so unique ids do not all fall into the same slots.
    ((crate::mix_key(hash) >> 32) as usize) & FINGER_MASK
}

fn bump_event_counts(hits: &[EHit], count: &mut [u8]) {
    let ptr = count.as_mut_ptr();
    let n = hits.len();
    let mut k = 0usize;
    while k < n {
        if k + PREFETCH_DIST < n {
            prefetch_count(ptr, finger(hits[k + PREFETCH_DIST].id_hash));
        }
        bump_count(ptr, hits[k].id_hash);
        k += 1;
    }
}

fn bump_click_counts(hits: &[CHit], count: &mut [u8]) {
    let ptr = count.as_mut_ptr();
    let n = hits.len();
    let mut k = 0usize;
    while k < n {
        if k + PREFETCH_DIST < n {
            prefetch_count(ptr, finger(hits[k + PREFETCH_DIST].id_hash));
        }
        bump_count(ptr, hits[k].id_hash);
        k += 1;
    }
}

#[inline(always)]
fn bump_count(ptr: *mut u8, hash: u64) {
    unsafe {
        let c = &mut *ptr.add(finger(hash));
        *c = c.saturating_add(1);
    }
}

#[inline(always)]
fn prefetch_count(ptr: *const u8, idx: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(
            ptr.add(idx) as *const i8,
            std::arch::x86_64::_MM_HINT_T0,
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (ptr, idx);
    }
}

fn resolve_events(threads: &[ThreadOut], shard: usize, suspects: &[Loc], emit: &mut ShardEmit) {
    if suspects.is_empty() {
        return;
    }
    let cap = table_cap(suspects.len());
    let mut slots = vec![empty_loc(); cap];
    let mask = cap - 1;
    for s in suspects {
        let mut i = slot_index(s.hash, mask);
        let mut steps = 0usize;
        loop {
            let src = slots[i].src;
            if src == EMPTY_SRC {
                slots[i] = *s;
                break;
            }
            if slots[i].hash == s.hash
                && same_event_id(threads, shard, src, slots[i].row, s.src, s.row)
            {
                // Smallest ingest wins. Equal ingest copies are identical.
                if event_ing(threads, shard, s.src, s.row) < event_ing(threads, shard, src, slots[i].row)
                {
                    slots[i].src = s.src;
                    slots[i].row = s.row;
                }
                break;
            }
            i = (i + 1) & mask;
            steps += 1;
            if steps > mask {
                panic!("event dedup table full");
            }
        }
    }
    for slot in &slots {
        if slot.src != EMPTY_SRC {
            let thread = &threads[slot.src as usize];
            let hit = &thread.events[shard][slot.row as usize];
            emit_event_hit(thread, hit, emit);
        }
    }
}

fn resolve_clicks(threads: &[ThreadOut], shard: usize, suspects: &[Loc], emit: &mut ShardEmit) {
    if suspects.is_empty() {
        return;
    }
    let cap = table_cap(suspects.len());
    let mut slots = vec![empty_loc(); cap];
    let mask = cap - 1;
    for s in suspects {
        let mut i = slot_index(s.hash, mask);
        let mut steps = 0usize;
        loop {
            let src = slots[i].src;
            if src == EMPTY_SRC {
                slots[i] = *s;
                break;
            }
            if slots[i].hash == s.hash && same_click_id(threads, shard, src, slots[i].row, s.src, s.row)
            {
                break;
            }
            i = (i + 1) & mask;
            steps += 1;
            if steps > mask {
                panic!("click dedup table full");
            }
        }
    }
    for slot in &slots {
        if slot.src != EMPTY_SRC {
            let thread = &threads[slot.src as usize];
            let hit = &thread.clicks[shard][slot.row as usize];
            emit_click_hit(thread, hit, emit);
        }
    }
}

fn empty_loc() -> Loc {
    Loc {
        hash: 0,
        src: EMPTY_SRC,
        row: 0,
    }
}

fn table_cap(n: usize) -> usize {
    n.next_power_of_two().saturating_mul(2).max(16)
}

#[inline(always)]
fn slot_index(hash: u64, mask: usize) -> usize {
    // Low bits select the shard. A multiply would keep those bits frozen in
    // the index, so fold the high half down before masking.
    let mixed = hash ^ hash.rotate_left(32);
    (mixed as usize) & mask
}

fn same_event_id(
    threads: &[ThreadOut],
    shard: usize,
    a_src: u32,
    a_row: u32,
    b_src: u32,
    b_row: u32,
) -> bool {
    let a = &threads[a_src as usize].events[shard][a_row as usize];
    let b = &threads[b_src as usize].events[shard][b_row as usize];
    // Both packed: the hash is the id. A packed key never equals a copied id.
    if a.id_len == 0 && b.id_len == 0 {
        return a.id_hash == b.id_hash;
    }
    if a.id_len == 0 || b.id_len == 0 {
        return false;
    }
    slice(&threads[a_src as usize].arena, a.id_off, a.id_len)
        == slice(&threads[b_src as usize].arena, b.id_off, b.id_len)
}

fn event_ing<'a>(threads: &'a [ThreadOut], shard: usize, src: u32, row: u32) -> &'a [u8] {
    let hit = &threads[src as usize].events[shard][row as usize];
    slice(&threads[src as usize].arena, hit.ing_off, hit.ing_len)
}

fn same_click_id(
    threads: &[ThreadOut],
    shard: usize,
    a_src: u32,
    a_row: u32,
    b_src: u32,
    b_row: u32,
) -> bool {
    let a = &threads[a_src as usize].clicks[shard][a_row as usize];
    let b = &threads[b_src as usize].clicks[shard][b_row as usize];
    if a.id_len == 0 && b.id_len == 0 {
        return a.id_hash == b.id_hash;
    }
    if a.id_len == 0 || b.id_len == 0 {
        return false;
    }
    slice(&threads[a_src as usize].arena, a.id_off, a.id_len)
        == slice(&threads[b_src as usize].arena, b.id_off, b.id_len)
}

fn emit_event_hit(thread: &ThreadOut, hit: &EHit, emit: &mut ShardEmit) {
    if hit.flags & OK == 0 {
        return;
    }
    // user_len == 0 on a kept row is a packed user. Intern reads the key.
    if hit.user_len == 0 {
        emit.events.push(Compact {
            user_hash: hit.user_hash,
            ts: hit.ts,
            user_off: 0,
            user_len: 0,
            cat: hit.cat,
            price: hit.price,
            purchase: hit.flags & BUY != 0,
        });
        return;
    }
    let user = slice(&thread.arena, hit.user_off, hit.user_len);
    let (user_off, user_len) = push_text(&mut emit.text, user);
    emit.events.push(Compact {
        user_hash: hit.user_hash,
        ts: hit.ts,
        user_off,
        user_len,
        cat: hit.cat,
        price: hit.price,
        purchase: hit.flags & BUY != 0,
    });
}

fn emit_click_hit(thread: &ThreadOut, hit: &CHit, emit: &mut ShardEmit) {
    if hit.flags & OK == 0 {
        return;
    }
    let (user_off, user_len) = if hit.user_len == 0 {
        (0, 0)
    } else {
        let user = slice(&thread.arena, hit.user_off, hit.user_len);
        push_text(&mut emit.text, user)
    };
    // Click-id ties are bytewise on the original id, not on the pack key.
    let (id_off, id_len) = if hit.id_len == 0 {
        let mut unpacked = [0u8; 9];
        let n = crate::unpack_id(hit.id_hash, &mut unpacked);
        debug_assert!(n > 0);
        push_text(&mut emit.text, &unpacked[..n])
    } else {
        let id = slice(&thread.arena, hit.id_off, hit.id_len);
        push_text(&mut emit.text, id)
    };
    emit.clicks.push(Clk {
        user_hash: hit.user_hash,
        ts: hit.ts,
        user_off,
        user_len,
        camp: hit.camp,
        id_off,
        id_len,
    });
}

fn mmap_anon(len: usize) -> *mut u8 {
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 2;
    const MAP_ANONYMOUS: i32 = 0x20;
    extern "C" {
        fn mmap(
            addr: *mut u8,
            len: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            offset: i64,
        ) -> *mut u8;
    }
    let ptr = unsafe { mmap(std::ptr::null_mut(), len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) };
    if ptr as usize == usize::MAX {
        panic!("dedup fingerprint mmap failed");
    }
    ptr
}

fn madvise_huge(ptr: *mut u8, len: usize) {
    const MADV_HUGEPAGE: i32 = 14;
    extern "C" {
        fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
    unsafe {
        let _ = madvise(ptr, len, MADV_HUGEPAGE);
    }
}

fn munmap_anon(ptr: *mut u8, len: usize) {
    extern "C" {
        fn munmap(addr: *mut u8, len: usize) -> i32;
    }
    unsafe {
        let _ = munmap(ptr, len);
    }
}

fn concat_parts(parts: Vec<ShardEmit>) -> (Vec<Compact>, Vec<Clk>, Vec<u8>) {
    let mut events = Vec::new();
    let mut clicks = Vec::new();
    let mut text = Vec::new();
    for part in parts {
        let base = text.len() as u32;
        text.extend_from_slice(&part.text);
        for mut row in part.events {
            row.user_off = row.user_off.wrapping_add(base);
            events.push(row);
        }
        for mut row in part.clicks {
            row.user_off = row.user_off.wrapping_add(base);
            row.id_off = row.id_off.wrapping_add(base);
            clicks.push(row);
        }
    }
    (events, clicks, text)
}

fn id_array(map: &FxMap<Vec<u8>, u32>, prefix: &[u8]) -> Vec<u32> {
    let mut max = 0usize;
    let mut any = false;
    for key in map.keys() {
        if let Some(n) = pref_num(key, prefix) {
            any = true;
            if n > max {
                max = n;
            }
        }
    }
    if !any || max > 200_000 {
        return Vec::new();
    }
    let mut arr = vec![EMPTY; max + 1];
    for (key, &id) in map {
        if let Some(n) = pref_num(key, prefix) {
            if n < arr.len() {
                arr[n] = id;
            }
        }
    }
    arr
}

fn lookup(s: &[u8], prefix: &[u8], arr: &[u32], map: &FxMap<Vec<u8>, u32>) -> Option<u32> {
    if !arr.is_empty() {
        if let Some(n) = pref_num(s, prefix) {
            if n < arr.len() {
                let v = arr[n];
                if v != EMPTY {
                    return Some(v);
                }
            }
            return None;
        }
    }
    map.get(s).copied()
}

fn pref_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    if s.len() <= prefix.len() || !s.starts_with(prefix) {
        return None;
    }
    let rest = &s[prefix.len()..];
    if rest.is_empty() || rest[0] == b'0' {
        return None;
    }
    let mut n: u32 = 0;
    for &b in rest {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as u32)?;
    }
    Some(n as usize)
}
