//! Parallel scan and hash dedup for the high-volume path.
//!
//! Each worker reads a byte range, parses into a string arena, and appends
//! compact rows sharded by id hash. Dedup is then an open-address table per
//! shard, so equal ids never meet in a single-threaded merge.

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
const VERIFY_SPANS: bool = true;

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

#[derive(Clone, Copy)]
struct DSlot {
    hash: u64,
    src: u32,
    row: u32,
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
    let cat = if product.is_empty() {
        unknown
    } else {
        lookup(product, b"p", prod_arr, products).unwrap_or(unknown)
    };
    let id_hash = hash_bytes(id);
    let user_hash = hash_bytes(user);
    let id_off = push_raw(&mut out.arena, id);
    let ing_off = push_raw(&mut out.arena, ing_b);
    let user_off = push_raw(&mut out.arena, user);
    let shard = bucket(id_hash, nshards);
    out.events[shard].push(EHit {
        id_hash,
        user_hash,
        ts: ts.unwrap_or(0),
        id_off,
        ing_off,
        user_off,
        id_len: id.len() as u32,
        ing_len: ing_b.len() as u32,
        user_len: user.len() as u32,
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
    let id_hash = hash_bytes(id);
    let user_hash = hash_bytes(user);
    let id_off = push_raw(&mut out.arena, id);
    let user_off = push_raw(&mut out.arena, user);
    let shard = bucket(id_hash, nshards);
    out.clicks[shard].push(CHit {
        id_hash,
        user_hash,
        ts: ts.unwrap_or(0),
        id_off,
        user_off,
        id_len: id.len() as u32,
        user_len: user.len() as u32,
        camp: camp.unwrap_or(0),
        flags,
    });
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
    dedup_events(threads, shard, &mut emit);
    dedup_clicks(threads, shard, &mut emit);
    emit
}

fn dedup_events(threads: &[ThreadOut], shard: usize, emit: &mut ShardEmit) {
    let mut keys = Vec::new();
    for (ti, thread) in threads.iter().enumerate() {
        for (ri, hit) in thread.events[shard].iter().enumerate() {
            keys.push(DSlot {
                hash: hit.id_hash,
                src: ti as u32,
                row: ri as u32,
            });
        }
    }
    if keys.is_empty() {
        return;
    }
    radix_hash(&mut keys);
    emit.events.reserve(keys.len());
    let mut i = 0usize;
    while i < keys.len() {
        let hash = keys[i].hash;
        let mut j = i + 1;
        while j < keys.len() && keys[j].hash == hash {
            j += 1;
        }
        if j == i + 1 {
            emit_event(threads, shard, keys[i], emit);
        } else {
            let group = &mut keys[i..j];
            group.sort_by(|a, b| {
                ev_id(threads, shard, *a)
                    .cmp(ev_id(threads, shard, *b))
                    .then_with(|| ev_ing(threads, shard, *a).cmp(ev_ing(threads, shard, *b)))
            });
            let mut prev: &[u8] = b"";
            let mut have = false;
            for key in group.iter().copied() {
                let id = ev_id(threads, shard, key);
                if have && id == prev {
                    continue;
                }
                prev = id;
                have = true;
                emit_event(threads, shard, key, emit);
            }
        }
        i = j;
    }
}

fn dedup_clicks(threads: &[ThreadOut], shard: usize, emit: &mut ShardEmit) {
    let mut keys = Vec::new();
    for (ti, thread) in threads.iter().enumerate() {
        for (ri, hit) in thread.clicks[shard].iter().enumerate() {
            keys.push(DSlot {
                hash: hit.id_hash,
                src: ti as u32,
                row: ri as u32,
            });
        }
    }
    if keys.is_empty() {
        return;
    }
    radix_hash(&mut keys);
    emit.clicks.reserve(keys.len());
    let mut i = 0usize;
    while i < keys.len() {
        let hash = keys[i].hash;
        let mut j = i + 1;
        while j < keys.len() && keys[j].hash == hash {
            j += 1;
        }
        if j == i + 1 {
            emit_click(threads, shard, keys[i], emit);
        } else {
            let group = &mut keys[i..j];
            group.sort_by(|a, b| cl_id(threads, shard, *a).cmp(cl_id(threads, shard, *b)));
            let mut prev: &[u8] = b"";
            let mut have = false;
            for key in group.iter().copied() {
                let id = cl_id(threads, shard, key);
                if have && id == prev {
                    continue;
                }
                prev = id;
                have = true;
                emit_click(threads, shard, key, emit);
            }
        }
        i = j;
    }
}

fn radix_hash(keys: &mut Vec<DSlot>) {
    let n = keys.len();
    if n < 2 {
        return;
    }
    let mut tmp = vec![DSlot { hash: 0, src: 0, row: 0 }; n];
    for pass in 0..8 {
        let shift = pass * 8;
        let mut hist = [0u32; 256];
        for key in keys.iter() {
            hist[((key.hash >> shift) & 255) as usize] += 1;
        }
        let mut sum = 0u32;
        for slot in hist.iter_mut() {
            let count = *slot;
            *slot = sum;
            sum += count;
        }
        for key in keys.iter() {
            let b = ((key.hash >> shift) & 255) as usize;
            tmp[hist[b] as usize] = *key;
            hist[b] += 1;
        }
        std::mem::swap(keys, &mut tmp);
    }
}

fn ev_id<'a>(threads: &'a [ThreadOut], shard: usize, key: DSlot) -> &'a [u8] {
    let hit = &threads[key.src as usize].events[shard][key.row as usize];
    slice(&threads[key.src as usize].arena, hit.id_off, hit.id_len)
}

fn ev_ing<'a>(threads: &'a [ThreadOut], shard: usize, key: DSlot) -> &'a [u8] {
    let hit = &threads[key.src as usize].events[shard][key.row as usize];
    slice(&threads[key.src as usize].arena, hit.ing_off, hit.ing_len)
}

fn cl_id<'a>(threads: &'a [ThreadOut], shard: usize, key: DSlot) -> &'a [u8] {
    let hit = &threads[key.src as usize].clicks[shard][key.row as usize];
    slice(&threads[key.src as usize].arena, hit.id_off, hit.id_len)
}

fn emit_event(threads: &[ThreadOut], shard: usize, key: DSlot, emit: &mut ShardEmit) {
    let thread = &threads[key.src as usize];
    let hit = &thread.events[shard][key.row as usize];
    if hit.flags & OK == 0 {
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

fn emit_click(threads: &[ThreadOut], shard: usize, key: DSlot, emit: &mut ShardEmit) {
    let thread = &threads[key.src as usize];
    let hit = &thread.clicks[shard][key.row as usize];
    if hit.flags & OK == 0 {
        return;
    }
    let user = slice(&thread.arena, hit.user_off, hit.user_len);
    let id = slice(&thread.arena, hit.id_off, hit.id_len);
    let (user_off, user_len) = push_text(&mut emit.text, user);
    let (id_off, id_len) = push_text(&mut emit.text, id);
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
