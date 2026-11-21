//! Shopping Sessions, std-only, tuned for the ~20M practice set.
//!
//! One parallel scan turns CSV into compact rows. Dedup is a radix sort on
//! the id hash, with byte compares only inside equal-hash groups. Person
//! ids are interned in hash shards, then sessions and assists run per person
//! shard against a dense campaign-day grid. The binary writes Parquet so the
//! scored process does not import PyArrow.

mod fast;
mod parquet;

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const PRICE_MAX: u32 = 10_000_000;
const OK: u8 = 1;
const BUY: u8 = 2;
const EMPTY: u32 = u32::MAX;

struct FxHasher(u64);
impl Default for FxHasher {
    fn default() -> Self {
        Self(0x517cc1b727220a95)
    }
}
impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[..8]);
            let x = u64::from_le_bytes(buf);
            self.0 = self.0.wrapping_add(x).wrapping_mul(0x517cc1b727220a95);
            bytes = &bytes[8..];
        }
        for &b in bytes {
            self.0 = self.0.wrapping_add(b as u64).wrapping_mul(0x517cc1b727220a95);
        }
    }
}
type FxBuild = BuildHasherDefault<FxHasher>;
type FxMap<K, V> = HashMap<K, V, FxBuild>;

fn fxmap<K, V>() -> FxMap<K, V> {
    HashMap::with_hasher(FxBuild::default())
}

#[inline(always)]
fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0x9E3779B185EBCA87;
    let mut i = 0;
    while i + 8 <= bytes.len() {
        let x = unsafe { std::ptr::read_unaligned(bytes.as_ptr().add(i) as *const u64) };
        h = (h ^ x).wrapping_mul(0xC2B2AE3D27D4EB4F);
        i += 8;
    }
    if i + 4 <= bytes.len() {
        let mut b = [0u8; 4];
        b.copy_from_slice(&bytes[i..i + 4]);
        h = (h ^ u32::from_le_bytes(b) as u64).wrapping_mul(0xC2B2AE3D27D4EB4F);
        i += 4;
    }
    while i < bytes.len() {
        h = (h ^ bytes[i] as u64).wrapping_mul(0xC2B2AE3D27D4EB4F);
        i += 1;
    }
    h ^= h >> 33;
    h
}

fn bucket(hash: u64, n: usize) -> usize {
    if n.is_power_of_two() {
        (hash as usize) & (n - 1)
    } else {
        (hash as usize) % n
    }
}

#[derive(Clone, Copy)]
struct RawEv {
    id_hash: u64,
    user_hash: u64,
    ts: i64,
    line_off: u32,
    id_rel: u32,
    id_len: u32,
    ing_rel: u32,
    ing_len: u32,
    user_rel: u32,
    user_len: u32,
    cat: u32,
    price: u32,
    buf: u16,
    flags: u8,
}

#[derive(Clone, Copy)]
struct RawClick {
    id_hash: u64,
    user_hash: u64,
    ts: i64,
    line_off: u32,
    id_rel: u32,
    id_len: u32,
    user_rel: u32,
    user_len: u32,
    camp: u32,
    buf: u16,
    flags: u8,
}

struct Shard {
    bufs: Vec<Vec<u8>>,
    events: Vec<RawEv>,
    clicks: Vec<RawClick>,
}

#[derive(Clone, Copy)]
struct Compact {
    user_hash: u64,
    ts: i64,
    user_off: u32,
    user_len: u32,
    cat: u32,
    price: u32,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Clk {
    user_hash: u64,
    ts: i64,
    user_off: u32,
    user_len: u32,
    camp: u32,
    id_off: u32,
    id_len: u32,
}

#[derive(Clone, Copy)]
struct Slim {
    ts: i64,
    person: u32,
    cat: u32,
    price: u32,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Click {
    ts: i64,
    person: u32,
    camp: u32,
    id_off: u32,
    id_len: u32,
}

#[derive(Clone, Copy, Default)]
struct DKey {
    hash: u64,
    shard: u32,
    row: u32,
}

struct Sess {
    person: u32,
    start: i64,
    day: i32,
    revenue: u64,
    has_purchase: bool,
}

struct UserRow {
    person: u32,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: u32,
}

struct CampOut {
    camp: u32,
    day: i32,
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[derive(Clone, Copy, Default)]
struct Cell {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

struct Store {
    cells: Vec<Cell>,
    map: FxMap<(u32, i32), Cell>,
    stride: usize,
    min_day: i32,
    ndays: usize,
    n_camp: u32,
    dense: bool,
}

impl Store {
    fn new(min_day: i32, ndays: usize, n_camp: u32) -> Self {
        let stride = n_camp as usize + 1;
        let cells_n = ndays.saturating_mul(stride);
        let dense = ndays > 0 && cells_n <= 2_000_000;
        Self {
            cells: if dense {
                vec![Cell::default(); cells_n]
            } else {
                Vec::new()
            },
            map: fxmap(),
            stride,
            min_day,
            ndays,
            n_camp,
            dense,
        }
    }

    fn cell_mut(&mut self, camp: u32, day: i32) -> &mut Cell {
        if self.dense {
            let di = (day as i64 - self.min_day as i64) as usize;
            return &mut self.cells[di * self.stride + camp as usize];
        }
        self.map.entry((camp, day)).or_default()
    }
}

#[derive(Clone, Copy)]
struct Slot {
    /// Packed id. 0 means the bytes did not pack; compare those with `name`.
    key: u64,
    id: u32,
}

struct UserShard {
    slots: Vec<Slot>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    /// Packed key for each span, or 0 when the name did not pack.
    keys: Vec<u64>,
    used: usize,
}

/// Pack 1..=9 bytes, each < 128, into a u64: 7 bits per byte and a marker at
/// bit `7 * len` so the length lives in the high bits. 0 means "does not pack".
#[inline(always)]
fn pack_id(bytes: &[u8]) -> u64 {
    let n = bytes.len();
    if n == 9 {
        return pack9(bytes);
    }
    if n == 8 {
        return pack8(bytes);
    }
    if n == 0 || n > 9 {
        return 0;
    }
    let mut hi = 0u8;
    for &b in bytes {
        hi |= b;
    }
    if hi >= 128 {
        return 0;
    }
    let mut key = 1u64;
    for &b in bytes {
        key = (key << 7) | u64::from(b);
    }
    key
}

#[inline(always)]
fn pack9(bytes: &[u8]) -> u64 {
    let w = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let b8 = u64::from(bytes[8]);
    if (w & 0x8080_8080_8080_8080) != 0 || b8 >= 128 {
        return 0;
    }
    let b0 = w & 0xff;
    let b1 = (w >> 8) & 0xff;
    let b2 = (w >> 16) & 0xff;
    let b3 = (w >> 24) & 0xff;
    let b4 = (w >> 32) & 0xff;
    let b5 = (w >> 40) & 0xff;
    let b6 = (w >> 48) & 0xff;
    let b7 = (w >> 56) & 0xff;
    (1u64 << 63)
        | (b0 << 56)
        | (b1 << 49)
        | (b2 << 42)
        | (b3 << 35)
        | (b4 << 28)
        | (b5 << 21)
        | (b6 << 14)
        | (b7 << 7)
        | b8
}

/// Inverse of `pack_id`. Returns 0 when `key` is 0 or not a packed id.
fn unpack_id(key: u64, out: &mut [u8; 9]) -> usize {
    if key == 0 {
        return 0;
    }
    let bits = 63 - key.leading_zeros();
    if bits == 0 || bits % 7 != 0 {
        return 0;
    }
    let n = (bits / 7) as usize;
    if n == 0 || n > 9 {
        return 0;
    }
    let mut i = 0usize;
    while i < n {
        let shift = 7 * (n - 1 - i);
        out[i] = ((key >> shift) & 0x7f) as u8;
        i += 1;
    }
    n
}

/// Spread a packed id. The raw key's low bits are the last characters, so
/// using it as a table index builds long probe chains.
#[inline(always)]
fn mix_key(key: u64) -> u64 {
    let mut h = key.wrapping_mul(0x9E3779B185EBCA87);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC2B2AE3D27D4EB4F);
    h ^= h >> 29;
    h
}

/// Shard and table index for an id. Packed ids mix the key; the key itself
/// stays the equality check. Unpacked ids use `hash_bytes`.
fn id_probe(bytes: &[u8]) -> u64 {
    let key = pack_id(bytes);
    if key != 0 {
        mix_key(key)
    } else {
        hash_bytes(bytes)
    }
}

/// `user_len == 0` stores the raw pack key in `user_hash`.
#[inline(always)]
fn user_probe(user_hash: u64, user_len: u32) -> u64 {
    if user_len == 0 {
        mix_key(user_hash)
    } else {
        user_hash
    }
}

#[inline(always)]
fn pack8(bytes: &[u8]) -> u64 {
    let w = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    if w & 0x8080_8080_8080_8080 != 0 {
        return 0;
    }
    let b0 = w & 0xff;
    let b1 = (w >> 8) & 0xff;
    let b2 = (w >> 16) & 0xff;
    let b3 = (w >> 24) & 0xff;
    let b4 = (w >> 32) & 0xff;
    let b5 = (w >> 40) & 0xff;
    let b6 = (w >> 48) & 0xff;
    let b7 = (w >> 56) & 0xff;
    (1u64 << 56)
        | (b0 << 49)
        | (b1 << 42)
        | (b2 << 35)
        | (b3 << 28)
        | (b4 << 21)
        | (b5 << 14)
        | (b6 << 7)
        | b7
}

impl UserShard {
    fn with_capacity(n: usize) -> Self {
        let slots = (n.max(1) * 2).next_power_of_two().max(16);
        Self {
            slots: vec![Slot { key: 0, id: EMPTY }; slots],
            arena: Vec::new(),
            spans: Vec::new(),
            keys: Vec::new(),
            used: 0,
        }
    }

    fn push_user(&mut self, key: u64, bytes: &[u8]) -> u32 {
        let id = self.spans.len() as u32;
        let off = self.arena.len();
        assert!(off <= u32::MAX as usize, "name arena exceeded 4 GiB");
        self.arena.extend_from_slice(bytes);
        self.spans.push((off as u32, bytes.len() as u32));
        self.keys.push(key);
        self.used += 1;
        id
    }

    fn rebuild_slots(&mut self) {
        let n = (self.used.max(1) * 2).next_power_of_two().max(16);
        let mut hashes = Vec::with_capacity(self.keys.len());
        for (id, &key) in self.keys.iter().enumerate() {
            let hash = if key != 0 {
                mix_key(key)
            } else {
                hash_bytes(self.name(id as u32))
            };
            hashes.push((hash, key, id as u32));
        }
        self.slots = vec![Slot { key: 0, id: EMPTY }; n];
        for (hash, key, id) in hashes {
            self.insert_slot(hash, key, id);
        }
    }

    fn name(&self, id: u32) -> &[u8] {
        let (off, len) = self.spans[id as usize];
        &self.arena[off as usize..off as usize + len as usize]
    }

    fn find(&self, hash: u64, bytes: &[u8]) -> Option<u32> {
        self.lookup(hash, pack_id(bytes), bytes)
    }

    #[inline(always)]
    fn lookup(&self, hash: u64, key: u64, bytes: &[u8]) -> Option<u32> {
        let n = self.slots.len();
        if n == 0 {
            return None;
        }
        let mask = n - 1;
        let mut i = (hash as usize) & mask;
        if key != 0 {
            for _ in 0..n {
                let slot = self.slots[i];
                if slot.id == EMPTY {
                    return None;
                }
                if slot.key == key {
                    return Some(slot.id);
                }
                i = (i + 1) & mask;
            }
            return None;
        }
        for _ in 0..n {
            let slot = self.slots[i];
            if slot.id == EMPTY {
                return None;
            }
            if slot.key == 0 && self.name(slot.id) == bytes {
                return Some(slot.id);
            }
            i = (i + 1) & mask;
        }
        None
    }

    fn insert_slot(&mut self, hash: u64, key: u64, id: u32) {
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        while self.slots[i].id != EMPTY {
            i = (i + 1) & mask;
        }
        self.slots[i] = Slot { key, id };
    }

    fn rehash(&mut self, n: usize) {
        let old = std::mem::replace(&mut self.slots, vec![Slot { key: 0, id: EMPTY }; n]);
        for slot in old {
            if slot.id != EMPTY {
                let hash = if slot.key != 0 {
                    mix_key(slot.key)
                } else {
                    hash_bytes(self.name(slot.id))
                };
                self.insert_slot(hash, slot.key, slot.id);
            }
        }
    }

    fn intern_packed(&mut self, key: u64) -> u32 {
        let hash = mix_key(key);
        if !self.slots.is_empty() {
            if let Some(id) = self.lookup(hash, key, b"") {
                return id;
            }
        }
        let mut buf = [0u8; 9];
        let n = unpack_id(key, &mut buf);
        self.intern(hash, &buf[..n])
    }

    fn intern(&mut self, hash: u64, bytes: &[u8]) -> u32 {
        let key = pack_id(bytes);
        if !self.slots.is_empty() {
            if let Some(id) = self.lookup(hash, key, bytes) {
                return id;
            }
        }
        if self.slots.is_empty() || (self.used + 1) * 10 >= self.slots.len() * 7 {
            let n = (self.slots.len().max(1) * 2).next_power_of_two().max(16);
            self.rehash(n);
        }
        let id = self.spans.len() as u32;
        let off = self.arena.len();
        assert!(off <= u32::MAX as usize, "name arena exceeded 4 GiB");
        self.arena.extend_from_slice(bytes);
        self.spans.push((off as u32, bytes.len() as u32));
        self.keys.push(key);
        self.insert_slot(hash, key, id);
        self.used += 1;
        id
    }
}

struct Names {
    shards: Vec<UserShard>,
    prefix: Vec<u32>,
}

impl Names {
    fn name(&self, person: u32) -> &[u8] {
        let s = shard_of(&self.prefix, person);
        let local = person - self.prefix[s];
        self.shards[s].name(local)
    }
}

fn shard_of(prefix: &[u32], id: u32) -> usize {
    let mut lo = 0usize;
    let mut hi = prefix.len() - 1;
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if prefix[mid] <= id {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

enum Job {
    Event(PathBuf),
    Click(PathBuf),
}

impl Job {
    fn path(&self) -> &Path {
        match self {
            Job::Event(p) | Job::Click(p) => p,
        }
    }
}

struct Queue {
    jobs: Vec<Job>,
    next: AtomicUsize,
}

fn main() {
    if let Err(err) = dispatch() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn dispatch() -> io::Result<()> {
    self_check();
    let mut args = env::args().skip(1);
    let first = args.next().expect("data dir");
    if first == "--suite" {
        let fixtures = PathBuf::from(args.next().expect("fixtures dir"));
        let scratch = PathBuf::from(args.next().expect("scratch dir"));
        return run_suite(&fixtures, &scratch);
    }
    let data = PathBuf::from(first);
    let scratch = PathBuf::from(args.next().expect("scratch dir"));
    fs::create_dir_all(&scratch)?;
    let output = args
        .next()
        .map(PathBuf::from)
        .or_else(|| env::var("OUTPUT_DIR").ok().map(PathBuf::from));
    if let Some(out) = output {
        execute(&data, &out, true, true)
    } else {
        execute(&data, &scratch, false, true)
    }
}

fn execute(data: &Path, dest: &Path, parquet: bool, verbose: bool) -> io::Result<()> {
    let started = Instant::now();
    let mut mark = started;
    let nthreads = if verbose { thread_count().max(1) } else { 1 };
    let avx = avx2_on();

    let manifest = fs::read_to_string(data.join("manifest.json"))?;
    let win_start = parse_ts(json_field(&manifest, "window_start").unwrap().as_bytes())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "window_start"))?;
    let win_end = parse_ts(json_field(&manifest, "window_end").unwrap().as_bytes())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "window_end"))?;
    let click_lo = win_start - WEEK_MS;
    let (min_day, ndays) = day_span(win_start, win_end);

    let (products, mut categories) = load_products(&data.join("products.csv"))?;
    let unknown = if let Some(i) = categories.iter().position(|c| c == b"unknown") {
        i as u32
    } else {
        let id = categories.len() as u32;
        categories.push(b"unknown".to_vec());
        id
    };
    let (campaigns, camp_names, channels) = load_campaigns(&data.join("campaigns.csv"))?;
    let n_camp = camp_names.len() as u32;
    mark = log(verbose, "read", mark);
    let _ = avx;

    let (events, clicks, text, links) = fast::ingest(
        data,
        &products,
        unknown,
        &campaigns,
        win_start,
        win_end,
        click_lo,
        nthreads,
        verbose,
    )?;
    mark = Instant::now();

    let (names, slim_parts, click_parts) = intern_all(events, clicks, &text, &links, nthreads);
    drop(links);
    mark = log(verbose, "intern", mark);

    let mut ev_buckets = vec![Vec::new(); nthreads];
    for part in slim_parts {
        for row in part {
            ev_buckets[row.person as usize % nthreads].push(row);
        }
    }
    let mut cl_buckets = vec![Vec::new(); nthreads];
    for part in click_parts {
        for row in part {
            cl_buckets[row.person as usize % nthreads].push(row);
        }
    }

    let n_cats = categories.len();
    let finished = std::thread::scope(|scope| {
        let mut guards = Vec::with_capacity(nthreads);
        let categories = &categories;
        let text = &text;
        for _ in 0..nthreads {
            let ev = ev_buckets.pop().unwrap();
            let cl = cl_buckets.pop().unwrap();
            guards.push(scope.spawn(move || {
                finish_bucket(ev, cl, text, categories, min_day, ndays, n_camp, n_cats)
            }));
        }
        let mut out = Vec::with_capacity(guards.len());
        for guard in guards {
            out.push(guard.join().expect("aggregate thread"));
        }
        out
    });
    drop(text);
    drop(ev_buckets);
    drop(cl_buckets);

    let mut users = Vec::new();
    let mut stores = Vec::with_capacity(finished.len());
    let mut sessions_n = 0usize;
    for (rows, store, sessions) in finished {
        sessions_n += sessions;
        users.extend(rows);
        stores.push(store);
    }
    let store = merge_stores(stores);
    let camps = camp_rows(&store);
    if verbose {
        eprintln!(
            "sessions {sessions_n} user_rows {} camp_rows {}",
            users.len(),
            camps.len()
        );
    }
    mark = log(verbose, "sessions", mark);

    if parquet {
        write_parquet(dest, &users, &camps, &names, &categories, &camp_names, &channels, n_camp)?;
    } else {
        write_user_bin(&dest.join("user.bin"), &users, &names, &categories)?;
        write_camp_bin(
            &dest.join("campaign.bin"),
            &camps,
            &camp_names,
            &channels,
            n_camp,
        )?;
    }
    log(verbose, "write", mark);
    if verbose {
        eprintln!("total: {:.3}s", started.elapsed().as_secs_f64());
    }
    Ok(())
}

fn day_span(win_start: i64, win_end: i64) -> (i32, usize) {
    if win_end <= win_start {
        return (win_start.div_euclid(DAY_MS) as i32, 1);
    }
    let min_day = win_start.div_euclid(DAY_MS) as i32;
    let max_day = (win_end - 1).div_euclid(DAY_MS) as i32;
    let n = (max_day as i64 - min_day as i64 + 1).max(1) as usize;
    (min_day, n)
}

fn scan_worker(
    queue: &Queue,
    products: &FxMap<Vec<u8>, u32>,
    unknown: u32,
    campaigns: &FxMap<Vec<u8>, u32>,
    win_start: i64,
    win_end: i64,
    click_lo: i64,
    avx: bool,
) -> io::Result<Shard> {
    let mut shard = Shard {
        bufs: Vec::new(),
        events: Vec::new(),
        clicks: Vec::new(),
    };
    loop {
        let i = queue.next.fetch_add(1, Ordering::Relaxed);
        if i >= queue.jobs.len() {
            break;
        }
        match &queue.jobs[i] {
            Job::Event(path) => {
                ingest_events(path, &mut shard, products, unknown, win_start, win_end, avx)?
            }
            Job::Click(path) => {
                ingest_clicks(path, &mut shard, campaigns, click_lo, win_end, avx)?
            }
        }
    }
    Ok(shard)
}

fn ingest_events(
    path: &Path,
    shard: &mut Shard,
    products: &FxMap<Vec<u8>, u32>,
    unknown: u32,
    win_start: i64,
    win_end: i64,
    avx: bool,
) -> io::Result<()> {
    let bytes = fs::read(path)?;
    if bytes.len() > u32::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file exceeds 4 GiB"));
    }
    shard.events.reserve(count_nl(&bytes, avx) + 1);
    let buf = shard.bufs.len() as u16;
    scan_events(&bytes, buf, &mut shard.events, products, unknown, win_start, win_end, avx);
    shard.bufs.push(bytes);
    Ok(())
}

fn ingest_clicks(
    path: &Path,
    shard: &mut Shard,
    campaigns: &FxMap<Vec<u8>, u32>,
    click_lo: i64,
    win_end: i64,
    avx: bool,
) -> io::Result<()> {
    let bytes = fs::read(path)?;
    if bytes.len() > u32::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file exceeds 4 GiB"));
    }
    shard.clicks.reserve(count_nl(&bytes, avx) + 1);
    let buf = shard.bufs.len() as u16;
    scan_clicks(&bytes, buf, &mut shard.clicks, campaigns, click_lo, win_end, avx);
    shard.bufs.push(bytes);
    Ok(())
}

fn scan_events(
    bytes: &[u8],
    buf: u16,
    out: &mut Vec<RawEv>,
    products: &FxMap<Vec<u8>, u32>,
    unknown: u32,
    win_start: i64,
    win_end: i64,
    avx: bool,
) {
    if bytes.is_empty() {
        return;
    }
    let header = find_nl(bytes, 0, avx);
    if header >= bytes.len() {
        return;
    }
    let mut i = header + 1;
    while i < bytes.len() {
        let j = find_nl(bytes, i, avx);
        if j > i {
            push_event(
                &bytes[i..j],
                i as u32,
                buf,
                out,
                products,
                unknown,
                win_start,
                win_end,
            );
        }
        if j >= bytes.len() {
            break;
        }
        i = j + 1;
    }
}

fn scan_clicks(
    bytes: &[u8],
    buf: u16,
    out: &mut Vec<RawClick>,
    campaigns: &FxMap<Vec<u8>, u32>,
    click_lo: i64,
    win_end: i64,
    avx: bool,
) {
    if bytes.is_empty() {
        return;
    }
    let header = find_nl(bytes, 0, avx);
    if header >= bytes.len() {
        return;
    }
    let mut i = header + 1;
    while i < bytes.len() {
        let j = find_nl(bytes, i, avx);
        if j > i {
            push_click(&bytes[i..j], i as u32, buf, out, campaigns, click_lo, win_end);
        }
        if j >= bytes.len() {
            break;
        }
        i = j + 1;
    }
}

fn push_event(
    line: &[u8],
    line_off: u32,
    buf: u16,
    out: &mut Vec<RawEv>,
    products: &FxMap<Vec<u8>, u32>,
    unknown: u32,
    win_start: i64,
    win_end: i64,
) {
    let mut rel = [0u32; 7];
    let mut len = [0u32; 7];
    if !split_unquoted(line, 7, &mut rel, &mut len) || len[0] == 0 {
        return;
    }
    let id = field(line, rel[0], len[0]);
    let user = field(line, rel[1], len[1]);
    let ts_b = field(line, rel[2], len[2]);
    let ing_b = field(line, rel[3], len[3]);
    let et = field(line, rel[4], len[4]);
    let product = field(line, rel[5], len[5]);
    let price_b = field(line, rel[6], len[6]);
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
        products.get(product).copied().unwrap_or(unknown)
    };
    out.push(RawEv {
        id_hash: hash_bytes(id),
        user_hash: hash_bytes(user),
        ts: ts.unwrap_or(0),
        line_off,
        id_rel: rel[0],
        id_len: len[0],
        ing_rel: rel[3],
        ing_len: len[3],
        user_rel: rel[1],
        user_len: len[1],
        cat,
        price,
        buf,
        flags,
    });
}

fn push_click(
    line: &[u8],
    line_off: u32,
    buf: u16,
    out: &mut Vec<RawClick>,
    campaigns: &FxMap<Vec<u8>, u32>,
    click_lo: i64,
    win_end: i64,
) {
    let mut rel = [0u32; 4];
    let mut len = [0u32; 4];
    if !split_unquoted(line, 4, &mut rel, &mut len) || len[0] == 0 {
        return;
    }
    let id = field(line, rel[0], len[0]);
    let user = field(line, rel[1], len[1]);
    let camp_b = field(line, rel[2], len[2]);
    let ts_b = field(line, rel[3], len[3]);
    let ts = parse_ts(ts_b);
    let camp = campaigns.get(camp_b).copied();
    let mut flags = 0u8;
    if !user.is_empty()
        && camp.is_some()
        && matches!(ts, Some(t) if t >= click_lo && t < win_end)
    {
        flags |= OK;
    }
    out.push(RawClick {
        id_hash: hash_bytes(id),
        user_hash: hash_bytes(user),
        ts: ts.unwrap_or(0),
        line_off,
        id_rel: rel[0],
        id_len: len[0],
        user_rel: rel[1],
        user_len: len[1],
        camp: camp.unwrap_or(0),
        buf,
        flags,
    });
}

#[inline(always)]
fn field(line: &[u8], rel: u32, len: u32) -> &[u8] {
    let s = rel as usize;
    &line[s..s + len as usize]
}

fn dedup_events(shards: &[Shard], text: &mut Vec<u8>) -> Vec<Compact> {
    let mut parts = Vec::with_capacity(shards.len());
    for (si, shard) in shards.iter().enumerate() {
        let mut keys = Vec::with_capacity(shard.events.len());
        for (ri, row) in shard.events.iter().enumerate() {
            keys.push(DKey {
                hash: row.id_hash,
                shard: si as u32,
                row: ri as u32,
            });
        }
        parts.push(keys);
    }
    std::thread::scope(|scope| {
        for part in &mut parts {
            scope.spawn(|| radix_sort(part));
        }
    });
    let mut out = Vec::new();
    let mut last = Vec::new();
    for_each_group(&parts, |group| {
        if group.len() > 1 {
            group.sort_by(|a, b| {
                ev_id(shards, a)
                    .cmp(ev_id(shards, b))
                    .then_with(|| ev_ing(shards, a).cmp(ev_ing(shards, b)))
            });
        }
        last.clear();
        let mut have = false;
        for key in group.iter() {
            if group.len() > 1 {
                let id = ev_id(shards, key);
                if have && id == last.as_slice() {
                    continue;
                }
                last.clear();
                last.extend_from_slice(id);
                have = true;
            }
            emit_event(shards, key, text, &mut out);
        }
    });
    out
}

fn dedup_clicks(shards: &[Shard], text: &mut Vec<u8>) -> Vec<Clk> {
    let mut parts = Vec::with_capacity(shards.len());
    for (si, shard) in shards.iter().enumerate() {
        let mut keys = Vec::with_capacity(shard.clicks.len());
        for (ri, row) in shard.clicks.iter().enumerate() {
            keys.push(DKey {
                hash: row.id_hash,
                shard: si as u32,
                row: ri as u32,
            });
        }
        parts.push(keys);
    }
    std::thread::scope(|scope| {
        for part in &mut parts {
            scope.spawn(|| radix_sort(part));
        }
    });
    let mut out = Vec::new();
    let mut last = Vec::new();
    for_each_group(&parts, |group| {
        if group.len() > 1 {
            group.sort_by(|a, b| cl_id(shards, a).cmp(cl_id(shards, b)));
        }
        last.clear();
        let mut have = false;
        for key in group.iter() {
            if group.len() > 1 {
                let id = cl_id(shards, key);
                if have && id == last.as_slice() {
                    continue;
                }
                last.clear();
                last.extend_from_slice(id);
                have = true;
            }
            emit_click(shards, key, text, &mut out);
        }
    });
    out
}

fn for_each_group(parts: &[Vec<DKey>], mut on_group: impl FnMut(&mut Vec<DKey>)) {
    let mut idx = vec![0usize; parts.len()];
    let mut group: Vec<DKey> = Vec::new();
    loop {
        let mut best: Option<usize> = None;
        for s in 0..parts.len() {
            if idx[s] < parts[s].len() {
                let h = parts[s][idx[s]].hash;
                let take = match best {
                    None => true,
                    Some(b) => h < parts[b][idx[b]].hash,
                };
                if take {
                    best = Some(s);
                }
            }
        }
        let Some(s) = best else {
            break;
        };
        let key = parts[s][idx[s]];
        idx[s] += 1;
        if !group.is_empty() && group[0].hash != key.hash {
            on_group(&mut group);
            group.clear();
        }
        group.push(key);
    }
    if !group.is_empty() {
        on_group(&mut group);
    }
}

fn radix_sort(keys: &mut Vec<DKey>) {
    let n = keys.len();
    if n < 2 {
        return;
    }
    let mut tmp = vec![DKey::default(); n];
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

fn ev_id<'a>(shards: &'a [Shard], key: &DKey) -> &'a [u8] {
    let shard = &shards[key.shard as usize];
    let ev = &shard.events[key.row as usize];
    slice(&shard.bufs[ev.buf as usize], ev.line_off + ev.id_rel, ev.id_len)
}

fn ev_ing<'a>(shards: &'a [Shard], key: &DKey) -> &'a [u8] {
    let shard = &shards[key.shard as usize];
    let ev = &shard.events[key.row as usize];
    slice(&shard.bufs[ev.buf as usize], ev.line_off + ev.ing_rel, ev.ing_len)
}

fn ev_user<'a>(shards: &'a [Shard], key: &DKey) -> &'a [u8] {
    let shard = &shards[key.shard as usize];
    let ev = &shard.events[key.row as usize];
    slice(&shard.bufs[ev.buf as usize], ev.line_off + ev.user_rel, ev.user_len)
}

fn cl_id<'a>(shards: &'a [Shard], key: &DKey) -> &'a [u8] {
    let shard = &shards[key.shard as usize];
    let row = &shard.clicks[key.row as usize];
    slice(&shard.bufs[row.buf as usize], row.line_off + row.id_rel, row.id_len)
}

fn cl_user<'a>(shards: &'a [Shard], key: &DKey) -> &'a [u8] {
    let shard = &shards[key.shard as usize];
    let row = &shard.clicks[key.row as usize];
    slice(
        &shard.bufs[row.buf as usize],
        row.line_off + row.user_rel,
        row.user_len,
    )
}

fn slice(buf: &[u8], off: u32, len: u32) -> &[u8] {
    let s = off as usize;
    &buf[s..s + len as usize]
}

fn emit_event(shards: &[Shard], key: &DKey, text: &mut Vec<u8>, out: &mut Vec<Compact>) {
    let ev = shards[key.shard as usize].events[key.row as usize];
    if ev.flags & OK == 0 {
        return;
    }
    let user = ev_user(shards, key);
    let (user_off, user_len) = push_text(text, user);
    out.push(Compact {
        user_hash: ev.user_hash,
        ts: ev.ts,
        user_off,
        user_len,
        cat: ev.cat,
        price: ev.price,
        purchase: ev.flags & BUY != 0,
    });
}

fn emit_click(shards: &[Shard], key: &DKey, text: &mut Vec<u8>, out: &mut Vec<Clk>) {
    let row = shards[key.shard as usize].clicks[key.row as usize];
    if row.flags & OK == 0 {
        return;
    }
    let user = cl_user(shards, key);
    let id = cl_id(shards, key);
    let (user_off, user_len) = push_text(text, user);
    let (id_off, id_len) = push_text(text, id);
    out.push(Clk {
        user_hash: row.user_hash,
        ts: row.ts,
        user_off,
        user_len,
        camp: row.camp,
        id_off,
        id_len,
    });
}

fn push_text(text: &mut Vec<u8>, bytes: &[u8]) -> (u32, u32) {
    let off = text.len();
    assert!(off <= u32::MAX as usize, "text arena exceeded 4 GiB");
    text.extend_from_slice(bytes);
    (off as u32, bytes.len() as u32)
}

fn intern_all(
    events: Vec<Compact>,
    clicks: Vec<Clk>,
    text: &[u8],
    links: &[(Vec<u8>, Vec<u8>)],
    nthreads: usize,
) -> (Names, Vec<Vec<Slim>>, Vec<Vec<Click>>) {
    let n = nthreads.max(1);
    let bins = n * USER_SUBS;
    let mut ev_counts = vec![0usize; bins];
    for row in &events {
        let p = user_probe(row.user_hash, row.user_len);
        ev_counts[bucket(p, n) * USER_SUBS + ((p >> 56) as usize)] += 1;
    }
    let mut cl_counts = vec![0usize; bins];
    for row in &clicks {
        let p = user_probe(row.user_hash, row.user_len);
        cl_counts[bucket(p, n) * USER_SUBS + ((p >> 56) as usize)] += 1;
    }
    let mut ev_flat = Vec::with_capacity(bins);
    let mut cl_flat = Vec::with_capacity(bins);
    for i in 0..bins {
        ev_flat.push(Vec::with_capacity(ev_counts[i]));
        cl_flat.push(Vec::with_capacity(cl_counts[i]));
    }
    for row in events {
        let p = user_probe(row.user_hash, row.user_len);
        ev_flat[bucket(p, n) * USER_SUBS + ((p >> 56) as usize)].push(row);
    }
    for row in clicks {
        let p = user_probe(row.user_hash, row.user_len);
        cl_flat[bucket(p, n) * USER_SUBS + ((p >> 56) as usize)].push(row);
    }
    let mut ev_by_shard = Vec::with_capacity(n);
    let mut cl_by_shard = Vec::with_capacity(n);
    for shard in 0..n {
        let mut ev_parts = Vec::with_capacity(USER_SUBS);
        let mut cl_parts = Vec::with_capacity(USER_SUBS);
        for sub in 0..USER_SUBS {
            ev_parts.push(std::mem::take(&mut ev_flat[shard * USER_SUBS + sub]));
            cl_parts.push(std::mem::take(&mut cl_flat[shard * USER_SUBS + sub]));
        }
        ev_by_shard.push(ev_parts);
        cl_by_shard.push(cl_parts);
    }
    drop(ev_flat);
    drop(cl_flat);

    let built = std::thread::scope(|scope| {
        let mut guards = Vec::with_capacity(n);
        for i in 0..n {
            let ev = ev_by_shard.pop().unwrap();
            let cl = cl_by_shard.pop().unwrap();
            let idx = n - 1 - i;
            guards.push(scope.spawn(move || intern_bucket(idx, ev, cl, text, links, n)));
        }
        let mut out = Vec::with_capacity(guards.len());
        for guard in guards {
            out.push(guard.join().expect("intern thread"));
        }
        out.reverse();
        out
    });

    let mut shards = Vec::with_capacity(n);
    let mut slims = Vec::with_capacity(n);
    let mut click_out = Vec::with_capacity(n);
    for (shard, slim, click) in built {
        shards.push(shard);
        slims.push(slim);
        click_out.push(click);
    }
    let mut prefix = vec![0u32; n + 1];
    for i in 0..n {
        prefix[i + 1] = prefix[i] + shards[i].spans.len() as u32;
    }
    let total = prefix[n] as usize;
    let mut parent: Vec<u32> = (0..total as u32).collect();
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (a, b) in links {
        let ha = id_probe(a);
        let hb = id_probe(b);
        let ga = global_id(&shards, &prefix, ha, a);
        let gb = global_id(&shards, &prefix, hb, b);
        union_ids(&mut parent, &shards, &prefix, ga, gb, &mut left, &mut right);
    }
    for i in 0..total {
        find(&mut parent, i as u32);
    }
    for (s, slim) in slims.iter_mut().enumerate() {
        for row in slim.iter_mut() {
            let g = prefix[s] + row.person;
            row.person = parent[g as usize];
        }
    }
    for (s, rows) in click_out.iter_mut().enumerate() {
        for row in rows.iter_mut() {
            let g = prefix[s] + row.person;
            row.person = parent[g as usize];
        }
    }
    (Names { shards, prefix }, slims, click_out)
}

const USER_SUBS: usize = 256;

/// Open-address map that lives in L2. One of these is reused per user sub-bucket,
/// so a whale is a hot slot instead of a random probe into a 32 MiB table.
struct GenMap {
    slots: Vec<Slot>,
    gen: Vec<u32>,
    stamp: u32,
    used: usize,
}

impl GenMap {
    fn new(cap: usize) -> Self {
        let n = cap.next_power_of_two().max(16);
        Self {
            slots: vec![Slot { key: 0, id: EMPTY }; n],
            gen: vec![0; n],
            stamp: 1,
            used: 0,
        }
    }

    fn clear(&mut self) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.gen.fill(0);
            self.stamp = 1;
        }
        self.used = 0;
    }

    fn ensure(&mut self, map: &UserShard) {
        if self.slots.is_empty() || (self.used + 1) * 10 >= self.slots.len() * 7 {
            let n = (self.slots.len().max(1) * 2).next_power_of_two().max(1024);
            self.rehash(n, map);
        }
    }

    fn rehash(&mut self, n: usize, map: &UserShard) {
        let old_slots = std::mem::replace(&mut self.slots, vec![Slot { key: 0, id: EMPTY }; n]);
        let old_gen = std::mem::replace(&mut self.gen, vec![0; n]);
        let stamp = self.stamp;
        self.used = 0;
        for i in 0..old_slots.len() {
            if old_gen[i] == stamp {
                let key = old_slots[i].key;
                let id = old_slots[i].id;
                let hash = if key != 0 {
                    mix_key(key)
                } else {
                    hash_bytes(map.name(id))
                };
                self.insert_fresh(key, id, hash);
            }
        }
    }

    fn insert_fresh(&mut self, key: u64, id: u32, hash: u64) {
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        loop {
            if self.gen[i] != self.stamp {
                self.gen[i] = self.stamp;
                self.slots[i] = Slot { key, id };
                self.used += 1;
                return;
            }
            i = (i + 1) & mask;
        }
    }

    fn find_key(&self, hash: u64, key: u64) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        for _ in 0..self.slots.len() {
            if self.gen[i] != self.stamp {
                return None;
            }
            if self.slots[i].key == key {
                return Some(self.slots[i].id);
            }
            i = (i + 1) & mask;
        }
        None
    }

    fn find_bytes(&self, map: &UserShard, hash: u64, bytes: &[u8]) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        for _ in 0..self.slots.len() {
            if self.gen[i] != self.stamp {
                return None;
            }
            let slot = self.slots[i];
            if slot.key == 0 && map.name(slot.id) == bytes {
                return Some(slot.id);
            }
            i = (i + 1) & mask;
        }
        None
    }

    fn intern_packed(&mut self, map: &mut UserShard, key: u64) -> u32 {
        let hash = mix_key(key);
        if let Some(id) = self.find_key(hash, key) {
            return id;
        }
        self.ensure(map);
        let mut buf = [0u8; 9];
        let n = unpack_id(key, &mut buf);
        let id = map.push_user(key, &buf[..n]);
        self.insert_fresh(key, id, hash);
        id
    }

    fn intern_bytes(&mut self, map: &mut UserShard, hash: u64, bytes: &[u8]) -> u32 {
        let key = pack_id(bytes);
        if key != 0 {
            return self.intern_packed(map, key);
        }
        if let Some(id) = self.find_bytes(map, hash, bytes) {
            return id;
        }
        self.ensure(map);
        let id = map.push_user(0, bytes);
        self.insert_fresh(0, id, hash);
        id
    }
}

fn intern_bucket(
    idx: usize,
    events: Vec<Vec<Compact>>,
    clicks: Vec<Vec<Clk>>,
    text: &[u8],
    links: &[(Vec<u8>, Vec<u8>)],
    nthreads: usize,
) -> (UserShard, Vec<Slim>, Vec<Click>) {
    let mut n_ev = 0usize;
    let mut n_cl = 0usize;
    for part in &events {
        n_ev += part.len();
    }
    for part in &clicks {
        n_cl += part.len();
    }
    let mut map = UserShard {
        slots: Vec::new(),
        arena: Vec::new(),
        spans: Vec::new(),
        keys: Vec::new(),
        used: 0,
    };
    let mut small = GenMap::new(4096);
    let mut slims = Vec::with_capacity(n_ev);
    let mut out_clicks = Vec::with_capacity(n_cl);
    for s in 0..USER_SUBS {
        small.clear();
        for row in &events[s] {
            let local = if row.user_len == 0 {
                small.intern_packed(&mut map, row.user_hash)
            } else {
                let bytes = &text[row.user_off as usize..row.user_off as usize + row.user_len as usize];
                small.intern_bytes(&mut map, row.user_hash, bytes)
            };
            slims.push(Slim {
                ts: row.ts,
                person: local,
                cat: row.cat,
                price: row.price,
                purchase: row.purchase,
            });
        }
        for row in &clicks[s] {
            let local = if row.user_len == 0 {
                small.intern_packed(&mut map, row.user_hash)
            } else {
                let bytes = &text[row.user_off as usize..row.user_off as usize + row.user_len as usize];
                small.intern_bytes(&mut map, row.user_hash, bytes)
            };
            out_clicks.push(Click {
                ts: row.ts,
                person: local,
                camp: row.camp,
                id_off: row.id_off,
                id_len: row.id_len,
            });
        }
    }
    map.rebuild_slots();
    for (a, b) in links {
        let ka = pack_id(a);
        if ka != 0 {
            if bucket(mix_key(ka), nthreads) == idx {
                map.intern_packed(ka);
            }
        } else {
            let ha = hash_bytes(a);
            if bucket(ha, nthreads) == idx {
                map.intern(ha, a);
            }
        }
        let kb = pack_id(b);
        if kb != 0 {
            if bucket(mix_key(kb), nthreads) == idx {
                map.intern_packed(kb);
            }
        } else {
            let hb = hash_bytes(b);
            if bucket(hb, nthreads) == idx {
                map.intern(hb, b);
            }
        }
    }
    (map, slims, out_clicks)
}

fn global_id(shards: &[UserShard], prefix: &[u32], hash: u64, bytes: &[u8]) -> u32 {
    let s = bucket(hash, shards.len());
    let local = shards[s].find(hash, bytes).expect("linked id missing from intern");
    prefix[s] + local
}

fn union_ids(
    parent: &mut [u32],
    shards: &[UserShard],
    prefix: &[u32],
    a: u32,
    b: u32,
    left: &mut Vec<u8>,
    right: &mut Vec<u8>,
) {
    let ra = find(parent, a);
    let rb = find(parent, b);
    if ra == rb {
        return;
    }
    left.clear();
    left.extend_from_slice(person_name(shards, prefix, ra));
    right.clear();
    right.extend_from_slice(person_name(shards, prefix, rb));
    if right.as_slice() < left.as_slice() {
        parent[ra as usize] = rb;
    } else {
        parent[rb as usize] = ra;
    }
}

fn person_name<'a>(shards: &'a [UserShard], prefix: &[u32], id: u32) -> &'a [u8] {
    let s = shard_of(prefix, id);
    shards[s].name(id - prefix[s])
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut root = x;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while x != root {
        let next = parent[x as usize];
        parent[x as usize] = root;
        x = next;
    }
    root
}

fn sort_events(events: &mut Vec<Slim>) {
    if events.len() < 2048 {
        events.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
        return;
    }
    let max_p = events.iter().map(|e| e.person).max().unwrap() as usize;
    if max_p > events.len().saturating_mul(8).saturating_add(1024) {
        events.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
        return;
    }
    let mut start = vec![0u32; max_p + 1];
    for event in events.iter() {
        start[event.person as usize] += 1;
    }
    let mut sum = 0u32;
    for count in start.iter_mut() {
        let n = *count;
        *count = sum;
        sum += n;
    }
    let mut cursor = start.clone();
    let mut out = vec![
        Slim {
            ts: 0,
            person: 0,
            cat: 0,
            price: 0,
            purchase: false,
        };
        events.len()
    ];
    for event in events.iter() {
        let i = cursor[event.person as usize] as usize;
        cursor[event.person as usize] += 1;
        out[i] = *event;
    }
    *events = out;
    for person in 0..=max_p {
        let a = start[person] as usize;
        let b = cursor[person] as usize;
        if b > a + 1 {
            events[a..b].sort_unstable_by_key(|event| event.ts);
        }
    }
}

fn sort_clicks(clicks: &mut Vec<Click>, text: &[u8]) {
    let cmp = |a: &Click, b: &Click| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| {
            let ia = &text[a.id_off as usize..a.id_off as usize + a.id_len as usize];
            let ib = &text[b.id_off as usize..b.id_off as usize + b.id_len as usize];
            ia.cmp(ib)
        })
    };
    if clicks.len() < 2048 {
        clicks.sort_unstable_by(cmp);
        return;
    }
    let max_p = clicks.iter().map(|c| c.person).max().unwrap() as usize;
    if max_p > clicks.len().saturating_mul(8).saturating_add(1024) {
        clicks.sort_unstable_by(cmp);
        return;
    }
    let mut start = vec![0u32; max_p + 1];
    for click in clicks.iter() {
        start[click.person as usize] += 1;
    }
    let mut sum = 0u32;
    for count in start.iter_mut() {
        let n = *count;
        *count = sum;
        sum += n;
    }
    let mut cursor = start.clone();
    let mut out = vec![
        Click {
            ts: 0,
            person: 0,
            camp: 0,
            id_off: 0,
            id_len: 0,
        };
        clicks.len()
    ];
    for click in clicks.iter() {
        let i = cursor[click.person as usize] as usize;
        cursor[click.person as usize] += 1;
        out[i] = *click;
    }
    *clicks = out;
    for person in 0..=max_p {
        let a = start[person] as usize;
        let b = cursor[person] as usize;
        if b > a + 1 {
            clicks[a..b].sort_unstable_by(|a, b| {
                a.ts.cmp(&b.ts).then_with(|| {
                    let ia = &text[a.id_off as usize..a.id_off as usize + a.id_len as usize];
                    let ib = &text[b.id_off as usize..b.id_off as usize + b.id_len as usize];
                    ia.cmp(ib)
                })
            });
        }
    }
}

fn finish_bucket(
    mut events: Vec<Slim>,
    mut clicks: Vec<Click>,
    text: &[u8],
    categories: &[Vec<u8>],
    min_day: i32,
    ndays: usize,
    n_camp: u32,
    n_cats: usize,
) -> (Vec<UserRow>, Store, usize) {
    sort_events(&mut events);
    sort_clicks(&mut clicks, text);
    let mut sessions = Vec::new();
    let mut users = Vec::new();
    let mut cat_rev = vec![0u64; n_cats];
    let mut touched = Vec::new();
    let mut i = 0;
    while i < events.len() {
        let person = events[i].person;
        let mut j = i + 1;
        while j < events.len() && events[j].person == person {
            j += 1;
        }
        consume_person(
            &events[i..j],
            &mut sessions,
            &mut users,
            &mut cat_rev,
            &mut touched,
            categories,
        );
        i = j;
    }
    let session_n = sessions.len();
    let mut store = Store::new(min_day, ndays, n_camp);
    attribute(&sessions, &clicks, &mut store);
    (users, store, session_n)
}

fn consume_person(
    events: &[Slim],
    sessions: &mut Vec<Sess>,
    users: &mut Vec<UserRow>,
    cat_rev: &mut [u64],
    touched: &mut Vec<u32>,
    categories: &[Vec<u8>],
) {
    if events.is_empty() {
        return;
    }
    let person = events[0].person;
    let mut prev: Option<i64> = None;
    let mut open = false;
    let mut start = 0i64;
    let mut day = 0i32;
    let mut revenue = 0u64;
    let mut has_purchase = false;
    let mut day_open = false;
    let mut day_id = 0i32;
    let mut day_sessions = 0u64;
    let mut day_events = 0u64;
    let mut day_revenue = 0u64;

    for event in events {
        let is_new = match prev {
            None => true,
            Some(ts) => event.ts - ts > GAP_MS,
        };
        if is_new {
            if open {
                sessions.push(Sess {
                    person,
                    start,
                    day,
                    revenue,
                    has_purchase,
                });
            }
            let d = event.ts.div_euclid(DAY_MS) as i32;
            if day_open && d != day_id {
                let top = take_top(cat_rev, touched, categories);
                users.push(UserRow {
                    person,
                    day: day_id,
                    sessions: day_sessions,
                    events: day_events,
                    revenue: day_revenue,
                    top,
                });
                day_sessions = 0;
                day_events = 0;
                day_revenue = 0;
            }
            day_open = true;
            day_id = d;
            day_sessions += 1;
            start = event.ts;
            day = d;
            revenue = 0;
            has_purchase = false;
            open = true;
        }
        day_events += 1;
        if event.purchase {
            let add = event.price as u64;
            revenue += add;
            day_revenue += add;
            has_purchase = true;
            let c = event.cat as usize;
            if cat_rev[c] == 0 {
                touched.push(event.cat);
            }
            cat_rev[c] += add;
        }
        prev = Some(event.ts);
    }
    if open {
        sessions.push(Sess {
            person,
            start,
            day,
            revenue,
            has_purchase,
        });
    }
    if day_open {
        let top = take_top(cat_rev, touched, categories);
        users.push(UserRow {
            person,
            day: day_id,
            sessions: day_sessions,
            events: day_events,
            revenue: day_revenue,
            top,
        });
    }
}

fn take_top(cat_rev: &mut [u64], touched: &mut Vec<u32>, names: &[Vec<u8>]) -> u32 {
    let mut best_cat = EMPTY;
    let mut best_rev = 0u64;
    for &c in touched.iter() {
        let r = cat_rev[c as usize];
        cat_rev[c as usize] = 0;
        if best_cat == EMPTY
            || r > best_rev
            || (r == best_rev && names[c as usize] < names[best_cat as usize])
        {
            best_rev = r;
            best_cat = c;
        }
    }
    touched.clear();
    best_cat
}

fn attribute(sessions: &[Sess], clicks: &[Click], store: &mut Store) {
    let n_camp = store.n_camp as usize;
    let mut cnt = vec![0u32; n_camp];
    let mut pos = vec![usize::MAX; n_camp];
    let mut active = Vec::new();
    let mut si = 0;
    let mut ci = 0;
    while si < sessions.len() {
        let person = sessions[si].person;
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1;
        }
        let c0 = ci;
        while ci < clicks.len() && clicks[ci].person == person {
            ci += 1;
        }
        let c1 = ci;
        let s0 = si;
        while si < sessions.len() && sessions[si].person == person {
            si += 1;
        }
        credit_person(&sessions[s0..si], &clicks[c0..c1], store);
        assist_person(
            &sessions[s0..si],
            &clicks[c0..c1],
            store,
            &mut cnt,
            &mut pos,
            &mut active,
        );
    }
}

fn credit_person(sessions: &[Sess], clicks: &[Click], store: &mut Store) {
    let organic = store.n_camp;
    let mut nxt = 0usize;
    for session in sessions {
        while nxt < clicks.len() && clicks[nxt].ts <= session.start {
            nxt += 1;
        }
        let campaign = if nxt == 0 {
            organic
        } else {
            let ts = clicks[nxt - 1].ts;
            let mut best = nxt - 1;
            while best > 0 && clicks[best - 1].ts == ts {
                best -= 1;
            }
            let within = (session.start as i128) - (clicks[best].ts as i128) <= WEEK_MS as i128;
            if within {
                clicks[best].camp
            } else {
                organic
            }
        };
        let cell = store.cell_mut(campaign, session.day);
        cell.sessions += 1;
        cell.revenue += session.revenue;
        if session.has_purchase {
            cell.purchasing += 1;
        }
    }
}

fn assist_person(
    sessions: &[Sess],
    clicks: &[Click],
    store: &mut Store,
    cnt: &mut [u32],
    pos: &mut [usize],
    active: &mut Vec<usize>,
) {
    let mut lo = 0usize;
    let mut hi = 0usize;
    for session in sessions {
        while hi < clicks.len() && clicks[hi].ts <= session.start {
            let c = clicks[hi].camp as usize;
            if cnt[c] == 0 {
                pos[c] = active.len();
                active.push(c);
            }
            cnt[c] += 1;
            hi += 1;
        }
        let cutoff = session.start as i128 - WEEK_MS as i128;
        while lo < hi && (clicks[lo].ts as i128) < cutoff {
            let c = clicks[lo].camp as usize;
            cnt[c] -= 1;
            if cnt[c] == 0 {
                let p = pos[c];
                let last = active.pop().unwrap();
                pos[c] = usize::MAX;
                if last != c {
                    active[p] = last;
                    pos[last] = p;
                }
            }
            lo += 1;
        }
        for &c in active.iter() {
            store.cell_mut(c as u32, session.day).assisted += 1;
        }
    }
    for &c in active.iter() {
        cnt[c] = 0;
        pos[c] = usize::MAX;
    }
    active.clear();
}

fn merge_stores(mut stores: Vec<Store>) -> Store {
    let Some(mut acc) = stores.pop() else {
        return Store::new(0, 1, 0);
    };
    for store in stores {
        if acc.dense {
            for (i, cell) in store.cells.into_iter().enumerate() {
                acc.cells[i].sessions += cell.sessions;
                acc.cells[i].purchasing += cell.purchasing;
                acc.cells[i].revenue += cell.revenue;
                acc.cells[i].assisted += cell.assisted;
            }
        } else {
            for (key, cell) in store.map {
                let slot = acc.map.entry(key).or_default();
                slot.sessions += cell.sessions;
                slot.purchasing += cell.purchasing;
                slot.revenue += cell.revenue;
                slot.assisted += cell.assisted;
            }
        }
    }
    acc
}

fn camp_rows(store: &Store) -> Vec<CampOut> {
    let mut rows = Vec::new();
    let organic = store.n_camp;
    let push = |rows: &mut Vec<CampOut>, camp: u32, day: i32, cell: &Cell| {
        let assisted = if camp == organic { 0 } else { cell.assisted };
        if cell.sessions == 0 && assisted == 0 {
            return;
        }
        rows.push(CampOut {
            camp,
            day,
            sessions: cell.sessions,
            purchasing: cell.purchasing,
            revenue: cell.revenue,
            assisted,
        });
    };
    if store.dense {
        for d in 0..store.ndays {
            for c in 0..store.stride {
                let cell = &store.cells[d * store.stride + c];
                push(&mut rows, c as u32, store.min_day + d as i32, cell);
            }
        }
    } else {
        for (&(camp, day), cell) in &store.map {
            push(&mut rows, camp, day, cell);
        }
    }
    rows
}

fn write_parquet(
    dest: &Path,
    users: &[UserRow],
    camps: &[CampOut],
    names: &Names,
    categories: &[Vec<u8>],
    camp_names: &[Vec<u8>],
    channels: &[Vec<u8>],
    n_camp: u32,
) -> io::Result<()> {
    let mut user_ids = Vec::new();
    let mut user_lens = Vec::with_capacity(users.len());
    let mut days = Vec::with_capacity(users.len());
    let mut sessions = Vec::with_capacity(users.len());
    let mut events = Vec::with_capacity(users.len());
    let mut revenue = Vec::with_capacity(users.len());
    let mut cats = Vec::new();
    let mut cat_lens = Vec::with_capacity(users.len());
    let mut present = Vec::with_capacity(users.len());
    for row in users {
        let name = names.name(row.person);
        user_ids.extend_from_slice(name);
        user_lens.push(name.len() as u32);
        days.push(row.day);
        sessions.push(row.sessions as i64);
        events.push(row.events as i64);
        revenue.push(row.revenue as i64);
        if row.top == EMPTY {
            present.push(0);
            cat_lens.push(0);
        } else {
            let text = &categories[row.top as usize];
            present.push(1);
            cats.extend_from_slice(text);
            cat_lens.push(text.len() as u32);
        }
    }
    let user_cols = vec![
        col_str("user_id", user_lens, user_ids),
        col_date("day", days),
        col_i64("sessions", sessions),
        col_i64("events", events),
        col_i64("revenue_cents", revenue),
        col_opt("top_category", cat_lens, cats, present),
    ];
    parquet::write_table(&dest.join("user_daily/part-0.parquet"), users.len(), &user_cols)?;

    let mut camp_ids = Vec::new();
    let mut camp_lens = Vec::with_capacity(camps.len());
    let mut chan = Vec::new();
    let mut chan_lens = Vec::with_capacity(camps.len());
    let mut days = Vec::with_capacity(camps.len());
    let mut sessions = Vec::with_capacity(camps.len());
    let mut purchasing = Vec::with_capacity(camps.len());
    let mut revenue = Vec::with_capacity(camps.len());
    let mut assisted = Vec::with_capacity(camps.len());
    for row in camps {
        let (name, channel): (&[u8], &[u8]) = if row.camp == n_camp {
            (b"organic", b"organic")
        } else {
            (&camp_names[row.camp as usize], &channels[row.camp as usize])
        };
        camp_ids.extend_from_slice(name);
        camp_lens.push(name.len() as u32);
        chan.extend_from_slice(channel);
        chan_lens.push(channel.len() as u32);
        days.push(row.day);
        sessions.push(row.sessions as i64);
        purchasing.push(row.purchasing as i64);
        revenue.push(row.revenue as i64);
        assisted.push(row.assisted as i64);
    }
    let camp_cols = vec![
        col_str("campaign_id", camp_lens, camp_ids),
        col_str("channel", chan_lens, chan),
        col_date("day", days),
        col_i64("sessions", sessions),
        col_i64("purchasing_sessions", purchasing),
        col_i64("revenue_cents", revenue),
        col_i64("assisted_sessions", assisted),
    ];
    parquet::write_table(
        &dest.join("campaign_daily/part-0.parquet"),
        camps.len(),
        &camp_cols,
    )
}

fn col_i64(name: &str, i64s: Vec<i64>) -> parquet::Col {
    parquet::Col {
        name: name.to_string(),
        kind: parquet::Kind::I64,
        i32s: Vec::new(),
        i64s,
        lens: Vec::new(),
        data: Vec::new(),
        present: Vec::new(),
    }
}

fn col_date(name: &str, i32s: Vec<i32>) -> parquet::Col {
    parquet::Col {
        name: name.to_string(),
        kind: parquet::Kind::Date,
        i32s,
        i64s: Vec::new(),
        lens: Vec::new(),
        data: Vec::new(),
        present: Vec::new(),
    }
}

fn col_str(name: &str, lens: Vec<u32>, data: Vec<u8>) -> parquet::Col {
    parquet::Col {
        name: name.to_string(),
        kind: parquet::Kind::Str,
        i32s: Vec::new(),
        i64s: Vec::new(),
        lens,
        data,
        present: Vec::new(),
    }
}

fn col_opt(name: &str, lens: Vec<u32>, data: Vec<u8>, present: Vec<u8>) -> parquet::Col {
    parquet::Col {
        name: name.to_string(),
        kind: parquet::Kind::StrOpt,
        i32s: Vec::new(),
        i64s: Vec::new(),
        lens,
        data,
        present,
    }
}

fn write_user_bin(path: &Path, rows: &[UserRow], names: &Names, categories: &[Vec<u8>]) -> io::Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    file.write_all(b"SSU1")?;
    file.write_all(&(rows.len() as u32).to_le_bytes())?;
    for row in rows {
        file.write_all(&row.day.to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.sessions as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.events as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.revenue as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&[if row.top == EMPTY { 0 } else { 1 }])?;
    }
    for row in rows {
        file.write_all(&(names.name(row.person).len() as u32).to_le_bytes())?;
    }
    for row in rows {
        let len = if row.top == EMPTY {
            0
        } else {
            categories[row.top as usize].len() as u32
        };
        file.write_all(&len.to_le_bytes())?;
    }
    for row in rows {
        file.write_all(names.name(row.person))?;
    }
    for row in rows {
        if row.top != EMPTY {
            file.write_all(&categories[row.top as usize])?;
        }
    }
    file.flush()
}

fn write_camp_bin(
    path: &Path,
    rows: &[CampOut],
    camp_names: &[Vec<u8>],
    channels: &[Vec<u8>],
    n_camp: u32,
) -> io::Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    file.write_all(b"SSC1")?;
    file.write_all(&(rows.len() as u32).to_le_bytes())?;
    for row in rows {
        file.write_all(&row.day.to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.sessions as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.purchasing as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.revenue as i64).to_le_bytes())?;
    }
    for row in rows {
        file.write_all(&(row.assisted as i64).to_le_bytes())?;
    }
    for row in rows {
        let name: &[u8] = if row.camp == n_camp {
            b"organic"
        } else {
            &camp_names[row.camp as usize]
        };
        file.write_all(&(name.len() as u32).to_le_bytes())?;
    }
    for row in rows {
        let channel: &[u8] = if row.camp == n_camp {
            b"organic"
        } else {
            &channels[row.camp as usize]
        };
        file.write_all(&(channel.len() as u32).to_le_bytes())?;
    }
    for row in rows {
        let name: &[u8] = if row.camp == n_camp {
            b"organic"
        } else {
            &camp_names[row.camp as usize]
        };
        file.write_all(name)?;
    }
    for row in rows {
        let channel: &[u8] = if row.camp == n_camp {
            b"organic"
        } else {
            &channels[row.camp as usize]
        };
        file.write_all(channel)?;
    }
    file.flush()
}

fn log(verbose: bool, name: &str, mark: Instant) -> Instant {
    if verbose {
        eprintln!("{name}: {:.3}s", mark.elapsed().as_secs_f64());
    }
    Instant::now()
}

fn thread_count() -> usize {
    env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return out;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("csv") && path.is_file() {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn load_products(path: &Path) -> io::Result<(FxMap<Vec<u8>, u32>, Vec<Vec<u8>>)> {
    let mut cats = Vec::new();
    let mut cat_of: FxMap<Vec<u8>, u32> = fxmap();
    let mut products = fxmap();
    let bytes = fs::read(path)?;
    for_lines(&bytes, false, |line| {
        let mut rel = [0u32; 2];
        let mut len = [0u32; 2];
        if !split_unquoted(line, 2, &mut rel, &mut len) {
            return;
        }
        let id = field(line, rel[0], len[0]).to_vec();
        let cat = field(line, rel[1], len[1]).to_vec();
        if id.is_empty() || products.contains_key(&id) {
            return;
        }
        let cid = if let Some(&id) = cat_of.get(&cat) {
            id
        } else {
            let id = cats.len() as u32;
            cat_of.insert(cat.clone(), id);
            cats.push(cat);
            id
        };
        products.insert(id, cid);
    });
    Ok((products, cats))
}

fn load_campaigns(path: &Path) -> io::Result<(FxMap<Vec<u8>, u32>, Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let mut names = Vec::new();
    let mut channels = Vec::new();
    let mut campaigns = fxmap();
    let bytes = fs::read(path)?;
    for_lines(&bytes, false, |line| {
        let mut rel = [0u32; 2];
        let mut len = [0u32; 2];
        if !split_unquoted(line, 2, &mut rel, &mut len) {
            return;
        }
        let id = field(line, rel[0], len[0]).to_vec();
        let channel = field(line, rel[1], len[1]).to_vec();
        if id.is_empty() || campaigns.contains_key(&id) {
            return;
        }
        let cid = names.len() as u32;
        names.push(id.clone());
        channels.push(channel);
        campaigns.insert(id, cid);
    });
    Ok((campaigns, names, channels))
}

fn parse_links(files: &[PathBuf], avx: bool) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for path in files {
        let bytes = fs::read(path)?;
        for_lines(&bytes, avx, |line| {
            let mut rel = [0u32; 2];
            let mut len = [0u32; 2];
            if !split_unquoted(line, 2, &mut rel, &mut len) {
                return;
            }
            let a = field(line, rel[0], len[0]);
            let b = field(line, rel[1], len[1]);
            if a.is_empty() || b.is_empty() {
                return;
            }
            out.push((a.to_vec(), b.to_vec()));
        });
    }
    Ok(out)
}

fn for_lines(bytes: &[u8], avx: bool, mut each: impl FnMut(&[u8])) {
    if bytes.is_empty() {
        return;
    }
    let header = find_nl(bytes, 0, avx);
    if header >= bytes.len() {
        return;
    }
    let mut i = header + 1;
    while i < bytes.len() {
        let j = find_nl(bytes, i, avx);
        if j > i {
            each(&bytes[i..j]);
        }
        if j >= bytes.len() {
            break;
        }
        i = j + 1;
    }
}

fn avx2_on() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[inline(always)]
fn find_nl(data: &[u8], start: usize, avx: bool) -> usize {
    #[cfg(target_arch = "x86_64")]
    if avx {
        return unsafe { find_nl_avx2(data, start) };
    }
    let _ = avx;
    find_nl_scalar(data, start)
}

fn find_nl_scalar(data: &[u8], start: usize) -> usize {
    data[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|p| start + p)
        .unwrap_or(data.len())
}

fn count_nl(data: &[u8], avx: bool) -> usize {
    #[cfg(target_arch = "x86_64")]
    if avx {
        return unsafe { count_nl_avx2(data) };
    }
    let _ = avx;
    data.iter().filter(|&&b| b == b'\n').count()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_nl_avx2(data: &[u8], start: usize) -> usize {
    use std::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    let len = data.len();
    let mut i = start;
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let ptr = data.as_ptr();
    while i + 32 <= len {
        let v = unsafe { _mm256_loadu_si256(ptr.add(i) as *const _) };
        let mask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) };
        if mask != 0 {
            return i + mask.trailing_zeros() as usize;
        }
        i += 32;
    }
    while i < len {
        if unsafe { *ptr.add(i) } == b'\n' {
            return i;
        }
        i += 1;
    }
    len
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_nl_avx2(data: &[u8]) -> usize {
    use std::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    let mut n = 0usize;
    let mut i = 0usize;
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let ptr = data.as_ptr();
    let len = data.len();
    while i + 32 <= len {
        let v = unsafe { _mm256_loadu_si256(ptr.add(i) as *const _) };
        let mask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) };
        n += mask.count_ones() as usize;
        i += 32;
    }
    while i < len {
        n += unsafe { (*ptr.add(i) == b'\n') as usize };
        i += 1;
    }
    n
}

#[inline(always)]
fn split_unquoted(line: &[u8], n: usize, rel: &mut [u32], len: &mut [u32]) -> bool {
    let mut i = 0usize;
    let blen = line.len();
    for f in 0..n {
        if i > blen {
            return false;
        }
        if i < blen && line[i] == b'"' {
            let start = i + 1;
            i = start;
            while i < blen && line[i] != b'"' {
                i += 1;
            }
            if i >= blen {
                return false;
            }
            let end = i;
            i += 1;
            if f + 1 < n {
                if i >= blen || line[i] != b',' {
                    return false;
                }
                i += 1;
            } else if i != blen {
                return false;
            }
            rel[f] = start as u32;
            len[f] = (end - start) as u32;
        } else {
            let start = i;
            while i < blen && line[i] != b',' {
                if line[i] == b'"' {
                    return false;
                }
                i += 1;
            }
            rel[f] = start as u32;
            len[f] = (i - start) as u32;
            if f + 1 < n {
                if i >= blen || line[i] != b',' {
                    return false;
                }
                i += 1;
            } else if i != blen {
                return false;
            }
        }
    }
    true
}

fn run_suite(fixtures: &Path, scratch: &Path) -> io::Result<()> {
    let started = Instant::now();
    let mut cases = Vec::new();
    for entry in fs::read_dir(fixtures)? {
        let path = entry?.path();
        if path.is_dir() && path.join("expected").is_dir() {
            cases.push(path);
        }
    }
    cases.sort();
    if cases.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "no fixtures"));
    }
    let mut failed = 0usize;
    for case in &cases {
        let name = case.file_name().and_then(|s| s.to_str()).unwrap_or("?");
        let mut data = case.join("data");
        if !data.is_dir() {
            data = case.join("input");
        }
        let out = scratch.join(name);
        fs::create_dir_all(&out)?;
        execute(&data, &out, false, false)?;
        let user_match = same_rows(
            &user_lines(&out.join("user.bin"))?,
            &csv_lines(&case.join("expected").join("user_daily.csv"))?,
        );
        let camp_match = same_rows(
            &campaign_lines(&out.join("campaign.bin"))?,
            &csv_lines(&case.join("expected").join("campaign_daily.csv"))?,
        );
        if user_match && camp_match {
            println!("ok   {name}");
        } else {
            failed += 1;
            if !user_match {
                show_diff(
                    name,
                    "user_daily",
                    &out.join("user.bin"),
                    &case.join("expected").join("user_daily.csv"),
                    true,
                )?;
            }
            if !camp_match {
                show_diff(
                    name,
                    "campaign_daily",
                    &out.join("campaign.bin"),
                    &case.join("expected").join("campaign_daily.csv"),
                    false,
                )?;
            }
        }
    }
    let passed = cases.len() - failed;
    println!("\n{passed}/{} fixtures passed", cases.len());
    eprintln!("suite: {:.3}s", started.elapsed().as_secs_f64());
    if failed > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{failed} fixtures failed"),
        ));
    }
    Ok(())
}

fn same_rows(got: &[Vec<u8>], expected: &[Vec<u8>]) -> bool {
    let mut got = got.to_vec();
    let mut expected = expected.to_vec();
    got.sort();
    expected.sort();
    got == expected
}

fn show_diff(case: &str, table: &str, bin: &Path, csv: &Path, user: bool) -> io::Result<()> {
    let got = if user { user_lines(bin)? } else { campaign_lines(bin)? };
    let expected = csv_lines(csv)?;
    eprintln!("FAIL {case} {table}");
    eprintln!("  expected {}", preview(&expected));
    eprintln!("  actual   {}", preview(&got));
    Ok(())
}

fn preview(rows: &[Vec<u8>]) -> String {
    let mut sorted = rows.to_vec();
    sorted.sort();
    let mut parts = Vec::new();
    for row in sorted.iter().take(6) {
        parts.push(String::from_utf8_lossy(row).into_owned());
    }
    if sorted.len() > 6 {
        parts.push(format!("... {} rows", sorted.len()));
    }
    format!("[{}]", parts.join(" | "))
}

fn csv_lines(path: &Path) -> io::Result<Vec<Vec<u8>>> {
    let data = fs::read(path)?;
    let mut rows = Vec::new();
    let mut header = true;
    for line in data.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if header {
            header = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        rows.push(line.to_vec());
    }
    Ok(rows)
}

fn user_lines(path: &Path) -> io::Result<Vec<Vec<u8>>> {
    let data = fs::read(path)?;
    if data.len() < 8 || &data[..4] != b"SSU1" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad user.bin"));
    }
    let n = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let mut off = 8usize;
    let days = take_bytes(&data, &mut off, 4 * n)?;
    let sessions = take_bytes(&data, &mut off, 8 * n)?;
    let events = take_bytes(&data, &mut off, 8 * n)?;
    let revenue = take_bytes(&data, &mut off, 8 * n)?;
    let present = take_bytes(&data, &mut off, n)?;
    let user_len = take_bytes(&data, &mut off, 4 * n)?;
    let cat_len = take_bytes(&data, &mut off, 4 * n)?;
    let mut rows = Vec::with_capacity(n);
    let mut user_at = off;
    let user_bytes: usize = (0..n).map(|i| u32_at(user_len, i) as usize).sum();
    off += user_bytes;
    let mut cat_at = off;
    let cat_bytes: usize = (0..n).map(|i| u32_at(cat_len, i) as usize).sum();
    off += cat_bytes;
    if off != data.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "user.bin trailing"));
    }
    for i in 0..n {
        let ul = u32_at(user_len, i) as usize;
        let cl = u32_at(cat_len, i) as usize;
        let user = &data[user_at..user_at + ul];
        user_at += ul;
        let cat = &data[cat_at..cat_at + cl];
        cat_at += cl;
        let mut line = Vec::with_capacity(ul + cl + 48);
        line.extend_from_slice(user);
        line.push(b',');
        push_day(&mut line, i32_at(days, i));
        line.push(b',');
        push_i64(&mut line, i64_at(sessions, i));
        line.push(b',');
        push_i64(&mut line, i64_at(events, i));
        line.push(b',');
        push_i64(&mut line, i64_at(revenue, i));
        line.push(b',');
        if present[i] == 1 {
            line.extend_from_slice(cat);
        }
        rows.push(line);
    }
    Ok(rows)
}

fn campaign_lines(path: &Path) -> io::Result<Vec<Vec<u8>>> {
    let data = fs::read(path)?;
    if data.len() < 8 || &data[..4] != b"SSC1" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad campaign.bin"));
    }
    let n = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let mut off = 8usize;
    let days = take_bytes(&data, &mut off, 4 * n)?;
    let sessions = take_bytes(&data, &mut off, 8 * n)?;
    let purchasing = take_bytes(&data, &mut off, 8 * n)?;
    let revenue = take_bytes(&data, &mut off, 8 * n)?;
    let assisted = take_bytes(&data, &mut off, 8 * n)?;
    let camp_len = take_bytes(&data, &mut off, 4 * n)?;
    let chan_len = take_bytes(&data, &mut off, 4 * n)?;
    let mut camp_at = off;
    let camp_bytes: usize = (0..n).map(|i| u32_at(camp_len, i) as usize).sum();
    off += camp_bytes;
    let mut chan_at = off;
    let chan_bytes: usize = (0..n).map(|i| u32_at(chan_len, i) as usize).sum();
    off += chan_bytes;
    if off != data.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "campaign.bin trailing"));
    }
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let nl = u32_at(camp_len, i) as usize;
        let hl = u32_at(chan_len, i) as usize;
        let name = &data[camp_at..camp_at + nl];
        camp_at += nl;
        let channel = &data[chan_at..chan_at + hl];
        chan_at += hl;
        let mut line = Vec::with_capacity(nl + hl + 64);
        line.extend_from_slice(name);
        line.push(b',');
        line.extend_from_slice(channel);
        line.push(b',');
        push_day(&mut line, i32_at(days, i));
        line.push(b',');
        push_i64(&mut line, i64_at(sessions, i));
        line.push(b',');
        push_i64(&mut line, i64_at(purchasing, i));
        line.push(b',');
        push_i64(&mut line, i64_at(revenue, i));
        line.push(b',');
        push_i64(&mut line, i64_at(assisted, i));
        rows.push(line);
    }
    Ok(rows)
}

fn take_bytes<'a>(data: &'a [u8], off: &mut usize, n: usize) -> io::Result<&'a [u8]> {
    let end = off
        .checked_add(n)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "overflow"))?;
    if end > data.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated"));
    }
    let slice = &data[*off..end];
    *off = end;
    Ok(slice)
}

fn u32_at(buf: &[u8], i: usize) -> u32 {
    let o = i * 4;
    u32::from_le_bytes(buf[o..o + 4].try_into().unwrap())
}
fn i32_at(buf: &[u8], i: usize) -> i32 {
    let o = i * 4;
    i32::from_le_bytes(buf[o..o + 4].try_into().unwrap())
}
fn i64_at(buf: &[u8], i: usize) -> i64 {
    let o = i * 8;
    i64::from_le_bytes(buf[o..o + 8].try_into().unwrap())
}
fn push_i64(line: &mut Vec<u8>, v: i64) {
    line.extend_from_slice(v.to_string().as_bytes());
}
fn push_day(line: &mut Vec<u8>, days: i32) {
    let (y, m, d) = ymd(days);
    line.extend_from_slice(format!("{y:04}-{m:02}-{d:02}").as_bytes());
}
fn ymd(days: i32) -> (i32, u32, u32) {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn json_field(text: &str, key: &str) -> Option<String> {
    let pat = format!("\"{key}\"");
    let i = text.find(&pat)?;
    let rest = &text[i + pat.len()..];
    let q = rest.find('"')?;
    let rest = &rest[q + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_u32_at(s: &[u8], i: usize, n: usize) -> Option<u32> {
    let mut v = 0u32;
    for &b in s.get(i..i + n)? {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u32;
    }
    Some(v)
}
fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}
fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}
fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
    if m <= 2 {
        y -= 1;
    }
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146097 + doe as i64 - 719468
}
fn ymd_to_ms(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> Option<i64> {
    if m < 1 || m > 12 || d < 1 || d > days_in_month(y, m) || hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    let secs = days * 86_400 + hh as i64 * 3600 + mm as i64 * 60 + ss as i64;
    Some(secs * 1000)
}
#[inline(always)]
fn parse_f1(s: &[u8]) -> Option<i64> {
    if s.len() != 20
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || s[19] != b'Z'
    {
        return None;
    }
    ymd_to_ms(
        parse_u32_at(s, 0, 4)? as i32,
        parse_u32_at(s, 5, 2)?,
        parse_u32_at(s, 8, 2)?,
        parse_u32_at(s, 11, 2)?,
        parse_u32_at(s, 14, 2)?,
        parse_u32_at(s, 17, 2)?,
    )
}
#[inline(always)]
fn parse_f2(s: &[u8]) -> Option<i64> {
    if s.len() != 25
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || (s[19] != b'+' && s[19] != b'-')
        || s[22] != b':'
    {
        return None;
    }
    let naive = ymd_to_ms(
        parse_u32_at(s, 0, 4)? as i32,
        parse_u32_at(s, 5, 2)?,
        parse_u32_at(s, 8, 2)?,
        parse_u32_at(s, 11, 2)?,
        parse_u32_at(s, 14, 2)?,
        parse_u32_at(s, 17, 2)?,
    )?;
    let off_h = parse_u32_at(s, 20, 2)?;
    let off_m = parse_u32_at(s, 23, 2)?;
    if off_h > 23 || off_m > 59 {
        return None;
    }
    let off = (off_h as i64 * 3600 + off_m as i64 * 60) * 1000;
    if s[19] == b'-' {
        Some(naive + off)
    } else {
        Some(naive - off)
    }
}
#[inline(always)]
fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if i == s.len() {
        return Some(0);
    }
    if s.len() - i > 18 {
        return None;
    }
    let mut v: i64 = 0;
    for &b in &s[i..] {
        v = v * 10 + (b - b'0') as i64;
    }
    Some(v)
}
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 => parse_f1(s),
        25 => parse_f2(s),
        _ => parse_f3(s),
    }
}
#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || s.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if i == s.len() || s.len() - i > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &b in &s[i..] {
        v = v * 10 + (b - b'0') as u32;
    }
    if (1..=PRICE_MAX).contains(&v) {
        Some(v)
    } else {
        None
    }
}

fn self_check() {
    assert_eq!(ymd(0), (1970, 1, 1));
    assert_eq!(ymd(20_729), (2026, 10, 3));
    assert_eq!(parse_ts(b"1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1_791_071_890_000));
    assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(1_791_071_890_000));
    assert_eq!(parse_ts(b"2026-10-03T00:00:00Z"), Some(1_790_985_600_000));
    assert_eq!(
        parse_ts(b"2026-10-03T19:58:10-00:00"),
        parse_ts(b"2026-10-03T19:58:10Z")
    );
    assert_eq!(parse_ts(b"1791071890000"), Some(1_791_071_890_000));
    assert_eq!(parse_ts(b"0001791071890000"), Some(1_791_071_890_000));
    assert_eq!(parse_ts(b"000"), Some(0));
    assert!(parse_ts(b"2026-10-03T24:00:00Z").is_none());
    assert!(parse_ts(b"2026-10-03T00:00:60Z").is_none());
    assert!(parse_ts(b"2026-02-29T00:00:00Z").is_none());
    assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
    assert!(parse_ts(b"2026-10-03T00:00:00+24:00").is_none());
    assert!(parse_ts(b"2026-10-03T00:00:00+05:60").is_none());
    assert!(parse_ts(b"2026-10-03T00:00:00+0200").is_none());
    assert!(parse_ts(b"2026-10-03T00:00:00+23:59").is_some());
    assert!(parse_ts(b"2026-10-03T00:00:00Z ").is_none());
    assert!(parse_ts("٢٥٠".as_bytes()).is_none());
    assert_eq!(parse_price(b"0250"), Some(250));
    assert_eq!(parse_price(b"10000000"), Some(10_000_000));
    assert!(parse_price(b"10000001").is_none());
    assert!(parse_price(b"0").is_none());
    let mut rel = [0u32; 7];
    let mut len = [0u32; 7];
    assert!(split_unquoted(b"\"u4\",\"u1\"", 2, &mut rel, &mut len));
    assert_eq!(field(b"\"u4\",\"u1\"", rel[0], len[0]), b"u4");
    assert_eq!(field(b"\"u4\",\"u1\"", rel[1], len[1]), b"u1");
    assert!(!split_unquoted(b"u1,u2,u3", 2, &mut rel, &mut len));
    assert!(!split_unquoted(b"u1,u5\"", 2, &mut rel, &mut len));
    let line = b"e3\r,u1,2026-10-03T10:15:00Z,2026-10-03T12:00:00Z,view,p1,";
    assert!(split_unquoted(line, 7, &mut rel, &mut len));
    assert_eq!(field(line, rel[0], len[0]), b"e3\r");

    assert_eq!(pack_id(b""), 0);
    assert_eq!(pack_id(b"0123456789"), 0);
    assert_eq!(pack_id(&[0x80]), 0);
    assert_eq!(pack_id(&[0x7f, 0x80]), 0);
    assert_ne!(pack_id(b"ab"), pack_id(b"ba"));
    assert_ne!(pack_id(b"a"), pack_id(b"a\0"));
    assert_ne!(pack_id(b"\0"), pack_id(b"\0\0"));
    assert_ne!(pack_id(b"abcdefghi"), pack_id(b"abcdefgh"));
    for sample in [
        b"a".as_slice(),
        b"\0",
        b"\0\0",
        b"u",
        b"uB",
        b"ua",
        &[0x7f],
        b"abcdefgh",
        b"abcdefghi",
        &[0x7f; 9],
        &[0x00; 9],
    ] {
        let key = pack_id(sample);
        assert_ne!(key, 0);
        assert_eq!(63 - key.leading_zeros(), (7 * sample.len()) as u32);
        let mut got = [0u8; 9];
        let n = sample.len();
        let mut rest = key;
        for i in (0..n).rev() {
            got[i] = (rest & 0x7f) as u8;
            rest >>= 7;
        }
        assert_eq!(rest, 1);
        assert_eq!(&got[..n], sample);
        let mut round = [0u8; 9];
        let rn = unpack_id(key, &mut round);
        assert_eq!(rn, n);
        assert_eq!(&round[..n], sample);
    }

    let mut shard = UserShard::with_capacity(1);
    let mut stored: Vec<(Vec<u8>, u32)> = Vec::new();
    for i in 0..40u8 {
        let bytes = vec![b'u', i + 1];
        let id = shard.intern(id_probe(&bytes), &bytes);
        stored.push((bytes, id));
    }
    let long = b"0123456789abcdef".to_vec();
    let hi = vec![0xff, b'a', b'b'];
    let nine = b"abcdefghi".to_vec();
    let id_long = shard.intern(id_probe(&long), &long);
    let id_hi = shard.intern(id_probe(&hi), &hi);
    let id_empty = shard.intern(id_probe(b""), b"");
    let id_z1 = shard.intern(id_probe(b"\0"), b"\0");
    let id_z2 = shard.intern(id_probe(b"\0\0"), b"\0\0");
    let id_zero = shard.intern(id_probe(&[0]), &[0]);
    let id_high = shard.intern(id_probe(&[0x80]), &[0x80]);
    let id_nine = shard.intern_packed(pack_id(&nine));
    assert_eq!(shard.intern_packed(pack_id(&nine)), id_nine);
    assert_eq!(shard.intern(id_probe(&long), &long), id_long);
    assert_eq!(shard.intern(id_probe(&hi), &hi), id_hi);
    assert_ne!(id_z1, id_z2);
    assert_ne!(id_empty, id_z1);
    assert_ne!(id_zero, id_high);
    for (bytes, id) in &stored {
        assert_eq!(shard.find(id_probe(bytes), bytes), Some(*id));
        assert_eq!(shard.name(*id), bytes.as_slice());
    }
    assert_eq!(shard.name(id_long), long.as_slice());
    assert_eq!(shard.name(id_hi), hi.as_slice());
    assert_eq!(shard.name(id_nine), nine.as_slice());
    assert_eq!(shard.name(id_empty), b"");
    assert_eq!(shard.find(id_probe(&[0x80]), &[0x80]), Some(id_high));
    assert_eq!(shard.find(id_probe(b"missing"), b"missing"), None);
    let up = shard.intern(id_probe(b"uB"), b"uB");
    let lo = shard.intern(id_probe(b"ua"), b"ua");
    assert_eq!(shard.name(up), b"uB");
    assert_eq!(shard.name(lo), b"ua");
    assert!(shard.name(up) < shard.name(lo));
}
