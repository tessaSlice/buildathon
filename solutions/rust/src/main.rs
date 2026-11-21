//! Shopping Sessions, std-only.
//!
//! Line rules match rules.md: only `\n` ends a row, a quote is valid only when
//! it wraps a whole field, and dedup on `event_id` happens before validation.
//! Session gaps are strictly over 30 minutes. Credit is the latest click in
//! the 7 days before the start, breaking timestamp ties by smallest `click_id`.
//! Assists are counted with a sliding window so a whale's campaigns are not
//! crossed with every one of their sessions.

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

static VERBOSE: AtomicBool = AtomicBool::new(true);

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const PRICE_MAX: u32 = 10_000_000;
const ORGANIC: u32 = u32::MAX;

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

struct Arena {
    buf: Vec<u8>,
}
impl Arena {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn push(&mut self, bytes: &[u8]) -> (u32, u32) {
        let off = self.buf.len();
        assert!(off <= u32::MAX as usize, "scratch arena exceeded 4 GiB");
        self.buf.extend_from_slice(bytes);
        (off as u32, bytes.len() as u32)
    }
    fn get(&self, off: u32, len: u32) -> &[u8] {
        let start = off as usize;
        &self.buf[start..start + len as usize]
    }
}

#[derive(Clone, Copy)]
struct EvRef {
    id_hash: u64,
    id_off: u32,
    id_len: u32,
    ingest_off: u32,
    ingest_len: u32,
    user_off: u32,
    user_len: u32,
    ts_off: u32,
    ts_len: u32,
    etype_off: u32,
    etype_len: u32,
    product_off: u32,
    product_len: u32,
    price_off: u32,
    price_len: u32,
}

struct Shard<T> {
    arena: Arena,
    rows: Vec<T>,
}

#[derive(Clone, Copy)]
struct ClickRef {
    id_hash: u64,
    user_off: u32,
    user_len: u32,
    camp_off: u32,
    camp_len: u32,
    ts_off: u32,
    ts_len: u32,
    id_off: u32,
    id_len: u32,
}



struct Ev {
    person: u32,
    ts: i64,
    cat: u32,
    price: u32,
    purchase: bool,
}

struct Sess {
    person: u32,
    start: i64,
    day: i32,
    revenue: u64,
    has_purchase: bool,
}

struct Click {
    person: u32,
    ts: i64,
    campaign: u32,
    id_off: u32,
    id_len: u32,
}

#[derive(Clone, Copy, Default)]
struct UserAgg {
    sessions: u64,
    events: u64,
    revenue: u64,
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[derive(Clone, Copy)]
struct Slot {
    hash: u64,
    id: u32,
}
const EMPTY_SLOT: u32 = u32::MAX;

struct Intern {
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    slots: Vec<Slot>,
    used: usize,
    parent: Vec<u32>,
}
impl Intern {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            spans: Vec::new(),
            slots: vec![Slot { hash: 0, id: EMPTY_SLOT }; 1024],
            used: 0,
            parent: Vec::new(),
        }
    }
    fn name(&self, id: u32) -> &[u8] {
        let (off, len) = self.spans[id as usize];
        &self.arena[off as usize..off as usize + len as usize]
    }
    fn lookup(&self, hash: u64, bytes: &[u8]) -> Option<u32> {
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        loop {
            let slot = self.slots[i];
            if slot.id == EMPTY_SLOT {
                return None;
            }
            if slot.hash == hash && self.name(slot.id) == bytes {
                return Some(slot.id);
            }
            i = (i + 1) & mask;
        }
    }
    fn rehash(&mut self) {
        let new_len = self.slots.len() * 2;
        let mut slots = vec![Slot { hash: 0, id: EMPTY_SLOT }; new_len];
        let mask = new_len - 1;
        for slot in &self.slots {
            if slot.id == EMPTY_SLOT {
                continue;
            }
            let mut i = (slot.hash as usize) & mask;
            while slots[i].id != EMPTY_SLOT {
                i = (i + 1) & mask;
            }
            slots[i] = *slot;
        }
        self.slots = slots;
    }
    fn intern(&mut self, bytes: &[u8]) -> u32 {
        let hash = hash_bytes(bytes);
        if let Some(id) = self.lookup(hash, bytes) {
            return id;
        }
        if (self.used + 1) * 10 >= self.slots.len() * 7 {
            self.rehash();
        }
        let id = self.spans.len() as u32;
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(bytes);
        self.spans.push((off, bytes.len() as u32));
        self.parent.push(id);
        let mask = self.slots.len() - 1;
        let mut i = (hash as usize) & mask;
        while self.slots[i].id != EMPTY_SLOT {
            i = (i + 1) & mask;
        }
        self.slots[i] = Slot { hash, id };
        self.used += 1;
        id
    }
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while x != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }
    fn union(&mut self, a: u32, b: u32) {
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        // The person is the bytewise-smallest id. Link the larger root under it.
        let rb_name = self.name(rb).to_vec();
        if rb_name.as_slice() < self.name(ra) {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
    }
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
    execute(&data, &scratch, true)
}

fn execute(data: &Path, scratch: &Path, verbose: bool) -> io::Result<()> {
    VERBOSE.store(verbose, Ordering::Relaxed);
    let started = Instant::now();
    let mut mark = started;

    let manifest = fs::read_to_string(data.join("manifest.json"))?;
    let win_start = parse_ts(json_field(&manifest, "window_start").unwrap().as_bytes())
        .expect("window_start");
    let win_end =
        parse_ts(json_field(&manifest, "window_end").unwrap().as_bytes()).expect("window_end");
    let click_lo = win_start - WEEK_MS;

    let (products, categories) = load_products(&data.join("products.csv"))?;
    let (campaigns, camp_names, channels) = load_campaigns(&data.join("campaigns.csv"))?;
    mark = log("read", mark);

    let nthreads = if verbose { thread_count() } else { 1 };
    let event_files = list_csv(&data.join("events"));
    let click_files = list_csv(&data.join("clicks"));
    let link_files = list_csv(&data.join("links"));

    let (event_shards, click_shards, links) =
        parse_parallel(event_files, click_files, link_files, nthreads)?;
    mark = log("ingest", mark);
    let event_kept = dedup_events(&event_shards);
    let click_kept = dedup_clicks(&click_shards);
    mark = log("dedup", mark);

    let mut people = Intern::new();
    for (a, b) in &links {
        if a.is_empty() || b.is_empty() {
            continue;
        }
        let ia = people.intern(a);
        let ib = people.intern(b);
        people.union(ia, ib);
    }
    mark = log("links", mark);

    let unknown = {
        // Resolved after products are loaded. `unknown` is not a file category.
        let mut cats = categories;
        if !cats.iter().any(|c| c == b"unknown") {
            cats.push(b"unknown".to_vec());
        }
        let id = cats.iter().position(|c| c == b"unknown").unwrap() as u32;
        (cats, id)
    };
    let categories = unknown.0;
    let unknown_cat = unknown.1;

    let parsed = validate_events(
        &event_shards,
        &event_kept,
        &products,
        unknown_cat,
        win_start,
        win_end,
        nthreads,
    );
    let mut events = Vec::with_capacity(parsed.len());
    for row in &parsed {
        let user = event_shards[row.shard as usize]
            .arena
            .get(row.user_off, row.user_len);
        let uid = people.intern(user);
        let person = people.find(uid);
        events.push(Ev {
            person,
            ts: row.ts,
            cat: row.cat,
            price: row.price,
            purchase: row.purchase,
        });
    }
    drop(parsed);
    drop(event_shards);
    mark = log("events", mark);

    events.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
    let (sessions, users, cat_rev) = sessionize(&events);
    drop(events);
    mark = log("sessions", mark);

    let mut click_arena = Arena::new();
    let mut clicks = Vec::with_capacity(click_kept.len());
    for (si, ri) in &click_kept {
        let shard = &click_shards[*si as usize];
        let row = shard.rows[*ri as usize];
        let user = shard.arena.get(row.user_off, row.user_len);
        let camp = shard.arena.get(row.camp_off, row.camp_len);
        let ts_raw = shard.arena.get(row.ts_off, row.ts_len);
        let id = shard.arena.get(row.id_off, row.id_len);
        if user.is_empty() {
            continue;
        }
        let Some(&campaign) = campaigns.get(camp) else { continue };
        let Some(ts) = parse_ts(ts_raw) else { continue };
        if ts < click_lo || ts >= win_end {
            continue;
        }
        let uid = people.intern(user);
        let person = people.find(uid);
        let (id_off, id_len) = click_arena.push(id);
        clicks.push(Click {
            person,
            ts,
            campaign,
            id_off,
            id_len,
        });
    }
    drop(click_shards);
    clicks.sort_unstable_by(|a, b| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| {
            click_arena
                .get(a.id_off, a.id_len)
                .cmp(click_arena.get(b.id_off, b.id_len))
        })
    });

    let n_camp = channels.len();
    let camp_rows = attribute(&sessions, &clicks, &click_arena, n_camp);
    drop(clicks);
    drop(sessions);
    mark = log("attribute", mark);

    let best = best_categories(&cat_rev, &categories);
    write_user(&scratch.join("user.bin"), &people, &users, &best, &categories)?;
    write_campaign(
        &scratch.join("campaign.bin"),
        &camp_rows,
        &camp_names,
        &channels,
    )?;
    log("write", mark);
    if verbose {
        eprintln!("total: {:.3}s", started.elapsed().as_secs_f64());
    }
    Ok(())
}

fn log(name: &str, mark: Instant) -> Instant {
    if VERBOSE.load(Ordering::Relaxed) {
        eprintln!("{name}: {:.3}s", mark.elapsed().as_secs_f64());
    }
    Instant::now()
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
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no fixtures",
        ));
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
        execute(&data, &out, false)?;
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
                show_diff(name, "user_daily", &out.join("user.bin"), &case.join("expected").join("user_daily.csv"), true)?;
            }
            if !camp_match {
                show_diff(name, "campaign_daily", &out.join("campaign.bin"), &case.join("expected").join("campaign_daily.csv"), false)?;
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
    let end = off.checked_add(n).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "overflow"))?;
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

fn parse_parallel(
    events: Vec<PathBuf>,
    clicks: Vec<PathBuf>,
    links: Vec<PathBuf>,
    nthreads: usize,
) -> io::Result<(Vec<Shard<EvRef>>, Vec<Shard<ClickRef>>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let event_handle = std::thread::spawn(move || parse_event_files(events, nthreads));
    let click_handle = std::thread::spawn(move || parse_click_files(clicks, nthreads));
    let link_rows = parse_links(&links)?;
    let event_part = event_handle.join().expect("event parse")?;
    let click_part = click_handle.join().expect("click parse")?;
    Ok((event_part, click_part, link_rows))
}

fn chunks(files: Vec<PathBuf>, nthreads: usize) -> Vec<Vec<PathBuf>> {
    if files.is_empty() {
        return Vec::new();
    }
    let n = nthreads.min(files.len()).max(1);
    let mut out = vec![Vec::new(); n];
    for (i, file) in files.into_iter().enumerate() {
        out[i % n].push(file);
    }
    out
}

fn parse_event_files(files: Vec<PathBuf>, nthreads: usize) -> io::Result<Vec<Shard<EvRef>>> {
    let groups = chunks(files, nthreads);
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let mut handles = Vec::new();
    for group in groups {
        handles.push(std::thread::spawn(move || {
            let mut part = Shard {
                arena: Arena::new(),
                rows: Vec::new(),
            };
            for path in group {
                ingest_events(&path, &mut part)?;
            }
            Ok::<Shard<EvRef>, io::Error>(part)
        }));
    }
    let mut shards = Vec::with_capacity(handles.len());
    for handle in handles {
        shards.push(handle.join().expect("event thread")?);
    }
    Ok(shards)
}

fn parse_click_files(files: Vec<PathBuf>, nthreads: usize) -> io::Result<Vec<Shard<ClickRef>>> {
    let groups = chunks(files, nthreads);
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let mut handles = Vec::new();
    for group in groups {
        handles.push(std::thread::spawn(move || {
            let mut part = Shard {
                arena: Arena::new(),
                rows: Vec::new(),
            };
            for path in group {
                ingest_clicks(&path, &mut part)?;
            }
            Ok::<Shard<ClickRef>, io::Error>(part)
        }));
    }
    let mut shards = Vec::with_capacity(handles.len());
    for handle in handles {
        shards.push(handle.join().expect("click thread")?);
    }
    Ok(shards)
}

fn dedup_events(shards: &[Shard<EvRef>]) -> Vec<(u16, u32)> {
    // The hash sits in the sort key so the n log n pass does not chase arenas.
    // Equal hashes are rare (duplicates and collisions) and resolved by bytes.
    let mut order: Vec<(u64, u16, u32)> = Vec::new();
    for (si, shard) in shards.iter().enumerate() {
        for (ri, row) in shard.rows.iter().enumerate() {
            order.push((row.id_hash, si as u16, ri as u32));
        }
    }
    order.sort_unstable();
    let mut i = 0;
    while i < order.len() {
        let hash = order[i].0;
        let mut j = i + 1;
        while j < order.len() && order[j].0 == hash {
            j += 1;
        }
        if j - i > 1 {
            order[i..j].sort_by(|a, b| {
                let ra = shards[a.1 as usize].rows[a.2 as usize];
                let rb = shards[b.1 as usize].rows[b.2 as usize];
                let ida = shards[a.1 as usize].arena.get(ra.id_off, ra.id_len);
                let idb = shards[b.1 as usize].arena.get(rb.id_off, rb.id_len);
                ida.cmp(idb).then_with(|| {
                    let ia = shards[a.1 as usize].arena.get(ra.ingest_off, ra.ingest_len);
                    let ib = shards[b.1 as usize].arena.get(rb.ingest_off, rb.ingest_len);
                    ia.cmp(ib)
                })
            });
        }
        i = j;
    }
    let mut kept = Vec::with_capacity(order.len());
    let mut prev_hash = 0u64;
    let mut prev_id: Vec<u8> = Vec::new();
    let mut have_prev = false;
    for (hash, si, ri) in order {
        let row = shards[si as usize].rows[ri as usize];
        let id = shards[si as usize].arena.get(row.id_off, row.id_len);
        if have_prev && hash == prev_hash && id == prev_id.as_slice() {
            continue;
        }
        prev_hash = hash;
        prev_id.clear();
        prev_id.extend_from_slice(id);
        have_prev = true;
        kept.push((si, ri));
    }
    kept
}

fn dedup_clicks(shards: &[Shard<ClickRef>]) -> Vec<(u16, u32)> {
    let mut order: Vec<(u64, u16, u32)> = Vec::new();
    for (si, shard) in shards.iter().enumerate() {
        for (ri, row) in shard.rows.iter().enumerate() {
            order.push((row.id_hash, si as u16, ri as u32));
        }
    }
    order.sort_unstable();
    let mut i = 0;
    while i < order.len() {
        let hash = order[i].0;
        let mut j = i + 1;
        while j < order.len() && order[j].0 == hash {
            j += 1;
        }
        if j - i > 1 {
            order[i..j].sort_by(|a, b| {
                let ra = shards[a.1 as usize].rows[a.2 as usize];
                let rb = shards[b.1 as usize].rows[b.2 as usize];
                shards[a.1 as usize]
                    .arena
                    .get(ra.id_off, ra.id_len)
                    .cmp(shards[b.1 as usize].arena.get(rb.id_off, rb.id_len))
            });
        }
        i = j;
    }
    let mut kept = Vec::with_capacity(order.len());
    let mut prev_hash = 0u64;
    let mut prev_id: Vec<u8> = Vec::new();
    let mut have_prev = false;
    for (hash, si, ri) in order {
        let row = shards[si as usize].rows[ri as usize];
        let id = shards[si as usize].arena.get(row.id_off, row.id_len);
        if have_prev && hash == prev_hash && id == prev_id.as_slice() {
            continue;
        }
        prev_hash = hash;
        prev_id.clear();
        prev_id.extend_from_slice(id);
        have_prev = true;
        kept.push((si, ri));
    }
    kept
}

fn for_data_lines(bytes: &[u8], mut each: impl FnMut(&[u8])) {
    let mut start = 0usize;
    let mut index = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            if index > 0 {
                let line = &bytes[start..i];
                if !line.is_empty() {
                    each(line);
                }
            }
            index += 1;
            start = i + 1;
        }
    }
    if index > 0 && start < bytes.len() {
        let line = &bytes[start..];
        if !line.is_empty() {
            each(line);
        }
    }
}

fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut i = 0usize;
    let len = line.len();
    for field in 0..N {
        if i > len {
            return false;
        }
        let start = i;
        if i < len && line[i] == b'"' {
            i += 1;
            while i < len && line[i] != b'"' {
                i += 1;
            }
            if i >= len {
                return false;
            }
            let end_quote = i;
            i += 1;
            if field + 1 < N {
                if i >= len || line[i] != b',' {
                    return false;
                }
                i += 1;
            } else if i != len {
                return false;
            }
            out[field] = &line[start..=end_quote];
        } else {
            while i < len && line[i] != b',' {
                if line[i] == b'"' {
                    return false;
                }
                i += 1;
            }
            out[field] = &line[start..i];
            if field + 1 < N {
                if i >= len || line[i] != b',' {
                    return false;
                }
                i += 1;
            } else if i != len {
                return false;
            }
        }
    }
    true
}

fn unquote(field: &[u8]) -> &[u8] {
    if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
        &field[1..field.len() - 1]
    } else {
        field
    }
}

fn ingest_events(path: &Path, part: &mut Shard<EvRef>) -> io::Result<()> {
    let bytes = fs::read(path)?;
    for_data_lines(&bytes, |line| {
        let mut raw = [b"".as_ref(); 7];
        if !split_fields::<7>(line, &mut raw) {
            return;
        }
        let id = unquote(raw[0]);
        if id.is_empty() {
            return;
        }
        let (id_off, id_len) = part.arena.push(id);
        let id_hash = hash_bytes(id);
        let (ingest_off, ingest_len) = part.arena.push(unquote(raw[3]));
        let (user_off, user_len) = part.arena.push(unquote(raw[1]));
        let (ts_off, ts_len) = part.arena.push(unquote(raw[2]));
        let (etype_off, etype_len) = part.arena.push(unquote(raw[4]));
        let (product_off, product_len) = part.arena.push(unquote(raw[5]));
        let (price_off, price_len) = part.arena.push(unquote(raw[6]));
        part.rows.push(EvRef {
            id_hash,
            id_off,
            id_len,
            ingest_off,
            ingest_len,
            user_off,
            user_len,
            ts_off,
            ts_len,
            etype_off,
            etype_len,
            product_off,
            product_len,
            price_off,
            price_len,
        });
    });
    Ok(())
}

fn ingest_clicks(path: &Path, part: &mut Shard<ClickRef>) -> io::Result<()> {
    let bytes = fs::read(path)?;
    for_data_lines(&bytes, |line| {
        let mut raw = [b"".as_ref(); 4];
        if !split_fields::<4>(line, &mut raw) {
            return;
        }
        let id = unquote(raw[0]);
        if id.is_empty() {
            return;
        }
        let (id_off, id_len) = part.arena.push(id);
        let id_hash = hash_bytes(id);
        let (user_off, user_len) = part.arena.push(unquote(raw[1]));
        let (camp_off, camp_len) = part.arena.push(unquote(raw[2]));
        let (ts_off, ts_len) = part.arena.push(unquote(raw[3]));
        part.rows.push(ClickRef {
            id_hash,
            user_off,
            user_len,
            camp_off,
            camp_len,
            ts_off,
            ts_len,
            id_off,
            id_len,
        });
    });
    Ok(())
}

fn parse_links(files: &[PathBuf]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for path in files {
        let bytes = fs::read(path)?;
        for_data_lines(&bytes, |line| {
            let mut raw = [b"".as_ref(); 2];
            if !split_fields::<2>(line, &mut raw) {
                return;
            }
            let a = unquote(raw[0]);
            let b = unquote(raw[1]);
            if a.is_empty() || b.is_empty() {
                return;
            }
            out.push((a.to_vec(), b.to_vec()));
        });
    }
    Ok(out)
}

fn load_products(path: &Path) -> io::Result<(FxMap<Vec<u8>, u32>, Vec<Vec<u8>>)> {
    let mut cats: Vec<Vec<u8>> = Vec::new();
    let mut cat_of: FxMap<Vec<u8>, u32> = fxmap();
    let mut products = fxmap();
    let bytes = fs::read(path)?;
    for_data_lines(&bytes, |line| {
        let mut raw = [b"".as_ref(); 2];
        if !split_fields::<2>(line, &mut raw) {
            return;
        }
        let id = unquote(raw[0]).to_vec();
        let cat = unquote(raw[1]).to_vec();
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
    for_data_lines(&bytes, |line| {
        let mut raw = [b"".as_ref(); 2];
        if !split_fields::<2>(line, &mut raw) {
            return;
        }
        let id = unquote(raw[0]).to_vec();
        let channel = unquote(raw[1]).to_vec();
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

struct ParsedEv {
    shard: u16,
    user_off: u32,
    user_len: u32,
    ts: i64,
    cat: u32,
    price: u32,
    purchase: bool,
}

fn validate_events(
    shards: &[Shard<EvRef>],
    kept: &[(u16, u32)],
    products: &FxMap<Vec<u8>, u32>,
    unknown_cat: u32,
    win_start: i64,
    win_end: i64,
    nthreads: usize,
) -> Vec<ParsedEv> {
    if kept.is_empty() {
        return Vec::new();
    }
    let threads = nthreads.max(1).min(kept.len());
    let mut parts = Vec::with_capacity(threads);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        let size = kept.len().div_ceil(threads);
        for t in 0..threads {
            let start = t * size;
            if start >= kept.len() {
                break;
            }
            let end = (start + size).min(kept.len());
            handles.push(scope.spawn(move || {
                validate_event_range(
                    shards,
                    &kept[start..end],
                    products,
                    unknown_cat,
                    win_start,
                    win_end,
                )
            }));
        }
        for handle in handles {
            parts.push(handle.join().expect("event validate"));
        }
    });
    let mut out = Vec::with_capacity(kept.len());
    for part in parts {
        out.extend(part);
    }
    out
}

fn validate_event_range(
    shards: &[Shard<EvRef>],
    kept: &[(u16, u32)],
    products: &FxMap<Vec<u8>, u32>,
    unknown_cat: u32,
    win_start: i64,
    win_end: i64,
) -> Vec<ParsedEv> {
    let mut out = Vec::with_capacity(kept.len());
    for &(si, ri) in kept {
        let shard = &shards[si as usize];
        let row = shard.rows[ri as usize];
        let user = shard.arena.get(row.user_off, row.user_len);
        let product = shard.arena.get(row.product_off, row.product_len);
        if user.is_empty() || product.is_empty() {
            continue;
        }
        let etype = shard.arena.get(row.etype_off, row.etype_len);
        let purchase = etype == b"purchase";
        if !purchase && etype != b"view" && etype != b"cart" {
            continue;
        }
        let Some(ts) = parse_ts(shard.arena.get(row.ts_off, row.ts_len)) else {
            continue;
        };
        if ts < win_start || ts >= win_end {
            continue;
        }
        let Some(ingest) = parse_ts(shard.arena.get(row.ingest_off, row.ingest_len)) else {
            continue;
        };
        if ingest - ts > LATE_MS {
            continue;
        }
        let price = if purchase {
            match parse_price(shard.arena.get(row.price_off, row.price_len)) {
                Some(p) => p,
                None => continue,
            }
        } else {
            0
        };
        let cat = products.get(product).copied().unwrap_or(unknown_cat);
        out.push(ParsedEv {
            shard: si,
            user_off: row.user_off,
            user_len: row.user_len,
            ts,
            cat,
            price,
            purchase,
        });
    }
    out
}

fn sessionize(events: &[Ev]) -> (Vec<Sess>, FxMap<(u32, i32), UserAgg>, FxMap<(u32, i32, u32), u64>) {
    let mut sessions = Vec::new();
    let mut users = fxmap();
    let mut cats = fxmap();
    let mut i = 0;
    let n = events.len();
    while i < n {
        let person = events[i].person;
        let mut prev: Option<i64> = None;
        let mut open = false;
        let mut start = 0i64;
        let mut day = 0i32;
        let mut count = 0u64;
        let mut revenue = 0u64;
        let mut has_purchase = false;
        let flush = |sessions: &mut Vec<Sess>,
                     users: &mut FxMap<(u32, i32), UserAgg>,
                     start: i64,
                     day: i32,
                     count: u64,
                     revenue: u64,
                     has_purchase: bool| {
            sessions.push(Sess {
                person,
                start,
                day,
                revenue,
                has_purchase,
            });
            let row = users.entry((person, day)).or_default();
            row.sessions += 1;
            row.events += count;
            row.revenue += revenue;
        };
        while i < n && events[i].person == person {
            let event = &events[i];
            let is_new = match prev {
                None => true,
                Some(ts) => event.ts - ts > GAP_MS,
            };
            if is_new {
                if open {
                    flush(
                        &mut sessions,
                        &mut users,
                        start,
                        day,
                        count,
                        revenue,
                        has_purchase,
                    );
                }
                start = event.ts;
                day = event.ts.div_euclid(DAY_MS) as i32;
                count = 0;
                revenue = 0;
                has_purchase = false;
                open = true;
            }
            count += 1;
            if event.purchase {
                revenue += event.price as u64;
                has_purchase = true;
                *cats.entry((person, day, event.cat)).or_insert(0) += event.price as u64;
            }
            prev = Some(event.ts);
            i += 1;
        }
        if open {
            flush(
                &mut sessions,
                &mut users,
                start,
                day,
                count,
                revenue,
                has_purchase,
            );
        }
    }
    (sessions, users, cats)
}

fn best_categories(
    rev: &FxMap<(u32, i32, u32), u64>,
    names: &[Vec<u8>],
) -> FxMap<(u32, i32), u32> {
    let mut best: FxMap<(u32, i32), (u64, u32)> = fxmap();
    for (&(person, day, cat), &revenue) in rev {
        match best.get_mut(&(person, day)) {
            None => {
                best.insert((person, day), (revenue, cat));
            }
            Some((best_rev, best_cat)) => {
                let better = revenue > *best_rev
                    || (revenue == *best_rev && names[cat as usize] < names[*best_cat as usize]);
                if better {
                    *best_rev = revenue;
                    *best_cat = cat;
                }
            }
        }
    }
    best.into_iter().map(|(k, (_r, c))| (k, c)).collect()
}

fn attribute(sessions: &[Sess], clicks: &[Click], arena: &Arena, n_camp: usize) -> FxMap<(u32, i32), CampAgg> {
    let _ = arena;
    let mut aggs = fxmap();
    let mut cnt = vec![0u32; n_camp];
    let mut pos = vec![usize::MAX; n_camp];
    let mut active: Vec<usize> = Vec::new();
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
        credit_person(&sessions[s0..si], &clicks[c0..c1], &mut aggs);
        assist_person(
            &sessions[s0..si],
            &clicks[c0..c1],
            &mut aggs,
            &mut cnt,
            &mut pos,
            &mut active,
        );
    }
    aggs
}

fn credit_person(sessions: &[Sess], clicks: &[Click], aggs: &mut FxMap<(u32, i32), CampAgg>) {
    // Clicks are ordered by ts, then click_id. The rightmost ts <= start is the
    // latest click; the left edge of that timestamp is the smallest click_id.
    let mut nxt = 0usize;
    for session in sessions {
        while nxt < clicks.len() && clicks[nxt].ts <= session.start {
            nxt += 1;
        }
        let campaign = if nxt == 0 {
            ORGANIC
        } else {
            let ts = clicks[nxt - 1].ts;
            let mut best = nxt - 1;
            while best > 0 && clicks[best - 1].ts == ts {
                best -= 1;
            }
            if session.start - clicks[best].ts <= WEEK_MS {
                clicks[best].campaign
            } else {
                ORGANIC
            }
        };
        let row = aggs.entry((campaign, session.day)).or_default();
        row.sessions += 1;
        row.revenue += session.revenue;
        if session.has_purchase {
            row.purchasing += 1;
        }
    }
}

fn assist_person(
    sessions: &[Sess],
    clicks: &[Click],
    aggs: &mut FxMap<(u32, i32), CampAgg>,
    cnt: &mut [u32],
    pos: &mut [usize],
    active: &mut Vec<usize>,
) {
    let mut lo = 0usize;
    let mut hi = 0usize;
    for session in sessions {
        while hi < clicks.len() && clicks[hi].ts <= session.start {
            let c = clicks[hi].campaign as usize;
            if cnt[c] == 0 {
                pos[c] = active.len();
                active.push(c);
            }
            cnt[c] += 1;
            hi += 1;
        }
        let cutoff = session.start - WEEK_MS;
        while lo < hi && clicks[lo].ts < cutoff {
            let c = clicks[lo].campaign as usize;
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
            aggs.entry((c as u32, session.day)).or_default().assisted += 1;
        }
    }
    for &c in active.iter() {
        cnt[c] = 0;
        pos[c] = usize::MAX;
    }
    active.clear();
}

fn write_user(
    path: &Path,
    people: &Intern,
    users: &FxMap<(u32, i32), UserAgg>,
    best: &FxMap<(u32, i32), u32>,
    categories: &[Vec<u8>],
) -> io::Result<()> {
    let mut file = BufWriter::new(File::create(path)?);
    file.write_all(b"SSU1")?;
    file.write_all(&(users.len() as u32).to_le_bytes())?;
    let mut days = Vec::with_capacity(users.len());
    let mut sessions = Vec::with_capacity(users.len());
    let mut events = Vec::with_capacity(users.len());
    let mut revenue = Vec::with_capacity(users.len());
    let mut present = Vec::with_capacity(users.len());
    let mut user_blob = Vec::new();
    let mut user_len = Vec::with_capacity(users.len());
    let mut cat_blob = Vec::new();
    let mut cat_len = Vec::with_capacity(users.len());
    for (&(person, day), row) in users {
        days.push(day);
        sessions.push(row.sessions as i64);
        events.push(row.events as i64);
        revenue.push(row.revenue as i64);
        let name = people.name(person);
        user_blob.extend_from_slice(name);
        user_len.push(name.len() as u32);
        if let Some(&cat) = best.get(&(person, day)) {
            let text = &categories[cat as usize];
            present.push(1u8);
            cat_blob.extend_from_slice(text);
            cat_len.push(text.len() as u32);
        } else {
            present.push(0u8);
            cat_len.push(0);
        }
    }
    write_i32s(&mut file, &days)?;
    write_i64s(&mut file, &sessions)?;
    write_i64s(&mut file, &events)?;
    write_i64s(&mut file, &revenue)?;
    file.write_all(&present)?;
    write_u32s(&mut file, &user_len)?;
    write_u32s(&mut file, &cat_len)?;
    file.write_all(&user_blob)?;
    file.write_all(&cat_blob)?;
    file.flush()
}

fn write_campaign(
    path: &Path,
    rows: &FxMap<(u32, i32), CampAgg>,
    names: &[Vec<u8>],
    channels: &[Vec<u8>],
) -> io::Result<()> {
    let n = rows
        .values()
        .filter(|row| row.sessions > 0 || row.assisted > 0)
        .count();
    let mut days = Vec::with_capacity(n);
    let mut sessions = Vec::with_capacity(n);
    let mut purchasing = Vec::with_capacity(n);
    let mut revenue = Vec::with_capacity(n);
    let mut assisted = Vec::with_capacity(n);
    let mut camp_blob = Vec::new();
    let mut camp_len = Vec::with_capacity(n);
    let mut chan_blob = Vec::new();
    let mut chan_len = Vec::with_capacity(n);
    for (&(camp, day), row) in rows {
        if row.sessions == 0 && row.assisted == 0 {
            continue;
        }
        days.push(day);
        sessions.push(row.sessions as i64);
        purchasing.push(row.purchasing as i64);
        revenue.push(row.revenue as i64);
        assisted.push(if camp == ORGANIC { 0i64 } else { row.assisted as i64 });
        let (name, channel): (&[u8], &[u8]) = if camp == ORGANIC {
            (b"organic", b"organic")
        } else {
            (&names[camp as usize], &channels[camp as usize])
        };
        camp_blob.extend_from_slice(name);
        camp_len.push(name.len() as u32);
        chan_blob.extend_from_slice(channel);
        chan_len.push(channel.len() as u32);
    }
    let mut file = BufWriter::new(File::create(path)?);
    file.write_all(b"SSC1")?;
    file.write_all(&(n as u32).to_le_bytes())?;
    write_i32s(&mut file, &days)?;
    write_i64s(&mut file, &sessions)?;
    write_i64s(&mut file, &purchasing)?;
    write_i64s(&mut file, &revenue)?;
    write_i64s(&mut file, &assisted)?;
    write_u32s(&mut file, &camp_len)?;
    write_u32s(&mut file, &chan_len)?;
    file.write_all(&camp_blob)?;
    file.write_all(&chan_blob)?;
    file.flush()
}

fn write_i32s(w: &mut impl Write, vals: &[i32]) -> io::Result<()> {
    for v in vals {
        w.write_all(&v.to_le_bytes())?;
    }
    Ok(())
}
fn write_i64s(w: &mut impl Write, vals: &[i64]) -> io::Result<()> {
    for v in vals {
        w.write_all(&v.to_le_bytes())?;
    }
    Ok(())
}
fn write_u32s(w: &mut impl Write, vals: &[u32]) -> io::Result<()> {
    for v in vals {
        w.write_all(&v.to_le_bytes())?;
    }
    Ok(())
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
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

fn parse_ts(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 => parse_f1(s),
        25 => parse_f2(s),
        _ => parse_f3(s),
    }
}

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
    let mut out = [b"".as_ref(); 2];
    assert!(split_fields::<2>(b"\"u4\",\"u1\"", &mut out));
    assert_eq!(unquote(out[0]), b"u4");
    assert_eq!(unquote(out[1]), b"u1");
    assert!(!split_fields::<2>(b"u1,u2,u3", &mut out));
    assert!(!split_fields::<2>(b"u1,u5\"", &mut out));
    let mut ev = [b"".as_ref(); 7];
    let line = b"e3\r,u1,2026-10-03T10:15:00Z,2026-10-03T12:00:00Z,view,p1,";
    assert!(split_fields::<7>(line, &mut ev));
    assert_eq!(unquote(ev[0]), b"e3\r");
}

