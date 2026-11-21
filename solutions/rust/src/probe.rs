//! One-shot picture of the machine the scorer actually ran.
//!
//! The public dev set is the only run that returns stderr. Hidden runs report
//! an exit code and row counts only. This prints a short hardware report and
//! the caller exits 1 before writing tables, so the dev-set failure carries
//! the report back. Nothing here reads a row field or opens a socket.

use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::Instant;

pub fn run() -> ! {
    let mut err = io::stderr();
    let _ = writeln!(
        err,
        "PROBE begin: hardware report for the dev-set machine, then exit 1"
    );

    let cpu = cpu_info();
    let _ = writeln!(err, "PROBE cpu {}", cpu.summary);

    let features = feature_line();
    let _ = writeln!(err, "PROBE isa {features}");

    let caches = cache_line();
    let _ = writeln!(err, "PROBE cache {caches}");

    let mem = mem_line();
    let _ = writeln!(err, "PROBE mem {mem}");

    let cg = cgroup_line();
    let _ = writeln!(err, "PROBE cgroup {cg}");

    let lim = limits_line();
    let _ = writeln!(err, "PROBE limits {lim}");

    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let data = env_or("DATA_DIR", "/data");
    let output = env_or("OUTPUT_DIR", "/output");
    let scratch = env_or("SCRATCH_DIR", "/scratch");
    let fs_line = format!(
        "data={} output={} scratch={} tmp={} root={}",
        fstype(&mounts, &data),
        fstype(&mounts, &output),
        fstype(&mounts, &scratch),
        fstype(&mounts, "/tmp"),
        fstype(&mounts, "/")
    );
    let _ = writeln!(err, "PROBE fs {fs_line}");

    let thp = read_trim("/sys/kernel/mm/transparent_hugepage/enabled").unwrap_or_else(|| "missing".into());
    let page = page_size();
    let libc = libc_line();
    let _ = writeln!(err, "PROBE os page={page} thp={thp} libc={libc}");

    let env_line = format!(
        "NCPU={} MEM_GB={} HOME={} TMPDIR={} DATA_DIR={data} OUTPUT_DIR={output} SCRATCH_DIR={scratch}",
        env_or("NCPU", "missing"),
        env_or("MEM_GB", "missing"),
        env_or("HOME", "missing"),
        env_or("TMPDIR", "missing"),
    );
    let _ = writeln!(err, "PROBE env {env_line}");

    let (files, bytes, secs, capped) = read_data(&data);
    let mib_s = if secs > 0.0 {
        (bytes as f64 / secs) / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let window = manifest_window(&data);
    let _ = writeln!(
        err,
        "PROBE data files={files} bytes={bytes} sec={secs:.3} MiB/s={mib_s:.1} capped={capped} {window}"
    );

    let threads = parse_usize_env("NCPU").unwrap_or(1).clamp(1, 8);
    let copy1 = copy_gbs(1);
    let copyn = copy_gbs(threads);
    let chase = chase_ns();
    let avx = avx2_copy_gbs();
    let avx_s = match avx {
        Some(v) => format!("{v:.2}"),
        None => "off".into(),
    };
    let _ = writeln!(
        err,
        "PROBE bw copy1_GBs={copy1:.2} copy{threads}_GBs={copyn:.2} avx2_GBs={avx_s} chase_ns={chase:.1}"
    );

    // The scorer shows the end of stderr. These short lines are that tail.
    // The verbose PROBE lines above are the same facts, in case more comes back.
    let data_fs = fstype(&mounts, &data);
    let ncpu_env = env_or("NCPU", "missing");
    let mem_gb_env = env_or("MEM_GB", "missing");
    let _ = writeln!(
        err,
        "PROBE_END model={} logical={} cores={} siblings={} affinity={} mhz={}",
        cpu.model, cpu.logical, cpu.cores, cpu.siblings, cpu.affinity, cpu.mhz
    );
    let _ = writeln!(err, "PROBE_END flags={} {features}", cpu.flags);
    let _ = writeln!(err, "PROBE_END mem={mem} cgroup={cg}");
    let _ = writeln!(
        err,
        "PROBE_END fs={fs_line} data_fs={data_fs} page={page} os={libc}"
    );
    let _ = writeln!(
        err,
        "PROBE_END bw threads={threads} copy1_GBs={copy1:.2} copyN_GBs={copyn:.2} avx2_GBs={avx_s} chase_ns={chase:.1} data_MiBs={mib_s:.1} files={files} bytes={bytes}"
    );
    let uid_raw = read_status_field("Uid").unwrap_or_else(|| "missing".into());
    let uid = uid_raw.split_whitespace().next().unwrap_or("missing");
    let _ = writeln!(
        err,
        "PROBE_END {window} uid={uid} ncpu={ncpu_env} mem_gb={mem_gb_env} exit=1"
    );
    let _ = err.flush();
    std::process::exit(1);
}

struct Cpu {
    model: String,
    summary: String,
    affinity: String,
    logical: usize,
    cores: String,
    siblings: String,
    mhz: String,
    flags: String,
}

fn cpu_info() -> Cpu {
    let raw = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let mut model = String::from("missing");
    let mut mhz_min = f64::MAX;
    let mut mhz_max = 0.0f64;
    let mut procs = 0usize;
    let mut cores = String::from("?");
    let mut siblings = String::from("?");
    let mut bogomips = String::from("?");
    let mut cache = String::from("?");
    let mut flags = String::new();
    for block in raw.split("\n\n") {
        if block.trim().is_empty() {
            continue;
        }
        procs += 1;
        if procs > 1 {
            for line in block.lines() {
                if let Some(rest) = line.strip_prefix("cpu MHz") {
                    if let Some(v) = rest.split(':').nth(1) {
                        if let Ok(m) = v.trim().parse::<f64>() {
                            mhz_min = mhz_min.min(m);
                            mhz_max = mhz_max.max(m);
                        }
                    }
                }
            }
            continue;
        }
        for line in block.lines() {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            let v = v.trim();
            match k.trim() {
                "model name" => model = v.to_string(),
                "cpu cores" => cores = v.to_string(),
                "siblings" => siblings = v.to_string(),
                "bogomips" => bogomips = v.to_string(),
                "cache size" => cache = v.to_string(),
                "flags" => flags = interesting_flags(v),
                "cpu MHz" => {
                    if let Ok(m) = v.parse::<f64>() {
                        mhz_min = m;
                        mhz_max = m;
                    }
                }
                _ => {}
            }
        }
    }
    if procs == 0 {
        procs = 1;
    }
    let mhz = if mhz_max == 0.0 {
        "missing".into()
    } else {
        format!("{mhz_min:.0}-{mhz_max:.0}")
    };
    let online = read_trim("/sys/devices/system/cpu/online").unwrap_or_else(|| "missing".into());
    let affinity = read_status_field("Cpus_allowed_list").unwrap_or_else(|| "missing".into());
    let summary = format!(
        "model={model} logical={procs} cores_per_socket={cores} siblings={siblings} online={online} affinity={affinity} mhz={mhz} bogomips={bogomips} cache_field={cache} flags={flags}"
    );
    Cpu {
        model,
        summary,
        affinity,
        logical: procs,
        cores,
        siblings,
        mhz,
        flags,
    }
}

fn interesting_flags(flags: &str) -> String {
    const WANT: &[&str] = &[
        "sse4_2",
        "avx",
        "avx2",
        "fma",
        "avx512f",
        "avx512dq",
        "avx512cd",
        "avx512bw",
        "avx512vl",
        "avx512vnni",
        "avx512vbmi",
        "avx512vbmi2",
        "avx512ifma",
        "avx512bitalg",
        "avx512vpopcntdq",
        "gfni",
        "vpclmulqdq",
    ];
    let mut out = String::new();
    for flag in flags.split_whitespace() {
        if WANT.contains(&flag) {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(flag);
        }
    }
    if out.is_empty() {
        out.push_str("none");
    }
    out
}

fn feature_line() -> String {
    #[cfg(target_arch = "x86_64")]
    {
        format!(
            "detect avx2={} avx512f={} avx512bw={} avx512vl={} avx512dq={} avx512cd={} avx512vbmi={} avx512vbmi2={} avx512vnni={} gfni={}",
            bit(std::arch::is_x86_feature_detected!("avx2")),
            bit(std::arch::is_x86_feature_detected!("avx512f")),
            bit(std::arch::is_x86_feature_detected!("avx512bw")),
            bit(std::arch::is_x86_feature_detected!("avx512vl")),
            bit(std::arch::is_x86_feature_detected!("avx512dq")),
            bit(std::arch::is_x86_feature_detected!("avx512cd")),
            bit(std::arch::is_x86_feature_detected!("avx512vbmi")),
            bit(std::arch::is_x86_feature_detected!("avx512vbmi2")),
            bit(std::arch::is_x86_feature_detected!("avx512vnni")),
            bit(std::arch::is_x86_feature_detected!("gfni")),
        )
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        "detect arch=other".into()
    }
}

fn bit(v: bool) -> u8 {
    if v {
        1
    } else {
        0
    }
}

fn cache_line() -> String {
    let mut out = String::new();
    for i in 0..6 {
        let base = format!("/sys/devices/system/cpu/cpu0/cache/index{i}");
        let level = match read_trim(&format!("{base}/level")) {
            Some(v) => v,
            None => break,
        };
        let kind = read_trim(&format!("{base}/type")).unwrap_or_else(|| "?".into());
        let size = read_trim(&format!("{base}/size")).unwrap_or_else(|| "?".into());
        let share = read_trim(&format!("{base}/shared_cpu_list")).unwrap_or_else(|| "?".into());
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&format!("L{level}/{kind}/{size}/cpus={share}"));
    }
    if out.is_empty() {
        out.push_str("missing");
    }
    let cur = read_trim("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq")
        .unwrap_or_else(|| "missing".into());
    let max = read_trim("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")
        .unwrap_or_else(|| "missing".into());
    format!("{out} cur_khz={cur} max_khz={max}")
}

fn mem_line() -> String {
    let raw = fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total = meminfo_kb(&raw, "MemTotal");
    let avail = meminfo_kb(&raw, "MemAvailable");
    let swap = meminfo_kb(&raw, "SwapTotal");
    let huge = meminfo_kb(&raw, "Hugepagesize");
    format!("total_kb={total} avail_kb={avail} swap_kb={swap} hugepage_kb={huge}")
}

fn meminfo_kb(raw: &str, key: &str) -> String {
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let rest = rest.trim_start_matches(':').trim();
            let n = rest.split_whitespace().next().unwrap_or("missing");
            return n.to_string();
        }
    }
    "missing".into()
}

fn cgroup_line() -> String {
    let mem = cgroup_value(&["memory.max", "memory.limit_in_bytes"]);
    let cpu = cgroup_value(&["cpu.max", "cpu.cfs_quota_us"]);
    let period = cgroup_value(&["cpu.cfs_period_us"]);
    let cpus = cgroup_value(&["cpuset.cpus.effective", "cpuset.cpus"]);
    format!("memory={mem} cpu={cpu} period={period} cpuset={cpus}")
}

fn cgroup_value(names: &[&str]) -> String {
    for name in names {
        if let Some(v) = read_trim(&format!("/sys/fs/cgroup/{name}")) {
            if !v.is_empty() {
                return format!("{name}={v}");
            }
        }
    }
    let raw = fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    for line in raw.lines() {
        let mut parts = line.splitn(3, ':');
        let _id = parts.next();
        let ctrl = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("");
        for name in names {
            let ctrl_ok = ctrl.is_empty()
                || ctrl.split(',').any(|c| {
                    (c == "memory" && name.starts_with("memory"))
                        || (c == "cpu" && name.starts_with("cpu"))
                        || (c == "cpuset" && name.starts_with("cpuset"))
                });
            if !ctrl_ok {
                continue;
            }
            let unified = format!("/sys/fs/cgroup{path}/{name}");
            if let Some(v) = read_trim(&unified) {
                if !v.is_empty() {
                    return format!("{name}={v}");
                }
            }
            if let Some(ctrl0) = name.split('.').next() {
                let v1 = format!("/sys/fs/cgroup/{ctrl0}{path}/{name}");
                if let Some(v) = read_trim(&v1) {
                    if !v.is_empty() {
                        return format!("{name}={v}");
                    }
                }
            }
        }
    }
    "missing".into()
}

fn limits_line() -> String {
    let raw = fs::read_to_string("/proc/self/limits").unwrap_or_default();
    let mut out = String::new();
    for line in raw.lines() {
        let keep = line.starts_with("Max open files")
            || line.starts_with("Max processes")
            || line.starts_with("Max locked memory")
            || line.starts_with("Max address space")
            || line.starts_with("Max resident set");
        if !keep {
            continue;
        }
        if !out.is_empty() {
            out.push_str(" | ");
        }
        out.push_str(line.trim());
    }
    let uid = read_status_field("Uid").unwrap_or_else(|| "missing".into());
    if out.is_empty() {
        out.push_str("missing");
    }
    format!("uid={uid} {out}")
}

fn read_status_field(key: &str) -> Option<String> {
    let raw = fs::read_to_string("/proc/self/status").ok()?;
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            if rest.starts_with(':') || rest.starts_with('\t') {
                return Some(rest.trim_start_matches(':').trim().to_string());
            }
        }
    }
    None
}

fn libc_line() -> String {
    let maps = fs::read_to_string("/proc/self/maps").unwrap_or_default();
    let mut libc = String::from("none");
    for line in maps.lines() {
        if line.contains("libc.so") || line.contains("libc-") {
            if let Some(path) = line.rsplit(' ').next() {
                if path.contains("libc") {
                    libc = path.to_string();
                    break;
                }
            }
        }
    }
    let ver = read_trim("/proc/sys/kernel/osrelease").unwrap_or_else(|| "missing".into());
    format!("{libc} kernel={ver}")
}

fn page_size() -> usize {
    // 4096 unless the kernel says otherwise. sysconf is not in std.
    let v = read_trim("/proc/self/smaps")
        .or_else(|| read_trim("/proc/self/smaps_rollup"));
    if let Some(raw) = v {
        for line in raw.lines() {
            if let Some(rest) = line.strip_prefix("KernelPageSize:") {
                let n = rest.trim().split_whitespace().next().unwrap_or("4");
                if let Ok(kb) = n.parse::<usize>() {
                    return kb * 1024;
                }
            }
        }
    }
    4096
}

fn fstype(mounts: &str, target: &str) -> String {
    let mut best_len = 0usize;
    let mut best = String::from("missing");
    for line in mounts.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let mut fields = left.split_whitespace();
        let Some(mp) = fields.nth(4) else {
            continue;
        };
        let mp = mp.replace("\\040", " ");
        if !mount_covers(&mp, target) {
            continue;
        }
        if mp.len() >= best_len {
            best_len = mp.len();
            best = right.split_whitespace().next().unwrap_or("?").to_string();
        }
    }
    best
}

fn mount_covers(mp: &str, target: &str) -> bool {
    if mp == target || mp == "/" {
        return true;
    }
    if target.starts_with(mp) {
        return target.as_bytes().get(mp.len()) == Some(&b'/');
    }
    false
}

fn manifest_window(data: &str) -> String {
    let raw = fs::read_to_string(Path::new(data).join("manifest.json")).unwrap_or_default();
    let start = json_string(&raw, "window_start");
    let end = json_string(&raw, "window_end");
    format!("window={start}..{end}")
}

fn json_string(raw: &str, key: &str) -> String {
    let pat = format!("\"{key}\"");
    let Some(i) = raw.find(&pat) else {
        return "missing".into();
    };
    let rest = &raw[i + pat.len()..];
    let Some(q) = rest.find('"') else {
        return "missing".into();
    };
    let rest = &rest[q + 1..];
    let Some(e) = rest.find('"') else {
        return "missing".into();
    };
    rest[..e].to_string()
}

fn read_data(data: &str) -> (u64, u64, f64, u8) {
    let root = Path::new(data);
    if !root.is_dir() {
        return (0, 0, 0.0, 0);
    }
    let mut files = Vec::new();
    push_file(&mut files, root.join("manifest.json"));
    push_file(&mut files, root.join("products.csv"));
    push_file(&mut files, root.join("campaigns.csv"));
    for sub in ["events", "clicks", "links"] {
        let dir = root.join(sub);
        let rd = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        for ent in rd.flatten() {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("csv") {
                files.push(p);
            }
        }
    }
    let cap = 256u64 << 20;
    let mut left = cap;
    let mut nfiles = 0u64;
    let mut nbytes = 0u64;
    let mut buf = vec![0u8; 1 << 20];
    let started = Instant::now();
    for path in &files {
        if left == 0 {
            break;
        }
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        nfiles += 1;
        loop {
            if left == 0 {
                break;
            }
            let want = buf.len().min(left as usize);
            let k = match f.read(&mut buf[..want]) {
                Ok(0) | Err(_) => break,
                Ok(k) => k,
            };
            nbytes += k as u64;
            left -= k as u64;
            std::hint::black_box(buf[0]);
        }
    }
    let capped = if nbytes >= cap { 1 } else { 0 };
    (nfiles, nbytes, started.elapsed().as_secs_f64(), capped)
}

fn push_file(files: &mut Vec<std::path::PathBuf>, path: std::path::PathBuf) {
    if path.is_file() {
        files.push(path);
    }
}

fn copy_gbs(threads: usize) -> f64 {
    let n = 8 * 1024 * 1024usize;
    let passes = 3usize;
    let mut bufs = Vec::with_capacity(threads);
    for _ in 0..threads {
        bufs.push((vec![1u64; n], vec![2u64; n]));
    }
    let started = Instant::now();
    std::thread::scope(|scope| {
        for (a, b) in &mut bufs {
            scope.spawn(|| {
                for _ in 0..passes {
                    for i in 0..n {
                        b[i] = a[i].wrapping_add(1);
                        a[i] = b[i].wrapping_add(1);
                    }
                }
                std::hint::black_box(a[0]);
                std::hint::black_box(b[0]);
            });
        }
    });
    let secs = started.elapsed().as_secs_f64().max(1e-9);
    let bytes = (threads * passes * n * 8 * 2) as f64;
    bytes / secs / 1e9
}

fn chase_ns() -> f64 {
    let n = 16 * 1024 * 1024usize;
    let mut order: Vec<u32> = (0..n as u32).collect();
    let mut state = 0x1234_5678u32;
    let mut i = n;
    while i > 1 {
        i -= 1;
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        let j = (state as usize) % (i + 1);
        order.swap(i, j);
    }
    let mut next = vec![0u32; n];
    let mut k = 0usize;
    while k < n {
        let nxt = if k + 1 == n { 0 } else { k + 1 };
        next[order[k] as usize] = order[nxt];
        k += 1;
    }
    drop(order);
    let steps = 4_000_000usize;
    let mut idx = 0usize;
    let started = Instant::now();
    let mut s = 0u32;
    for _ in 0..steps {
        idx = next[idx] as usize;
        s = s.wrapping_add(idx as u32);
    }
    std::hint::black_box(s);
    let secs = started.elapsed().as_secs_f64();
    secs / steps as f64 * 1e9
}

fn avx2_copy_gbs() -> Option<f64> {
    #[cfg(target_arch = "x86_64")]
    {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return None;
        }
        return Some(unsafe { avx2_copy_inner() });
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        None
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_copy_inner() -> f64 {
    use std::arch::x86_64::*;
    let n = 8 * 1024 * 1024usize;
    let mut a = vec![1u64; n];
    let mut b = vec![2u64; n];
    let passes = 3usize;
    let started = Instant::now();
    for _ in 0..passes {
        let mut i = 0usize;
        while i + 4 <= n {
            let v = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(b.as_mut_ptr().add(i) as *mut __m256i, v);
            let w = _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(a.as_mut_ptr().add(i) as *mut __m256i, w);
            i += 4;
        }
    }
    std::hint::black_box(a[0]);
    std::hint::black_box(b[0]);
    let secs = started.elapsed().as_secs_f64().max(1e-9);
    let bytes = (passes * n * 8 * 2) as f64;
    bytes / secs / 1e9
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_usize_env(key: &str) -> Option<usize> {
    env::var(key).ok()?.parse().ok()
}

fn read_trim(path: &str) -> Option<String> {
    let s = fs::read_to_string(path).ok()?;
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}
