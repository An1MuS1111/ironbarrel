//! ironbarrel macro-benchmark: YCSB-style workloads with HDR latency histograms.
//!
//!   cargo run --release --example ironbarrel_bench -- --dir /mnt/nvme/bench --workload a
//!
//! MODES
//!   --mode run       (default) load keys, reopen, then run a timed workload
//!   --mode recover   build a dataset, then time cold-process open() with and without hint files
//!
//! WORKLOADS (--workload, or --read-pct N for a custom read/update split)
//!   a  50% read / 50% update      b  95% read / 5% update     c  100% read
//!   f  50% read / 50% read-modify-write                       w  100% update
//!
//! KEY FLAGS (defaults in brackets)
//!   --dir PATH            REQUIRED. Must be on the disk you want to measure (not tmpfs).
//!   --keys N [1000000]    --value-size B [256]    --threads N [4]
//!   --duration S [30]     --warmup S [5]          --dist zipfian|uniform [zipfian]
//!   --sync never|always|interval:<ms> [never]
//!   --target-ops N [0]    >0 = open-loop at N total ops/s (latency measured from the INTENDED
//!                         start time, so queueing delay is not hidden: no coordinated omission).
//!                         0 = closed-loop / max throughput (tail latencies are optimistic).
//!   --merge-every S [0]   run db.merge() every S seconds during the measured window
//!   --auto-merge-mb N [0] enable auto_merge with this garbage threshold
//!   --max-file-mb N [32]  --seed N  --churn N [2] (recover mode)
//!   --cold                pause so you can drop the page cache before the measured phase
//!   --keep                do not delete the run directory afterwards

use hdrhistogram::Histogram;
use ironbarrel::hint_file::HINT_FILE_SUFFIX;
use ironbarrel::{IronBarrel, Options, SyncStrategy};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

const KEY_LEN: usize = 16;
const READ: usize = 0;
const UPDATE: usize = 1;
const RMW: usize = 2;
const OP_NAMES: [&str; 3] = ["read", "update", "rmw"];

fn die(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2)
}

struct Args {
    map: HashMap<String, String>,
}

impl Args {
    fn parse() -> Self {
        let mut map = HashMap::new();
        let mut it = std::env::args().skip(1).peekable();
        while let Some(arg) = it.next() {
            let Some(name) = arg.strip_prefix("--") else {
                die(&format!("unexpected argument: {arg}"))
            };
            let takes_value = it.peek().is_some_and(|v| !v.starts_with("--"));
            let value = if takes_value {
                it.next().unwrap()
            } else {
                "true".to_string()
            };
            map.insert(name.to_string(), value);
        }
        Self { map }
    }
    fn get<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        match self.map.get(name) {
            Some(v) => v
                .parse::<T>()
                .unwrap_or_else(|_| die(&format!("bad value for --{name}: {v}"))),
            None => default,
        }
    }
    fn string(&self, name: &str, default: &str) -> String {
        self.map
            .get(name)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
    fn flag(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }
}

fn parse_sync(s: &str) -> SyncStrategy {
    match s {
        "never" => SyncStrategy::Never,
        "always" => SyncStrategy::Always,
        _ => match s
            .strip_prefix("interval:")
            .and_then(|ms| ms.parse::<u64>().ok())
        {
            Some(ms) => SyncStrategy::Interval(Duration::from_millis(ms)),
            None => die("--sync must be never | always | interval:<ms>"),
        },
    }
}

fn sync_label(s: SyncStrategy) -> String {
    match s {
        SyncStrategy::Never => "never".into(),
        SyncStrategy::Always => "always".into(),
        SyncStrategy::Interval(d) => format!("interval:{}", d.as_millis()),
    }
}

struct Cfg {
    run_dir: PathBuf,
    keys: u64,
    value_size: usize,
    threads: usize,
    duration: Duration,
    warmup: Duration,
    sync: SyncStrategy,
    max_file_bytes: u64,
    target_ops: u64,
    zipf: bool,
    merge_every: Duration,
    auto_merge_mb: u64,
    seed: u64,
    cold: bool,
}

fn parse_cfg(a: &Args, run_dir: PathBuf) -> Cfg {
    let value_size: usize = a.get("value-size", 256);
    if value_size < 8 {
        die("--value-size must be >= 8");
    }
    Cfg {
        run_dir,
        keys: a.get::<u64>("keys", 1_000_000).max(2),
        value_size,
        threads: a.get::<usize>("threads", 4).max(1),
        duration: Duration::from_secs_f64(a.get("duration", 30.0)),
        warmup: Duration::from_secs_f64(a.get("warmup", 5.0)),
        sync: parse_sync(&a.string("sync", "never")),
        max_file_bytes: a.get::<u64>("max-file-mb", 32) * 1024 * 1024,
        target_ops: a.get("target-ops", 0),
        zipf: match a.string("dist", "zipfian").as_str() {
            "zipfian" => true,
            "uniform" => false,
            _ => die("--dist must be zipfian|uniform"),
        },
        merge_every: Duration::from_secs_f64(a.get("merge-every", 0.0)),
        auto_merge_mb: a.get("auto-merge-mb", 0),
        seed: a.get("seed", 0xC0FFEE),
        cold: a.flag("cold"),
    }
}

fn options(cfg: &Cfg, sync: SyncStrategy, allow_auto_merge: bool) -> Options {
    let mut o = Options::new(&cfg.run_dir)
        .max_file_size(cfg.max_file_bytes)
        .sync_strategy(sync);
    if allow_auto_merge && cfg.auto_merge_mb > 0 {
        o = o
            .auto_merge(true)
            .merge_threshold_bytes(cfg.auto_merge_mb * 1024 * 1024);
    }
    o
}

//  RNG, keys, Zipfian

/// splitmix64: good statistical quality, no dependencies.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64, theta: f64) -> Self {
        let zeta = |n: u64| -> f64 { (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum() };
        let zetan = zeta(n);
        let zeta2 = zeta(2);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self {
            n,
            theta,
            alpha,
            zetan,
            eta,
        }
    }
    fn rank(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let r = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        r.min(self.n - 1)
    }
}

/// Scramble ranks so hot keys are spread over the keyspace instead of clustered at id 0.
fn fnv64(mut x: u64) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for _ in 0..8 {
        h ^= x & 0xff;
        h = h.wrapping_mul(0x0100_0000_01b3);
        x >>= 8;
    }
    h
}

fn make_key(id: u64) -> [u8; KEY_LEN] {
    let mut k = *b"user000000000000";
    let mut n = id;
    for i in (4..KEY_LEN).rev() {
        k[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    k
}

fn new_hist() -> Histogram<u64> {
    // 1 ns .. 60 s, 3 significant digits
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

struct Acc {
    hist: [Histogram<u64>; 3],
    misses: u64,
    errors: u64,
    first_err: Option<String>,
}
impl Acc {
    fn new() -> Self {
        Self {
            hist: [new_hist(), new_hist(), new_hist()],
            misses: 0,
            errors: 0,
            first_err: None,
        }
    }
    fn fail(&mut self, e: String) {
        self.errors += 1;
        if self.first_err.is_none() {
            self.first_err = Some(e);
        }
    }
    fn absorb(&mut self, o: &Acc) {
        for k in 0..3 {
            self.hist[k].add(&o.hist[k]).unwrap();
        }
        self.misses += o.misses;
        self.errors += o.errors;
        if self.first_err.is_none() {
            self.first_err = o.first_err.clone();
        }
    }
}

struct MergeRec {
    secs: f64,
    input_files: usize,
    reclaimed: u64,
}

fn window(cfg: &Cfg, t0: Instant) -> (Instant, Instant) {
    let from = t0 + cfg.warmup;
    (from, from + cfg.duration)
}

fn wait_until(t: Instant) {
    loop {
        let now = Instant::now();
        if now >= t {
            return;
        }
        let rem = t - now;
        if rem > Duration::from_micros(200) {
            std::thread::sleep(rem - Duration::from_micros(100));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn proc_kb(field: &str) -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// Bytes passed to write(2) by this process (engine-level write volume, incl. hint files).
fn proc_wchar() -> u64 {
    fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("wchar:")
                    .and_then(|v| v.trim().parse::<u64>().ok())
            })
        })
        .unwrap_or(0)
}

fn fs_type(path: &Path) -> String {
    let p = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let mounts = fs::read_to_string("/proc/mounts").unwrap_or_default();
    mounts
        .lines()
        .filter_map(|l| {
            let mut f = l.split_whitespace();
            let (_dev, mp, ty) = (f.next()?, f.next()?, f.next()?);
            p.starts_with(mp).then(|| (mp.len(), ty.to_string()))
        })
        .max_by_key(|(n, _)| *n)
        .map(|(_, t)| t)
        .unwrap_or_else(|| "unknown".into())
}

fn pause_for_cache_drop() {
    println!("\n>> Drop the page cache now:  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches");
    println!(">> then press Enter to continue");
    let mut s = String::new();
    let _ = io::stdin().read_line(&mut s);
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1000.0
}

#[derive(Clone, Copy)]
struct Mix {
    read: u32,
    update: u32, // remainder (100 - read - update) is read-modify-write
}

fn do_get(db: &IronBarrel, key: &[u8; KEY_LEN], expect_len: usize, acc: &mut Acc) {
    match db.get(key) {
        Ok(Some(v)) if v.len() == expect_len => {}
        Ok(Some(_)) => acc.fail("value length mismatch".into()),
        Ok(None) => acc.misses += 1,
        Err(e) => acc.fail(e.to_string()),
    }
}

fn do_put(db: &IronBarrel, key: &[u8; KEY_LEN], val: &[u8], acc: &mut Acc) {
    if let Err(e) = db.put(key, val) {
        acc.fail(e.to_string());
    }
}

fn worker(
    db: &IronBarrel,
    cfg: &Cfg,
    mix: Mix,
    zipf: Option<&Zipf>,
    tid: usize,
    t0: Instant,
) -> Acc {
    let (from, end) = window(cfg, t0);
    let mut rng = Rng(cfg.seed ^ (tid as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut acc = Acc::new();
    let mut val = vec![0u8; cfg.value_size];
    for b in val.iter_mut() {
        *b = rng.next_u64() as u8;
    }
    // Open loop: each thread owns an equal slice of the target rate.
    let interval = (cfg.target_ops > 0)
        .then(|| Duration::from_secs_f64(cfg.threads as f64 / cfg.target_ops as f64));
    let mut next = t0;
    let mut counter = (tid as u64) << 48;

    loop {
        let intended = match interval {
            Some(iv) => {
                let t = next;
                next += iv;
                wait_until(t);
                t
            }
            None => Instant::now(),
        };
        if intended >= end {
            break;
        }

        let roll = (rng.next_u64() % 100) as u32;
        let id = match zipf {
            Some(z) => fnv64(z.rank(rng.next_f64())) % cfg.keys,
            None => rng.next_u64() % cfg.keys,
        };
        let key = make_key(id);
        let kind = if roll < mix.read {
            READ
        } else if roll < mix.read + mix.update {
            UPDATE
        } else {
            RMW
        };

        match kind {
            READ => do_get(db, &key, cfg.value_size, &mut acc),
            UPDATE => {
                counter += 1;
                val[..8].copy_from_slice(&counter.to_le_bytes());
                do_put(db, &key, &val, &mut acc);
            }
            _ => {
                do_get(db, &key, cfg.value_size, &mut acc);
                counter += 1;
                val[..8].copy_from_slice(&counter.to_le_bytes());
                do_put(db, &key, &val, &mut acc);
            }
        }

        // Latency is measured from the INTENDED start, so a stalled system is charged for
        // the time requests would have spent queued (coordinated-omission correction).
        if intended >= from {
            let ns = intended.elapsed().as_nanos() as u64;
            acc.hist[kind].saturating_record(ns.max(1));
        }
    }
    acc
}

fn merge_loop(db: &IronBarrel, cfg: &Cfg, t0: Instant) -> Vec<MergeRec> {
    let (from, end) = window(cfg, t0);
    let mut out = Vec::new();
    let mut next = from + cfg.merge_every;
    while next < end {
        wait_until(next);
        let t = Instant::now();
        match db.merge() {
            Ok(r) => out.push(MergeRec {
                secs: t.elapsed().as_secs_f64(),
                input_files: r.input_files.len(),
                reclaimed: r.bytes_reclaimed(),
            }),
            Err(e) => eprintln!("merge failed: {e}"),
        }
        next += cfg.merge_every;
    }
    out
}

/// Sequential load with SyncStrategy::Never (we measure the workload's sync cost, not the load's).
fn load(cfg: &Cfg, passes: u64) -> Res<(Duration, ironbarrel::Stats)> {
    let db = IronBarrel::open(options(cfg, SyncStrategy::Never, false))?;
    let mut rng = Rng(cfg.seed);
    let val: Vec<u8> = (0..cfg.value_size).map(|_| rng.next_u64() as u8).collect();
    let t = Instant::now();
    for _ in 0..passes {
        for id in 0..cfg.keys {
            let k = make_key(id);
            db.put(k, &val)?;
        }
    }
    db.sync()?;
    let took = t.elapsed();
    let stats = db.stats()?;
    db.close()?;
    Ok((took, stats))
}

fn print_table(agg: &Acc, secs: f64) {
    println!(
        "{:<7}{:>11}{:>12}{:>10}{:>10}{:>10}{:>10}{:>11}{:>11}   (latency in µs)",
        "op", "count", "ops/s", "mean", "p50", "p99", "p99.9", "p99.99", "max"
    );
    for (k, h) in agg.hist.iter().enumerate() {
        if h.is_empty() {
            continue;
        }
        println!(
            "{:<7}{:>11}{:>12.0}{:>10.1}{:>10.1}{:>10.1}{:>10.1}{:>11.1}{:>11.1}",
            OP_NAMES[k],
            h.len(),
            h.len() as f64 / secs,
            h.mean() / 1e3,
            us(h.value_at_quantile(0.50)),
            us(h.value_at_quantile(0.99)),
            us(h.value_at_quantile(0.999)),
            us(h.value_at_quantile(0.9999)),
            us(h.max()),
        );
    }
}

fn run_workload(a: &Args, cfg: &Cfg) -> Res<()> {
    let name = a.string("workload", "a");
    let (label, mix) = match a.map.get("read-pct") {
        Some(p) => {
            let r: u32 = p
                .parse()
                .unwrap_or_else(|_| die("--read-pct must be an integer 0..=100"));
            if r > 100 {
                die("--read-pct must be 0..=100");
            }
            (
                format!("r{r}"),
                Mix {
                    read: r,
                    update: 100 - r,
                },
            )
        }
        None => {
            let m = match name.as_str() {
                "a" => Mix {
                    read: 50,
                    update: 50,
                },
                "b" => Mix {
                    read: 95,
                    update: 5,
                },
                "c" => Mix {
                    read: 100,
                    update: 0,
                },
                "f" => Mix {
                    read: 50,
                    update: 0,
                },
                "w" => Mix {
                    read: 0,
                    update: 100,
                },
                _ => die("--workload must be one of a|b|c|f|w (or use --read-pct N)"),
            };
            (name.clone(), m)
        }
    };

    println!("loading {} keys ...", cfg.keys);
    let (load_time, _) = load(cfg, 1)?;
    println!(
        "  load: {:.2}s ({:.0} puts/s, sync=never)",
        load_time.as_secs_f64(),
        cfg.keys as f64 / load_time.as_secs_f64()
    );
    if cfg.cold {
        pause_for_cache_drop();
    }

    let t = Instant::now();
    let db = IronBarrel::open(options(cfg, cfg.sync, true))?;
    println!("  open: {:.3}s", t.elapsed().as_secs_f64());

    let zipf = cfg.zipf.then(|| Zipf::new(cfg.keys, 0.99));
    let t0 = Instant::now() + Duration::from_millis(100);
    let (from, end) = window(cfg, t0);

    let (agg, merges, wchar) = std::thread::scope(|s| {
        let dbr = &db;
        let zr = zipf.as_ref();
        let workers: Vec<_> = (0..cfg.threads)
            .map(|tid| s.spawn(move || worker(dbr, cfg, mix, zr, tid, t0)))
            .collect();
        let merger =
            (!cfg.merge_every.is_zero()).then(|| s.spawn(move || merge_loop(dbr, cfg, t0)));

        wait_until(from);
        let w0 = proc_wchar();
        wait_until(end);
        let w1 = proc_wchar();

        let mut agg = Acc::new();
        for w in workers {
            agg.absorb(&w.join().unwrap());
        }
        let merges = merger.map(|m| m.join().unwrap()).unwrap_or_default();
        (agg, merges, w1.saturating_sub(w0))
    });

    let secs = cfg.duration.as_secs_f64();
    println!();
    print_table(&agg, secs);

    if agg.errors > 0 || agg.misses > 0 {
        println!(
            "\n!! errors={} misses={} first_error={:?}  (misses should be 0: every key was preloaded)",
            agg.errors, agg.misses, agg.first_err
        );
    }
    for (i, m) in merges.iter().enumerate() {
        println!(
            "merge #{i}: {:.2}s, {} input files, {:.1} MiB reclaimed",
            m.secs,
            m.input_files,
            m.reclaimed as f64 / 1048576.0
        );
    }

    let written_ops = agg.hist[UPDATE].len() + agg.hist[RMW].len();
    let user_bytes = written_ops * (KEY_LEN + cfg.value_size) as u64;
    if user_bytes > 0 && wchar > 0 {
        println!(
            "\nwrite amplification (write(2) bytes / user key+value bytes): {:.2}x \
             [baseline from the 15 B header alone is {:.2}x; includes merge rewrites and hint files]",
            wchar as f64 / user_bytes as f64,
            (15 + KEY_LEN + cfg.value_size) as f64 / (KEY_LEN + cfg.value_size) as f64
        );
    }

    let st = db.stats()?; // O(n): call once, after the measured window
    println!(
        "disk: {} files, {:.1} MiB on disk, {:.1} MiB live => space amplification {:.2}x",
        st.total_data_files,
        st.total_disk_bytes as f64 / 1048576.0,
        st.live_data_bytes as f64 / 1048576.0,
        st.total_disk_bytes as f64 / st.live_data_bytes.max(1) as f64
    );
    println!(
        "process: VmRSS={} MiB, VmHWM={} MiB",
        proc_kb("VmRSS:") / 1024,
        proc_kb("VmHWM:") / 1024
    );
    db.close()?;

    let total_ops: u64 = agg.hist.iter().map(|h| h.len()).sum();
    let p99 = |k: usize| us(agg.hist[k].value_at_quantile(0.99));
    println!(
        "\nRESULT workload={label} threads={} keys={} value={} dist={} sync={} target_ops={} merge_every={:?} \
         ops_s={:.0} read_p99_us={:.1} update_p99_us={:.1} rmw_p99_us={:.1} errors={}",
        cfg.threads,
        cfg.keys,
        cfg.value_size,
        if cfg.zipf { "zipfian" } else { "uniform" },
        sync_label(cfg.sync),
        cfg.target_ops,
        cfg.merge_every,
        total_ops as f64 / secs,
        p99(READ),
        p99(UPDATE),
        p99(RMW),
        agg.errors
    );
    Ok(())
}

/// Child process: opens the existing dir exactly once so RSS and open time are clean numbers.
fn child_open(a: &Args) -> Res<()> {
    let dir = PathBuf::from(a.string("run-dir", ""));
    let rss_before = proc_kb("VmRSS:");
    let t = Instant::now();
    let db = IronBarrel::open(Options::new(&dir))?;
    let secs = t.elapsed().as_secs_f64();
    let (rss_after, peak) = (proc_kb("VmRSS:"), proc_kb("VmHWM:")); // before len(): it clones every key
    let keys = db.len()?;
    let delta_kb = rss_after.saturating_sub(rss_before);
    println!(
        "open={secs:.3}s keys={keys} keydir_rss={:.1}MiB (~{} B/key) peak_rss={:.1}MiB",
        delta_kb as f64 / 1024.0,
        delta_kb * 1024 / keys.max(1) as u64,
        peak as f64 / 1024.0
    );
    db.close()?;
    Ok(())
}

fn run_recover(a: &Args, cfg: &Cfg) -> Res<()> {
    let churn: u64 = a.get("churn", 2);
    println!(
        "building dataset: {} keys x {churn} passes of {} B values ...",
        cfg.keys, cfg.value_size
    );
    let (took, st) = load(cfg, churn)?;
    println!(
        "  built in {:.1}s: {} files, {:.1} MiB on disk, {:.1} MiB live",
        took.as_secs_f64(),
        st.total_data_files,
        st.total_disk_bytes as f64 / 1048576.0,
        st.live_data_bytes as f64 / 1048576.0
    );

    let exe = std::env::current_exe()?;
    for (label, strip_hints) in [
        ("with hint files   ", false),
        ("WITHOUT hints (scan)", true),
    ] {
        if strip_hints {
            for e in fs::read_dir(&cfg.run_dir)? {
                let p = e?.path();
                let is_hint = p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(HINT_FILE_SUFFIX));
                if is_hint {
                    fs::remove_file(p)?;
                }
            }
        }
        if cfg.cold {
            pause_for_cache_drop();
        }
        let out = Command::new(&exe)
            .args(["--mode", "_open", "--run-dir"])
            .arg(&cfg.run_dir)
            .output()?;
        println!("{label}: {}", String::from_utf8_lossy(&out.stdout).trim());
        if !out.status.success() {
            eprintln!("child failed: {}", String::from_utf8_lossy(&out.stderr));
        }
    }
    Ok(())
}

fn main() {
    let a = Args::parse();
    let mode = a.string("mode", "run");
    let result = match mode.as_str() {
        "_open" => child_open(&a),
        "run" | "recover" => {
            let base = a.string("dir", "");
            if base.is_empty() {
                die("--dir <path on the disk you want to measure> is required");
            }
            let base = PathBuf::from(base);
            fs::create_dir_all(&base).unwrap_or_else(|e| die(&format!("cannot create --dir: {e}")));
            // Always work in our own subdirectory so cleanup can never touch the user's data.
            let run_dir = base.join(format!("ironbarrel-bench-{}", std::process::id()));
            fs::create_dir_all(&run_dir)
                .unwrap_or_else(|e| die(&format!("cannot create run dir: {e}")));

            let cfg = parse_cfg(&a, run_dir.clone());
            let fst = fs_type(&base);
            if matches!(fst.as_str(), "tmpfs" | "ramfs") {
                eprintln!(
                    "WARNING: --dir is on {fst}; fsync is free there and results say nothing about disk behavior."
                );
            }
            println!(
                "ironbarrel bench | mode={mode} fs={fst} dir={} | threads={} keys={} value={}B dist={} sync={} \
                 target_ops={} warmup={:?} duration={:?} merge_every={:?} auto_merge_mb={} max_file_mb={} cores={}",
                base.display(),
                cfg.threads,
                cfg.keys,
                cfg.value_size,
                if cfg.zipf { "zipfian" } else { "uniform" },
                sync_label(cfg.sync),
                cfg.target_ops,
                cfg.warmup,
                cfg.duration,
                cfg.merge_every,
                cfg.auto_merge_mb,
                cfg.max_file_bytes >> 20,
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(0),
            );

            let r = if mode == "run" {
                run_workload(&a, &cfg)
            } else {
                run_recover(&a, &cfg)
            };
            if !a.flag("keep") {
                let _ = fs::remove_dir_all(&run_dir);
            }
            r
        }
        other => die(&format!("unknown --mode {other}")),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
