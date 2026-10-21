//! Criterion micro-benchmarks for ironbarrel.
//!
//! These cover the pieces where statistical rigor on small deltas matters:
//! record build/CRC/encode, KeyDir operations, and the page-cached read/write
//! paths. Anything involving fsync, merge, or tail latency lives in the macro
//! harness (`examples/ironbarrel_bench.rs`), because Criterion's mean/CI model
//! is the wrong tool for it.
//!
//! ```text
//! Run all:     IRONBARREL_BENCH_DIR=/path/on/real/disk cargo bench --bench micro
//! Filter:      cargo bench --bench micro -- keydir
//! Smoke test:  cargo bench --bench micro -- --test
//! Save:        cargo bench --bench micro -- --save-baseline before
//! Compare:     cargo bench --bench micro -- --baseline before
//! ```
//!
//! The default scratch dir is `std::env::temp_dir()`, which is often tmpfs.
//! Point `IRONBARREL_BENCH_DIR` at the disk you actually care about.
//!
//! Disk-backed fixtures are built lazily, so filtered runs only pay for the
//! benchmarks they select.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ironbarrel::keydir::KeyDir;
use ironbarrel::record::{HEADER_SIZE, IndexEntry, Record};
use ironbarrel::{IronBarrel, Options};
use std::cell::OnceCell;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Once;
use std::time::{Duration, Instant};

const VALUE_SIZES: [usize; 3] = [64, 1024, 16 * 1024];
const KEY_LEN: usize = 16;

/// Target dataset size for the cached-read benchmark.
const READ_DATASET_BYTES: u64 = 128 << 20;
/// Bounds on the number of keys in the cached-read dataset.
const READ_MIN_KEYS: u64 = 1_000;
const READ_MAX_KEYS: u64 = 200_000;

/// Number of distinct keys overwritten by the put benchmark.
const PUT_KEYS: u64 = 10_000;
/// Soft cap on log bytes written per put fixture before it is recycled
/// (outside the timed region). Bounds scratch-disk usage regardless of how
/// many iterations Criterion asks for.
const PUT_DISK_BUDGET: u64 = 512 << 20;

/// xorshift64*: tiny, fast, deterministic. Keeps rand-crate API churn out of the picture.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // xorshift has a fixed point at zero.
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    #[inline]
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[inline]
    fn below(&mut self, n: u64) -> usize {
        (self.next() % n) as usize
    }
}

/// Fixed-width, zero-padded key: `user000000000042`.
fn make_key(id: u64) -> [u8; KEY_LEN] {
    let mut k = *b"user000000000000";
    let mut n = id;
    for i in (4..KEY_LEN).rev() {
        k[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    k
}

/// Pre-built keys `[start, start + n)` so key formatting stays out of timed loops.
fn make_keys(start: u64, n: u64) -> Vec<[u8; KEY_LEN]> {
    (start..start + n).map(make_key).collect()
}

fn scratch(prefix: &str) -> tempfile::TempDir {
    static ANNOUNCE: Once = Once::new();
    let root = std::env::var_os("IRONBARREL_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&root)
        .unwrap_or_else(|e| panic!("cannot create bench dir {}: {e}", root.display()));
    ANNOUNCE.call_once(|| {
        eprintln!(
            "ironbarrel micro: scratch dir = {} (set IRONBARREL_BENCH_DIR to change)",
            root.display()
        );
    });
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(&root)
        .unwrap_or_else(|e| panic!("cannot create temp dir in {}: {e}", root.display()))
}

/// A database living in its own scratch directory; removed on drop.
struct DbFixture {
    db: IronBarrel,
    _dir: tempfile::TempDir,
}

impl DbFixture {
    fn open(prefix: &str) -> Self {
        let dir = scratch(prefix);
        let db = IronBarrel::open(Options::new(dir.path())).expect("open bench db");
        Self { db, _dir: dir }
    }

    fn close(self) {
        self.db.close().expect("close bench db");
    }
}

fn bench_record(c: &mut Criterion) {
    let mut g = c.benchmark_group("record");
    let key = make_key(42);
    for &size in &VALUE_SIZES {
        let value = vec![0xABu8; size];
        g.throughput(Throughput::Bytes((HEADER_SIZE + KEY_LEN + size) as u64));

        // Setup (the clones) is excluded from timing; this measures CRC + header build + encode.
        g.bench_with_input(BenchmarkId::new("build+crc+encode", size), &size, |b, _| {
            b.iter_batched(
                || (key.to_vec(), value.clone()),
                |(k, v)| {
                    let rec = Record::new_standard(k, v).expect("build record");
                    black_box(rec.encode().expect("encode record"))
                },
                BatchSize::SmallInput,
            )
        });

        let rec = Record::new_standard(key.to_vec(), value.clone()).expect("build record");
        g.bench_with_input(BenchmarkId::new("verify_crc", size), &size, |b, _| {
            b.iter(|| black_box(black_box(&rec).verify_crc()))
        });
    }
    g.finish();
}

fn bench_keydir(c: &mut Criterion) {
    const N: u64 = 200_000;
    let entry = |i: u64| IndexEntry {
        file_id: 1,
        offset: i * 100,
        total_sz: 100,
        tstamp: 1,
        is_tombstone: false,
    };

    // Cheap enough (~tens of ms) to build eagerly.
    let hit_keys = make_keys(0, N);
    let miss_keys = make_keys(N, N);
    let kd = KeyDir::new();
    for (i, k) in hit_keys.iter().enumerate() {
        kd.put(k.to_vec(), entry(i as u64));
    }

    let mut g = c.benchmark_group("keydir");
    g.throughput(Throughput::Elements(1));
    let mut rng = Rng::new(0x1234_5678_9abc_def1);

    g.bench_function("get_hit", |b| {
        b.iter(|| black_box(kd.get(&hit_keys[rng.below(N)])))
    });
    g.bench_function("get_miss", |b| {
        b.iter(|| black_box(kd.get(&miss_keys[rng.below(N)])))
    });
    // Includes the key Vec allocation, because KeyDir::put takes ownership of it.
    g.bench_function("put_overwrite", |b| {
        b.iter(|| {
            let i = rng.below(N);
            black_box(kd.put(hit_keys[i].to_vec(), entry(i as u64)))
        })
    });
    g.finish();
}

struct ReadFixture {
    db: DbFixture,
    hit_keys: Vec<[u8; KEY_LEN]>,
    miss_keys: Vec<[u8; KEY_LEN]>,
}

impl ReadFixture {
    fn build(size: usize) -> Self {
        let n = (READ_DATASET_BYTES / (size + HEADER_SIZE + KEY_LEN) as u64)
            .clamp(READ_MIN_KEYS, READ_MAX_KEYS);
        let db = DbFixture::open("ib-read-");
        let value = vec![0x5Au8; size];
        let hit_keys = make_keys(0, n);
        for k in &hit_keys {
            db.db.put(k, &value).expect("load put");
        }
        db.db.sync().expect("load sync");
        // Touch every value once so the measured phase is genuinely page-cache resident.
        for k in &hit_keys {
            black_box(db.db.get(k).expect("warm get"));
        }
        Self {
            db,
            miss_keys: make_keys(n, n),
            hit_keys,
        }
    }
}

/// Full get() path against a pre-populated store. Page-cache resident, so this
/// measures engine overhead (KeyDir lookup, file lock, pread, alloc, CRC),
/// NOT disk speed.
fn bench_db_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("db_read_cached");
    for &size in &VALUE_SIZES {
        let fixture: OnceCell<ReadFixture> = OnceCell::new();
        let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15);

        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("get_hit", size), &size, |b, &size| {
            let f = fixture.get_or_init(|| ReadFixture::build(size));
            let n = f.hit_keys.len() as u64;
            b.iter(|| black_box(f.db.db.get(f.hit_keys[rng.below(n)]).unwrap()))
        });

        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::new("get_miss", size), &size, |b, &size| {
            let f = fixture.get_or_init(|| ReadFixture::build(size));
            let n = f.miss_keys.len() as u64;
            b.iter(|| black_box(f.db.db.get(f.miss_keys[rng.below(n)]).unwrap()))
        });

        if let Some(f) = fixture.into_inner() {
            f.db.close();
        }
    }
    g.finish();
}

/// Single-threaded put with `SyncStrategy::Never`: write(2) + CRC + KeyDir update + locks.
///
/// The log is append-only, so the database is recycled (closed, deleted, rebuilt)
/// outside the timed region every `PUT_DISK_BUDGET` bytes to keep disk usage bounded.
fn bench_db_put(c: &mut Criterion) {
    let mut g = c.benchmark_group("db_put_sync_never");
    g.sample_size(20).measurement_time(Duration::from_secs(5));

    let keys = make_keys(0, PUT_KEYS);
    for &size in &VALUE_SIZES {
        let value = vec![0x5Au8; size];
        let bytes_per_put = (HEADER_SIZE + KEY_LEN + size) as u64;
        let mut rng = Rng::new(0xDEAD_BEEF_CAFE_F00D);
        let mut slot: Option<(DbFixture, u64)> = None;

        let fresh = || {
            let f = DbFixture::open("ib-put-");
            // Pre-populate so every measured put is an overwrite.
            for k in &keys {
                f.db.put(k, &value).expect("load put");
            }
            (f, 0u64)
        };

        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("overwrite_10k_keys", size), &size, |b, _| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                let mut remaining = iters;
                while remaining > 0 {
                    if let Some((f, _)) = slot.take_if(|(_, w)| *w >= PUT_DISK_BUDGET) {
                        f.close();
                    }
                    let (f, written) = slot.get_or_insert_with(fresh);
                    let room = (PUT_DISK_BUDGET - *written).div_ceil(bytes_per_put).max(1);
                    let chunk = remaining.min(room);

                    let start = Instant::now();
                    for _ in 0..chunk {
                        f.db.put(keys[rng.below(PUT_KEYS)], &value).unwrap();
                    }
                    elapsed += start.elapsed();

                    *written += chunk * bytes_per_put;
                    remaining -= chunk;
                }
                elapsed
            })
        });

        if let Some((f, _)) = slot.take() {
            f.close();
        }
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets = bench_record, bench_keydir, bench_db_read, bench_db_put
}
criterion_main!(benches);
