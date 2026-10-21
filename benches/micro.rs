//! Criterion micro-benchmarks for ironbarrel.
//!
//! These cover the pieces where statistical rigor on small deltas matters:
//! record build/CRC/encode, KeyDir operations, and the page-cached read path.
//! Anything involving fsync, merge, or tail latency lives in the macro harness
//! (`examples/ironbarrel_bench.rs`), because Criterion's mean/CI model is the
//! wrong tool for it.
//!
//! Run:   IRONBARREL_BENCH_DIR=/path/on/real/disk cargo bench --bench micro
//! Save:  cargo bench --bench micro -- --save-baseline before
//! Diff:  cargo bench --bench micro -- --baseline before
//!
//! NOTE: the default scratch dir is `std::env::temp_dir()`. Point
//! IRONBARREL_BENCH_DIR at the disk you actually care about.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use ironbarrel::keydir::KeyDir;
use ironbarrel::record::{IndexEntry, Record, HEADER_SIZE};
use ironbarrel::{IronBarrel, Options};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

const VALUE_SIZES: [usize; 3] = [64, 1024, 16 * 1024];
const KEY_LEN: usize = 16;

/// xorshift64*: tiny, fast, deterministic. Keeps rand-crate API churn out of the picture.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
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

fn scratch(prefix: &str) -> tempfile::TempDir {
    let root = std::env::var_os("IRONBARREL_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .unwrap()
}

fn bench_record(c: &mut Criterion) {
    let mut g = c.benchmark_group("record");
    for &size in &VALUE_SIZES {
        let key = make_key(42);
        let value = vec![0xABu8; size];
        g.throughput(Throughput::Bytes((HEADER_SIZE + KEY_LEN + size) as u64));

        // Setup (the clones) is excluded from timing; this measures CRC + header build + encode.
        g.bench_with_input(BenchmarkId::new("build+crc+encode", size), &size, |b, _| {
            b.iter_batched(
                || (key.to_vec(), value.clone()),
                |(k, v)| {
                    let rec = Record::new_standard(k, v).unwrap();
                    black_box(rec.encode().unwrap())
                },
                BatchSize::SmallInput,
            )
        });

        let rec = Record::new_standard(key.to_vec(), value.clone()).unwrap();
        g.bench_with_input(BenchmarkId::new("verify_crc", size), &size, |b, _| {
            b.iter(|| black_box(rec.verify_crc()))
        });
    }
    g.finish();
}

fn bench_keydir(c: &mut Criterion) {
    const N: u64 = 200_000;
    let kd = KeyDir::new();
    let entry = |i: u64| IndexEntry {
        file_id: 1,
        offset: i * 100,
        total_sz: 100,
        tstamp: 1,
        is_tombstone: false,
    };
    for i in 0..N {
        kd.put(make_key(i).to_vec(), entry(i));
    }

    let mut g = c.benchmark_group("keydir");
    let mut rng = Rng(0x1234_5678_9abc_def1);
    g.bench_function("get_hit", |b| {
        b.iter(|| black_box(kd.get(&make_key(rng.next() % N))))
    });
    g.bench_function("get_miss", |b| {
        b.iter(|| black_box(kd.get(&make_key(N + rng.next() % N))))
    });
    g.bench_function("put_overwrite", |b| {
        b.iter(|| {
            let i = rng.next() % N;
            black_box(kd.put(make_key(i).to_vec(), entry(i)))
        })
    });
    g.finish();
}

/// Full get() path against a pre-populated store. Mostly page-cache hits, so this
/// measures engine overhead (lock, pread, alloc, CRC, copy), NOT disk speed.
fn bench_db_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("db_read_cached");
    for &size in &VALUE_SIZES {
        // ~128 MiB dataset, capped so small values don't take forever to load.
        let n = ((128u64 << 20) / (size as u64 + 32)).clamp(1_000, 200_000);
        let dir = scratch("ib-read-");
        let db = IronBarrel::open(Options::new(dir.path())).unwrap();
        let value = vec![0x5Au8; size];
        for i in 0..n {
            db.put(make_key(i), &value).unwrap();
        }
        db.sync().unwrap();

        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("get_hit", size), &size, |b, _| {
            b.iter(|| black_box(db.get(make_key(rng.next() % n)).unwrap()))
        });
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::new("get_miss", size), &size, |b, _| {
            b.iter(|| black_box(db.get(make_key(n + rng.next() % n)).unwrap()))
        });
        db.close().unwrap();
    }
    g.finish();
}

/// Single-threaded put with SyncStrategy::Never: write(2) + CRC + KeyDir update + lock.
/// The log grows for the whole run (hundreds of MB at small values), hence the short window.
fn bench_db_put(c: &mut Criterion) {
    let mut g = c.benchmark_group("db_put_sync_never");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for &size in &VALUE_SIZES {
        let dir = scratch("ib-put-");
        let db = IronBarrel::open(Options::new(dir.path())).unwrap();
        let value = vec![0x5Au8; size];
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("overwrite_10k_keys", size), &size, |b, _| {
            b.iter(|| db.put(make_key(rng.next() % 10_000), &value).unwrap())
        });
        db.close().unwrap();
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
