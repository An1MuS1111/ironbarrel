# ironbarrel: A log-structured hash table key-value storage engine

<p align="center">
    <!-- <a href="https://crates.io/crates/ironbarrel-rs"><img src="https://img.shields.io/crates/v/ironbarrel-rs?label=version&style=flat-square"></a> -->
    <!-- &nbsp; -->
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/built_with-Rust-dea584.svg?style=flat-square&logo=rust&logoColor=white"></a>
    &nbsp;
    <!-- <a href="https://github.com/An1MuS1111/ironbarrel/actions"><img src="https://img.shields.io/github/actions/workflow/status/An1MuS1111/ironbarrel/ci.yml?style=flat-square&branch=master"></a> -->
    <!-- &nbsp; -->
    <a href="https://github.com/An1MuS1111/ironbarrel/blob/master/Cargo.toml"><img src="https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-00bfff.svg?style=flat-square"></a>
</p>

ironbarrel is implemented through core concepts of [bitcask](https://riak.com/assets/bitcask-intro.pdf) key/value store architecture.

## Usage

### 1. Basic CRUD Operations

```rust
use ironbarrel::{IronBarrel, Result};

fn main() -> Result<()> {
    // Open a database directory (creates it if it does not exist)
    let db = IronBarrel::open_default("./data/mydb")?;

    // CREATE / UPDATE: Insert key-value pairs (strings, byte slices, numbers)
    db.put("user:1001", "Alice Smith")?;
    db.put(b"user:1002", b"Bob Jones")?;

    // READ: Look up a value by key
    if let Some(val) = db.get("user:1001")? {
        println!("Found user: {}", String::from_utf8_lossy(&val));
    }

    // CHECK: Verify key existence without reading the full payload
    if db.contains("user:1002")? {
        println!("User 1002 exists in the database.");
    }

    // DELETE: Mark a key as deleted (appends a tombstone)
    db.delete("user:1002")?;

    // Database statistics
    let stats = db.stats()?;
    println!("Live keys: {}, Data files: {}", stats.total_keys, stats.total_data_files);

    // Explicitly sync to disk and close (lock released on close or drop)
    db.close()?;
    Ok(())
}
```

### 2. Multi-Threaded Concurrency

`IronBarrel` instances use an internal `Arc` and are cheap to clone across threads. Concurrent readers execute lock-free against the in-memory index; writers serialize appends safely.

```rust
use ironbarrel::{IronBarrel, Result};
use std::thread;

fn main() -> Result<()> {
    let db = IronBarrel::open_default("./data/concurrent_db")?;
    let mut handles = Vec::new();

    // Spawn concurrent reader threads
    for thread_id in 0..4 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let key = format!("item:{}", thread_id);
            if let Ok(Some(val)) = db_clone.get(&key) {
                println!("Thread {} read: {:?}", thread_id, val);
            }
        }));
    }

    // Spawn a writer thread
    let writer_db = db.clone();
    handles.push(thread::spawn(move || {
        for i in 0..100 {
            let _ = writer_db.put(format!("item:{}", i), format!("val_{}", i));
        }
    }));

    for h in handles {
        h.join().unwrap();
    }

    Ok(())
}
```

### 3. Advanced Configuration (Durability & TTL)

Control file roll-over thresholds, sync strategies, and automatic key expiration:

```rust
use ironbarrel::{IronBarrel, Options, Result, SyncStrategy};
use std::time::Duration;

fn main() -> Result<()> {
    let options = Options::new("./data/production_db")
        // Roll over active data file after reaching 64 MB
        .max_file_size(64 * 1024 * 1024)
        // Tune durability: Never, Always, or Interval (fsync periodically)
        .sync_strategy(SyncStrategy::Interval(Duration::from_millis(100)))
        // Automatically hide and evict keys older than 3600 seconds (1 hour)
        .expiry_secs(3600);

    let db = IronBarrel::open(options)?;
    db.put("session:token_xyz", "user_auth_payload")?;

    Ok(())
}
```

### 4. Key-Value Iteration

Iterate through all live key-value records in sorted lexicographical order:

```rust
use ironbarrel::{IronBarrel, Result};

fn main() -> Result<()> {
    let db = IronBarrel::open_default("./data/mydb")?;

    for item in db.iter()? {
        let (key, value) = item?;
        println!(
            "Key: {} => Value: {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    Ok(())
}
```

### 5. Compaction & Garbage Collection (`merge`)

In ironbarrel, updates and deletions append new records to data logs. Call `merge()` to reclaim disk space from obsolete records:

```rust
use ironbarrel::{IronBarrel, Result};

fn main() -> Result<()> {
    let db = IronBarrel::open_default("./data/mydb")?;

    // Compacts immutable data files and writes fresh hint files (.hint)
    let report = db.merge()?;

    println!("Compacted {} input files", report.input_files.len());
    println!("Reclaimed {} bytes of disk space", report.bytes_reclaimed());

    Ok(())
}
```

## Inspiration

* [bitcask](<https://github.com/basho/bitcask>)  A Log-Structured Hash Table for Fast Key/Value Data.
* [Mini-LSM](<https://github.com/skyzh/mini-lsm>)  is a hands-on course in database internals for systems and backend engineers.

## License

The ironbarrel codebase is under [MIT](license-MIT) & [Apache 2.0 license](LICENSE-APACHE).
