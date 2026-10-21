//! # Use Case: Durability Tuning and Sync Strategies
//!
//! This example demonstrates how to control disk durability:
//! - Using `SyncStrategy::Interval` to periodically fsync in the background/on writes.
//! - Using `SyncStrategy::Never` for maximum ingestion speed, followed by a manual `db.sync()` call.

use ironbarrel::{IronBarrel, Options, Result, SyncStrategy};
use std::time::Duration;

fn main() -> Result<()> {
    let db_path = "./data/durability_db";

    // Configure periodic durability: fsync at most once every 500 milliseconds
    let options =
        Options::new(db_path).sync_strategy(SyncStrategy::Interval(Duration::from_millis(500)));

    let db = IronBarrel::open(options)?;
    println!("Opened database with interval-based durability sync (500ms).");

    // Perform high-frequency writes
    for i in 0..5000 {
        db.put(format!("log_event:{}", i), b"transaction_committed")?;
    }
    println!("Wrote 5,000 log events with interval sync.");

    // Alternatively, if using SyncStrategy::Never for bulk ingestion,
    // you can force an explicit manual flush to disk when a batch completes:
    db.sync()?;
    println!("Manually forced fsync on active data file.");

    db.close()?;
    Ok(())
}
