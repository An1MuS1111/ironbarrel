//! # Use Case: TTL Expiration and File Rollover Configuration
//!
//! This example demonstrates advanced configuration options:
//! - Setting a custom active file size limit (`max_file_size`) to trigger automatic rollovers.
//! - Configuring a global key expiration window (`expiry_secs`) so old keys are ignored or evicted.
//! - Checking database statistics (`stats()`) like live bytes versus total disk space.

use ironbarrel::{IronBarrel, Options, Result};
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    let db_path = "./data/ttl_example";

    // Configure custom engine parameters
    let options = Options::new(db_path)
        .max_file_size(4 * 1024 * 1024) // Roll over active file every 4 MB
        .expiry_secs(2); // Keys expire after 2 seconds

    let db = IronBarrel::open(options)?;
    println!("Configured database opened with 2-second TTL expiration.");

    // Insert a record
    db.put(b"session:token_xyz", b"user_auth_data_123")?;
    println!("Inserted session token.");

    // Immediate read works
    let immediate = db.get(b"session:token_xyz")?;
    println!("Immediate read: found = {}", immediate.is_some());

    // Wait for TTL expiration window to pass
    println!("Waiting 3 seconds for TTL expiration...");
    thread::sleep(Duration::from_secs(3));

    // Read after expiration returns None and triggers atomic eviction
    let expired = db.get(b"session:token_xyz")?;
    println!(
        "Read after TTL: found = {} (expected false)",
        expired.is_some()
    );

    // Inspect database stats
    let stats = db.stats()?;
    println!(
        "Database Stats -> Total Keys: {}, Active File ID: {}",
        stats.total_keys, stats.active_file_id
    );

    db.close()?;
    Ok(())
}
