//! # Use Case: Multi-Threaded Concurrent Readers and Writers
//!
//! This example demonstrates thread-safe sharing using `ironbarrel`'s cheap `Arc` clone implementation:
//! - Spawning 1 writer thread that appends records sequentially.
//! - Spawning multiple concurrent reader threads that query keys simultaneously.
//! - Showing how readers execute lock-free against `KeyDir` and stateless positional reads (`pread`).

use ironbarrel::{IronBarrel, Result};
use std::thread;

fn main() -> Result<()> {
    let db_path = "./data/concurrent_db";
    let db = IronBarrel::open_default(db_path)?;

    // Pre-populate some keys for readers to query
    for i in 0..1000 {
        db.put(format!("item:{}", i), format!("value_{}", i))?;
    }
    println!("Pre-loaded 1,000 items into the database.");

    let mut handles = Vec::new();

    // Spawn 4 concurrent reader threads
    for thread_id in 0..4 {
        let db_clone = db.clone(); // Cheap Arc clone wrapping IronBarrelInner
        let handle = thread::spawn(move || -> Result<()> {
            for i in 0..250 {
                let key = format!("item:{}", (thread_id * 250 + i) % 1000);
                let _val = db_clone.get(key.as_bytes())?;
            }
            println!("Reader thread {} finished 250 reads.", thread_id);
            Ok(())
        });
        handles.push(handle);
    }

    // Spawn 1 writer thread adding new items concurrently
    let writer_db = db.clone();
    let writer_handle = thread::spawn(move || -> Result<()> {
        for i in 1000..1500 {
            writer_db.put(format!("item:{}", i), format!("new_value_{}", i))?;
        }
        println!("Writer thread finished adding 500 new items.");
        Ok(())
    });
    handles.push(writer_handle);

    // Wait for all threads to complete
    for h in handles {
        h.join().unwrap()?;
    }

    println!("All concurrent worker threads completed successfully.");
    db.close()?;
    Ok(())
}
