//! # Use Case: Basic CRUD Operations
//!
//! This example demonstrates the fundamental lifecycle of an `ironbarrel` database instance:
//! - Opening a database with default options.
//! - Inserting key-value pairs (`put`).
//! - Retrieving and verifying values (`get`).
//! - Updating existing keys.
//! - Deleting keys using deletion tombstones (`delete`).
//! - Gracefully closing the database instance (`close`).

use ironbarrel::{IronBarrel, Result};

fn main() -> Result<()> {
    // Target directory where ironbarrel will store its .ironbarrel.data and .hint files
    let db_path = "./data/basic_crud";

    // Open the database instance (creates directory and acquires process lock)
    let db = IronBarrel::open_default(db_path)?;
    println!("Database opened successfully at {}", db_path);

    // CREATE (Put)
    db.put(b"user:1001", b"Alice Smith")?;
    db.put(b"user:1002", b"Bob Jones")?;
    println!("Inserted two users.");

    // READ (Get)
    if let Some(val) = db.get(b"user:1001")? {
        println!("Fetched user:1001 -> {}", String::from_utf8_lossy(&val));
    }

    // UPDATE (Put with existing key)
    db.put(b"user:1001", b"Alice Smith-Updated")?;
    let updated = db.get(b"user:1001")?.unwrap();
    println!("Updated user:1001 -> {}", String::from_utf8_lossy(&updated));

    // DELETE (Delete with tombstone)
    db.delete(b"user:1002")?;
    assert!(db.get(b"user:1002")?.is_none());
    println!("Deleted user:1002 (successfully returned None).");

    // Graceful Close (flushes buffers, seals hint files, releases directory lock)
    db.close()?;
    println!("Database closed gracefully.");

    Ok(())
}
