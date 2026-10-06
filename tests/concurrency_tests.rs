use ironbarrel::{IronBarrel, Result};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_concurrent_reads_and_writes() -> Result<()> {
    let dir = tempdir()?;
    let db = IronBarrel::open_default(dir.path())?;

    // Seed initial keys
    for i in 0..100 {
        db.put(
            format!("key_{}", i).as_bytes(),
            format!("val_{}", i).as_bytes(),
        )?;
    }

    let mut handles = Vec::new();

    // Spawn 5 writer threads
    for t in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let k = format!("concurrent_t{}_k{}", t, i);
                let v = format!("concurrent_t{}_v{}", t, i);
                db_clone.put(k.as_bytes(), v.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    // Spawn 5 reader threads
    for _ in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let k = format!("key_{}", i);
                let val = db_clone.get(k.as_bytes()).unwrap();
                assert!(val.is_some());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(db.len()?, 600); // 100 initial + 5 * 100 concurrent

    db.close()?;
    Ok(())
}

#[test]
fn test_concurrent_updates_to_one_key_preserve_a_complete_value() -> Result<()> {
    let dir = tempdir()?;
    let db = IronBarrel::open_default(dir.path())?;
    let mut handles = Vec::new();

    for thread_id in 0..8 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for iteration in 0..100 {
                let value = format!("thread-{thread_id}-iteration-{iteration}");
                db.put(b"shared", value.as_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let value = db.get(b"shared")?.expect("shared key should exist");
    let value = String::from_utf8(value).expect("value should be UTF-8");
    assert!(value.starts_with("thread-"));
    assert!(value.contains("-iteration-"));
    Ok(())
}
