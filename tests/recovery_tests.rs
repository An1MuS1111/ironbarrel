use ironbarrel::{IronBarrel, Options, Result};
use tempfile::tempdir;

#[test]
fn test_reopen_database_recovery() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_path_buf();

    {
        let db = IronBarrel::open_default(&path)?;
        db.put(b"user:1", b"Alice")?;
        db.put(b"user:2", b"Bob")?;
        db.put(b"user:3", b"Charlie")?;
        db.delete(b"user:2")?;
        db.close()?;
    }

    // Re-open database
    {
        let db = IronBarrel::open_default(&path)?;
        assert_eq!(db.get(b"user:1")?, Some(b"Alice".to_vec()));
        assert_eq!(db.get(b"user:2")?, None);
        assert_eq!(db.get(b"user:3")?, Some(b"Charlie".to_vec()));
        assert_eq!(db.len()?, 2);
        db.close()?;
    }

    Ok(())
}

#[test]
fn test_active_file_rollover_recovery() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_path_buf();

    // Set small max file size to trigger multiple rollover files
    let opts = Options::new(&path).max_file_size(100);

    {
        let db = IronBarrel::open(opts.clone())?;
        for i in 0..50 {
            let k = format!("key_{:02}", i);
            let v = format!("value_{:04}", i);
            db.put(k.as_bytes(), v.as_bytes())?;
        }

        let stats = db.stats()?;
        assert!(stats.total_data_files > 1);
        db.close()?;
    }

    // Re-open database and verify all 50 keys recovered across files
    {
        let db = IronBarrel::open(opts)?;
        assert_eq!(db.len()?, 50);
        for i in 0..50 {
            let k = format!("key_{:02}", i);
            let v = format!("value_{:04}", i);
            assert_eq!(db.get(k.as_bytes())?, Some(v.as_bytes().to_vec()));
        }
        db.close()?;
    }

    Ok(())
}

#[test]
fn test_overwrite_recovery_with_targeted_tombstones() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_path_buf();
    let opts = Options::new(&path).max_file_size(64);

    {
        let db = IronBarrel::open(opts.clone())?;
        db.put(b"key", b"first")?;
        db.put(b"key", b"second")?;
        db.close()?;
    }

    let db = IronBarrel::open(opts)?;
    assert_eq!(db.get(b"key")?, Some(b"second".to_vec()));
    Ok(())
}
