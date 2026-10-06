use ironbarrel::{IronBarrel, Options, Result};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn expired_keys_are_hidden_from_get_keys_and_len() -> Result<()> {
    let dir = tempdir()?;
    let db = IronBarrel::open(Options::new(dir.path()).expiry_secs(1))?;
    db.put(b"expires", b"value")?;

    thread::sleep(Duration::from_secs(2));

    assert_eq!(db.get(b"expires")?, None);
    assert!(db.keys()?.is_empty());
    assert_eq!(db.len()?, 0);
    assert!(db.is_empty()?);
    Ok(())
}

#[test]
fn fresh_keys_are_retained_by_merge() -> Result<()> {
    let dir = tempdir()?;
    let db = IronBarrel::open(Options::new(dir.path()).expiry_secs(60).max_file_size(64))?;
    db.put(b"live", b"value")?;
    let report = db.merge()?;

    assert_eq!(db.get(b"live")?, Some(b"value".to_vec()));
    assert!(report.keys_rewritten >= 1);
    Ok(())
}
