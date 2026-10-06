use ironbarrel::{BarrelError, IronBarrel, Options, Result, SyncStrategy};
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn invalid_options_are_rejected_before_opening() {
    let dir = tempdir().unwrap();

    let zero_file_size = IronBarrel::open(Options::new(dir.path()).max_file_size(0));
    assert!(matches!(
        zero_file_size,
        Err(BarrelError::InvalidConfiguration(_))
    ));

    let zero_key_size = IronBarrel::open(Options::new(dir.path()).max_key_size(0));
    assert!(matches!(
        zero_key_size,
        Err(BarrelError::InvalidConfiguration(_))
    ));
}

#[test]
fn all_sync_strategies_are_accepted() -> Result<()> {
    let dir = tempdir()?;
    for (name, strategy) in [
        ("never", SyncStrategy::Never),
        ("always", SyncStrategy::Always),
        ("interval", SyncStrategy::Interval(Duration::from_millis(1))),
    ] {
        let path = dir.path().join(name);
        let db = IronBarrel::open(Options::new(&path).sync_strategy(strategy))?;
        db.put(b"key", b"value")?;
        db.sync()?;
        db.close()?;
    }
    Ok(())
}

#[test]
fn configured_key_limit_applies_to_delete_as_well_as_put() -> Result<()> {
    let dir = tempdir()?;
    let db = IronBarrel::open(Options::new(dir.path()).max_key_size(3))?;
    assert!(matches!(
        db.delete(b"long-key"),
        Err(BarrelError::ExceedsSizeLimit(8))
    ));
    Ok(())
}
