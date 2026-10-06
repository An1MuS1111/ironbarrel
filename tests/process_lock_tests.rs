use ironbarrel::{BarrelError, IronBarrel, Result};
use tempfile::tempdir;

#[test]
fn only_one_read_write_instance_can_hold_the_directory_lock() -> Result<()> {
    let dir = tempdir()?;
    let first = IronBarrel::open_default(dir.path())?;
    let second = IronBarrel::open_default(dir.path());

    assert!(matches!(second, Err(BarrelError::DatabaseLocked(_))));
    first.close()?;

    let reopened = IronBarrel::open_default(dir.path())?;
    reopened.close()?;
    Ok(())
}
