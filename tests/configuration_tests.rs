use ironbarrel::{BarrelError, IronBarrel, Options};
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
