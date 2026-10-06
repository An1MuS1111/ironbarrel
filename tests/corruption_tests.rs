use ironbarrel::{BarrelError, IronBarrel, Result};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn first_data_file(directory: &std::path::Path) -> PathBuf {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("data"))
        .expect("database should contain a data file")
}

#[test]
fn crc_corruption_is_reported_by_get() -> Result<()> {
    let dir = tempdir()?;
    {
        let db = IronBarrel::open_default(dir.path())?;
        db.put(b"key", b"value")?;
        db.close()?;
    }

    let data_path = first_data_file(dir.path());
    let mut bytes = fs::read(&data_path)?;
    let last_byte = bytes.len() - 1;
    bytes[last_byte] ^= 0xFF;
    fs::write(&data_path, bytes)?;

    let db = IronBarrel::open_default(dir.path())?;
    assert!(matches!(
        db.get(b"key"),
        Err(BarrelError::CrcMismatch { .. })
    ));
    Ok(())
}

#[test]
fn invalid_hint_falls_back_to_data_recovery() -> Result<()> {
    let dir = tempdir()?;
    {
        let db = IronBarrel::open_default(dir.path())?;
        db.put(b"key", b"value")?;
        db.close()?;
    }

    let hint_path = fs::read_dir(dir.path())?
        .map(|entry| entry.map(|item| item.path()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("hint"))
        .expect("database should contain a hint file");
    let mut hint = fs::read(&hint_path)?;
    hint[0] ^= 0xFF;
    fs::write(hint_path, hint)?;

    let db = IronBarrel::open_default(dir.path())?;
    assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
    Ok(())
}
