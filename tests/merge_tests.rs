use ironbarrel::{IronBarrel, Options, Result};
use tempfile::tempdir;

#[test]
fn test_merge_compaction() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_path_buf();

    // Small file size to force multiple file rotations
    let opts = Options::new(&path).max_file_size(150);
    let db = IronBarrel::open(opts)?;

    // Insert keys, then overwrite & delete many
    for i in 0..100 {
        db.put(format!("key_{}", i).as_bytes(), b"initial_payload_bytes")?;
    }

    for i in 0..50 {
        db.put(format!("key_{}", i).as_bytes(), b"updated_payload_bytes")?;
    }

    for i in 50..80 {
        db.delete(format!("key_{}", i).as_bytes())?;
    }

    let stats_before = db.stats()?;
    assert!(stats_before.reclaimable_bytes > 0);

    // Trigger merge compaction
    let merge_report = db.merge()?;
    assert!(!merge_report.output_files.is_empty());
    assert!(merge_report.bytes_reclaimed() > 0);

    let stats_after = db.stats()?;
    assert!(stats_after.total_disk_bytes < stats_before.total_disk_bytes);

    // Verify key values after compaction
    for i in 0..50 {
        assert_eq!(
            db.get(format!("key_{}", i).as_bytes())?,
            Some(b"updated_payload_bytes".to_vec())
        );
    }
    for i in 50..80 {
        assert_eq!(db.get(format!("key_{}", i).as_bytes())?, None);
    }
    for i in 80..100 {
        assert_eq!(
            db.get(format!("key_{}", i).as_bytes())?,
            Some(b"initial_payload_bytes".to_vec())
        );
    }

    db.close()?;
    Ok(())
}
