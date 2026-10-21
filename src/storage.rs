//! Main storage engine implementation for file implementation

use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Instant;

use crate::config::{Options, SyncStrategy};
use crate::data_file::{DATA_FILE_SUFFIX, DataFile};
use crate::error::{BarrelError, Result};
use crate::hint_file::{HINT_FILE_SUFFIX, HintFileReader, hint_file_name, validate_hint_file};
use crate::iterator::IronBarrelIterator;
use crate::keydir::KeyDir;
use crate::lock::LockFile;
use crate::merge::{Compactor, allocate_file_id};
use crate::merge_report::MergeReport;
use crate::record::{HEADER_SIZE, IndexEntry, MAX_VALUE_SIZE, Record, current_timestamp_secs};
use crate::stats::Stats;
use crate::types::{FileId, ImmutableFiles};

pub(crate) struct IronBarrelInner {
    pub(crate) options: Options,
    /// `KeyDir` will be used in all the readers, writers, recovery, and merge oeprations
    /// The `Arc` allows these operations to share the same KeyDir without copying the index.
    pub(crate) keydir: Arc<KeyDir>,
    /// The `active_file` receives the new record
    /// We use `RwLock` to protect replacement during file rotation.
    /// While the inner `Arc` allows readers to keep using the previous
    ///  active file after it has been moved into `immutable_files`.
    pub(crate) active_file: RwLock<Arc<DataFile>>,
    pub(crate) immutable_files: RwLock<ImmutableFiles>,
    pub(crate) next_file_id: AtomicU32,
    pub(crate) lock_file: RwLock<Option<LockFile>>,
    pub(crate) is_closed: AtomicBool,
    /// This lock covers read-modify-write sequences, including reading the
    /// previous KeyDir entry, creating targeted tombstones, appending records,
    /// and publishing the new KeyDir entry. File-level locks alone do not
    /// make those multi-step operations atomic.
    pub(crate) write_lock: Mutex<()>,
    /// Last successful periodic sync, expressed as Unix seconds.
    pub(crate) last_sync: Mutex<Instant>,
}

impl IronBarrelInner {
    /// Resolve an index entry without holding the file-map lock during I/O.
    fn file_for_entry(&self, entry: &IndexEntry) -> Result<Arc<DataFile>> {
        let active = self.active_file.read();
        if active.file_id() == entry.file_id {
            return Ok(active.clone());
        }
        drop(active);

        let imm = self.immutable_files.read();
        if let Some(file) = imm.get(&entry.file_id) {
            Ok(file.clone())
        } else {
            Err(BarrelError::CorruptedRecord {
                offset: entry.offset,
                reason: format!("Data file {} not found", entry.file_id),
            })
        }
    }

    /// Read value for index entry from corresponding active or immutable data file.
    pub fn read_value_for_entry(&self, key: &[u8], entry: &IndexEntry) -> Result<Option<Vec<u8>>> {
        let file = self.file_for_entry(entry)?;
        file.read_verified_value(key, entry.offset, entry.total_sz)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }

        let entry = match self.keydir.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        // Check key expiration and evict atomically if still expired
        if let Some(exp_secs) = self.options.expiry_secs {
            let cutoff = current_timestamp_secs().saturating_sub(exp_secs);
            if entry.tstamp < cutoff {
                self.keydir.remove_if(key, |curr| curr.tstamp < cutoff);
                return Ok(None);
            }
        }

        self.read_value_for_entry(key, &entry)
    }
}

impl Drop for IronBarrelInner {
    fn drop(&mut self) {
        if !self.is_closed.load(Ordering::Acquire) && !self.options.read_only {
            let active = self.active_file.read();
            let _ = active.seal_hint_file();
            let _ = active.sync();
        }
    }
}

/// Apply a scanned record using the same semantics as the live write path.
/// Targeted tombstones only invalidate a historical physical record; they do
/// not delete the current logical key during recovery.
fn apply_recovered_record(keydir: &KeyDir, record: Record, index: IndexEntry) {
    if record.is_tombstone() && record.header.value_sz == 16 {
        return;
    }
    keydir.put(record.key, index);
}

#[derive(Clone)]
pub struct IronBarrel {
    pub(crate) inner: Arc<IronBarrelInner>,
}

impl IronBarrel {
    /// Open database with custom options.
    pub fn open(options: Options) -> Result<Self> {
        options.validate()?;

        fs::create_dir_all(&options.directory)?;

        // Acquire process lock on directory (ironbarrel.write.lock)
        let lock_file = LockFile::acquire(&options.directory, options.read_only)?;

        let keydir = Arc::new(KeyDir::new());

        let mut data_file_ids = Vec::new();
        let mut hint_file_ids = std::collections::HashSet::new();

        // Parse file IDs from filename patterns
        for entry in fs::read_dir(&options.directory)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && let Some(fname) = path.file_name().and_then(|n| n.to_str())
            {
                if let Some(id_str) = fname.strip_suffix(DATA_FILE_SUFFIX)
                    && let Ok(file_id) = id_str.parse::<u32>()
                {
                    data_file_ids.push(file_id);
                } else if let Some(id_str) = fname.strip_suffix(HINT_FILE_SUFFIX)
                    && let Ok(file_id) = id_str.parse::<u32>()
                {
                    hint_file_ids.insert(file_id);
                }
            }
        }

        data_file_ids.sort_unstable();

        let mut immutable_files = BTreeMap::new();

        // Rebuild KeyDir from existing files
        for &file_id in data_file_ids.iter() {
            let data_file = Arc::new(DataFile::open(
                &options.directory,
                file_id,
                options.read_only,
            )?);

            let hint_path = options.directory.join(hint_file_name(file_id));

            let is_valid_hint = hint_file_ids.contains(&file_id)
                && validate_hint_file(&hint_path, data_file.size());

            if is_valid_hint {
                // Fast recovery: Load index from hint file
                if let Ok(mut hint_reader) = HintFileReader::open(&options.directory, file_id) {
                    hint_reader.scan_entries(|key, idx| {
                        keydir.put(key, idx);
                    })?;
                } else {
                    data_file.scan_records(|record, idx| {
                        apply_recovered_record(&keydir, record, idx);
                        Ok(())
                    })?;
                }
            } else {
                // No valid hint file available: scan data file records sequentially
                data_file.scan_records(|record, idx| {
                    apply_recovered_record(&keydir, record, idx);
                    Ok(())
                })?;
            }

            immutable_files.insert(file_id, data_file);
        }

        let current_ts = current_timestamp_secs();
        let last_file_id = data_file_ids.last().copied().unwrap_or(0);

        let active_file_id = if options.read_only {
            last_file_id
        } else if current_ts > last_file_id {
            current_ts
        } else {
            last_file_id.saturating_add(1)
        };

        let active_file = if options.read_only {
            if let Some(file) = immutable_files.remove(&active_file_id) {
                file
            } else {
                return Err(BarrelError::CorruptedRecord {
                    offset: 0,
                    reason: "read-only database has no data file".into(),
                });
            }
        } else {
            Arc::new(DataFile::create(&options.directory, active_file_id)?)
        };

        let next_file_id = AtomicU32::new(active_file_id.saturating_add(1));

        let inner = Arc::new(IronBarrelInner {
            options,
            keydir,
            active_file: RwLock::new(active_file),
            immutable_files: RwLock::new(immutable_files),
            next_file_id,
            lock_file: RwLock::new(Some(lock_file)),
            is_closed: AtomicBool::new(false),
            write_lock: Mutex::new(()),
            last_sync: Mutex::new(Instant::now()),
        });

        Ok(Self { inner })
    }

    /// Open a database with default options at specified directory.
    pub fn open_default<P: AsRef<Path>>(directory: P) -> Result<Self> {
        Self::open(Options::new(directory))
    }

    /// Rotate active data file when size limit is reached.
    fn rotate_active_file(&self, incoming_bytes: u64, force: bool) -> Result<()> {
        let mut active_lock = self.inner.active_file.write();

        if active_lock.size() == 0 {
            return Ok(());
        }

        if !force
            && active_lock.size().saturating_add(incoming_bytes) < self.inner.options.max_file_size
        {
            return Ok(());
        }

        active_lock.seal_hint_file()?;
        active_lock.sync()?;

        let old_active_id = active_lock.file_id();
        let old_active = active_lock.clone();

        let mut imm_lock = self.inner.immutable_files.write();
        imm_lock.insert(old_active_id, old_active);

        let new_file_id = allocate_file_id(&self.inner.next_file_id);
        let new_active = Arc::new(DataFile::create(
            &self.inner.options.directory,
            new_file_id,
        )?);

        *active_lock = new_active;

        Ok(())
    }

    /// Append records and publish their index entries while the write lock is held.
    fn write_records_and_update_index_locked(&self, records: Vec<Record>) -> Result<()> {
        let total_sz: u64 = records
            .iter()
            .map(|r| HEADER_SIZE as u64 + r.key.len() as u64 + r.value.len() as u64)
            .sum();

        self.rotate_active_file(total_sz, false)?;

        let active = self.inner.active_file.read();
        let index_entries = active.write_records_batch(&records)?;

        // Publish the primary record to KeyDir immediately after writing so in-memory
        // and on-disk states stay consistent.
        if !records.is_empty() {
            self.inner
                .keydir
                .put(records[0].key.clone(), index_entries[0]);
        }

        let should_sync = match self.inner.options.sync_strategy {
            SyncStrategy::Never => false,
            SyncStrategy::Always => true,
            SyncStrategy::Interval(interval) => {
                let mut last_sync = self.inner.last_sync.lock();
                if last_sync.elapsed() >= interval {
                    *last_sync = Instant::now();
                    true
                } else {
                    false
                }
            }
        };

        if should_sync {
            active.sync()?;
        }

        Ok(())
    }

    /// Trigger manual data merge/compaction over immutable files.
    pub fn merge(&self) -> Result<MergeReport> {
        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        let _write_guard = self.inner.write_lock.lock();
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }

        self.rotate_active_file(0, true)?;
        let snapshot_files = self.inner.immutable_files.read().clone();
        let input_files = snapshot_files.keys().copied().collect::<Vec<FileId>>();
        let bytes_before = snapshot_files.values().map(|file| file.size()).sum();
        let max_file_size = self.inner.options.max_file_size;
        let expiry_secs = self.inner.options.expiry_secs;
        let dir = &self.inner.options.directory;

        let merged_ids = Compactor::run_merge(
            dir,
            &self.inner.keydir,
            &self.inner.immutable_files,
            &snapshot_files,
            max_file_size,
            expiry_secs,
            &self.inner.next_file_id,
        )?;

        let imm_read = self.inner.immutable_files.read();
        let bytes_after = merged_ids
            .iter()
            .filter_map(|file_id| imm_read.get(file_id).map(|file| file.size()))
            .sum();

        Ok(MergeReport {
            input_files,
            output_files: merged_ids,
            keys_rewritten: self.inner.keydir.len(),
            bytes_before,
            bytes_after,
        })
    }

    /// Fsync active data file to disk.
    pub fn sync(&self) -> Result<()> {
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }
        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }
        let active = self.inner.active_file.read();
        active.sync()?;
        Ok(())
    }

    /// Get vector of all active keys in database, filtering out expired keys.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>> {
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }
        let now = current_timestamp_secs();
        let all_keys = self.inner.keydir.keys();
        if let Some(exp_secs) = self.inner.options.expiry_secs {
            let cutoff = now.saturating_sub(exp_secs);
            let mut valid_keys = Vec::new();
            for k in all_keys {
                if let Some(entry) = self.inner.keydir.get(&k) {
                    if entry.tstamp >= cutoff {
                        valid_keys.push(k);
                    } else {
                        self.inner.keydir.remove_if(&k, |curr| curr.tstamp < cutoff);
                    }
                }
            }
            Ok(valid_keys)
        } else {
            Ok(all_keys)
        }
    }

    /// Number of active keys in database.
    pub fn len(&self) -> Result<usize> {
        Ok(self.keys()?.len())
    }

    /// Check if database contains zero keys.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return snapshot iterator over all key-value pairs in lexicographical key order.
    pub fn iter(&self) -> Result<IronBarrelIterator> {
        let keys = self.keys()?;
        Ok(IronBarrelIterator::new(self.inner.clone(), keys))
    }

    /// Return summary statistics describing current database state.
    pub fn stats(&self) -> Result<Stats> {
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }

        let total_keys = self.len()?;
        let active_file = self.inner.active_file.read();
        let active_file_id = active_file.file_id();

        let imm = self.inner.immutable_files.read();
        let total_data_files = 1 + imm.len();

        let mut total_disk_bytes = active_file.size();
        for file in imm.values() {
            total_disk_bytes += file.size();
        }

        let live_data_bytes = self.inner.keydir.live_bytes();

        Ok(Stats::new(
            total_keys,
            active_file_id,
            total_data_files,
            total_disk_bytes,
            live_data_bytes,
        ))
    }

    /// Run automatic compaction synchronously after a successful write when
    /// configured garbage exceeds the configured threshold. Synchronous
    /// compaction keeps lifecycle ownership simple and makes failures visible.
    fn maybe_auto_merge(&self) -> Result<()> {
        if !self.inner.options.auto_merge {
            return Ok(());
        }
        let stats = self.stats()?;
        if stats.reclaimable_bytes >= self.inner.options.merge_threshold_bytes {
            self.merge()?;
        }
        Ok(())
    }

    /// Insert or update key-value pair in database.
    pub fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let (key_bytes, val_bytes) = (key.as_ref(), value.as_ref());

        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        if key_bytes.len() > self.inner.options.max_key_size {
            return Err(BarrelError::ExceedsSizeLimit {
                field: "key_bytes".into(),
                size: key_bytes.len(),
            });
        }

        if val_bytes.len() > MAX_VALUE_SIZE {
            return Err(BarrelError::ExceedsSizeLimit {
                field: "key_bytes".into(),
                size: key_bytes.len(),
            });
        }

        let write_guard = self.inner.write_lock.lock();
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }

        // Rotate before checking `active_file_id` so targeted tombstones accurately
        // reflect whether the previous record resides in an immutable file.
        let worst_case_batch_sz =
            (HEADER_SIZE * 2 + key_bytes.len() * 2 + val_bytes.len() + 16) as u64;
        self.rotate_active_file(worst_case_batch_sz, false)?;

        let old_idx = self.inner.keydir.get(key_bytes);
        let active_file_id = self.inner.active_file.read().file_id();

        let new_record = Record::new_standard(key_bytes.to_vec(), val_bytes.to_vec())?;
        let mut batch = vec![new_record];

        let stale = old_idx.filter(|idx| idx.file_id != active_file_id);

        if let Some(idx) = stale {
            batch.push(Record::new_targeted_tombstone(
                key_bytes.to_vec(),
                idx.file_id,
                idx.offset,
            )?);
        }

        self.write_records_and_update_index_locked(batch)?;
        drop(write_guard);

        self.maybe_auto_merge()
    }

    /// Get value for key. Returns `Ok(None)` if key does not exist or is tombstoned.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.inner.get(key.as_ref())
    }

    /// Delete key from database by appending a tombstone record matching V2 behavior.
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        let key_bytes = key.as_ref();

        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        if key_bytes.len() > self.inner.options.max_key_size {
            return Err(BarrelError::ExceedsSizeLimit {
                field: "key_bytes".into(),
                size: key_bytes.len(),
            });
        }

        let write_guard = self.inner.write_lock.lock();
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Err(BarrelError::DatabaseClosed);
        }

        // Deleting a missing key is a no-op.
        if self.inner.keydir.get(key_bytes).is_none() {
            return Ok(());
        }

        let record = Record::new_deletion(key_bytes.to_vec())?;
        self.write_records_and_update_index_locked(vec![record])?;
        drop(write_guard);

        self.maybe_auto_merge()
    }

    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Close the database instance gracefully, sealing active hint file and flushing data.
    pub fn close(&self) -> Result<()> {
        let _write_guard = self.inner.write_lock.lock();
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Ok(());
        }

        if !self.inner.options.read_only {
            let active = self.inner.active_file.read();
            active.seal_hint_file()?;
            active.sync()?;
            drop(active);
        }

        // Explicitly release directory lock on close
        self.inner.lock_file.write().take();
        self.inner.is_closed.store(true, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn merge_produces_valid_sealed_hint_files_and_accurate_report() -> Result<()> {
        let dir = tempdir()?;
        let db = IronBarrel::open(Options::new(dir.path()).max_file_size(128))?;

        db.put(b"k1", b"val1_initial")?;
        db.put(b"k2", b"val2_initial")?;
        db.put(b"k1", b"val1_updated")?;

        let report = db.merge()?;
        assert!(report.bytes_after > 0);
        assert_eq!(report.keys_rewritten, 2);
        assert!(!report.output_files.is_empty());

        // Verify every compacted file has a valid, uncorrupted hint file on disk
        for &merged_id in &report.output_files {
            let data_path = dir.path().join(crate::data_file::data_file_name(merged_id));
            let hint_path = dir.path().join(crate::hint_file::hint_file_name(merged_id));
            let data_len = fs::metadata(&data_path)?.len();
            assert!(
                validate_hint_file(&hint_path, data_len),
                "Hint file for merged file {merged_id} failed validation"
            );
        }

        db.close()?;

        let reopened = IronBarrel::open(Options::new(dir.path()).read_only(true))?;
        assert_eq!(reopened.get(b"k1")?, Some(b"val1_updated".to_vec()));
        assert_eq!(reopened.get(b"k2")?, Some(b"val2_initial".to_vec()));
        reopened.close()?;
        Ok(())
    }

    #[test]
    fn read_only_stats_do_not_double_count_active_file() -> Result<()> {
        let dir = tempdir()?;
        let db = IronBarrel::open_default(dir.path())?;
        db.put(b"alpha", b"beta")?;
        let rw_stats = db.stats()?;
        db.close()?;

        let ro_db = IronBarrel::open(Options::new(dir.path()).read_only(true))?;
        let ro_stats = ro_db.stats()?;
        assert_eq!(ro_stats.total_data_files, rw_stats.total_data_files);
        assert_eq!(ro_stats.total_disk_bytes, rw_stats.total_disk_bytes);
        assert_eq!(ro_stats.reclaimable_bytes, 0);
        ro_db.close()?;
        Ok(())
    }

    #[test]
    fn corrupted_oversized_header_in_scan_does_not_oom() -> Result<()> {
        let dir = tempdir()?;
        let df = DataFile::create(dir.path(), 99)?;
        let valid = Record::new_standard(b"ok".to_vec(), b"good".to_vec())?;
        let valid_len = df.write_record(&valid)?.total_sz as u64;
        df.sync()?;
        let path = df.path().to_path_buf();
        drop(df);

        // Append a fake header claiming a 4 GiB value_sz at the tail
        let mut bogus_header = [0u8; HEADER_SIZE];
        bogus_header[8] = 1; // RecordType::Standard
        bogus_header[11..15].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut f = fs::OpenOptions::new().append(true).open(&path)?;
        std::io::Write::write_all(&mut f, &bogus_header)?;
        f.sync_all()?;
        drop(f);

        let reopened = DataFile::open(dir.path(), 99, false)?;
        let scanned = reopened.scan_records(|_, _| Ok(()))?;
        assert_eq!(scanned, valid_len);
        assert_eq!(fs::metadata(&path)?.len(), valid_len);
        Ok(())
    }
}
