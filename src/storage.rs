//! Main storage engine implementation for file implementation

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::config::{Options, SyncStrategy};
use crate::data_file::DataFile;
use crate::error::{BarrelError, Result};
use crate::hint_file::{HintFileReader, validate_hint_file};
use crate::indexer::IndexEntry;
use crate::keydir::KeyDir;
use crate::lock::LockFile;
use crate::merge::Compactor;
use crate::merge_report::MergeReport;
use crate::record::{HEADER_SIZE, Record, current_timestamp_secs};
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
    pub(crate) write_lock: parking_lot::Mutex<()>,
    /// Last successful periodic sync, expressed as Unix seconds.
    pub(crate) last_sync_secs: AtomicU64,
}

impl IronBarrelInner {
    /// Resolve an index entry without holding the file-map lock during I/O.
    fn file_for_entry(&self, entry: &crate::indexer::IndexEntry) -> Result<Arc<DataFile>> {
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
    pub fn read_value_for_entry(
        &self,
        key: &[u8],
        entry: &crate::indexer::IndexEntry,
    ) -> Result<Option<Vec<u8>>> {
        let file = self.file_for_entry(entry)?;
        file.read_verified_value(key, entry.offset, entry.total_sz)
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
                if fname.ends_with(".ironbarrel.data") || fname.ends_with(".data") {
                    let id_str = fname
                        .trim_end_matches(".ironbarrel.data")
                        .trim_end_matches(".data");
                    if let Ok(file_id) = id_str.parse::<u32>() {
                        data_file_ids.push(file_id);
                    }
                } else if fname.ends_with(".ironbarrel.hint") || fname.ends_with(".hint") {
                    let id_str = fname
                        .trim_end_matches(".ironbarrel.hint")
                        .trim_end_matches(".hint");
                    if let Ok(file_id) = id_str.parse::<u32>() {
                        hint_file_ids.insert(file_id);
                    }
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

            let hint_path = options
                .directory
                .join(crate::hint_file::hint_file_name(file_id));

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
            last_file_id + 1
        };

        let active_file = if options.read_only {
            if let Some(file) = immutable_files.get(&active_file_id) {
                file.clone()
            } else {
                return Err(BarrelError::CorruptedRecord {
                    offset: 0,
                    reason: "read-only database has no data file".into(),
                });
            }
        } else {
            Arc::new(DataFile::create(&options.directory, active_file_id)?)
        };

        let next_file_id = AtomicU32::new(active_file_id + 1);

        let inner = Arc::new(IronBarrelInner {
            options,
            keydir,
            active_file: RwLock::new(active_file),
            immutable_files: RwLock::new(immutable_files),
            next_file_id,
            lock_file: RwLock::new(Some(lock_file)),
            is_closed: AtomicBool::new(false),
            write_lock: parking_lot::Mutex::new(()),
            last_sync_secs: AtomicU64::new(current_timestamp_secs() as u64),
        });

        Ok(Self { inner })
    }

    /// Open a database with default options at specified directory.
    pub fn open_default<P: AsRef<Path>>(directory: P) -> Result<Self> {
        Self::open(Options::new(directory))
    }

    /// Rotate active data file when size limit is reached.
    fn rotate_active_file(&self, force: bool) -> Result<()> {
        let mut active_lock = self.inner.active_file.write();

        if !force && active_lock.size() < self.inner.options.max_file_size {
            return Ok(());
        }

        active_lock.seal_hint_file()?;
        active_lock.sync()?;

        let old_active_id = active_lock.file_id();
        let old_active = active_lock.clone();

        let mut imm_lock = self.inner.immutable_files.write();
        imm_lock.insert(old_active_id, old_active);

        let now = current_timestamp_secs();
        let next_allocated = self.inner.next_file_id.fetch_add(1, Ordering::SeqCst);
        let new_file_id = std::cmp::max(now, next_allocated);

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

        let active = self.inner.active_file.read();

        if active.size() + total_sz >= self.inner.options.max_file_size {
            drop(active);
            self.rotate_active_file(false)?;
        } else {
            drop(active);
        }

        let active = self.inner.active_file.read();
        let index_entries = active.write_records_batch(&records)?;

        let should_sync = match self.inner.options.sync_strategy {
            SyncStrategy::Never => false,
            SyncStrategy::Always => true,
            SyncStrategy::Interval(interval) => {
                let interval_secs = interval.as_secs().max(1);
                current_timestamp_secs() as u64
                    >= self.inner.last_sync_secs.load(Ordering::Relaxed) + interval_secs
            }
        };

        if should_sync {
            active.sync()?;
            self.inner
                .last_sync_secs
                .store(current_timestamp_secs() as u64, Ordering::Relaxed);
        }

        // Only put the first record into the index (the primary data or explicit deletion)
        if !records.is_empty() {
            self.inner
                .keydir
                .put(records[0].key.clone(), index_entries[0]);
        }

        Ok(())
    }

    /// Trigger manual data merge/compaction over immutable files.
    pub fn merge(&self) -> Result<MergeReport> {
        if self.inner.is_closed.load(Ordering::Relaxed) {
            return Err(BarrelError::DatabaseClosed);
        }
        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        let _write_guard = self.inner.write_lock.lock();
        self.rotate_active_file(true)?;
        let immutable_files = self.inner.immutable_files.read().clone();
        let input_files = immutable_files.keys().copied().collect::<Vec<FileId>>();
        let bytes_before = immutable_files.values().map(|file| file.size()).sum();
        let max_file_size = self.inner.options.max_file_size;
        let expiry_secs = self.inner.options.expiry_secs;
        let dir = &self.inner.options.directory;

        let merged_ids = Compactor::run_merge(
            dir,
            &self.inner.keydir,
            &immutable_files,
            max_file_size,
            expiry_secs,
            &self.inner.next_file_id,
        )?;

        if !merged_ids.is_empty() {
            let mut imm_write = self.inner.immutable_files.write();

            for &old_id in immutable_files.keys() {
                imm_write.remove(&old_id);
            }

            for &new_id in &merged_ids {
                if let Ok(data_file) = DataFile::open(dir, new_id, false) {
                    imm_write.insert(new_id, Arc::new(data_file));
                }
            }
        } else {
            // All immutable files were dead/expired and removed
            let mut imm_write = self.inner.immutable_files.write();
            for &old_id in immutable_files.keys() {
                imm_write.remove(&old_id);
            }
        }

        self.rotate_active_file(true)?;
        let bytes_after = merged_ids
            .iter()
            .filter_map(|file_id| {
                self.inner
                    .immutable_files
                    .read()
                    .get(file_id)
                    .map(|file| file.size())
            })
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
        if self.inner.is_closed.load(Ordering::Relaxed) {
            return Err(BarrelError::DatabaseClosed);
        }
        let active = self.inner.active_file.read();
        active.sync()?;
        Ok(())
    }

    /// Get vector of all active keys in database, filtering out expired keys.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>> {
        if self.inner.is_closed.load(Ordering::Relaxed) {
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
                        self.inner.keydir.remove(&k);
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

    /// Return summary statistics describing current database state.
    pub fn stats(&self) -> Result<Stats> {
        if self.inner.is_closed.load(Ordering::Relaxed) {
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

        if self.inner.is_closed.load(Ordering::Relaxed) {
            return Err(BarrelError::DatabaseClosed);
        }

        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        if key_bytes.len() > self.inner.options.max_key_size {
            return Err(BarrelError::ExceedsSizeLimit(key_bytes.len()));
        }

        let write_guard = self.inner.write_lock.lock();

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
        let key_bytes = key.as_ref();

        if self.inner.is_closed.load(Ordering::Relaxed) {
            return Err(BarrelError::DatabaseClosed);
        }

        let entry = match self.inner.keydir.get(key_bytes) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        if let Some(exp) = self.inner.options.expiry_secs
            && entry.tstamp < current_timestamp_secs().saturating_sub(exp)
        {
            return Ok(None);
        }

        self.inner.read_value_for_entry(key_bytes, &entry)
    }

    /// Delete key from database by appending a tombstone record matching V2 behavior.
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        let key_bytes = key.as_ref();

        if self.inner.is_closed.load(Ordering::Relaxed) {
            return Err(BarrelError::DatabaseClosed);
        }
        if self.inner.options.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        if key_bytes.len() > self.inner.options.max_key_size {
            return Err(BarrelError::ExceedsSizeLimit(key_bytes.len()));
        }
        {
            let _write_guard = self.inner.write_lock.lock();

            // Match Basho behavior: deleting a missing key is a no-op.
            if self.inner.keydir.get(key_bytes).is_none() {
                return Ok(());
            }

            let record = Record::new_deletion(key_bytes.to_vec())?;
            self.write_records_and_update_index_locked(vec![record])?;

            self.inner.keydir.remove(key_bytes);
        }

        self.maybe_auto_merge()
    }

    /// Close the database instance gracefully, sealing active hint file and flushing data.
    pub fn close(&self) -> Result<()> {
        let _write_guard = self.inner.write_lock.lock();
        if self.inner.is_closed.load(Ordering::Acquire) {
            return Ok(());
        }

        let active = self.inner.active_file.read();
        active.seal_hint_file()?;
        active.sync()?;
        drop(active);

        // Explicitly release directory lock on close
        self.inner.lock_file.write().take();
        self.inner.is_closed.store(true, Ordering::Release);
        Ok(())
    }
}
