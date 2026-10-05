//! Main storage engine implementation for file implementation

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};

use crate::config::Options;
use crate::data_file::DataFile;
use crate::error::{BarrelError, Result};
use crate::hint_file::{HintFileReader, validate_hint_file};
use crate::indexer::IndexEntry;
use crate::keydir::KeyDir;
use crate::lock::LockFile;
use crate::record::{Record, current_timestamp_secs};

/// Persistent identifier of a data and hint file pair.
pub type FileId = u32;

/// Shared immutable data-file handle.
pub type SharedDataFile = Arc<DataFile>;

/// Files that are no longer receiving appends.
pub type ImmutableFiles = BTreeMap<FileId, SharedDataFile>;

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

/// Apply a scanned record using the same semantics as the live write path.
/// Targeted tombstones only invalidate a historical physical record; they do
/// not delete the current logical key during recovery.
fn apply_recovered_record(keydir: &KeyDir, record: Record, index: IndexEntry) {
    if record.is_tombstone() && record.header.value_sz == 16 {
        return;
    }
    keydir.put(record.key, index);
}

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
}
