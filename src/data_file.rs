//! DataFile manager for active and immutable append-only log files

use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::error::{BarrelError, Result};
use crate::hint_file::HintFileWriter;
use crate::indexer::IndexEntry;
use crate::record::{HEADER_SIZE, Record, decode_header};

/// Data file name extension matching
pub const DATA_FILE_SUFFIX: &str = ".ironbarrel.data";

/// Helper function to format file ID timestamp into data filename
pub fn data_file_name(file_id: u32) -> String {
    format!("{}{}", file_id, DATA_FILE_SUFFIX)
}

/// Append-only data log file.
#[derive(Debug)]
pub struct DataFile {
    /// Numeric file identifier (unix timestamp).
    file_id: u32,
    /// Absolute path to data file on disk.
    path: PathBuf,
    /// Inner file handle wrapped in RwLock for positional read/write operations.
    file: Arc<RwLock<File>>,
    /// Current write offset (file length in bytes).
    write_offset: AtomicU64,
    /// Is file open in read-only mode.
    read_only: bool,
    /// Hint file writer for active file writes.
    hint_writer: RwLock<Option<HintFileWriter>>,
}

impl DataFile {
    /// Create new data file for writing and initialize companion hint writer.
    pub fn create<P: AsRef<Path>>(dir: P, file_id: u32) -> Result<Self> {
        let path = dir.as_ref().join(data_file_name(file_id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let hint_writer = HintFileWriter::create(dir.as_ref(), file_id).ok();

        Ok(Self {
            file_id,
            path,
            file: Arc::new(RwLock::new(file)),
            write_offset: AtomicU64::new(0),
            read_only: false,
            hint_writer: RwLock::new(hint_writer),
        })
    }

    /// Open existing data file.
    pub fn open<P: AsRef<Path>>(dir: P, file_id: u32, read_only: bool) -> Result<Self> {
        let path = dir.as_ref().join(data_file_name(file_id));
        let file = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .open(&path)?;

        let metadata = file.metadata()?;
        let size = metadata.len();

        Ok(Self {
            file_id,
            path,
            file: Arc::new(RwLock::new(file)),
            write_offset: AtomicU64::new(size),
            read_only,
            hint_writer: RwLock::new(None),
        })
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return current byte length of file.
    pub fn size(&self) -> u64 {
        self.write_offset.load(Ordering::Relaxed)
    }

    pub fn write_record(&self, record: &Record) -> Result<IndexEntry> {
        if self.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        let encoded = record.encode()?;
        let total_sz = encoded.len() as u32;

        let mut lock = self.file.write();
        let offset = lock.seek(SeekFrom::End(0))?;

        lock.write_all(&encoded)?;

        self.write_offset
            .store(offset + total_sz as u64, Ordering::Relaxed);

        let is_tombstone = record.is_tombstone();

        let index_entry = IndexEntry {
            file_id: self.file_id,
            offset,
            total_sz,
            tstamp: record.header.tstamp,
            is_tombstone,
        };

        // Checks the type of tombstone. The ordinary tombstone's value_sz = 0.
        // But the targeted tombstone's value_sz = 16. which contains file_id + offset
        let is_targeted_tombstone = is_tombstone && record.header.value_sz == 16;
        if !is_targeted_tombstone && let Some(ref mut hw) = *self.hint_writer.write() {
            hw.write_index_entry(&record.key, &index_entry)?;
        }

        Ok(index_entry)
    }

    /// Reads a full record at `offset`. And verifies CRC.
    pub fn read_record(&self, offset: u64, total_sz: u32) -> Result<Record> {
        if total_sz < HEADER_SIZE as u32 {
            return Err(BarrelError::CorruptedRecord {
                offset,
                reason: "record is smaller than its header".into(),
            });
        }
        let mut buf = vec![0u8; total_sz as usize];

        #[cfg(unix)]
        {
            let lock = self.file.read();
            lock.read_exact_at(&mut buf, offset)?;
        }

        #[cfg(not(unix))]
        {
            let mut lock = self.file.write();
            lock.seek(SeekFrom::Start(offset))?;
            lock.read_exact(&mut buf)?;
        }

        let mut cursor = &buf[..];
        let Ok(Some(header)) = decode_header(&mut cursor) else {
            return Err(BarrelError::CorruptedRecord {
                offset,
                reason: "Incomplete record header".into(),
            });
        };

        let mut key = vec![0u8; header.key_sz as usize];
        let mut value = vec![0u8; header.value_sz as usize];

        cursor.read_exact(&mut key)?;
        cursor.read_exact(&mut value)?;

        let record = Record { header, key, value };
        record.validate().map_err(|error| match error {
            BarrelError::CrcMismatch {
                expected, actual, ..
            } => BarrelError::CrcMismatch {
                offset,
                expected,
                actual,
            },
            BarrelError::CorruptedRecord { reason, .. } => {
                BarrelError::CorruptedRecord { offset, reason }
            }
            other => other,
        })?;

        Ok(record)
    }
}
