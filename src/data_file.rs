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
use crate::record::{HEADER_SIZE, Record, calculate_crc, decode_header};

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

    /// Read and validate an indexed record while allocating only the value
    /// returned to the caller. The key and payload remain borrowed from one
    /// positional-read buffer, avoiding two extra allocations on every get.
    pub fn read_verified_value(
        &self,
        key: &[u8],
        offset: u64,
        total_sz: u32,
    ) -> Result<Option<Vec<u8>>> {
        if total_sz < HEADER_SIZE as u32 {
            return Err(BarrelError::CorruptedRecord {
                offset,
                reason: "record is smaller than its header".into(),
            });
        }
        let mut buf = vec![0u8; total_sz as usize];

        #[cfg(unix)]
        self.file.read().read_exact_at(&mut buf, offset)?;

        #[cfg(not(unix))]
        {
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buf)?;
        }

        let mut cursor = &buf[..];
        let header = decode_header(&mut cursor)?.ok_or_else(|| BarrelError::CorruptedRecord {
            offset,
            reason: "incomplete record header".into(),
        })?;

        let payload_len = (header.key_sz as usize)
            .checked_add(header.value_sz as usize)
            .ok_or_else(|| BarrelError::CorruptedRecord {
                offset,
                reason: "record payload size overflow".into(),
            })?;

        if HEADER_SIZE + payload_len != buf.len() {
            return Err(BarrelError::CorruptedRecord {
                offset,
                reason: "record size does not match its header".into(),
            });
        }

        let key_end = HEADER_SIZE + header.key_sz as usize;
        let record_key = &buf[HEADER_SIZE..key_end];
        let value = &buf[key_end..];
        if record_key != key {
            return Err(BarrelError::CorruptedRecord {
                offset,
                reason: "index key does not match record key".into(),
            });
        }

        let actual = calculate_crc(
            header.tstamp,
            header.record_type,
            header.key_sz,
            header.value_sz,
            record_key,
            value,
        );

        if actual != header.crc {
            return Err(BarrelError::CrcMismatch {
                offset,
                expected: header.crc,
                actual,
            });
        }

        if header.record_type == u8::from(crate::record::RecordType::Tombstone) {
            return Ok(None);
        }

        Ok(Some(value.to_vec()))
    }

    /// Fast positional read of value payload starting at `value_pos`.
    pub fn read_value(&self, value_pos: u64, value_sz: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; value_sz as usize];

        #[cfg(unix)]
        {
            let lock = self.file.read();
            lock.read_exact_at(&mut buf, value_pos)?;
        }

        #[cfg(not(unix))]
        {
            let mut lock = self.file.write();
            lock.seek(SeekFrom::Start(value_pos))?;
            lock.read_exact(&mut buf)?;
        }

        Ok(buf)
    }

    /// Scan all records sequentially from offset 0 to EOF.
    pub fn scan_records<F>(&self, mut callback: F) -> Result<u64>
    where
        F: FnMut(Record, IndexEntry) -> Result<()>,
    {
        let mut lock = self.file.write();
        lock.seek(SeekFrom::Start(0))?;

        let mut offset: u64 = 0;
        let file_len = lock.metadata()?.len();

        while offset < file_len {
            let record_pos = offset;

            let header = match decode_header(&mut *lock) {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(err) => {
                    tracing::error!(error = ?err, "Failed to decode header");
                    break;
                }
            };

            let mut key = vec![0u8; header.key_sz as usize];
            let mut value = vec![0u8; header.value_sz as usize];

            if lock.read_exact(&mut key).is_err() || lock.read_exact(&mut value).is_err() {
                break;
            }

            let record = Record {
                header: header.clone(),
                key,
                value,
            };

            record.validate().map_err(|error| match error {
                BarrelError::CrcMismatch {
                    expected, actual, ..
                } => BarrelError::CrcMismatch {
                    offset: record_pos,
                    expected,
                    actual,
                },
                BarrelError::CorruptedRecord { reason, .. } => BarrelError::CorruptedRecord {
                    offset: record_pos,
                    reason,
                },
                other => other,
            })?;

            let total_sz = HEADER_SIZE as u32 + header.key_sz as u32 + header.value_sz;

            let index_entry = IndexEntry {
                file_id: self.file_id,
                offset: record_pos,
                total_sz,
                tstamp: header.tstamp,
                is_tombstone: record.is_tombstone(),
            };

            offset += total_sz as u64;
            callback(record, index_entry)?;
        }

        if offset < file_len && !self.read_only {
            let _ = lock.set_len(offset);
            self.write_offset.store(offset, Ordering::Relaxed);
        }

        Ok(offset)
    }

    /// Flush in-memory OS buffers to disk (fsync).
    pub fn sync(&self) -> Result<()> {
        let lock = self.file.read();
        lock.sync_all()?;
        if let Some(ref mut hw) = *self.hint_writer.write() {
            let _ = hw.flush();
        }
        Ok(())
    }

    /// Seal companion hint file by writing trailer CRC and syncing.
    pub fn seal_hint_file(&self) -> Result<()> {
        if let Some(ref mut hw) = *self.hint_writer.write() {
            hw.seal()?;
        }
        Ok(())
    }

    pub fn write_records_batch(
        &self,
        records: &[crate::record::Record],
    ) -> Result<Vec<crate::indexer::IndexEntry>> {
        if self.read_only {
            return Err(BarrelError::ReadOnlyMode);
        }

        let mut lock = self.file.write();
        let mut current_offset = self.write_offset.load(Ordering::Relaxed);
        lock.seek(SeekFrom::Start(current_offset))?;
        let mut index_entries = Vec::with_capacity(records.len());

        let mut hw_lock = self.hint_writer.write();

        for record in records {
            let encoded = record.encode()?;
            let total_sz = encoded.len() as u32;
            lock.write_all(&encoded)?;

            let index_entry = IndexEntry {
                file_id: self.file_id,
                offset: current_offset,
                total_sz,
                tstamp: record.header.tstamp,
                is_tombstone: record.is_tombstone(),
            };

            let is_targeted = record.is_tombstone() && record.header.value_sz == 16;
            if !is_targeted && let Some(hw) = hw_lock.as_mut() {
                hw.write_index_entry(&record.key, &index_entry)?;
            }

            index_entries.push(index_entry);
            current_offset += total_sz as u64;
        }

        self.write_offset.store(current_offset, Ordering::Relaxed);
        Ok(index_entries)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::hint_file::HintFileReader;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn write_record_returns_sequential_index_entries() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 1)?;
        let first = Record::new_standard(b"key1".to_vec(), b"value1".to_vec())?;
        let second = Record::new_standard(b"key2".to_vec(), b"value2".to_vec())?;
        let first_size = first.encode()?.len() as u64;

        let first_index = data_file.write_record(&first)?;
        let second_index = data_file.write_record(&second)?;

        assert_eq!(first_index.file_id, 1);
        assert_eq!(first_index.offset, 0);
        assert_eq!(first_index.total_sz, first.encode()?.len() as u32);
        assert!(!first_index.is_tombstone);
        assert_eq!(second_index.offset, first_size);
        assert_eq!(data_file.size(), first_size + second.encode()?.len() as u64);

        let recovered_first = data_file.read_record(first_index.offset, first_index.total_sz)?;
        let recovered_second = data_file.read_record(second_index.offset, second_index.total_sz)?;
        assert_eq!(recovered_first.key, b"key1");
        assert_eq!(recovered_second.value, b"value2");
        Ok(())
    }

    #[test]
    fn opening_existing_file_restores_size_and_read_only_writes_fail() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 2)?;
        let record = Record::new_standard(b"key".to_vec(), b"value".to_vec())?;
        let expected_size = record.encode()?.len() as u64;
        data_file.write_record(&record)?;
        data_file.sync()?;
        drop(data_file);

        let read_only = DataFile::open(dir.path(), 2, true)?;
        assert_eq!(read_only.size(), expected_size);
        assert!(matches!(
            read_only.write_record(&record),
            Err(BarrelError::ReadOnlyMode)
        ));
        Ok(())
    }

    #[test]
    fn targeted_tombstones_are_excluded_from_hint_files() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 3)?;
        let ordinary = Record::new_deletion(b"ordinary".to_vec())?;
        let targeted = Record::new_targeted_tombstone(b"targeted".to_vec(), 7, 42)?;

        let ordinary_index = data_file.write_record(&ordinary)?;
        let targeted_index = data_file.write_record(&targeted)?;
        data_file.seal_hint_file()?;

        assert!(ordinary_index.is_tombstone);
        assert!(targeted_index.is_tombstone);

        let mut reader = HintFileReader::open(dir.path(), 3)?;
        let mut entries = Vec::new();
        reader.scan_entries(|key, entry| entries.push((key, entry)))?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"ordinary");
        assert!(entries[0].1.is_tombstone);
        Ok(())
    }

    #[test]
    fn read_verified_value_checks_key_and_tombstones() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 4)?;
        let record = Record::new_standard(b"key".to_vec(), b"value".to_vec())?;
        let index = data_file.write_record(&record)?;

        assert_eq!(
            data_file.read_verified_value(b"key", index.offset, index.total_sz)?,
            Some(b"value".to_vec())
        );
        assert!(matches!(
            data_file.read_verified_value(b"wrong", index.offset, index.total_sz),
            Err(BarrelError::CorruptedRecord { .. })
        ));

        let tombstone = Record::new_deletion(b"deleted".to_vec())?;
        let tombstone_index = data_file.write_record(&tombstone)?;
        assert_eq!(
            data_file.read_verified_value(
                b"deleted",
                tombstone_index.offset,
                tombstone_index.total_sz
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn read_record_reports_invalid_size_and_crc_offset() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 5)?;
        let record = Record::new_standard(b"key".to_vec(), b"value".to_vec())?;
        let index = data_file.write_record(&record)?;

        assert!(matches!(
            data_file.read_record(9, (HEADER_SIZE - 1) as u32),
            Err(BarrelError::CorruptedRecord { offset: 9, .. })
        ));

        let path = data_file.path().to_path_buf();
        drop(data_file);
        let mut bytes = fs::read(&path)?;
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&path, bytes)?;

        let reopened = DataFile::open(dir.path(), 5, true)?;
        assert!(matches!(
            reopened.read_record(index.offset, index.total_sz),
            Err(BarrelError::CrcMismatch { offset: 0, .. })
        ));
        Ok(())
    }

    #[test]
    fn scan_records_truncates_incomplete_tail_in_writable_mode() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 6)?;
        let record = Record::new_standard(b"key".to_vec(), b"value".to_vec())?;
        let valid_size = data_file.write_record(&record)?.total_sz as u64;
        data_file.sync()?;
        let path = data_file.path().to_path_buf();
        drop(data_file);

        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&[1, 2, 3])?;
        file.sync_all()?;

        let reopened = DataFile::open(dir.path(), 6, false)?;
        let mut count = 0;
        let scanned = reopened.scan_records(|_, _| {
            count += 1;
            Ok(())
        })?;

        assert_eq!(count, 1);
        assert_eq!(scanned, valid_size);
        assert_eq!(reopened.size(), valid_size);
        assert_eq!(fs::metadata(path)?.len(), valid_size);
        Ok(())
    }

    #[test]
    fn read_only_scan_does_not_truncate_incomplete_tail() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 7)?;
        let record = Record::new_standard(b"key".to_vec(), b"value".to_vec())?;
        let valid_size = data_file.write_record(&record)?.total_sz as u64;
        data_file.sync()?;
        let path = data_file.path().to_path_buf();
        drop(data_file);

        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&[1, 2, 3])?;
        file.sync_all()?;
        let size_before = fs::metadata(&path)?.len();

        let read_only = DataFile::open(dir.path(), 7, true)?;
        let scanned = read_only.scan_records(|_, _| Ok(()))?;

        assert_eq!(scanned, valid_size);
        assert_eq!(fs::metadata(path)?.len(), size_before);
        Ok(())
    }

    #[test]
    fn batch_write_returns_sequential_offsets() -> Result<()> {
        let dir = tempdir()?;
        let data_file = DataFile::create(dir.path(), 8)?;
        let records = vec![
            Record::new_standard(b"one".to_vec(), b"1".to_vec())?,
            Record::new_deletion(b"two".to_vec())?,
            Record::new_standard(b"three".to_vec(), b"333".to_vec())?,
        ];
        let expected_sizes: Vec<u32> = records
            .iter()
            .map(|record| record.encode().unwrap().len() as u32)
            .collect();

        let entries = data_file.write_records_batch(&records)?;

        assert_eq!(entries.len(), records.len());
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[1].offset, expected_sizes[0] as u64);
        assert_eq!(
            entries[2].offset,
            (expected_sizes[0] + expected_sizes[1]) as u64
        );
        assert_eq!(
            data_file.size(),
            expected_sizes.iter().map(|&size| size as u64).sum()
        );
        assert!(entries[1].is_tombstone);
        Ok(())
    }
}
