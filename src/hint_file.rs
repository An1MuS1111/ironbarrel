//! Hint file writer and reader for accelerating startup KeyDir recovery,

use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::indexer::IndexEntry;
use crate::record::{HINT_RECORD_SZ, HintEntry, MAX_OFFSET, create_hint_trailer};

/// File suffix for hint index files (`.ironbarrel.hint`).
pub const HINT_FILE_SUFFIX: &str = ".ironbarrel.hint";

/// Helper function to format file ID into hint filename
pub fn hint_file_name(file_id: u32) -> String {
    format!("{}{}", file_id, HINT_FILE_SUFFIX)
}

/// Writer for generating companion hint files with trailer CRC validation.
#[derive(Debug)]
pub struct HintFileWriter {
    #[allow(dead_code)]
    file_id: u32,
    path: PathBuf,
    writer: BufWriter<File>,
    crc_hasher: Hasher,
    is_sealed: bool,
}

impl HintFileWriter {
    /// Create new hint file for writing.
    pub fn create<P: AsRef<Path>>(dir: P, file_id: u32) -> Result<Self> {
        let path = dir.as_ref().join(hint_file_name(file_id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self {
            file_id,
            path,
            writer: BufWriter::new(file),
            crc_hasher: Hasher::new(),
            is_sealed: false,
        })
    }

    /// Write an index entry into the hint file.
    pub fn write_index_entry(&mut self, key: &[u8], entry: &IndexEntry) -> Result<()> {
        let hint = HintEntry {
            tstamp: entry.tstamp,
            key_sz: key.len() as u16,
            total_sz: entry.total_sz,
            is_tombstone: entry.is_tombstone,
            offset: entry.offset,
            key: key.to_vec(),
        };

        let buf = hint.encode()?;
        self.crc_hasher.update(&buf);
        self.writer.write_all(&buf)?;
        Ok(())
    }

    /// Flush writer buffers to disk without writing final trailer.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Seal hint file by writing the 18-byte trailer record with final hint CRC32 and sync to disk.
    pub fn seal(&mut self) -> Result<()> {
        if self.is_sealed {
            return Ok(());
        }

        let final_crc = self.crc_hasher.clone().finalize();
        let trailer = create_hint_trailer(final_crc);
        self.writer.write_all(&trailer)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.is_sealed = true;
        Ok(())
    }

    /// Return path of hint file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Reader for fast startup recovery from hint files.
#[derive(Debug)]
pub struct HintFileReader {
    file_id: u32,
    reader: BufReader<File>,
}

impl HintFileReader {
    /// Open existing hint file for reading.
    pub fn open<P: AsRef<Path>>(dir: P, file_id: u32) -> Result<Self> {
        let path = dir.as_ref().join(hint_file_name(file_id));
        let file = File::open(&path)?;

        Ok(Self {
            file_id,
            reader: BufReader::new(file),
        })
    }

    /// Scan all entries in hint file and invoke callback for each entry.
    pub fn scan_entries<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(Vec<u8>, IndexEntry),
    {
        while let Some(hint) = HintEntry::decode(&mut self.reader)? {
            let index_entry = IndexEntry {
                file_id: self.file_id,
                offset: hint.offset,
                total_sz: hint.total_sz,
                tstamp: hint.tstamp,
                is_tombstone: hint.is_tombstone,
            };

            callback(hint.key, index_entry);
        }

        Ok(())
    }
}

/// Validate whether a hint file is intact
pub fn validate_hint_file<P: AsRef<Path>>(path: P, data_file_size: u64) -> bool {
    let Ok(file) = File::open(&path) else {
        return false;
    };

    let Ok(meta) = file.metadata() else {
        return false;
    };

    let file_len = meta.len();
    if file_len < HINT_RECORD_SZ as u64 {
        return false;
    }

    // Every sealed hint file ends with an 18-bit trailer
    // So the payload is everything before the trailer
    let payload_len = file_len - HINT_RECORD_SZ as u64;
    let mut reader = BufReader::new(file);
    let mut hasher = Hasher::new();

    // The hint file could be much bigger than the memory
    // So a 64 KiB buffer is allocated so the program
    // doesn't need to load the entire file into memeory
    let mut buf = vec![0u8; 65536];
    let mut read_bytes: u64 = 0;

    // Calculate how many bytes remaining and read exactly that amount.
    // Then feed the bytes into the CRC32 hasher and track the number of bytes read.
    while read_bytes < payload_len {
        let to_read = std::cmp::min(buf.len() as u64, payload_len - read_bytes) as usize;
        if reader.read_exact(&mut buf[..to_read]).is_err() {
            return false;
        }
        hasher.update(&buf[..to_read]);
        read_bytes += to_read as u64;
    }

    let mut trailer = [0u8; HINT_RECORD_SZ];
    if reader.read_exact(&mut trailer).is_err() {
        return false;
    }

    let mut cursor = &trailer[..];
    let tstamp = cursor.read_u32::<BigEndian>().unwrap_or(1);
    let key_sz = cursor.read_u16::<BigEndian>().unwrap_or(1);
    let expect_crc = cursor.read_u32::<BigEndian>().unwrap_or(0);
    let offset_packed = cursor.read_u64::<BigEndian>().unwrap_or(0);

    let is_tomb = (offset_packed >> 63) == 1;
    let offset = offset_packed & MAX_OFFSET;

    // This ensures the final 18 bytes are actually a trailer and not an ordinary hint entry
    if tstamp != 0 || key_sz != 0 || !is_tomb || offset != MAX_OFFSET {
        return false;
    }

    // And the CRC stored in the trailer must match the CRC calculated over every preceding hint entry byte
    let calculated_crc = hasher.finalize();
    if expect_crc != calculated_crc {
        return false;
    }

    let Ok(verify_file) = File::open(&path) else {
        return false;
    };

    let mut verify_reader = BufReader::new(verify_file);
    let max_data_end = data_file_size.saturating_add(1);
    loop {
        match HintEntry::decode(&mut verify_reader) {
            Ok(Some(hint)) => {
                let Some(end) = hint.offset.checked_add(hint.total_sz as u64) else {
                    return false;
                };

                if end > max_data_end {
                    return false;
                }
            }
            Ok(None) => break,
            Err(_) => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_hint_file_write_and_read() {
        let dir = tempdir().unwrap();
        let file_id = 1700000000;

        let mut writer = HintFileWriter::create(dir.path(), file_id).unwrap();
        let entry1 = IndexEntry {
            file_id,
            offset: 0,
            total_sz: 35,
            tstamp: 1700000000,
            is_tombstone: false,
        };
        let entry2 = IndexEntry {
            file_id,
            offset: 35,
            total_sz: 45,
            tstamp: 1700000005,
            is_tombstone: false,
        };

        writer.write_index_entry(b"key1", &entry1).unwrap();
        writer.write_index_entry(b"key2", &entry2).unwrap();
        writer.seal().unwrap();

        assert!(validate_hint_file(writer.path(), 80));

        let mut reader = HintFileReader::open(dir.path(), file_id).unwrap();
        let mut recovered = Vec::new();
        reader
            .scan_entries(|key, entry| {
                recovered.push((key, entry));
            })
            .unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].0, b"key1");
        assert_eq!(recovered[0].1, entry1);
        assert_eq!(recovered[1].0, b"key2");
        assert_eq!(recovered[1].1, entry2);
    }

    #[test]
    fn invalid_hint_entry_is_rejected_even_with_valid_trailer_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(hint_file_name(1700000000));
        let malformed_payload = [0x01];
        let mut hasher = Hasher::new();
        hasher.update(&malformed_payload);
        let trailer = create_hint_trailer(hasher.finalize());

        let mut file = File::create(&path).unwrap();
        file.write_all(&malformed_payload).unwrap();
        file.write_all(&trailer).unwrap();
        file.sync_all().unwrap();

        assert!(!validate_hint_file(&path, 1));
    }
}
