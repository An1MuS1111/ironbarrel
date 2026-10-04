use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{BarrelError, Result};

/// Hard limit on key sizes.
pub const MAX_KEY_SIZE: usize = (1 << 16) - 1; // 65,535 bytes

/// [CRC32(4) | Tstamp(4) | Type(1) | KeySz(2) | ValSz(4)]
pub const HEADER_SIZE: usize = 15;

/// Constant size of the hint file entry header (18 bytes).
pub const HINT_RECORD_SZ: usize = 18;

/// Bit mask for offset in hint file (63-bit offset with bit 63 as tombstone flag).
pub const MAX_OFFSET: u64 = 0x7FFFFFFFFFFFFFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Standard = 0x01,
    Tombstone = 0x02,
}

impl From<RecordType> for u8 {
    fn from(record_type: RecordType) -> Self {
        match record_type {
            RecordType::Standard => 0x01,
            RecordType::Tombstone => 0x02,
        }
    }
}

/// Data File Entry Header (15 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub crc: u32,
    pub tstamp: u32,
    pub record_type: u8,
    pub key_sz: u16,
    pub value_sz: u32,
}

/// A complete Data File Entry (Header + Payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub header: Header,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    /// Create a standard data record (put operation).
    pub fn new_standard(key: Vec<u8>, value: Vec<u8>) -> Result<Self> {
        if key.len() > MAX_KEY_SIZE {
            return Err(BarrelError::ExceedsSizeLimit(key.len()));
        }
        let tstamp = current_timestamp_secs();
        let record_type = u8::from(RecordType::Standard);
        let key_sz = key.len() as u16;
        let value_sz = value.len() as u32;

        let crc = calculate_crc(tstamp, record_type, key_sz, value_sz, &key, &value);

        Ok(Self {
            header: Header {
                crc,
                tstamp,
                record_type,
                key_sz,
                value_sz,
            },
            key,
            value,
        })
    }

    /// Create an explicit deletion tombstone.
    pub fn new_deletion(key: Vec<u8>) -> Result<Self> {
        if key.len() > MAX_KEY_SIZE {
            return Err(BarrelError::ExceedsSizeLimit(key.len()));
        }
        let tstamp = current_timestamp_secs();
        let record_type = u8::from(RecordType::Tombstone);
        let key_sz = key.len() as u16;
        let value_sz = 0;
        let value = Vec::new();

        let crc = calculate_crc(tstamp, record_type, key_sz, value_sz, &key, &value);

        Ok(Self {
            header: Header {
                crc,
                tstamp,
                record_type,
                key_sz,
                value_sz,
            },
            key,
            value,
        })
    }

    /// Create a targeted tombstone (cross-file overwrite kill order).
    pub fn new_targeted_tombstone(
        key: Vec<u8>,
        target_file_id: u32,
        target_offset: u64,
    ) -> Result<Self> {
        if key.len() > MAX_KEY_SIZE {
            return Err(BarrelError::ExceedsSizeLimit(key.len()));
        }

        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(&(target_file_id as u64).to_be_bytes());
        value.extend_from_slice(&target_offset.to_be_bytes());

        let tstamp = current_timestamp_secs();
        let record_type = u8::from(RecordType::Tombstone);
        let key_sz = key.len() as u16;
        let value_sz = 16;

        let crc = calculate_crc(tstamp, record_type, key_sz, value_sz, &key, &value);

        Ok(Self {
            header: Header {
                crc,
                tstamp,
                record_type,
                key_sz,
                value_sz,
            },
            key,
            value,
        })
    }

    /// Encode record into binary format.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let total_size = HEADER_SIZE + self.key.len() + self.value.len();
        let mut buf = Vec::with_capacity(total_size);

        buf.write_u32::<BigEndian>(self.header.crc)?;
        buf.write_u32::<BigEndian>(self.header.tstamp)?;
        buf.write_u8(self.header.record_type)?;
        buf.write_u16::<BigEndian>(self.header.key_sz)?;
        buf.write_u32::<BigEndian>(self.header.value_sz)?;
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        Ok(buf)
    }

    /// Verify CRC32 checksum of this record.
    pub fn verify_crc(&self) -> bool {
        let expected = calculate_crc(
            self.header.tstamp,
            self.header.record_type,
            self.header.key_sz,
            self.header.value_sz,
            &self.key,
            &self.value,
        );
        expected == self.header.crc
    }

    /// Check if record is a tombstone deletion marker.
    pub fn is_tombstone(&self) -> bool {
        self.header.record_type == u8::from(RecordType::Tombstone)
    }

    /// Validate the structural invariants that must hold before a record is
    /// used by the storage engine which prevents malformed lengths and unknown
    /// record types from turning into oversized allocations or false entries.
    pub fn validate(&self) -> Result<()> {
        if self.header.record_type != u8::from(RecordType::Standard)
            && self.header.record_type != u8::from(RecordType::Tombstone)
        {
            return Err(BarrelError::CorruptedRecord {
                offset: 0,
                reason: format!("unknown record type {}", self.header.record_type),
            });
        }

        if self.key.len() != self.header.key_sz as usize
            || self.value.len() != self.header.value_sz as usize
        {
            return Err(BarrelError::CorruptedRecord {
                offset: 0,
                reason: "record payload lengths do not match header".into(),
            });
        }

        if self.is_tombstone() && self.header.value_sz != 0 && self.header.value_sz != 16 {
            return Err(BarrelError::CorruptedRecord {
                offset: 0,
                reason: "invalid tombstone payload length".into(),
            });
        }

        if !self.verify_crc() {
            let calculated = calculate_crc(
                self.header.tstamp,
                self.header.record_type,
                self.header.key_sz,
                self.header.value_sz,
                &self.key,
                &self.value,
            );

            return Err(BarrelError::CrcMismatch {
                offset: 0,
                expected: self.header.crc,
                actual: calculated,
            });
        }
        Ok(())
    }
}

/// Hint file entry representing key index metadata persisted to companion `.hint` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HintEntry {
    pub tstamp: u32,
    pub key_sz: u16,
    pub total_sz: u32,
    pub is_tombstone: bool,
    pub offset: u64,
    pub key: Vec<u8>,
}

impl HintEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let total_size = HINT_RECORD_SZ + self.key.len();
        let mut buf = Vec::with_capacity(total_size);

        buf.write_u32::<BigEndian>(self.tstamp)?;
        buf.write_u16::<BigEndian>(self.key_sz)?;
        buf.write_u32::<BigEndian>(self.total_sz)?;

        let tomb_int: u64 = if self.is_tombstone { 1 } else { 0 };
        let offset_packed = (tomb_int << 63) | (self.offset & MAX_OFFSET);
        buf.write_u64::<BigEndian>(offset_packed)?;

        buf.extend_from_slice(&self.key);

        Ok(buf)
    }

    pub fn decode<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut header_buf = [0u8; HINT_RECORD_SZ];
        match reader.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(BarrelError::Io(e)),
        }

        let mut cursor = &header_buf[..];
        let tstamp = cursor.read_u32::<BigEndian>()?;
        let key_sz = cursor.read_u16::<BigEndian>()?;
        let total_sz = cursor.read_u32::<BigEndian>()?;
        let offset_packed = cursor.read_u64::<BigEndian>()?;

        let is_tombstone = (offset_packed >> 63) == 1;
        let offset = offset_packed & MAX_OFFSET;

        if tstamp == 0 && key_sz == 0 && offset == MAX_OFFSET {
            return Ok(None);
        }

        let mut key = vec![0u8; key_sz as usize];
        reader.read_exact(&mut key)?;

        Ok(Some(Self {
            tstamp,
            key_sz,
            total_sz,
            is_tombstone,
            offset,
            key,
        }))
    }
}

pub fn create_hint_trailer(hint_crc: u32) -> [u8; HINT_RECORD_SZ] {
    let mut buf = [0u8; HINT_RECORD_SZ];
    BigEndian::write_u32(&mut buf[0..4], 0);
    BigEndian::write_u16(&mut buf[4..6], 0);
    BigEndian::write_u32(&mut buf[6..10], hint_crc);
    let offset_packed = (1u64 << 63) | MAX_OFFSET;
    BigEndian::write_u64(&mut buf[10..18], offset_packed);
    buf
}

pub fn calculate_crc(
    tstamp: u32,
    record_type: u8,
    key_sz: u16,
    value_sz: u32,
    key: &[u8],
    value: &[u8],
) -> u32 {
    let mut hasher = Hasher::new();

    let mut ts_buf = [0u8; 4];
    BigEndian::write_u32(&mut ts_buf, tstamp);
    hasher.update(&ts_buf);

    hasher.update(&[record_type]);

    let mut ksz_buf = [0u8; 2];
    BigEndian::write_u16(&mut ksz_buf, key_sz);
    hasher.update(&ksz_buf);

    let mut vsz_buf = [0u8; 4];
    BigEndian::write_u32(&mut vsz_buf, value_sz);
    hasher.update(&vsz_buf);

    hasher.update(key);
    hasher.update(value);

    hasher.finalize()
}

pub fn decode_header<R: Read>(reader: &mut R) -> Result<Option<Header>> {
    let mut buf = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(BarrelError::Io(e)),
    }

    let mut cursor = &buf[..];
    let crc = cursor.read_u32::<BigEndian>()?;
    let tstamp = cursor.read_u32::<BigEndian>()?;
    let record_type = cursor.read_u8()?;
    let key_sz = cursor.read_u16::<BigEndian>()?;
    let value_sz = cursor.read_u32::<BigEndian>()?;

    Ok(Some(Header {
        crc,
        tstamp,
        record_type,
        key_sz,
        value_sz,
    }))
}

pub fn current_timestamp_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time prior to UNIX epoch")
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encoding() {
        let record = Record::new_standard(b"foo".to_vec(), b"bar_value".to_vec()).unwrap();
        assert!(record.verify_crc());

        let encoded = record.encode().unwrap();
        assert_eq!(encoded.len(), HEADER_SIZE + 3 + 9);

        let mut cursor = &encoded[..];
        let header = decode_header(&mut cursor).unwrap().unwrap();
        assert_eq!(header.record_type, 1);
        assert_eq!(header.key_sz, 3);
        assert_eq!(header.value_sz, 9);
        assert_eq!(header.crc, record.header.crc);
    }
}
