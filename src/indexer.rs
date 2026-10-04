//! Metadata representation for items held in the in-memory KeyDir index.

use crate::record::HEADER_SIZE;

/// Index metadata stored in KeyDir mapping a key to its physical position on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// Identifier (timestamp integer) of the `.ironbarrel.data` file.
    pub file_id: u32,
    /// Absolute byte offset in the data file where the 15-byte record header starts.
    pub offset: u64,
    /// Total byte size of the data record (15 + key size + value size).
    pub total_sz: u32,
    /// Unix timestamp in seconds when the entry was recorded.
    pub tstamp: u32,
    /// Is this record a tombstone deletion marker.
    pub is_tombstone: bool,
}

impl IndexEntry {
    /// Compute the byte offset where the value payload starts.
    pub fn value_pos(&self, key_sz: u16) -> u64 {
        self.offset + HEADER_SIZE as u64 + key_sz as u64
    }

    /// Compute the byte size of the value payload.
    pub fn value_sz(&self, key_sz: u16) -> u32 {
        self.total_sz
            .saturating_sub(HEADER_SIZE as u32 + key_sz as u32)
    }
}
