//! Custom error types and Result alias for ironbarrel storage engine operations.

use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarrelError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Data corruption detected via CRC32 checksum mismatch.
    #[error("CRC32 mismatch at offset {offset}: expected {expected:#010x}, found {actual:#010x}")]
    CrcMismatch {
        /// File byte offset where corrupt entry starts.
        offset: u64,
        expected: u32,
        actual: u32,
    },

    /// Corrupted or malformed entry record encountered during parsing.
    #[error("Corrupted record at offset {offset}: {reason}")]
    CorruptedRecord {
        /// File byte offset of corrupt record.
        offset: u64,
        reason: String,
    },

    /// Database directory lock failure (another process already opened the db).
    #[error("Database lock error: {0}")]
    DatabaseLocked(String),

    /// Invalid configuration options provided.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Requested key was not found in KeyDir index or was deleted (tombstone).
    #[error("Key not found")]
    KeyNotFound,

    /// Database instance is in read-only mode and write operation was attempted.
    #[error("Database is open in read-only mode")]
    ReadOnlyMode,

    /// Database instance has already been closed.
    #[error("Database has been closed")]
    DatabaseClosed,

    /// Key or value exceeds max allowed length.
    #[error("Data size exceeds limit: {0} bytes")]
    ExceedsSizeLimit(usize),

    /// Unexpected internal system error.
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, BarrelError>;
