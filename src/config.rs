//! Configuration options for ironbarrel storage engine

use crate::error::{BarrelError, Result};
use crate::record::MAX_KEY_SIZE;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Frequency and policy for flushing write buffers to persistent disk (fsync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncStrategy {
    #[default]
    Never,
    /// Explicitly call `fsync` after every single write operation (`o_sync` / `sync_on_put`).
    Always,
    /// Periodically call `fsync` in a background task at specified time intervals.
    Interval(Duration),
}

/// ironbarrel configuration settings.
#[derive(Debug, Clone)]
pub struct Options {
    /// Directory path where data files, hint files, and lock files reside.
    pub directory: PathBuf,

    /// Maximum active file size in bytes before rolling over to a new active data file (default: 32 MB).
    pub max_file_size: u64,

    /// Optional key expiration time in seconds
    pub expiry_secs: Option<u32>,

    /// Synchronization durability strategy (default: `SyncStrategy::Never`).
    pub sync_strategy: SyncStrategy,

    /// Open database in read-only mode (default: false).
    pub read_only: bool,

    /// Enable automatic background merge / compaction (default: false).
    pub auto_merge: bool,

    /// Minimum reclaimable garbage size in bytes before auto-merge triggers (default: 64 MB).
    pub merge_threshold_bytes: u64,

    /// Maximum allowed key size in bytes (default: 65,535 bytes).
    pub max_key_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./ironbarrel"),
            max_file_size: 32 * 1024 * 1024, // 32 MB
            expiry_secs: None,
            sync_strategy: SyncStrategy::Never,
            read_only: false,
            auto_merge: false,
            merge_threshold_bytes: 64 * 1024 * 1024, // 64 MB
            max_key_size: MAX_KEY_SIZE,              // 65,535 bytes
        }
    }
}

impl Options {
    /// Create new configuration with default parameters targeting specified directory.
    pub fn new<P: AsRef<Path>>(directory: P) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Set maximum active file size before rollover.
    pub fn max_file_size(mut self, size: u64) -> Self {
        self.max_file_size = size;
        self
    }

    /// Set the maximum key length accepted by put and delete operations.
    pub fn max_key_size(mut self, size: usize) -> Self {
        self.max_key_size = size;
        self
    }

    /// Set key expiration TTL in seconds.
    pub fn expiry_secs(mut self, secs: u32) -> Self {
        self.expiry_secs = Some(secs);
        self
    }

    /// Set synchronization strategy.
    pub fn sync_strategy(mut self, strategy: SyncStrategy) -> Self {
        self.sync_strategy = strategy;
        self
    }

    /// Set read-only mode flag.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Enable or disable automatic background compaction.
    pub fn auto_merge(mut self, auto_merge: bool) -> Self {
        self.auto_merge = auto_merge;
        self
    }

    /// Set minimum reclaimable bytes threshold for compaction.
    pub fn merge_threshold_bytes(mut self, bytes: u64) -> Self {
        self.merge_threshold_bytes = bytes;
        self
    }

    /// Validate options for self-consistency.
    pub fn validate(&self) -> Result<()> {
        if self.max_file_size == 0 {
            return Err(BarrelError::InvalidConfiguration(
                "max_file_size must be greater than 0".into(),
            ));
        }

        if self.max_key_size == 0 || self.max_key_size > MAX_KEY_SIZE {
            return Err(BarrelError::InvalidConfiguration(format!(
                "max_key_size must be between 1 and {}",
                MAX_KEY_SIZE
            )));
        }
        Ok(())
    }
}
