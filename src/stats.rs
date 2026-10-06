//! Database status metrics and disk space statistics.

/// Summary statistics describing current state of the database engine.
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// Number of active non-deleted keys currently held in the KeyDir index.
    pub total_keys: usize,

    /// Identifier of current active data file receiving writes.
    pub active_file_id: u32,

    /// Count of total data files (active + immutable) in storage directory.
    pub total_data_files: usize,

    /// Total disk footprint in bytes across all `.data` log files.
    pub total_disk_bytes: u64,

    /// Total byte size of live records referenced by KeyDir.
    pub live_data_bytes: u64,

    /// Estimated dead/garbage bytes eligible for reclamation via merge compaction.
    pub reclaimable_bytes: u64,

    /// Ratio of reclaimable dead data relative to total disk space (0.0 to 1.0).
    pub reclaimable_ratio: f64,
}

impl Stats {
    pub fn new(
        total_keys: usize,
        active_file_id: u32,
        total_data_files: usize,
        total_disk_bytes: u64,
        live_data_bytes: u64,
    ) -> Self {
        let reclaimable_bytes = total_disk_bytes.saturating_sub(live_data_bytes);
        let reclaimable_ratio = if total_disk_bytes > 0 {
            reclaimable_bytes as f64 / total_disk_bytes as f64
        } else {
            0.0
        };

        Self {
            total_keys,
            active_file_id,
            total_data_files,
            total_disk_bytes,
            live_data_bytes,
            reclaimable_bytes,
            reclaimable_ratio,
        }
    }
}
