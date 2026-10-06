//! Results returned by a completed compaction.

use crate::types::FileId;

/// Observable summary of a merge operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeReport {
    /// Immutable files selected as compaction input.
    pub input_files: Vec<FileId>,
    /// New data files produced by compaction.
    pub output_files: Vec<FileId>,
    /// Number of live records copied to output files.
    pub keys_rewritten: usize,
    /// Total input bytes observed before compaction.
    pub bytes_before: u64,
    /// Total output bytes produced by compaction.
    pub bytes_after: u64,
}

impl MergeReport {
    /// Bytes reclaimed by the merge, saturating if accounting is inconsistent.
    #[must_use]
    pub fn bytes_reclaimed(&self) -> u64 {
        self.bytes_before.saturating_sub(self.bytes_after)
    }
}
