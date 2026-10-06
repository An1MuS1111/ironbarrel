use std::collections::BTreeMap;
use std::sync::Arc;

use crate::data_file::DataFile;

/// Persistent identifier of a data and hint file pair.
pub type FileId = u32;

/// Shared immutable data-file handle.
pub type SharedDataFile = Arc<DataFile>;

/// Files that are no longer receiving appends.
pub type ImmutableFiles = BTreeMap<FileId, SharedDataFile>;
