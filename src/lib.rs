//! # ironbarrel is a Key-Value Storage Engine in Idiomatic Rust
//!
//! An open-source, high-performance, log-structured hash table key-value storage engine
//! inspired by the original Bitcask design paper (*Justin Sheehy & Marc de Kruijf, Basho Technologies*).

pub mod config;
pub mod data_file;
pub mod error;
pub mod hint_file;
pub mod indexer;
pub mod keydir;
pub mod lock;
pub mod merge;
mod merge_report;
pub mod record;
pub mod stats;
pub mod storage;
mod types;

pub use config::{Options, SyncStrategy};
pub use error::{BarrelError, Result};
pub use indexer::IndexEntry;
pub use merge_report::MergeReport;
pub use stats::Stats;
pub use storage::IronBarrel;
