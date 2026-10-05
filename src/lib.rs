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
pub mod record;
pub mod storage;
