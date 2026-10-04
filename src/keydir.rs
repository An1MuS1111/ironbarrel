//! Concurrent in-memory KeyDir hash table for ironbarrel.

use crate::indexer::IndexEntry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory KeyDir index.
#[derive(Debug)]
pub struct KeyDir {
    /// Concurrent map from byte key to disk IndexEntry.
    entries: DashMap<Vec<u8>, IndexEntry>,
    /// Accumulated total size in bytes of live records currently indexed.
    live_bytes: AtomicU64,
}

impl KeyDir {
    /// Create new empty KeyDir.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            live_bytes: AtomicU64::new(0),
        }
    }

    /// Insert or update an index entry for key.
    /// Returns previous `IndexEntry` if key was previously indexed.
    pub fn put(&self, key: Vec<u8>, entry: IndexEntry) -> Option<IndexEntry> {
        let entry_total_sz = entry.total_sz as u64;

        if entry.is_tombstone {
            // Tombstone entry: remove key from active index
            if let Some((_, old_entry)) = self.entries.remove(&key) {
                self.live_bytes
                    .fetch_sub(old_entry.total_sz as u64, Ordering::Relaxed);
                Some(old_entry)
            } else {
                None
            }
        } else {
            // Standard entry: update or insert
            if let Some(old_entry) = self.entries.insert(key, entry) {
                self.live_bytes
                    .fetch_sub(old_entry.total_sz as u64, Ordering::Relaxed);
                self.live_bytes.fetch_add(entry_total_sz, Ordering::Relaxed);
                Some(old_entry)
            } else {
                self.live_bytes.fetch_add(entry_total_sz, Ordering::Relaxed);
                None
            }
        }
    }

    /// Get `IndexEntry` for key. Returns `None` if key does not exist or is tombstoned.
    pub fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        self.entries.get(key).map(|r| *r.value())
    }

    /// Remove key from KeyDir and returns old entry if existed.
    pub fn remove(&self, key: &[u8]) -> Option<IndexEntry> {
        if let Some((_, old_entry)) = self.entries.remove(key) {
            self.live_bytes
                .fetch_sub(old_entry.total_sz as u64, Ordering::Relaxed);
            Some(old_entry)
        } else {
            None
        }
    }

    /// Check whether non-tombstone key exists in KeyDir.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Number of active keys in KeyDir.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if KeyDir is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return total byte size of all active live records on disk.
    pub fn live_bytes(&self) -> u64 {
        self.live_bytes.load(Ordering::Relaxed)
    }

    /// Return all active keys.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.entries.iter().map(|r| r.key().clone()).collect()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.entries.clear();
        self.live_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for KeyDir {
    fn default() -> Self {
        Self::new()
    }
}
