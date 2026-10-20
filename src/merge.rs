//! Background data compaction and garbage collection (Merge Process)
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::RwLock;

use crate::data_file::{DataFile, data_file_name};
use crate::error::BarrelError;
use crate::error::Result;
use crate::hint_file::hint_file_name;
use crate::keydir::KeyDir;
use crate::record::{IndexEntry, current_timestamp_secs};
use crate::types::ImmutableFiles;

/// Monotonically allocate a unique `FileId` that never regresses behind wall-clock time
/// or collides with previously allocated file IDs.
pub(crate) fn allocate_file_id(next_file_id: &AtomicU32) -> u32 {
    let now = current_timestamp_secs();
    loop {
        let current = next_file_id.load(Ordering::SeqCst);
        let allocated = std::cmp::max(now, current);
        let next = allocated.saturating_add(1);
        if next_file_id
            .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return allocated;
        }
    }
}

pub struct Compactor;

impl Compactor {
    /// Execute compaction merge operation over target immutable files.
    pub fn run_merge(
        dir: &Path,
        keydir: &KeyDir,
        immutable_files_lock: &RwLock<ImmutableFiles>,
        snapshot_files: &BTreeMap<u32, Arc<DataFile>>,
        max_file_size: u64,
        expiry_secs: Option<u32>,
        next_file_id: &AtomicU32,
    ) -> Result<Vec<u32>> {
        // If there no immutable files there is nothing to compact
        if snapshot_files.is_empty() {
            return Ok(Vec::new());
        }

        let now = current_timestamp_secs();
        // if TTL is configured we calculate the oldest allowed timestamp
        // using saturating_sub() prevents underflow if the
        // expiry duration is larger than the current timestamp
        let cutoff = expiry_secs.map(|exp| now.saturating_sub(exp));
        let mut current_merged_id = allocate_file_id(next_file_id);
        let mut merged_file_ids = vec![current_merged_id];

        // DataFile::create already initializes and manages the companion HintFileWriter
        let mut current_data_file = DataFile::create(dir, current_merged_id)?;

        // We store a staging buffer that holds (key, old_file_id, old_offset, new_index_entry)
        // So, every live record that was copied to a new compacted data file
        // and now needs its in-memory KeyDir pointer updated
        let mut keydir_updates: Vec<(Vec<u8>, u32, u64, IndexEntry)> = Vec::new();

        for (&old_file_id, old_data_file) in snapshot_files {
            old_data_file.scan_records(|record, idx| {
                if record.is_tombstone() {
                    if record.header.value_sz == 16 {
                        let target_file_id =
                            u64::from_be_bytes(record.value[..8].try_into().map_err(|_| {
                                BarrelError::CorruptedRecord {
                                    offset: idx.offset,
                                    reason: "malformed targeted tombstone".into(),
                                }
                            })?) as u32;
                        if snapshot_files.contains_key(&target_file_id) {
                            // Drop targeted tombstone (target is currently being merged)
                            return Ok(());
                        } else {
                            // Target is NOT being merged right now. Forward this tombstone.
                            if current_data_file.size() >= max_file_size {
                                current_data_file.seal_hint_file()?;
                                current_data_file.sync()?;
                                current_merged_id = allocate_file_id(next_file_id);
                                merged_file_ids.push(current_merged_id);
                                current_data_file = DataFile::create(dir, current_merged_id)?;
                            }
                            current_data_file.write_record(&record)?;
                            return Ok(());
                        }
                    } else {
                        // Drop standard explicit deletion tombstone
                        return Ok(());
                    }
                }

                // Check TTL expiration
                if let Some(c) = cutoff
                    && record.header.tstamp < c
                {
                    // Expired; drop!
                    return Ok(());
                }

                // Check if this record is still live in KeyDir, if it's dead we will skip the key
                let is_live = if let Some(live_idx) = keydir.get(&record.key) {
                    live_idx.file_id == old_file_id && live_idx.offset == idx.offset
                } else {
                    false
                };

                if !is_live {
                    // Dead version of key; skip!
                    return Ok(());
                }

                // Roll over merged output file if it reached max_file_size
                if current_data_file.size() >= max_file_size {
                    current_data_file.seal_hint_file()?;
                    current_data_file.sync()?;
                    current_merged_id = allocate_file_id(next_file_id);
                    merged_file_ids.push(current_merged_id);
                    current_data_file = DataFile::create(dir, current_merged_id)?;
                }

                // Write live record to new merged file
                let new_idx = current_data_file.write_record(&record)?;
                keydir_updates.push((record.key, old_file_id, idx.offset, new_idx));
                Ok(())
            })?;
        }

        // Finalize or remove empty merged file
        if current_data_file.size() == 0 {
            let data_path = current_data_file.path().to_path_buf();
            let hint_path = dir.join(hint_file_name(current_merged_id));
            drop(current_data_file);
            let _ = fs::remove_file(data_path);
            let _ = fs::remove_file(hint_path);
            merged_file_ids.retain(|&id| id != current_merged_id);
        } else {
            current_data_file.seal_hint_file()?;
            current_data_file.sync()?;
            drop(current_data_file);
        }

        // We register new merged files in `immutable_files_lock` BEFORE updating KeyDir
        // so concurrent lock-free readers never encounter an unknown `FileId`.
        let mut imm_write = immutable_files_lock.write();
        for &new_id in &merged_file_ids {
            let data_file = DataFile::open(dir, new_id, true)?;
            imm_write.insert(new_id, Arc::new(data_file));
        }
        drop(imm_write);

        // Atomically update KeyDir entries if they still point to the old record.
        for (key, old_file_id, old_offset, new_idx) in keydir_updates {
            keydir.replace_if(key, new_idx, |curr| {
                curr.file_id == old_file_id && curr.offset == old_offset
            });
        }

        // Atomically evict any expired keys from KeyDir.
        if let Some(c) = cutoff {
            for key in keydir.keys() {
                keydir.remove_if(&key, |entry| entry.tstamp < c);
            }
        }

        // Remove old handles from `immutable_files_lock` BEFORE unlinking from disk.
        let mut imm_write = immutable_files_lock.write();
        for &old_id in snapshot_files.keys() {
            imm_write.remove(&old_id);
        }
        drop(imm_write);

        // Remove old merged immutable data files and hint files from disk.
        for &old_file_id in snapshot_files.keys() {
            let data_path = dir.join(data_file_name(old_file_id));
            let hint_path = dir.join(hint_file_name(old_file_id));

            if let Err(e) = fs::remove_file(&data_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(BarrelError::Io(e));
            }
            let _ = fs::remove_file(&hint_path);
        }

        Ok(merged_file_ids)
    }
}
