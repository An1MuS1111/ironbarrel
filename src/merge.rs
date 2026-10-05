//! Background data compaction and garbage collection (Merge Process)
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::data_file::DataFile;
use crate::error::BarrelError;
use crate::error::Result;
use crate::hint_file::HintFileWriter;
use crate::indexer::IndexEntry;
use crate::keydir::KeyDir;
use crate::record::current_timestamp_secs;

pub struct Compactor;

impl Compactor {
    /// Execute compaction merge operation over target immutable files.
    pub fn run_merge(
        dir: &Path,
        keydir: &KeyDir,
        immutable_files: &BTreeMap<u32, Arc<DataFile>>,
        max_file_size: u64,
        expiry_secs: Option<u32>,
        next_file_id: &AtomicU32,
    ) -> Result<Vec<u32>> {
        // If there no immutable files there is nothing to compact
        if immutable_files.is_empty() {
            return Ok(Vec::new());
        }

        let now = current_timestamp_secs();
        // if TTL is configured we calculate the oldest allowed timestamp
        // using saturating_sub() prevents underflow if the
        // expiry duration is larger than the current timestamp
        let cutoff = expiry_secs.map(|exp| now.saturating_sub(exp));

        let mut merged_file_ids = Vec::new();
        // the record with timestamp before the cutoff are expired
        let ts = current_timestamp_secs();
        let mut nfi = next_file_id.fetch_add(1, Ordering::SeqCst);
        if ts > nfi {
            next_file_id.store(ts + 1, Ordering::SeqCst);
            nfi = ts;
        }

        let mut current_merged_id = nfi;
        merged_file_ids.push(current_merged_id);

        let mut current_data_file = DataFile::create(dir, current_merged_id)?;
        let mut current_hint_writer = HintFileWriter::create(dir, current_merged_id)?;

        let mut keydir_updates: Vec<(Vec<u8>, IndexEntry)> = Vec::new();

        for (&old_file_id, old_data_file) in immutable_files {
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
                        if immutable_files.contains_key(&target_file_id) {
                            // Drop targeted tombstone (target is currently being merged)
                            return Ok(());
                        } else {
                            // Target is NOT being merged right now. Forward this tombstone.
                            if current_data_file.size() >= max_file_size {
                                current_hint_writer.seal()?;
                                current_data_file.sync()?;
                                // As the file ids are timestamp based we ensure
                                // the merged file ID is not behind the current timestamp
                                // or older than ID already in use
                                let ts = current_timestamp_secs();
                                let mut nfi = next_file_id.fetch_add(1, Ordering::SeqCst);
                                if ts > nfi {
                                    next_file_id.store(ts + 1, Ordering::SeqCst);
                                    nfi = ts;
                                }
                                current_merged_id = nfi;
                                merged_file_ids.push(current_merged_id);
                                current_data_file = DataFile::create(dir, current_merged_id)?;
                                current_hint_writer =
                                    HintFileWriter::create(dir, current_merged_id)?;
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

                // Check if current merged data file has reached max_file_size
                if current_data_file.size() >= max_file_size {
                    current_hint_writer.seal()?;
                    current_data_file.sync()?;

                    let ts = current_timestamp_secs();
                    let mut nfi = next_file_id.fetch_add(1, Ordering::SeqCst);
                    if ts > nfi {
                        next_file_id.store(ts + 1, Ordering::SeqCst);
                        nfi = ts;
                    }
                    current_merged_id = nfi;
                    merged_file_ids.push(current_merged_id);

                    current_data_file = DataFile::create(dir, current_merged_id)?;
                    current_hint_writer = HintFileWriter::create(dir, current_merged_id)?;
                }

                // Write live record to new merged file
                let new_idx = current_data_file.write_record(&record)?;
                keydir_updates.push((record.key.clone(), new_idx));
                Ok(())
            })?;
        }

        // Finalize or remove empty merged file
        if current_data_file.size() == 0 {
            let data_path = current_data_file.path().to_path_buf();
            let hint_path = current_hint_writer.path().to_path_buf();
            drop(current_data_file);
            drop(current_hint_writer);
            let _ = fs::remove_file(data_path);
            let _ = fs::remove_file(hint_path);
            merged_file_ids.retain(|&id| id != current_merged_id);
        } else {
            current_hint_writer.seal()?;
            current_data_file.sync()?;
        }

        // Atomically update KeyDir for all merged entries
        for (key, new_idx) in keydir_updates {
            if let Some(current_idx) = keydir.get(&key)
                && immutable_files.contains_key(&current_idx.file_id)
            {
                keydir.put(key, new_idx);
            }
        }

        // If TTL expired keys exist in keydir, remove them
        if let Some(c) = cutoff {
            for key in keydir.keys() {
                if let Some(entry) = keydir.get(&key)
                    && entry.tstamp < c
                {
                    keydir.remove(&key);
                }
            }
        }

        // Remove old merged immutable data files and hint files from disk
        for &old_file_id in immutable_files.keys() {
            let data_path = dir.join(crate::data_file::data_file_name(old_file_id));
            let hint_path = dir.join(crate::hint_file::hint_file_name(old_file_id));

            let _ = fs::remove_file(data_path);
            let _ = fs::remove_file(hint_path);
        }

        Ok(merged_file_ids)
    }
}
