//! An implementation of directory lock manager using OS file locks.
//!
//! ironbarrel guarantees process safety by acquiring an exclusive file lock within the database directory.
//! Which prevents concurrent write instances from corrupting data files.

use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{BarrelError, Result};

/// Lock filename inside ironbarrel database directory `ironbarrel.write.lock`.
pub const LOCK_FILE_NAME: &str = "ironbarrel.write.lock";

/// Directory lock handle. Automatically unlocks on drop.
#[derive(Debug)]
pub struct LockFile {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
}

impl LockFile {
    /// Acquire exclusive lock on database directory.
    pub fn acquire<P: AsRef<Path>>(dir: P, read_only: bool) -> Result<Self> {
        let path = dir.as_ref().join(LOCK_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        if read_only {
            // Acquire shared lock for read-only instances
            file.try_lock_shared().map_err(|_| {
                BarrelError::DatabaseLocked(format!(
                    "Failed to acquire shared lock on directory {}",
                    dir.as_ref().display()
                ))
            })?;
        } else {
            // Acquire exclusive lock for read-write instances
            file.try_lock_exclusive().map_err(|_| {
                BarrelError::DatabaseLocked(format!(
                    "Failed to acquire exclusive lock on directory {}. Another instance may be running.",
                    dir.as_ref().display()
                ))
            })?;
        }

        Ok(Self { path, file })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_lock_file_exclusivity() {
        let dir = tempdir().unwrap();
        let lock1 = LockFile::acquire(dir.path(), false);
        assert!(lock1.is_ok());

        // Second exclusive lock attempt should fail
        let lock2 = LockFile::acquire(dir.path(), false);
        assert!(lock2.is_err());

        // Drop first lock
        drop(lock1);

        // Third attempt after drop should succeed
        let lock3 = LockFile::acquire(dir.path(), false);
        assert!(lock3.is_ok());
    }

    #[test]
    fn shared_locks_can_coexist_but_exclusive_locks_cannot() {
        let dir = tempdir().unwrap();
        let shared1 = LockFile::acquire(dir.path(), true);
        assert!(shared1.is_ok());

        let shared2 = LockFile::acquire(dir.path(), true);
        assert!(shared2.is_ok());

        let exclusive = LockFile::acquire(dir.path(), false);
        assert!(exclusive.is_err());
    }

    #[test]
    fn exclusive_lock_blocks_shared_lock() {
        let dir = tempdir().unwrap();
        let exclusive = LockFile::acquire(dir.path(), false);
        assert!(exclusive.is_ok());

        let shared = LockFile::acquire(dir.path(), true);
        assert!(shared.is_err());
    }

    #[test]
    fn dropping_lock_allows_next_lock() {
        let dir = tempdir().unwrap();
        let lock = LockFile::acquire(dir.path(), false);
        assert!(lock.is_ok());

        drop(lock);

        let next_lock = LockFile::acquire(dir.path(), false);
        assert!(next_lock.is_ok());
    }
}
