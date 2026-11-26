//! An implementation of directory lock manager using OS file locks.
//!
//! ironbarrel guarantees process safety by acquiring an exclusive file lock within the database directory.
//! Which prevents concurrent write instances from corrupting data files.

use std::fs::{File, OpenOptions, TryLockError};
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

        let attempt = if read_only {
            // Shared lock: any number of read-only instances may hold it.
            file.try_lock_shared()
        } else {
            // Exclusive lock: one read-write instance, and no readers.
            file.try_lock()
        };

        match attempt {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let reason = if read_only {
                    "a read-write instance holds the exclusive lock"
                } else {
                    "another instance may be running"
                };
                return Err(BarrelError::DatabaseLocked(format!(
                    "Failed to acquire {} lock on directory {}: {reason}.",
                    if read_only { "shared" } else { "exclusive" },
                    dir.as_ref().display()
                )));
            }
            // A real I/O failure (permissions, unsupported filesystem) is not
            // "locked", so report it as the I/O error it is.
            Err(TryLockError::Error(e)) => return Err(BarrelError::Io(e)),
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
