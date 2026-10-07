//! Key-value snapshot iterators, range queries, and prefix filtering.

use std::sync::Arc;

use crate::error::Result;
use crate::storage::IronBarrelInner;

/// Iterator over key-value pairs yielding `(Vec<u8>, Vec<u8>)`.
#[must_use = "iterators do nothing until consumed"]
pub struct IronBarrelIterator {
    inner: Arc<IronBarrelInner>,
    keys: Vec<Vec<u8>>,
    index: usize,
}

impl IronBarrelIterator {
    pub(crate) fn new(inner: Arc<IronBarrelInner>, mut keys: Vec<Vec<u8>>) -> Self {
        // Return keys in lexicographical order for predictable iteration
        keys.sort();
        Self {
            inner,
            keys,
            index: 0,
        }
    }
}

impl Iterator for IronBarrelIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.keys.len() {
            let key = &self.keys[self.index];
            self.index += 1;

            match self.inner.get(key) {
                Ok(Some(val)) => return Some(Ok((key.clone(), val))),
                Ok(None) => continue, // Key deleted concurrently, skip
                Err(e) => return Some(Err(e)),
            }
        }

        None
    }
}
