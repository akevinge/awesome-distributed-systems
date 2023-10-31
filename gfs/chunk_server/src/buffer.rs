use std::{num::NonZeroUsize, sync::Mutex};

use lru::LruCache;

pub struct Buffer {
    buffer: Mutex<LruCache<String, Vec<u8>>>,
}

impl Buffer {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            buffer: Mutex::new(LruCache::new(capacity)),
        }
    }
    pub fn insert(&self, k: String, v: Vec<u8>) {
        self.buffer.lock().unwrap().put(k, v);
    }

    pub fn pop(&self, k: &String) -> Option<Vec<u8>> {
        self.buffer.lock().unwrap().pop(k)
    }
}
