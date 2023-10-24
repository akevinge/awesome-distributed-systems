use std::{collections::HashMap as StdHashMap, hash::Hash, sync::Mutex};

#[derive(Debug, Default)]
pub struct ConcurrentHashMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    hash_map: Mutex<StdHashMap<K, V>>,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            hash_map: Mutex::new(StdHashMap::new()),
        }
    }
    pub fn len(&self) -> usize {
        self.hash_map.lock().unwrap().len()
    }

    pub fn insert(&self, key: K, value: V) {
        self.hash_map.lock().unwrap().insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.hash_map.lock().unwrap().get(key).cloned()
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.hash_map.lock().unwrap().remove(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.hash_map.lock().unwrap().contains_key(key)
    }
}
