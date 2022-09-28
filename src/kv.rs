use crate::page::PageCache;
use crate::storage::StorageManager;
use std::collections::HashMap;

/// A key-value store used for system metadata.
#[derive(Debug)]
pub struct KVStore<T: StorageManager> {
    /// Maps the key to the location in storage.
    _store: HashMap<String, u64>,
    _page_cache: PageCache<T>,
}

impl<T: StorageManager> KVStore<T> {
    /// Creates a KVStore.
    pub fn new(page_cache: PageCache<T>) -> Self {
        KVStore {
            _store: HashMap::new(),
            _page_cache: page_cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::InMemoryStorageManager;

    #[test]
    fn create_kv_store() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        let _ = KVStore::new(page_cache);
    }
}
