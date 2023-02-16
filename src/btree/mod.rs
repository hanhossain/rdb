mod node;

use crate::page::PageCache;
use crate::storage::StorageManager;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct BTreeStoreInner<S: StorageManager> {
    _page_cache: Arc<PageCache<S>>,
    /// Location of the last page. Allows us to quickly create a new page.
    _last_page_start: u64,
}

/// A B+-Tree used to store most data.
#[derive(Debug)]
pub struct BTreeStore<S: StorageManager>(Arc<RwLock<BTreeStoreInner<S>>>);

impl<S: StorageManager> BTreeStore<S> {
    /// Creates a BTreeStore.
    pub fn new(page_cache: Arc<PageCache<S>>) -> Self {
        let inner = BTreeStoreInner {
            _page_cache: page_cache,
            _last_page_start: 0,
        };
        BTreeStore(Arc::new(RwLock::new(inner)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::InMemoryStorageManager;

    #[test]
    fn create_store() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        let _ = BTreeStore::new(Arc::new(page_cache));
    }
}
