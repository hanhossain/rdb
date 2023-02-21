mod node;
mod tuple;

use crate::btree::node::NodeRefMut;
use crate::btree::tuple::Tuple;
use crate::kv::KVStore;
use crate::page::PageCache;
use crate::schema::Schema;
use crate::storage::StorageManager;
use std::sync::Arc;
use tokio::io::Result;
use tokio::sync::RwLock;

const RECORD_DATA_NEW_PAGE_START_KEY: &str = "RECORD_DATA_NEW_PAGE_START";

#[derive(Debug)]
struct BTreeStoreInner<S: StorageManager> {
    page_cache: Arc<PageCache<S>>,
    /// Location of the next new page. Allows us to quickly create a new page.
    new_page_start: u64,
    /// Store to handle metadata.
    kv_store: Arc<KVStore<S>>,
}

/// A B+-Tree used to store most data.
#[derive(Debug)]
pub struct BTreeStore<S: StorageManager>(Arc<RwLock<BTreeStoreInner<S>>>);

impl<S: StorageManager> BTreeStore<S> {
    /// Creates a BTreeStore.
    pub async fn new(page_cache: Arc<PageCache<S>>, kv_store: Arc<KVStore<S>>) -> Result<Self> {
        let new_page_start =
            if let Some(page_start) = kv_store.get(RECORD_DATA_NEW_PAGE_START_KEY).await? {
                page_start
            } else {
                let page_start = 0;
                kv_store
                    .put(RECORD_DATA_NEW_PAGE_START_KEY, &page_start)
                    .await?;
                page_start
            };
        let inner = BTreeStoreInner {
            page_cache,
            new_page_start,
            kv_store,
        };
        Ok(BTreeStore(Arc::new(RwLock::new(inner))))
    }

    pub async fn insert(&self, tuple: Tuple, schema: &Schema) -> Result<()> {
        let lease = self.0.write().await;
        let page = lease.page_cache.get_page(0).await?;
        let mut page = page.write().await;
        let mut node = NodeRefMut::from_page(&mut page, schema);
        node.insert(tuple, schema);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::node::leaf;
    use crate::page;
    use crate::schema::{Column, DataType};
    use crate::storage::tests::InMemoryStorageManager;

    #[tokio::test]
    async fn create_store() {
        let storage_manager = InMemoryStorageManager::new();
        let kv_store = Arc::new(KVStore::new(Arc::new(PageCache::new(
            storage_manager.clone(),
            "metadata",
            2,
        ))));
        let page_cache = PageCache::new(storage_manager.clone(), "test", 2);
        let _ = BTreeStore::new(Arc::new(page_cache), kv_store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn insert_root() {
        let storage_manager = InMemoryStorageManager::new();
        let kv_store = Arc::new(KVStore::new(Arc::new(PageCache::new(
            storage_manager.clone(),
            "metadata",
            2,
        ))));
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let btree = BTreeStore::new(Arc::clone(&page_cache), kv_store)
            .await
            .unwrap();

        let schema = Schema::new(vec![Column::new("c1", DataType::Int32)], "c1");
        let tuple = Tuple {
            columns: vec![tuple::Column::Int32(3)],
        };

        btree.insert(tuple, &schema).await.unwrap();
        assert_eq!(page_cache.flush().await.unwrap(), 1);

        let page = page_cache.get_page(0).await.unwrap();
        let page = page.read().await;
        let buffer = &page.buffer()[page::HEADER_SIZE + node::HEADER_SIZE..];
        assert_eq!(buffer[0], 1);
        assert_eq!(buffer[leaf::header::HEADER_SIZE], 3);
    }
}
