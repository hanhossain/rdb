use crate::page::{start_of_page, PageCache};
use crate::storage::StorageManager;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::Result;
use tokio::sync::Mutex;

#[derive(Debug)]
struct KVStoreInner<T: StorageManager> {
    /// Maps the key to the location in storage.
    store: HashMap<String, u64>,
    page_cache: Arc<PageCache<T>>,
    next_location: u64,
}

/// A key-value store used for system metadata.
#[derive(Debug, Clone)]
pub struct KVStore<S: StorageManager>(Arc<Mutex<KVStoreInner<S>>>);

impl<S: StorageManager> KVStore<S> {
    /// Creates a KVStore.
    pub fn new(page_cache: Arc<PageCache<S>>) -> Self {
        let inner = KVStoreInner {
            store: HashMap::new(),
            page_cache,
            next_location: 0,
        };
        KVStore(Arc::new(Mutex::new(inner)))
    }

    pub async fn insert<T: Serialize + DeserializeOwned + Debug>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<()> {
        let mut lease = self.0.lock().await;
        let pair = KVPair {
            location: lease.next_location,
            key: key.to_string(),
            value: bincode::serialize(value).unwrap(),
        };

        let page_location = start_of_page(pair.location);
        let offset = (pair.location - page_location) as usize;
        let page = lease.page_cache.get_page(page_location).await?;
        let mut page = page.write().await;
        let buffer = page.buffer_mut();

        let size = bincode::serialized_size(&pair).unwrap();
        bincode::serialize_into(&mut buffer[offset..offset + size as usize], &pair).unwrap();

        lease.store.insert(key.to_string(), pair.location);
        lease.next_location += size;

        Ok(())
    }

    pub async fn get<T: DeserializeOwned + Debug>(&self, key: &str) -> Result<Option<T>> {
        let lease = self.0.lock().await;
        let result = match lease.store.get(key) {
            None => None,
            Some(location) => {
                let page_location = start_of_page(*location);
                let page = lease.page_cache.get_page(page_location).await?;
                let page = page.read().await;
                let offset = (*location - page_location) as usize;
                let pair: KVPair = bincode::deserialize(&page.buffer()[offset..]).unwrap();
                let value: T = bincode::deserialize(&pair.value).unwrap();
                Some(value)
            }
        };
        Ok(result)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct KVPair {
    #[serde(skip)]
    location: u64,
    key: String,
    value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::InMemoryStorageManager;

    #[test]
    fn create_kv_store() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        let _ = KVStore::new(Arc::new(page_cache));
    }

    #[tokio::test]
    async fn get_and_insert_struct() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct TestContent {
            int: i32,
        }

        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        // insert 1
        let expected1 = TestContent { int: 1 };
        kv_store.insert("expected1", &expected1).await.unwrap();

        // insert 2
        let expected2 = TestContent { int: 2 };
        kv_store.insert("expected2", &expected2).await.unwrap();

        // Flush page cache. This should ensure all TestContent is the InMemoryStorageManager.
        page_cache.flush().await.unwrap();
        let actual1: TestContent = kv_store.get("expected1").await.unwrap().unwrap();
        assert_eq!(expected1, actual1);

        let actual2: TestContent = kv_store.get("expected2").await.unwrap().unwrap();
        assert_eq!(expected2, actual2);
    }
}
