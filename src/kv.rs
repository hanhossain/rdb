use crate::page::{start_of_page, PageCache, HEADER_SIZE};
use crate::storage::{StorageManager, PAGE_SIZE};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;
use tokio::io::Result;
use tokio::sync::{Mutex, MutexGuard};

const KEY_SIZE_SIZE: usize = size_of::<u64>();
const VALUE_SIZE_SIZE: usize = size_of::<u64>();

/// The serialized key-value pair.
///
/// Memory layout:
/// | key size | key | value size | value |
#[derive(Debug, Serialize, Deserialize)]
struct KVPair {
    #[serde(skip)]
    location: u64,
    key: String,
    value: Vec<u8>,
}

impl KVPair {
    fn is_empty(&self) -> bool {
        self.key.is_empty() && self.value.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct KVEntryContext {
    pub location: u64,
    pub size: u64,
}

#[derive(Debug)]
struct KVStoreInner<T: StorageManager> {
    /// Maps the key to the location in storage.
    store: HashMap<String, u64>,
    page_cache: Arc<PageCache<T>>,
    next_location: u64,
}

// TODO: need to be able to save entries larger than 4096 bytes
/// A key-value store used for system metadata.
#[derive(Debug, Clone)]
pub struct KVStore<S: StorageManager>(Arc<Mutex<KVStoreInner<S>>>);

impl<S: StorageManager> KVStore<S> {
    /// Creates a KVStore.
    pub fn new(page_cache: Arc<PageCache<S>>) -> Self {
        let inner = KVStoreInner {
            store: HashMap::new(),
            page_cache,
            next_location: HEADER_SIZE as u64,
        };
        KVStore(Arc::new(Mutex::new(inner)))
    }

    pub async fn open(page_cache: Arc<PageCache<S>>) -> Result<Self> {
        let mut page_location = 0;
        let mut next_location = HEADER_SIZE as u64;
        let mut store = HashMap::new();

        loop {
            let page = page_cache.get_page(page_location).await?;

            // exclusive lease b/c we don't need anything else reading it while we open the store
            let lease = page.write().await;

            // Page is only empty if it's the last page
            if lease.is_empty() {
                break;
            }

            let buffer = lease.buffer();
            let mut offset = HEADER_SIZE;

            loop {
                let pair: KVPair = bincode::deserialize(&buffer[offset..]).unwrap();
                if pair.is_empty() {
                    break;
                }

                if start_of_page(next_location) < page_location {
                    next_location = page_location + HEADER_SIZE as u64;
                }

                store.insert(pair.key, next_location);

                // figure out where the next pair is
                let key_size =
                    u64::from_ne_bytes(buffer[offset..offset + KEY_SIZE_SIZE].try_into().unwrap());
                let value_index = offset + KEY_SIZE_SIZE + key_size as usize;
                let value_size = u64::from_ne_bytes(
                    buffer[value_index..value_index + VALUE_SIZE_SIZE]
                        .try_into()
                        .unwrap(),
                );
                let size =
                    KEY_SIZE_SIZE + key_size as usize + VALUE_SIZE_SIZE + value_size as usize;
                offset += size;
                next_location += size as u64;
            }

            page_location += PAGE_SIZE as u64;
        }

        let inner = KVStoreInner {
            next_location,
            page_cache,
            store,
        };
        Ok(KVStore(Arc::new(Mutex::new(inner))))
    }

    pub async fn insert<T: Serialize + DeserializeOwned + PartialEq>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<Option<KVEntryContext>> {
        let mut lease = self.0.lock().await;

        if let Some(existing_item) = self.get_internal::<T>(key, &lease).await? {
            if &existing_item == value {
                return Ok(None);
            }
        }

        let mut pair = KVPair {
            location: lease.next_location,
            key: key.to_string(),
            value: bincode::serialize(value).unwrap(),
        };

        let mut page_location = start_of_page(pair.location);
        let mut offset = (pair.location - page_location) as usize;
        let size = bincode::serialized_size(&pair).unwrap();

        if offset + size as usize >= PAGE_SIZE {
            // next page
            page_location += PAGE_SIZE as u64;

            // pair will be at the start of the next page
            pair.location = page_location + HEADER_SIZE as u64;
            offset = HEADER_SIZE;
        }

        let page = lease.page_cache.get_page(page_location).await?;
        let mut page = page.write().await;
        page.header.size += size as u16;
        let header = page.header;
        let buffer = page.buffer_mut();

        bincode::serialize_into(&mut buffer[offset..offset + size as usize], &pair).unwrap();
        bincode::serialize_into(&mut buffer[..HEADER_SIZE], &header).unwrap();

        lease.store.insert(key.to_string(), pair.location);
        lease.next_location = pair.location + size;

        Ok(Some(KVEntryContext {
            size,
            location: pair.location,
        }))
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let lease = self.0.lock().await;
        self.get_internal(key, &lease).await
    }

    async fn get_internal<'a, T: DeserializeOwned>(
        &'a self,
        key: &str,
        lease: &MutexGuard<'a, KVStoreInner<S>>,
    ) -> Result<Option<T>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::InMemoryStorageManager;
    use tokio::sync::Notify;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct TestContent {
        int: i32,
    }

    #[test]
    fn create_kv_store() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        let _ = KVStore::new(Arc::new(page_cache));
    }

    #[tokio::test]
    async fn get_and_insert_struct() {
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
        let flushed_count = page_cache.flush().await.unwrap();
        assert_eq!(flushed_count, 1);

        let actual1: TestContent = kv_store.get("expected1").await.unwrap().unwrap();
        assert_eq!(expected1, actual1);

        let actual2: TestContent = kv_store.get("expected2").await.unwrap().unwrap();
        assert_eq!(expected2, actual2);
    }

    #[tokio::test]
    async fn insert_same_struct() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        // insert 1
        let expected = TestContent { int: 1 };
        kv_store
            .insert("expected", &expected)
            .await
            .unwrap()
            .unwrap();
        page_cache.flush().await.unwrap();

        // insert 2
        let context2 = kv_store.insert("expected", &expected).await.unwrap();
        page_cache.flush().await.unwrap();
        assert!(context2.is_none());
    }

    #[tokio::test]
    async fn insert_and_update_struct() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        // insert 1
        let original = TestContent { int: 1 };
        let orig_ctx = kv_store
            .insert("expected", &original)
            .await
            .unwrap()
            .unwrap();
        page_cache.flush().await.unwrap();

        let expected = TestContent { int: 42 };
        let new_ctx = kv_store
            .insert("expected", &expected)
            .await
            .unwrap()
            .unwrap();
        page_cache.flush().await.unwrap();

        // verify it's not using the same location on disk
        assert_ne!(new_ctx, orig_ctx);

        let actual: TestContent = kv_store.get("expected").await.unwrap().unwrap();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn load_from_file() {
        let storage_manager = InMemoryStorageManager::new();
        let expected = TestContent { int: 1 };

        {
            let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
            let kv_store = KVStore::new(page_cache.clone());

            // insert
            kv_store.insert("expected", &expected).await.unwrap();
            page_cache.flush().await.unwrap();

            // verify inserted
            let actual1: TestContent = kv_store.get("expected").await.unwrap().unwrap();
            assert_eq!(expected, actual1);
        }

        {
            let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
            let kv_store = KVStore::open(page_cache).await.unwrap();

            // verify expected exists
            let actual: TestContent = kv_store.get("expected").await.unwrap().unwrap();
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn overflow_into_second_page() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager, "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        let mut expected = Vec::new();

        // insert one
        let value = TestContent { int: 0 };
        let context = kv_store.insert("0", &value).await.unwrap().unwrap();
        expected.push(value);

        // calculate how many need to be inserted to overflow
        let page_data_size = PAGE_SIZE - HEADER_SIZE;
        let max_records_per_page = page_data_size as u64 / context.size;
        let total_records = max_records_per_page + 1;

        for i in 1..total_records {
            let value = TestContent { int: i as i32 };
            let _context = kv_store
                .insert(&i.to_string(), &value)
                .await
                .unwrap()
                .unwrap();
            expected.push(value);
        }
        let pages_flushed = page_cache.flush().await.unwrap();

        assert_eq!(pages_flushed, 2);

        let mut actual = Vec::new();
        for i in 0..total_records {
            let value: TestContent = kv_store.get(&i.to_string()).await.unwrap().unwrap();
            actual.push(value);
        }

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn load_two_pages_from_file() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let mut expected = Vec::new();

        let total_records = {
            let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
            let kv_store = KVStore::new(page_cache.clone());

            // insert one
            let value = TestContent { int: 0 };
            let context = kv_store.insert("0", &value).await.unwrap().unwrap();
            expected.push(value);

            // calculate how many need to be inserted to overflow
            let page_data_size = PAGE_SIZE - HEADER_SIZE;
            let max_records_per_page = page_data_size as u64 / context.size;
            let total_records = max_records_per_page + 1;

            for i in 1..total_records {
                let value = TestContent { int: i as i32 };
                kv_store
                    .insert(&i.to_string(), &value)
                    .await
                    .unwrap()
                    .unwrap();
                expected.push(value);
            }

            let pages_flushed = page_cache.flush().await.unwrap();
            assert_eq!(pages_flushed, 2);
            total_records
        };

        {
            let page_cache = Arc::new(PageCache::new(storage_manager, "test", 2));
            let kv_store = KVStore::open(page_cache).await.unwrap();

            let mut actual = Vec::new();
            for i in 0..total_records {
                let value: TestContent = kv_store.get(&i.to_string()).await.unwrap().unwrap();
                actual.push(value);
            }

            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn parallel_reads() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        // insert and flush
        let expected = TestContent { int: 42 };
        kv_store.insert("expected", &expected).await.unwrap();
        page_cache.flush().await.unwrap();

        let tx1 = Arc::new(Notify::new());
        let rx1 = tx1.clone();
        let tx2 = Arc::new(Notify::new());
        let rx2 = tx2.clone();

        let kv_store1 = kv_store.clone();
        let t1 = tokio::spawn(async move {
            let actual: TestContent = kv_store1.get("expected").await.unwrap().unwrap();
            tx1.notify_one();
            rx2.notified().await;
            actual
        });
        let t2 = tokio::spawn(async move {
            let actual: TestContent = kv_store.get("expected").await.unwrap().unwrap();
            tx2.notify_one();
            rx1.notified().await;
            actual
        });

        let (actual1, actual2) = tokio::join!(t1, t2);
        assert_eq!(actual1.unwrap(), expected);
        assert_eq!(actual2.unwrap(), expected);
    }
}
