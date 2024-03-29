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
const IS_DELETED_SIZE: usize = size_of::<bool>();

/// The serialized key-value pair.
///
/// Memory layout:
/// | key size | key | value size | value | is_deleted |
#[derive(Debug, Serialize, Deserialize)]
struct KVPair {
    #[serde(skip)]
    location: u64,
    key: String,
    value: Vec<u8>,
    /// Tombstone to denote whether the record was removed.
    is_deleted: bool,
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
#[derive(Debug)]
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
            let page_length = HEADER_SIZE + lease.header.size as usize;

            loop {
                // if there's mo more data in this page we can finish reading it
                if offset == page_length {
                    break;
                }

                let pair: KVPair = bincode::deserialize(&buffer[offset..page_length]).unwrap();
                if pair.is_empty() {
                    break;
                }

                if start_of_page(next_location) < page_location {
                    next_location = page_location + HEADER_SIZE as u64;
                }

                if !pair.is_deleted {
                    store.insert(pair.key, next_location);
                }

                // figure out where the next pair is
                let key_size =
                    u64::from_ne_bytes(buffer[offset..offset + KEY_SIZE_SIZE].try_into().unwrap());
                let value_index = offset + KEY_SIZE_SIZE + key_size as usize;
                let value_size = u64::from_ne_bytes(
                    buffer[value_index..value_index + VALUE_SIZE_SIZE]
                        .try_into()
                        .unwrap(),
                );
                let size = KEY_SIZE_SIZE
                    + key_size as usize
                    + VALUE_SIZE_SIZE
                    + value_size as usize
                    + IS_DELETED_SIZE;
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

    /// Inserts the key-value pair if the key does not exist, then returns the location of the record.
    /// Otherwise, if the key exists and
    /// - the value matches, then returns `None`.
    /// - the value does not match, then inserts the updated value and returns the location of the record.
    pub async fn put<T: Serialize + DeserializeOwned + PartialEq>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<Option<KVEntryContext>> {
        let mut lease = self.0.lock().await;

        if let Some(mut existing_record) = self.get_internal(key, &lease).await? {
            let existing_item: T = bincode::deserialize(&existing_record.value).unwrap();
            if &existing_item == value {
                return Ok(None);
            }

            existing_record.is_deleted = true;
            let page = lease.page_cache.get_page(existing_record.location).await?;
            let mut page = page.write().await;
            let offset = existing_record.location % PAGE_SIZE as u64;
            let offset_end = offset + bincode::serialized_size(&existing_record).unwrap();
            bincode::serialize_into(
                &mut page.buffer_mut()[offset as usize..offset_end as usize],
                &existing_record,
            )
            .unwrap();
        }

        let mut pair = KVPair {
            location: lease.next_location,
            key: key.to_string(),
            value: bincode::serialize(value).unwrap(),
            is_deleted: false,
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
        let entry = self
            .get_internal(key, &lease)
            .await?
            .map(|kvp| bincode::deserialize(&kvp.value).unwrap());
        Ok(entry)
    }

    /// Removes a key from the KVStore. Returns the original object if it existed.
    pub async fn remove(&self, key: &str) -> Result<bool> {
        let mut lease = self.0.lock().await;
        if let Some(mut entry) = self.get_internal(key, &lease).await? {
            if !entry.is_deleted {
                entry.is_deleted = true;
                let page = lease.page_cache.get_page(entry.location).await?;
                let mut page = page.write().await;

                let buffer = page.buffer_mut();
                let offset = entry.location % PAGE_SIZE as u64;
                let size = bincode::serialized_size(&entry).unwrap() as usize;

                bincode::serialize_into(
                    &mut buffer[offset as usize..offset as usize + size],
                    &entry,
                )
                .unwrap();
                lease.store.remove(key);
                Ok(true)
            } else {
                unreachable!();
            }
        } else {
            Ok(false)
        }
    }

    async fn get_internal<'a>(
        &'a self,
        key: &str,
        lease: &MutexGuard<'a, KVStoreInner<S>>,
    ) -> Result<Option<KVPair>> {
        let result = match lease.store.get(key) {
            None => None,
            Some(location) => {
                let page_location = start_of_page(*location);
                let page = lease.page_cache.get_page(page_location).await?;
                let page = page.read().await;
                let offset = (*location - page_location) as usize;
                let mut pair: KVPair = bincode::deserialize(&page.buffer()[offset..]).unwrap();
                pair.location = *location;
                Some(pair)
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
        kv_store.put("expected1", &expected1).await.unwrap();

        // insert 2
        let expected2 = TestContent { int: 2 };
        kv_store.put("expected2", &expected2).await.unwrap();

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
        kv_store.put("expected", &expected).await.unwrap().unwrap();
        page_cache.flush().await.unwrap();

        // insert 2
        let context2 = kv_store.put("expected", &expected).await.unwrap();
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
        let orig_ctx = kv_store.put("expected", &original).await.unwrap().unwrap();
        page_cache.flush().await.unwrap();

        let expected = TestContent { int: 42 };
        let new_ctx = kv_store.put("expected", &expected).await.unwrap().unwrap();
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
            kv_store.put("expected", &expected).await.unwrap();
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
        let context = kv_store.put("0", &value).await.unwrap().unwrap();
        expected.push(value);

        // calculate how many need to be inserted to overflow
        let page_data_size = PAGE_SIZE - HEADER_SIZE;
        let max_records_per_page = page_data_size as u64 / context.size;
        let total_records = max_records_per_page + 1;

        for i in 1..total_records {
            let value = TestContent { int: i as i32 };
            let _context = kv_store.put(&i.to_string(), &value).await.unwrap().unwrap();
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
            let context = kv_store.put("0", &value).await.unwrap().unwrap();
            expected.push(value);

            // calculate how many need to be inserted to overflow
            let page_data_size = PAGE_SIZE - HEADER_SIZE;
            let max_records_per_page = page_data_size as u64 / context.size;
            let total_records = max_records_per_page + 1;

            for i in 1..total_records {
                let value = TestContent { int: i as i32 };
                kv_store.put(&i.to_string(), &value).await.unwrap().unwrap();
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
        let kv_store = Arc::new(KVStore::new(page_cache.clone()));

        // insert and flush
        let expected = TestContent { int: 42 };
        kv_store.put("expected", &expected).await.unwrap();
        page_cache.flush().await.unwrap();

        let tx1 = Arc::new(Notify::new());
        let rx1 = tx1.clone();
        let tx2 = Arc::new(Notify::new());
        let rx2 = tx2.clone();

        let kv_store1 = Arc::clone(&kv_store);
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

    #[tokio::test]
    async fn remove_item() {
        // initialize kv store
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
        let kv_store = KVStore::new(page_cache.clone());

        // insert 1
        let expected1 = TestContent { int: 1 };
        kv_store.put("expected1", &expected1).await.unwrap();

        // insert 2
        let expected2 = TestContent { int: 2 };
        kv_store.put("expected2", &expected2).await.unwrap();

        let flushed_count = page_cache.flush().await.unwrap();
        assert_eq!(flushed_count, 1);

        // remove 1
        kv_store.remove("expected1").await.unwrap();

        let flushed_count = page_cache.flush().await.unwrap();
        assert_eq!(flushed_count, 1);

        let actual1: Option<TestContent> = kv_store.get("expected1").await.unwrap();
        assert_eq!(actual1, None);

        let actual2: TestContent = kv_store.get("expected2").await.unwrap().unwrap();
        assert_eq!(expected2, actual2);
    }

    #[tokio::test]
    async fn remove_then_load_from_file() {
        let storage_manager = InMemoryStorageManager::new();
        let expected = TestContent { int: 1 };

        {
            let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
            let kv_store = KVStore::new(page_cache.clone());

            // insert 1
            kv_store.put("expected1", &expected).await.unwrap();

            // insert 2
            kv_store.put("expected2", &expected).await.unwrap();

            let removed = kv_store.remove("expected1").await.unwrap();
            assert!(removed);

            // flush page
            let pages_flushed = page_cache.flush().await.unwrap();
            assert_eq!(pages_flushed, 1);
        }

        {
            let page_cache = Arc::new(PageCache::new(storage_manager.clone(), "test", 2));
            let kv_store = KVStore::open(page_cache).await.unwrap();

            // item 1 should not exist
            let actual: Option<TestContent> = kv_store.get("expected1").await.unwrap();
            assert_eq!(actual, None);

            // item 2 should exist
            let actual: TestContent = kv_store.get("expected2").await.unwrap().unwrap();
            assert_eq!(expected, actual);
        }
    }
}
