use crate::storage::{StorageManager, PAGE_SIZE};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::io::Result;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Eq, PartialEq)]
pub struct Page {
    dirty: bool,
    buffer: [u8; PAGE_SIZE],
}

impl Page {
    fn new() -> Self {
        Page {
            dirty: false,
            buffer: [0u8; PAGE_SIZE],
        }
    }
}

/// An LRU page cache that supports flushing pages with async.
pub struct PageCache<T: StorageManager> {
    path: String,
    store: Mutex<LruCache<u64, Arc<RwLock<Page>>>>,
    storage_manager: T,
}

impl<T: StorageManager> PageCache<T> {
    /// Creates the LRU page cache with a max capacity of `capacity`. This will panic if the
    /// capacity is 0.
    pub fn new(storage_manager: T, path: &str, capacity: usize) -> Self {
        assert_ne!(capacity, 0, "Capacity cannot be 0.");

        PageCache {
            path: path.to_string(),
            store: Mutex::new(LruCache::new(NonZeroUsize::try_from(capacity).unwrap())),
            storage_manager,
        }
    }

    /// Gets a page from the cache.
    pub async fn get_page(&self, location: u64) -> Result<Arc<RwLock<Page>>> {
        let mut pages = self.store.lock().await;

        if !pages.contains(&location) && pages.len() == usize::from(pages.cap()) {
            if let Some((_, prev)) = pages.pop_lru() {
                // wait until no other threads have a reference to this page
                while Arc::strong_count(&prev) > 1 {
                    tokio::task::yield_now().await;
                }

                // Safety: This is the only reference to this page due to 1) this page was removed
                // from the lru while the lru was under an exclusive lock, and 2) we checked above
                // that no other threads have a reference to this page.
                let prev = unsafe { Arc::try_unwrap(prev).unwrap_unchecked() };
                let prev_page = prev.into_inner();
                if prev_page.dirty {
                    self.storage_manager
                        .write(&self.path, location, &prev_page.buffer)
                        .await?;
                }
            }
        }

        if let Some(page) = pages.get(&location) {
            return Ok(Arc::clone(page));
        }

        let mut page = Page::new();
        self.storage_manager
            .read(&self.path, location, &mut page.buffer)
            .await?;

        let page = Arc::new(RwLock::new(page));
        pages.put(location, Arc::clone(&page));
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryStorageManager;

    #[test]
    #[should_panic]
    fn create_page_cache_zero_capacity() {
        let storage_manager = InMemoryStorageManager::new();
        let _ = PageCache::new(storage_manager, "test", 0);
    }

    #[tokio::test]
    async fn get_single_page() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        page_cache.get_page(0).await.unwrap();

        assert_eq!(page_cache.store.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn get_same_page_twice() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);
        let page1 = page_cache.get_page(0).await.unwrap();
        let page2 = page_cache.get_page(0).await.unwrap();

        assert_eq!(*page1.read().await, *page2.read().await);
    }

    #[tokio::test]
    async fn get_and_evict_page() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);

        // get and release page 1
        let _ = page_cache.get_page(1).await.unwrap();

        let page2 = page_cache.get_page(2).await.unwrap();
        {
            let mut lease = page2.write().await;
            lease.dirty = true;
            lease.buffer[0] = 2;
        }

        let page3 = page_cache.get_page(3).await.unwrap();
        {
            let mut lease = page3.write().await;
            lease.dirty = true;
            lease.buffer[0] = 3;
        }

        let mut pages = page_cache.store.lock().await;

        // verify page 0 was evicted
        assert_eq!(2, pages.len());
        assert!(pages.get(&0).is_none());

        // verify pages 1 and 2 are still cached
        assert_eq!(2, pages.get(&2).unwrap().read().await.buffer[0]);
        assert_eq!(3, pages.get(&3).unwrap().read().await.buffer[0]);
    }

    #[tokio::test]
    async fn get_and_mutate_different_pages() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = Arc::new(PageCache::new(storage_manager, "test", 2));

        // insert two values into cache
        let _ = page_cache.get_page(0).await.unwrap();
        let _ = page_cache.get_page(1).await.unwrap();

        // get and mutate the values in parallel
        let cache1 = Arc::clone(&page_cache);
        let task1 = tokio::spawn(async move {
            let page = cache1.get_page(0).await.unwrap();
            let mut lease = page.write().await;
            lease.dirty = true;
            lease.buffer[..5].copy_from_slice(b"hello");
        });

        let cache2 = Arc::clone(&page_cache);
        let task2 = tokio::spawn(async move {
            let page = cache2.get_page(1).await.unwrap();
            let mut lease = page.write().await;
            lease.dirty = true;
            lease.buffer[..5].copy_from_slice(b"world");
        });

        let _ = tokio::join!(task1, task2);

        // verify content was updated
        assert_eq!(
            &page_cache.get_page(0).await.unwrap().read().await.buffer[..5],
            b"hello"
        );
        assert_eq!(
            &page_cache.get_page(1).await.unwrap().read().await.buffer[..5],
            b"world"
        );
    }

    #[tokio::test]
    async fn flush_on_evict() {
        let storage_manager = InMemoryStorageManager::new();
        let page_cache = PageCache::new(storage_manager, "test", 2);

        // insert two values into cache and mark as dirty
        let page1 = {
            let page = &page_cache.get_page(0).await.unwrap();
            page.write().await.dirty = true;
            Arc::downgrade(page)
        };
        let page2 = {
            let page = &page_cache.get_page(1).await.unwrap();
            page.write().await.dirty = true;
            Arc::downgrade(page)
        };

        // insert third value into cache to evict the first one
        let page3 = Arc::downgrade(&page_cache.get_page(2).await.unwrap());

        // verify first page flushed
        assert!(page1.upgrade().is_none());

        // verify other pages did not
        assert!(page2.upgrade().is_some());
        assert!(page3.upgrade().is_some());
    }
}
