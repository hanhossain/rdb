use async_trait::async_trait;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// An LRU page cache that supports flushing pages with async.
pub struct PageCache<T: Paged>(Mutex<LruCache<usize, Arc<RwLock<T>>>>);

impl<T: Paged> PageCache<T> {
    /// Creates the LRU page cache with a max capacity of `capacity`. This will panic if the
    /// capacity is 0.
    pub fn new(capacity: usize) -> PageCache<T> {
        if capacity == 0 {
            panic!("Capacity cannot be 0.");
        }

        PageCache(Mutex::new(LruCache::new(capacity)))
    }

    /// Gets a page from the cache.
    pub async fn get_page(&self, location: usize) -> Arc<RwLock<T>> {
        let mut pages = self.0.lock().await;
        match pages.get(&location) {
            Some(page) => Arc::clone(page),
            None => {
                let page = Arc::new(RwLock::new(T::open(location).await));
                pages.put(location, Arc::clone(&page));
                page
            }
        }
    }
}

#[async_trait]
pub trait Paged {
    async fn open(location: usize) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Page<T>(Option<T>);

    #[async_trait]
    impl<T> Paged for Page<T> {
        async fn open(_location: usize) -> Page<T> {
            Page(None)
        }
    }

    #[tokio::test]
    async fn get_single_page() {
        let page_cache: PageCache<Page<()>> = PageCache::new(2);
        page_cache.get_page(0).await;

        assert_eq!(page_cache.0.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn get_same_page_twice() {
        let page_cache: PageCache<Page<()>> = PageCache::new(2);
        let page1 = page_cache.get_page(0).await;
        let page2 = page_cache.get_page(0).await;

        assert_eq!(*page1.read().await, *page2.read().await);
    }

    #[tokio::test]
    async fn get_and_evict_page() {
        let page_cache: PageCache<Page<i32>> = PageCache::new(2);

        {
            // get and release page 1
            page_cache.get_page(1).await;
        }

        let page2 = page_cache.get_page(2).await;
        {
            let mut lease = page2.write().await;
            lease.0 = Some(2);
        }

        let page3 = page_cache.get_page(3).await;
        {
            let mut lease = page3.write().await;
            lease.0 = Some(3);
        }

        let pages = &mut page_cache.0.lock().await;

        // verify page 0 was evicted
        assert_eq!(2, pages.len());
        assert!(pages.get(&0).is_none());

        // verify pages 1 and 2 are still cached
        assert_eq!(Some(2), pages.get(&2).unwrap().read().await.0);
        assert_eq!(Some(3), pages.get(&3).unwrap().read().await.0);
    }

    #[tokio::test]
    async fn get_and_mutate_different_pages() {
        let page_cache: Arc<PageCache<Page<String>>> = Arc::new(PageCache::new(2));

        // insert two values into cache
        let _ = page_cache.get_page(0).await;
        let _ = page_cache.get_page(1).await;

        // get and mutate the values in parallel
        let cache1 = Arc::clone(&page_cache);
        let task1 = tokio::spawn(async move {
            let x = cache1.get_page(0).await;
            let mut lease = x.write().await;
            lease.0 = Some(String::from("hello"));
        });

        let cache2 = Arc::clone(&page_cache);
        let task2 = tokio::spawn(async move {
            let x = cache2.get_page(1).await;
            let mut lease = x.write().await;
            lease.0 = Some(String::from("world"));
        });

        let _ = tokio::join!(task1, task2);

        // verify content was updated
        assert_eq!(
            page_cache.get_page(0).await.read().await.0,
            Some(String::from("hello"))
        );
        assert_eq!(
            page_cache.get_page(1).await.read().await.0,
            Some(String::from("world"))
        );
    }
}
