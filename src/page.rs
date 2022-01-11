use async_trait::async_trait;
use lru::LruCache;
use std::sync::Arc;
use tokio::sync::Mutex;

/// An LRU page cache that supports flushing pages with async.
pub struct PageCache<T: Paged> {
    content: Mutex<PageCacheContent<T>>,
}

impl<T: Paged> PageCache<T> {
    /// Creates the LRU page cache with a max capacity of `capacity`. This will panic if the
    /// capacity is 0.
    pub fn new(capacity: usize) -> PageCache<T> {
        if capacity == 0 {
            panic!("Capacity cannot be 0.");
        }

        PageCache {
            content: Mutex::new(PageCacheContent {
                cache: LruCache::new(capacity),
            }),
        }
    }

    /// Gets a page from the cache.
    pub async fn get_page(&mut self, location: usize) -> Arc<T> {
        let mut cache_content = self.content.lock().await;
        match cache_content.cache.get(&location) {
            Some(page) => page.clone(),
            None => {
                let page = Arc::new(T::open(location).await);
                cache_content.cache.put(location, page.clone());
                page
            }
        }
    }
}

struct PageCacheContent<T: Paged> {
    cache: LruCache<usize, Arc<T>>,
}

#[async_trait]
pub trait Paged {
    async fn open(location: usize) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Page;

    #[async_trait]
    impl Paged for Page {
        async fn open(_location: usize) -> Page {
            Page {}
        }
    }

    #[tokio::test]
    async fn get_single_page() {
        let mut page_cache: PageCache<Page> = PageCache::new(2);
        page_cache.get_page(0).await;

        assert_eq!(page_cache.content.get_mut().cache.len(), 1);
    }

    #[tokio::test]
    async fn get_same_page_twice() {
        let mut page_cache: PageCache<Page> = PageCache::new(2);
        let page1 = page_cache.get_page(0).await;
        let page2 = page_cache.get_page(0).await;

        assert_eq!(*page1, *page2);
    }

    #[tokio::test]
    async fn get_and_evict_page() {
        let mut page_cache: PageCache<Page> = PageCache::new(2);

        {
            // get and release page 0
            page_cache.get_page(0).await;
        }

        let page2 = page_cache.get_page(1).await;
        let page3 = page_cache.get_page(2).await;

        let pages = &mut page_cache.content.get_mut().cache;

        // verify page 0 was evicted
        assert_eq!(2, pages.len());
        assert_eq!(None, pages.get(&0));

        // verify pages 1 and 2 are still cached
        assert_eq!(Some(&page2), pages.get(&1));
        assert_eq!(Some(&page3), pages.get(&2));
    }
}
