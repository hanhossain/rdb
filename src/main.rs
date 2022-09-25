#![warn(clippy::pedantic)]
use async_trait::async_trait;
use rdb::file::FileManager;
use rdb::page::{PageCache, Paged};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    println!("Hello, world!");

    // get a 4KiB buffer and write a string to it
    let page_cache: PageCache<Page> = PageCache::new(1);
    let size = {
        let page = page_cache.get_page(0).await;
        let mut lease = page.write().await;
        let buffer = b"hello world";
        lease.file.write(0, buffer).await?;
        buffer.len()
    };
    {
        let page = page_cache.get_page(0).await;
        let mut lease = page.write().await;
        let mut buffer = vec![0u8; size];
        lease.file.read(0, &mut buffer).await?;
        let s = std::str::from_utf8(&buffer).unwrap();
        println!("{}", &s[..size]);
    }
    Ok(())
}

struct Page {
    file: FileManager,
}

#[async_trait]
impl Paged for Page {
    async fn open(_location: u64) -> Self {
        let file = FileManager::open("test.dat").await.unwrap();
        Page { file }
    }

    async fn close(self) {}
}
