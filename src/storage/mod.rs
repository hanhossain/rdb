pub mod file;

use async_trait::async_trait;
#[cfg(test)]
use std::collections::HashMap;
use tokio::io::Result;
#[cfg(test)]
use tokio::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;

/// Manages IO operations for storage.
#[async_trait]
pub trait StorageManager {
    /// Reads into `buffer` from `offset`.
    async fn read(&self, path: &str, offset: u64, buffer: &mut [u8]) -> Result<()>;

    /// Writes `buffer` to the `offset`.
    async fn write(&self, path: &str, offset: u64, buffer: &[u8]) -> Result<()>;
}

#[cfg(test)]
/// In-memory storage manager used in tests.
pub struct InMemoryStorageManager(Mutex<HashMap<(String, u64), [u8; PAGE_SIZE]>>);

#[cfg(test)]
impl InMemoryStorageManager {
    pub fn new() -> Self {
        InMemoryStorageManager(Mutex::new(HashMap::new()))
    }
}

#[cfg(test)]
#[async_trait]
impl StorageManager for InMemoryStorageManager {
    async fn read(&self, path: &str, offset: u64, buffer: &mut [u8]) -> Result<()> {
        let mut lease = self.0.lock().await;
        let key = (path.to_string(), offset);

        if let Some(bytes) = lease.get(&key) {
            buffer.copy_from_slice(bytes);
            return Ok(());
        }

        let bytes = [0u8; PAGE_SIZE];
        lease.insert(key, bytes);
        buffer.copy_from_slice(&bytes);
        Ok(())
    }

    async fn write(&self, path: &str, offset: u64, buffer: &[u8]) -> Result<()> {
        let mut lease = self.0.lock().await;

        let key = (path.to_string(), offset);
        let mut bytes = [0u8; PAGE_SIZE];
        bytes.copy_from_slice(buffer);

        lease.insert(key, bytes);
        Ok(())
    }
}
