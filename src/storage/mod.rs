pub mod file;

use async_trait::async_trait;
use tokio::io::Result;

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
pub mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[cfg(test)]
    #[derive(Clone, Debug)]
    /// In-memory storage manager used in tests.
    pub struct InMemoryStorageManager(pub Arc<Mutex<HashMap<(String, u64), [u8; PAGE_SIZE]>>>);

    #[cfg(test)]
    impl InMemoryStorageManager {
        pub fn new() -> Self {
            InMemoryStorageManager(Arc::new(Mutex::new(HashMap::new())))
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
}
