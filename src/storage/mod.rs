pub mod file;

use async_trait::async_trait;
use tokio::io::Result;

/// Manages IO operations for storage.
#[async_trait]
pub trait StorageManager {
    type Manager;

    /// Opens a handle to `path`.
    async fn open(path: &str) -> Result<Self::Manager>;

    /// Reads into `buffer` from `offset`.
    async fn read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<()>;

    /// Writes `buffer` to the `offset`.
    async fn write(&mut self, offset: u64, buffer: &[u8]) -> Result<()>;
}
