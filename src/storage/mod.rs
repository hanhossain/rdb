pub mod file;

use async_trait::async_trait;
use tokio::io::Result;

/// Manages IO operations for storage.
#[async_trait]
pub trait StorageManager {
    /// Creates an instance of `StorageManager`.
    fn new() -> Self;

    /// Reads into `buffer` from `offset`.
    async fn read(&mut self, path: &str, offset: u64, buffer: &mut [u8]) -> Result<()>;

    /// Writes `buffer` to the `offset`.
    async fn write(&mut self, path: &str, offset: u64, buffer: &[u8]) -> Result<()>;
}
