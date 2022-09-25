use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Result, SeekFrom};

/// Manages a file's IO operations.
pub struct FileManager {
    file: File,
}

impl FileManager {
    /// Opens a file handle to `path`. This will create the file if it does not exist.
    pub async fn open(path: impl AsRef<Path>) -> Result<FileManager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        Ok(FileManager { file })
    }

    /// Reads into `buffer` from `offset`.
    pub async fn read(&mut self, offset: u64, buffer: &mut [u8]) -> Result<()> {
        // go to the offset
        let _ = self.file.seek(SeekFrom::Start(offset)).await?;

        // read from the offset
        let _ = self.file.read_exact(buffer).await?;

        Ok(())
    }

    /// Writes `buffer` to the `offset`.
    pub async fn write(&mut self, offset: u64, buffer: &[u8]) -> Result<()> {
        // go to the offset
        let _ = self.file.seek(SeekFrom::Start(offset)).await?;

        // write at the offset
        self.file.write_all(buffer).await?;

        // flush to ensure we save to disk before we return
        self.flush().await
    }

    /// Flushes pending changes to disk. This will attempt to sync all OS-internal metadata.
    async fn flush(&mut self) -> Result<()> {
        self.file.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TestConfig {
        filepath: PathBuf,
    }

    impl TestConfig {
        fn generate() -> TestConfig {
            let file = Uuid::new_v4();
            let mut filepath = std::env::temp_dir();
            filepath.push(file.to_string());

            TestConfig { filepath }
        }
    }

    impl Drop for TestConfig {
        fn drop(&mut self) {
            std::fs::remove_file(&self.filepath).unwrap();
        }
    }

    #[tokio::test]
    async fn open() {
        let config = TestConfig::generate();

        FileManager::open(&config.filepath).await.unwrap();
    }

    #[tokio::test]
    async fn write_read_zero_offset() {
        let config = TestConfig::generate();
        let mut file = FileManager::open(&config.filepath).await.unwrap();

        let buffer = [0xDE, 0xAD, 0xBE, 0xEF];
        file.write(0, &buffer).await.unwrap();

        let mut buffer_read = [0u8; 4];
        file.read(0, &mut buffer_read).await.unwrap();

        assert_eq!(buffer, buffer_read);
    }

    #[tokio::test]
    async fn write_read_nonzero_offset() {
        let config = TestConfig::generate();
        let mut file = FileManager::open(&config.filepath).await.unwrap();

        let buffer = [0xDE, 0xAD, 0xBE, 0xEF];
        file.write(0, &buffer).await.unwrap();

        let mut buffer_read = [0u8; 2];
        file.read(1, &mut buffer_read).await.unwrap();

        assert_eq!(&buffer[1..3], &buffer_read);
    }
}
