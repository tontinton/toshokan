use std::ops::Range;

use async_trait::async_trait;
use opendal::Reader;
use tantivy::{
    directory::{FileHandle, OwnedBytes},
    HasLen,
};
use tokio::runtime::Handle;

pub struct OpenDalFileHandle {
    handle: Handle,
    reader: Reader,
    size: usize,
}

impl OpenDalFileHandle {
    pub fn new(handle: Handle, reader: Reader, size: usize) -> Self {
        Self {
            handle,
            reader,
            size,
        }
    }
}

impl std::fmt::Debug for OpenDalFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OpenDalFileHandle({})", &self.size)
    }
}

#[async_trait]
impl FileHandle for OpenDalFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        self.handle.block_on(self.read_bytes_async(range))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let mut bytes = Vec::new();
        let size = self
            .reader
            .read_into(&mut bytes, range.start as u64..range.end as u64)
            .await?;
        assert_eq!(size, bytes.len());
        Ok(OwnedBytes::new(bytes))
    }
}

impl HasLen for OpenDalFileHandle {
    fn len(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use async_tempfile::TempFile;
    use color_eyre::eyre::Result;
    use opendal::Operator;
    use tokio::{io::AsyncWriteExt, task::spawn_blocking};

    use super::*;

    #[tokio::test]
    async fn opendal_reader_read_file() -> Result<()> {
        let mut file = TempFile::new().await?;

        file.write_all(b"abcdefgh").await?;
        let path = file.file_path();

        let mut builder = opendal::services::Fs::default();
        builder.root(path.parent().unwrap().to_str().unwrap());

        let op = Operator::new(builder)?.finish();

        let reader = OpenDalFileHandle::new(
            Handle::current(),
            op.reader_with(path.file_name().unwrap().to_str().unwrap())
                .await?,
            8,
        );

        let bytes = spawn_blocking(move || reader.read_bytes(0..reader.len())).await??;
        assert_eq!(bytes.to_vec(), b"abcdefgh".to_vec());

        Ok(())
    }
}
