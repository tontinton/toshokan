use std::{ops::Range, path::Path};

use opendal::BlockingReader;
use tantivy::{
    directory::{FileHandle, OwnedBytes},
    HasLen,
};
use tokio::fs::metadata;

pub struct OpenDalFileHandle {
    size: u64,
    reader: BlockingReader,
}

impl OpenDalFileHandle {
    pub async fn from_path(path: &Path, reader: BlockingReader) -> std::io::Result<Self> {
        let size = metadata(path).await?.len();
        Ok(Self::new(size, reader))
    }

    fn new(size: u64, reader: BlockingReader) -> Self {
        Self { size, reader }
    }
}

impl std::fmt::Debug for OpenDalFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FileReader({})", &self.size)
    }
}

impl FileHandle for OpenDalFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let mut bytes = Vec::new();
        self.reader
            .read_into(&mut bytes, range.start as u64..range.end as u64)?;
        Ok(OwnedBytes::new(bytes))
    }
}

impl HasLen for OpenDalFileHandle {
    fn len(&self) -> usize {
        self.size as usize
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

        let op = Operator::new(builder)?.finish().blocking();

        let reader = OpenDalFileHandle::from_path(
            path,
            op.reader_with(path.file_name().unwrap().to_str().unwrap())
                .call()?,
        )
        .await?;

        let bytes = spawn_blocking(move || reader.read_bytes(0..reader.len())).await??;
        assert_eq!(bytes.to_vec(), b"abcdefgh".to_vec());

        Ok(())
    }
}
