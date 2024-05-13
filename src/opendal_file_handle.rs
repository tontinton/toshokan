use std::{fs::metadata, ops::Range, path::Path};

use opendal::BlockingReader;
use tantivy::{
    directory::{FileHandle, OwnedBytes},
    HasLen,
};

pub struct OpenDalFileHandle {
    size: u64,
    reader: BlockingReader,
}

impl OpenDalFileHandle {
    pub fn from_path(path: &Path, reader: BlockingReader) -> std::io::Result<Self> {
        let size = metadata(path)?.len();
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
            .read_into(&mut bytes, range.start as u64..range.end as u64)
            .map_err(Into::<std::io::Error>::into)?;
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
    use std::io::Write;

    use opendal::{layers::BlockingLayer, BlockingOperator, Operator};
    use tempfile::NamedTempFile;

    use crate::RUNTIME;

    use super::*;

    #[test]
    fn opendal_reader_read_file() -> anyhow::Result<()> {
        let mut file = NamedTempFile::new()?;

        file.write_all(b"abcdefgh")?;
        let path = file.into_temp_path();
        let path_buf = path.to_path_buf();

        let mut builder = opendal::services::Fs::default();
        builder.root(path_buf.parent().unwrap().to_str().unwrap());

        let _guard = RUNTIME.enter();
        let op: BlockingOperator = Operator::new(builder)?
            .layer(BlockingLayer::create()?)
            .finish()
            .blocking();

        let reader = OpenDalFileHandle::from_path(
            &path,
            op.reader_with(&path_buf.file_name().unwrap().to_str().unwrap())
                .call()?,
        )?;

        assert_eq!(
            reader.read_bytes(0..reader.len())?.to_vec(),
            b"abcdefgh".to_vec()
        );

        path.close()?;

        Ok(())
    }
}
