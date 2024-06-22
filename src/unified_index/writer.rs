use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    ops::Range,
    path::{Path, PathBuf},
};

use bincode::Options;
use color_eyre::eyre::{bail, Result};
use futures::future::try_join_all;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWrite},
};

use crate::bincode::bincode_options;

use super::{FileCache, IndexFooter};

struct FileReader {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    file_name: PathBuf,
}

impl FileReader {
    async fn from_path(dir: &Path, file_name: PathBuf) -> std::io::Result<Self> {
        Ok(Self::new(
            Box::new(File::open(dir.join(&file_name)).await?),
            file_name,
        ))
    }

    fn new(reader: Box<dyn AsyncRead + Send + Unpin>, file_name: PathBuf) -> Self {
        Self { reader, file_name }
    }
}

pub struct UnifiedIndexWriter {
    file_readers: Vec<FileReader>,
    file_offsets: HashMap<PathBuf, Range<u64>>,
}

impl UnifiedIndexWriter {
    pub async fn from_file_paths(
        dir: &Path,
        file_names: HashSet<PathBuf>,
    ) -> std::io::Result<Self> {
        let file_readers = try_join_all(
            file_names
                .into_iter()
                .map(|file_name| FileReader::from_path(dir, file_name)),
        )
        .await?;
        Ok(Self::new(file_readers))
    }

    fn new(file_readers: Vec<FileReader>) -> Self {
        Self {
            file_readers,
            file_offsets: HashMap::new(),
        }
    }

    pub async fn write<W>(mut self, writer: &mut W, cache: FileCache) -> Result<(u64, u64)>
    where
        W: AsyncWrite + Unpin,
    {
        let mut written = 0u64;
        for mut file_reader in self.file_readers {
            let start = written;
            let file_name = file_reader.file_name;
            written += tokio::io::copy(&mut file_reader.reader, writer).await?;
            self.file_offsets.insert(file_name, start..written);
        }

        let footer_bytes =
            bincode_options().serialize(&IndexFooter::new(self.file_offsets, cache))?;
        let footer_len = footer_bytes.len() as u64;

        let footer_written = tokio::io::copy(&mut Cursor::new(footer_bytes), writer).await?;
        if footer_written < footer_len {
            bail!(
                "written less than expected: {} < {}",
                footer_written,
                footer_len,
            );
        }

        Ok((written + footer_len, footer_len))
    }

    #[cfg(test)]
    pub async fn write_without_cache<W>(self, writer: &mut W) -> Result<(u64, u64)>
    where
        W: AsyncWrite + Unpin,
    {
        use std::collections::BTreeMap;

        self.write(writer, BTreeMap::new()).await
    }
}

#[cfg(test)]
mod tests {
    use std::{io::SeekFrom, path::Path, sync::Arc};

    use async_tempfile::TempFile;
    use color_eyre::eyre::Result;
    use futures::try_join;
    use tantivy::{
        directory::{FileSlice, OwnedBytes},
        Directory,
    };
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    use crate::unified_index::unified_directory::UnifiedDirectory;

    use super::*;

    #[tokio::test]
    async fn unified_index_write_then_read_2_files() -> Result<()> {
        let mut file1 = TempFile::new().await?;
        let mut file2 = TempFile::new().await?;
        try_join!(file1.write_all(b"hello"), file2.write_all(b"world"))?;
        try_join!(
            file1.seek(SeekFrom::Start(0)),
            file2.seek(SeekFrom::Start(0))
        )?;

        let writer = UnifiedIndexWriter::new(vec![
            FileReader::new(Box::new(file1), PathBuf::from("a")),
            FileReader::new(Box::new(file2), PathBuf::from("b")),
        ]);

        let mut buf = vec![];
        let (_, footer_len) = writer.write_without_cache(&mut buf).await?;

        let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(buf)));
        let dir = UnifiedDirectory::open_with_len(file_slice, footer_len as usize)?;
        assert_eq!(dir.atomic_read(Path::new("a"))?, b"hello".to_vec());
        assert_eq!(dir.atomic_read(Path::new("b"))?, b"world".to_vec());

        Ok(())
    }
}
