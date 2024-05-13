use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Cursor, Read},
    ops::Range,
    path::{Path, PathBuf},
};

use bincode::Options;
use color_eyre::eyre::{bail, Result};

use crate::bincode::bincode_options;

use super::IndexFooter;

struct FileReader {
    reader: Box<dyn Read>,
    file_name: PathBuf,
}

impl FileReader {
    fn from_path(dir: &Path, file_name: PathBuf) -> std::io::Result<Self> {
        Ok(Self::new(
            Box::new(File::open(dir.join(&file_name))?),
            file_name,
        ))
    }

    fn new(reader: Box<dyn Read>, file_name: PathBuf) -> Self {
        Self { reader, file_name }
    }
}

pub struct UnifiedIndexWriter {
    file_readers: Vec<FileReader>,
    file_offsets: HashMap<PathBuf, Range<u64>>,
}

impl UnifiedIndexWriter {
    pub fn from_file_paths(dir: &Path, file_names: HashSet<PathBuf>) -> std::io::Result<Self> {
        let file_readers = file_names
            .into_iter()
            .map(|path| FileReader::from_path(dir, path))
            .collect::<std::io::Result<Vec<FileReader>>>()?;
        Ok(Self::new(file_readers))
    }

    fn new(file_readers: Vec<FileReader>) -> Self {
        Self {
            file_readers,
            file_offsets: HashMap::new(),
        }
    }

    pub fn write<W: std::io::Write>(mut self, writer: &mut W) -> Result<(u64, u64)> {
        let mut written = 0u64;
        for mut file_reader in self.file_readers {
            let start = written;
            written += std::io::copy(&mut file_reader.reader, writer)?;
            self.file_offsets
                .insert(file_reader.file_name, start..written);
        }

        let footer_bytes = bincode_options().serialize(&IndexFooter::new(self.file_offsets))?;
        let footer_len = footer_bytes.len() as u64;

        let footer_written = std::io::copy(&mut Cursor::new(footer_bytes), writer)?;
        if footer_written < footer_len {
            bail!(
                "written less than expected: {} < {}",
                footer_written,
                footer_len,
            );
        }

        Ok((written + footer_len, footer_len))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Seek, SeekFrom, Write},
        path::Path,
        sync::Arc,
    };

    use color_eyre::eyre::Result;
    use tantivy::{
        directory::{FileSlice, OwnedBytes},
        Directory,
    };
    use tempfile::tempfile;

    use crate::unified_index::directory::UnifiedDirectory;

    use super::*;

    #[test]
    fn unified_index_write_then_read_2_files() -> Result<()> {
        let mut file1 = tempfile()?;
        let mut file2 = tempfile()?;
        file1.write_all(b"hello")?;
        file2.write_all(b"world")?;
        file1.seek(SeekFrom::Start(0))?;
        file2.seek(SeekFrom::Start(0))?;

        let writer = UnifiedIndexWriter::new(vec![
            FileReader::new(Box::new(file1), PathBuf::from("a")),
            FileReader::new(Box::new(file2), PathBuf::from("b")),
        ]);

        let mut buf = vec![];
        let (_, footer_len) = writer.write(&mut buf)?;

        let file_slice = FileSlice::new(Arc::new(OwnedBytes::new(buf)));
        let dir = UnifiedDirectory::open_with_len(file_slice, footer_len as usize)?;
        assert_eq!(dir.atomic_read(Path::new("a"))?, b"hello".to_vec());
        assert_eq!(dir.atomic_read(Path::new("b"))?, b"world".to_vec());

        Ok(())
    }
}
