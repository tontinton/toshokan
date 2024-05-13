use std::{
    collections::HashMap,
    io::{Cursor, Read},
    path::{Path, PathBuf},
};

use bincode::Options;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tantivy::{
    directory::{FileSlice, ManagedDirectory, OwnedBytes},
    Directory, HasLen,
};

use crate::bincode::bincode_options;

const CHUNK_SIZE: usize = 1024 * 1024; // 1MB
const VERSION: u32 = 1;

static META_FILEPATH: Lazy<&'static Path> = Lazy::new(|| Path::new("meta.json"));

struct FileSliceReader {
    len: usize,
    slice_left: FileSlice,
    chunk: OwnedBytes,
    bytes_read: usize,
}

impl FileSliceReader {
    fn split_chunk(
        slice: FileSlice,
        left_to_read: usize,
    ) -> std::io::Result<(FileSlice, OwnedBytes)> {
        let chunk_size = left_to_read.min(CHUNK_SIZE);
        let (chunk_slice, next_slice) = slice.split(chunk_size);
        Ok((next_slice, chunk_slice.read_bytes()?))
    }

    fn new(slice: FileSlice) -> std::io::Result<Self> {
        let len = slice.len();
        let (slice_left, chunk) = Self::split_chunk(slice, len)?;
        Ok(Self {
            len,
            slice_left,
            chunk,
            bytes_read: 0,
        })
    }
}

impl Read for FileSliceReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.bytes_read < self.len {
            let n = self.chunk.read(buf)?;
            if n == 0 {
                let left = self.len - self.bytes_read;
                (self.slice_left, self.chunk) = Self::split_chunk(self.slice_left.clone(), left)?;
            } else {
                self.bytes_read += n;
                return Ok(n);
            }
        }

        Ok(0)
    }
}

struct FileReader {
    path: PathBuf,
    reader: Box<dyn Read>,
}

impl FileReader {
    fn from_directory_and_path(directory: &dyn Directory, path: PathBuf) -> anyhow::Result<Self> {
        let reader: Box<dyn Read> = if path == *META_FILEPATH {
            Box::new(Cursor::new(directory.atomic_read(&path)?))
        } else {
            Box::new(FileSliceReader::new(directory.open_read(&path)?)?)
        };

        Ok(FileReader { path, reader })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexFooter {
    file_map: HashMap<PathBuf, (u64, u64)>,
    version: u32,
}

pub struct SingleFileIndexWriter {
    file_readers: Vec<FileReader>,
    file_map: HashMap<PathBuf, (u64, u64)>,
}

impl SingleFileIndexWriter {
    pub fn from_managed_directory(directory: &ManagedDirectory) -> anyhow::Result<Self> {
        let file_readers = directory
            .list_managed_files()
            .into_iter()
            .map(|path| FileReader::from_directory_and_path(directory, path))
            .collect::<anyhow::Result<Vec<FileReader>>>()?;

        Ok(Self {
            file_readers,
            file_map: HashMap::new(),
        })
    }

    pub fn write<W: std::io::Write>(mut self, writer: &mut W) -> anyhow::Result<(u64, u64)> {
        let mut written = 0u64;
        for mut file_reader in self.file_readers {
            let start = written;
            written += std::io::copy(&mut file_reader.reader, writer)?;
            self.file_map.insert(file_reader.path, (start, written));
        }

        let bytes = bincode_options().serialize(&IndexFooter {
            version: VERSION,
            file_map: self.file_map,
        })?;
        let footer = std::io::copy(&mut Cursor::new(bytes), writer)?;

        Ok((written + footer, footer))
    }
}
