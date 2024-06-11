use std::{
    collections::BTreeMap,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use color_eyre::eyre::Result;
use tantivy::{
    directory::{error::OpenReadError, FileHandle, MmapDirectory, OwnedBytes},
    Directory, HasLen, Index, IndexReader, ReloadPolicy,
};

use super::utils::macros::read_only_directory;

// Don't store slices that are too large in the cache, except the term and the store index.
const CACHE_SLICE_SIZE_LIMIT: usize = 10_000_000;

// For each file, there are byte ranges stored as the value.
// Storing start of range as key, and end in value together with the buffer,
// as BTreeMap requires a key that implements Ord.
pub type RangeCache = BTreeMap<u64, (u64, Arc<[u8]>)>;
pub type FileCache = BTreeMap<PathBuf, RangeCache>;

#[derive(Debug)]
pub struct RecordingDirectory<D: Directory> {
    inner: Arc<D>,
    cache: Arc<Mutex<FileCache>>,
}

impl<D: Directory> RecordingDirectory<D> {
    pub fn wrap(directory: D) -> Self {
        RecordingDirectory {
            inner: Arc::new(directory),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn record(&self, path: &Path, bytes: &[u8], offset: u64) -> std::io::Result<()> {
        let path_str = path.to_string_lossy();
        if !path_str.ends_with("store")
            && !path_str.ends_with("term")
            && bytes.len() > CACHE_SLICE_SIZE_LIMIT
        {
            return Ok(());
        }

        let mut guard = self.cache.lock().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Some other holder of the mutex panicked.",
            )
        })?;

        guard.entry(path.to_path_buf()).or_default().insert(
            offset,
            (
                offset + bytes.len() as u64,
                Arc::from(bytes.to_vec().into_boxed_slice()),
            ),
        );

        Ok(())
    }
}

impl<D: Directory> Clone for RecordingDirectory<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<D: Directory> Directory for RecordingDirectory<D> {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let inner = self.inner.get_file_handle(path)?;

        Ok(Arc::new(RecordingFileHandle {
            inner,
            directory: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let payload = self.inner.atomic_read(path)?;
        self.record(path, &payload, 0)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;
        Ok(payload)
    }

    read_only_directory!();
}

struct RecordingFileHandle<D: Directory> {
    directory: RecordingDirectory<D>,
    inner: Arc<dyn FileHandle>,
    path: PathBuf,
}

impl<D: Directory> FileHandle for RecordingFileHandle<D> {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let start = range.start as u64;
        let payload = self.inner.read_bytes(range)?;
        self.directory.record(&self.path, &payload, start)?;
        Ok(payload)
    }
}

impl<D: Directory> std::fmt::Debug for RecordingFileHandle<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DebugProxyFileHandle({:?})", &self.inner)
    }
}

impl<D: Directory> HasLen for RecordingFileHandle<D> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

pub fn build_file_cache(path: &Path) -> Result<FileCache> {
    let directory = MmapDirectory::open(path)?;
    let recording_directory = RecordingDirectory::wrap(directory);
    let index = Index::open(recording_directory.clone())?;
    let schema = index.schema();
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    for (field, field_entry) in schema.fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        for reader in searcher.segment_readers() {
            reader.inverted_index(field)?;
        }
    }

    let guard = recording_directory.cache.lock().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "Some other holder of the mutex panicked.",
        )
    })?;

    Ok(guard.clone())
}
