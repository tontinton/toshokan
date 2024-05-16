use std::{ops::Range, path::Path, sync::Arc};

use bincode::Options;
use color_eyre::eyre::Result;
use tantivy::{
    directory::{error::OpenReadError, FileHandle, FileSlice, OwnedBytes},
    Directory, HasLen,
};

use crate::bincode::bincode_options;

use super::{file_cache::RangeCache, utils::macros::read_only_directory, IndexFooter};

#[derive(Debug, Clone)]
struct CachedFileHandle {
    raw_file_handle: Arc<dyn FileHandle>,
    cache: RangeCache,
}

impl CachedFileHandle {
    fn new(raw_file_handle: Arc<dyn FileHandle>, cache: RangeCache) -> Self {
        Self {
            raw_file_handle,
            cache,
        }
    }
}

impl FileHandle for CachedFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if let Some((end, bytes)) = self.cache.get(&(range.start as u64)).cloned() {
            if end == range.end as u64 {
                return Ok(OwnedBytes::new(bytes));
            }
        }
        self.raw_file_handle.read_bytes(range)
    }
}

impl HasLen for CachedFileHandle {
    fn len(&self) -> usize {
        self.raw_file_handle.len()
    }
}

#[derive(Debug, Clone)]
pub struct UnifiedDirectory {
    slice: FileSlice,
    footer: IndexFooter,
}

impl UnifiedDirectory {
    pub fn open_with_len(slice: FileSlice, footer_len: usize) -> Result<Self> {
        let (slice, footer_slice) = slice.split_from_end(footer_len);
        let footer_bytes = footer_slice.read_bytes()?;
        let footer = bincode_options().deserialize(&footer_bytes)?;
        Ok(Self { slice, footer })
    }
}

impl Directory for UnifiedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let range = self
            .footer
            .file_offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let slice = self.slice.slice(range.start as usize..range.end as usize);
        if let Some(cache) = self.footer.cache.get(path).cloned() {
            Ok(Arc::new(CachedFileHandle::new(Arc::new(slice), cache)))
        } else {
            Ok(Arc::new(slice))
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        let payload = file_slice
            .read_bytes()
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(payload.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.footer.file_offsets.contains_key(path))
    }

    read_only_directory!();
}
