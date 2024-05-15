use std::{path::Path, sync::Arc};

use bincode::Options;
use color_eyre::eyre::Result;
use tantivy::{
    directory::{error::OpenReadError, FileHandle, FileSlice},
    Directory,
};

use crate::bincode::bincode_options;

use super::{utils::macros::read_only_directory, IndexFooter};

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
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let range = self
            .footer
            .file_offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(self.slice.slice(range.start as usize..range.end as usize))
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
