use std::{path::Path, sync::Arc};

use color_eyre::eyre::Result;
use tantivy::{
    directory::{
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
        DirectoryLock, FileHandle, Lock, RamDirectory, WatchHandle,
    },
    Directory, Index, IndexMeta,
};

fn build_combined_meta(mut dirs: Vec<Box<dyn Directory>>) -> tantivy::Result<IndexMeta> {
    assert!(!dirs.is_empty());
    let mut combined_meta = Index::open(dirs.pop().unwrap())?.load_metas()?;
    while let Some(dir) = dirs.pop() {
        let meta = Index::open(dir)?.load_metas()?;
        combined_meta.segments.extend(meta.segments);
    }
    Ok(combined_meta)
}

#[derive(Debug, Clone)]
pub struct MergeDirectory {
    input_dirs: Vec<Box<dyn Directory>>,
    output_dir: Box<dyn Directory>,
}

impl MergeDirectory {
    pub fn new(
        input_dirs: Vec<Box<dyn Directory>>,
        output_dir: Box<dyn Directory>,
    ) -> Result<Self> {
        if input_dirs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Merge requires at least 1 input directory.",
            )
            .into());
        }

        let meta = build_combined_meta(input_dirs.clone())?;
        let meta_dir = Box::new(RamDirectory::create());
        meta_dir.atomic_write(
            Path::new("meta.json"),
            serde_json::to_string(&meta)?.as_bytes(),
        )?;

        // Always read the output dir and combined meta dir before any of the input dir meta files.
        let mut input_dirs_with_meta: Vec<Box<dyn Directory>> =
            Vec::with_capacity(input_dirs.len() + 2);
        input_dirs_with_meta.push(output_dir.clone());
        input_dirs_with_meta.push(meta_dir);
        input_dirs_with_meta.extend(input_dirs);

        Ok(Self {
            input_dirs: input_dirs_with_meta,
            output_dir,
        })
    }

    fn get_directory_containing_path(&self, path: &Path) -> Result<&dyn Directory, OpenReadError> {
        for dir in &self.input_dirs {
            if dir.exists(path)? {
                return Ok(dir.as_ref());
            }
        }
        Err(OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

impl Directory for MergeDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.get_directory_containing_path(path)?
            .get_file_handle(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.get_directory_containing_path(path)?.atomic_read(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        match self.get_directory_containing_path(path) {
            Ok(_) => Ok(true),
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(())
    }

    fn open_write(&self, path: &Path) -> Result<tantivy::directory::WritePtr, OpenWriteError> {
        self.output_dir.open_write(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.output_dir.atomic_write(path, data)
    }

    fn watch(&self, callback: tantivy::directory::WatchCallback) -> tantivy::Result<WatchHandle> {
        self.output_dir.watch(callback)
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        self.output_dir.sync_directory()
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(|| {})))
    }
}
