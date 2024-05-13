pub mod macros {
    macro_rules! read_only_directory {
        () => {
            fn atomic_write(&self, _path: &Path, _data: &[u8]) -> std::io::Result<()> {
                unimplemented!("read-only")
            }

            fn delete(&self, _path: &Path) -> Result<(), tantivy::directory::error::DeleteError> {
                unimplemented!("read-only")
            }

            fn open_write(
                &self,
                _path: &Path,
            ) -> Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
                unimplemented!("read-only")
            }

            fn sync_directory(&self) -> std::io::Result<()> {
                unimplemented!("read-only")
            }

            fn watch(
                &self,
                _watch_callback: tantivy::directory::WatchCallback,
            ) -> tantivy::Result<tantivy::directory::WatchHandle> {
                Ok(tantivy::directory::WatchHandle::empty())
            }

            fn acquire_lock(
                &self,
                _lock: &tantivy::directory::Lock,
            ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
                Ok(tantivy::directory::DirectoryLock::from(Box::new(|| {})))
            }
        };
    }

    pub(crate) use read_only_directory;
}
