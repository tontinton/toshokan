pub mod file_cache;
pub mod unified_directory;
pub mod utils;
pub mod writer;

use std::{collections::HashMap, ops::Range, path::PathBuf};

use serde::{Deserialize, Serialize};

use self::file_cache::FileCache;

const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexFooter {
    file_offsets: HashMap<PathBuf, Range<u64>>,
    cache: FileCache,
    version: u32,
}

impl IndexFooter {
    pub fn new(file_offsets: HashMap<PathBuf, Range<u64>>, cache: FileCache) -> Self {
        Self {
            file_offsets,
            cache,
            version: VERSION,
        }
    }
}
