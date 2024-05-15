pub mod unified_directory;
pub mod utils;
pub mod writer;

use std::{collections::HashMap, ops::Range, path::PathBuf};

use serde::{Deserialize, Serialize};

const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFooter {
    cache: HashMap<(PathBuf, Range<u64>), Vec<u8>>,
    file_offsets: HashMap<PathBuf, Range<u64>>,
    version: u32,
}

impl IndexFooter {
    pub fn new(file_offsets: HashMap<PathBuf, Range<u64>>) -> Self {
        Self {
            cache: HashMap::new(),
            file_offsets,
            version: VERSION,
        }
    }
}
