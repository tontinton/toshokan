pub mod directory;
pub mod utils;
pub mod writer;

use std::{collections::HashMap, ops::Range, path::PathBuf};

use serde::{Deserialize, Serialize};

const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFooter {
    file_offsets: HashMap<PathBuf, Range<u64>>,
    version: u32,
}

impl IndexFooter {
    pub fn new(file_offsets: HashMap<PathBuf, Range<u64>>) -> Self {
        Self {
            file_offsets,
            version: VERSION,
        }
    }
}
