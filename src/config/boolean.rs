use serde::{Deserialize, Serialize};
use tantivy::schema::NumericOptions;

use super::default_true;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BooleanFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,

    #[serde(default = "default_true")]
    pub indexed: bool,

    #[serde(default = "default_true")]
    pub parse_string: bool,
}

impl From<BooleanFieldConfig> for NumericOptions {
    fn from(config: BooleanFieldConfig) -> Self {
        let mut options = NumericOptions::default();
        if config.stored {
            options = options.set_stored();
        }
        if config.indexed {
            options = options.set_indexed();
        }
        if config.fast {
            options = options.set_fast();
        }
        options
    }
}
