use serde::{Deserialize, Serialize};
use tantivy::schema::NumericOptions;

use super::default_true;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumberFieldType {
    U64,
    I64,
    F64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumberFieldConfig {
    #[serde(rename = "type")]
    pub type_: NumberFieldType,

    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,

    #[serde(default = "default_true")]
    pub indexed: bool,

    #[serde(default = "default_true")]
    pub parse_string: bool,
}

impl From<NumberFieldConfig> for NumericOptions {
    fn from(config: NumberFieldConfig) -> Self {
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
