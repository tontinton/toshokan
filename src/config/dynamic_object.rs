use serde::{Deserialize, Serialize};
use tantivy::schema::{JsonObjectOptions, TextFieldIndexing};

use super::{
    default_true,
    text::{FastTextFieldType, IndexedTextFieldType},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DynamicObjectFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: FastTextFieldType,

    #[serde(default)]
    pub indexed: IndexedTextFieldType,

    #[serde(default = "default_true")]
    pub expand_dots: bool,
}

impl From<DynamicObjectFieldConfig> for JsonObjectOptions {
    fn from(config: DynamicObjectFieldConfig) -> Self {
        let mut options = JsonObjectOptions::default();
        if config.stored {
            options = options.set_stored();
        }
        options = options.set_fast(config.fast.into());
        match config.indexed {
            IndexedTextFieldType::False => {}
            IndexedTextFieldType::True => {
                options = options.set_indexing_options(TextFieldIndexing::default());
            }
            IndexedTextFieldType::Indexed(config) => {
                options = options.set_indexing_options(
                    TextFieldIndexing::default().set_index_option(config.record),
                );
            }
        }
        if config.expand_dots {
            options = options.set_expand_dots_enabled();
        }
        options
    }
}
