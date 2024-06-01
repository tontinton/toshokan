use serde::{Deserialize, Serialize};
use tantivy::schema::{IndexRecordOption, JsonObjectOptions, TextFieldIndexing};

use super::{default_true, FastFieldNormalizerType, FieldTokenizerType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDynamicObjectFieldConfig {
    #[serde(default)]
    pub record: IndexRecordOption,

    #[serde(default = "default_tokenizer")]
    pub tokenizer: FieldTokenizerType,
}

fn default_tokenizer() -> FieldTokenizerType {
    FieldTokenizerType::Raw
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IndexedDynamicObjectFieldType {
    False,
    #[default]
    True,
    Indexed(IndexedDynamicObjectFieldConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicObjectFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default = "default_fast_normalizer")]
    pub fast: FastFieldNormalizerType,

    #[serde(default)]
    pub indexed: IndexedDynamicObjectFieldType,

    #[serde(default = "default_true")]
    pub expand_dots: bool,
}

fn default_fast_normalizer() -> FastFieldNormalizerType {
    FastFieldNormalizerType::True
}

impl From<DynamicObjectFieldConfig> for JsonObjectOptions {
    fn from(config: DynamicObjectFieldConfig) -> Self {
        let mut options = JsonObjectOptions::default();
        if config.stored {
            options = options.set_stored();
        }
        options = options.set_fast(config.fast.into());
        match config.indexed {
            IndexedDynamicObjectFieldType::False => {}
            IndexedDynamicObjectFieldType::True => {
                options = options.set_indexing_options(TextFieldIndexing::default());
            }
            IndexedDynamicObjectFieldType::Indexed(config) => {
                options = options.set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(config.record)
                        .set_tokenizer(config.tokenizer.into()),
                );
            }
        }
        if config.expand_dots {
            options = options.set_expand_dots_enabled();
        }
        options
    }
}
