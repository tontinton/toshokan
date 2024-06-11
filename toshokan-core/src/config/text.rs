use serde::{Deserialize, Serialize};
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

use super::{default_true, FastFieldNormalizerType, FieldTokenizerType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedTextFieldConfig {
    #[serde(default)]
    pub record: IndexRecordOption,

    #[serde(default = "default_true")]
    pub fieldnorms: bool,

    #[serde(default = "default_tokenizer")]
    pub tokenizer: FieldTokenizerType,
}

fn default_tokenizer() -> FieldTokenizerType {
    FieldTokenizerType::Default
}

impl Default for IndexedTextFieldConfig {
    fn default() -> Self {
        Self {
            record: IndexRecordOption::default(),
            fieldnorms: true,
            tokenizer: FieldTokenizerType::Default,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IndexedTextFieldType {
    False,
    #[default]
    True,
    Indexed(IndexedTextFieldConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default = "default_fast_normalizer")]
    pub fast: FastFieldNormalizerType,

    #[serde(default)]
    pub indexed: IndexedTextFieldType,
}

fn default_fast_normalizer() -> FastFieldNormalizerType {
    FastFieldNormalizerType::False
}

impl From<TextFieldConfig> for TextOptions {
    fn from(config: TextFieldConfig) -> Self {
        let mut options = TextOptions::default();
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
                    TextFieldIndexing::default()
                        .set_index_option(config.record)
                        .set_fieldnorms(config.fieldnorms)
                        .set_tokenizer(config.tokenizer.into()),
                );
            }
        }
        options
    }
}
