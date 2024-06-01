use serde::{Deserialize, Serialize};
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

use super::default_true;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FastTextFieldType {
    #[default]
    Disabled,
    Raw,
    Lowercase,
}

impl From<FastTextFieldType> for Option<&str> {
    fn from(value: FastTextFieldType) -> Self {
        match value {
            FastTextFieldType::Disabled => None,
            FastTextFieldType::Raw => Some("raw"),
            FastTextFieldType::Lowercase => Some("lowercase"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexedTextFieldConfig {
    #[serde(default)]
    pub record: IndexRecordOption,

    #[serde(default = "default_true")]
    pub fieldnorms: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IndexedTextFieldType {
    False,
    #[default]
    True,
    Indexed(IndexedTextFieldConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TextFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: FastTextFieldType,

    #[serde(default)]
    pub indexed: IndexedTextFieldType,
}

impl From<TextFieldConfig> for TextOptions {
    fn from(config: TextFieldConfig) -> Self {
        let mut text_options = TextOptions::default();
        if config.stored {
            text_options = text_options.set_stored();
        }
        text_options = text_options.set_fast(config.fast.into());
        match config.indexed {
            IndexedTextFieldType::False => {}
            IndexedTextFieldType::True => {
                text_options = text_options.set_indexing_options(TextFieldIndexing::default());
            }
            IndexedTextFieldType::Indexed(config) => {
                text_options = text_options.set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(config.record)
                        .set_fieldnorms(config.fieldnorms),
                );
            }
        }
        text_options
    }
}
