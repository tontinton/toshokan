use std::{collections::HashMap, ops::Deref, path::Path};

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use tantivy::{
    schema::{IndexRecordOption, TextFieldIndexing, TextOptions},
    DateOptions, DateTimePrecision,
};
use tokio::fs::read_to_string;

const VERSION: u32 = 1;

fn default_version() -> u32 {
    VERSION
}

fn default_true() -> bool {
    true
}

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DateTimeFastPrecisionType {
    #[default]
    False,
    True,
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl From<DateTimeFastPrecisionType> for Option<DateTimePrecision> {
    fn from(value: DateTimeFastPrecisionType) -> Self {
        use DateTimeFastPrecisionType::*;
        match value {
            False => None,
            True | Seconds => Some(DateTimePrecision::Seconds),
            Milliseconds => Some(DateTimePrecision::Milliseconds),
            Microseconds => Some(DateTimePrecision::Microseconds),
            Nanoseconds => Some(DateTimePrecision::Nanoseconds),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateTimeFormatType {
    Iso8601,
    Rfc2822,
    Rfc3339,
    Timestamp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimeFormats(Vec<DateTimeFormatType>);

impl Default for DateTimeFormats {
    fn default() -> Self {
        Self(vec![
            DateTimeFormatType::Rfc3339,
            DateTimeFormatType::Timestamp,
        ])
    }
}

impl Deref for DateTimeFormats {
    type Target = Vec<DateTimeFormatType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DateTimeFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default = "default_true")]
    pub indexed: bool,

    #[serde(default)]
    pub fast: DateTimeFastPrecisionType,

    #[serde(default)]
    pub formats: DateTimeFormats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text(TextFieldConfig),
    Datetime(DateTimeFieldConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingConfig {
    #[serde(rename = "type")]
    pub type_: FieldType,
}

impl MappingConfig {
    pub fn is_indexed(&self) -> bool {
        use FieldType::*;
        match &self.type_ {
            Text(config) => !matches!(config.indexed, IndexedTextFieldType::False),
            Datetime(config) => config.indexed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSchema {
    #[serde(default)]
    pub mappings: HashMap<String, MappingConfig>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    time_field: Option<String>,
}

impl IndexSchema {
    pub fn get_indexed_fields(&self) -> Vec<String> {
        self.mappings
            .iter()
            .filter(|(_, v)| v.is_indexed())
            .map(|(k, _)| k)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexConfig {
    pub name: String,
    pub path: String,

    #[serde(default = "default_version")]
    version: u32,

    #[serde(default)]
    pub schema: IndexSchema,
}

impl IndexConfig {
    pub async fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config_str = read_to_string(path).await?;
        Ok(serde_yaml::from_str(&config_str)?)
    }
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

impl From<DateTimeFieldConfig> for DateOptions {
    fn from(config: DateTimeFieldConfig) -> Self {
        let mut date_options = DateOptions::default();
        if config.stored {
            date_options = date_options.set_stored();
        }
        if config.indexed {
            date_options = date_options.set_indexed();
        }
        if let Some(precision) = config.fast.into() {
            date_options = date_options.set_fast().set_precision(precision);
        }
        date_options
    }
}
