pub mod boolean;
pub mod datetime;
pub mod dynamic_object;
pub mod ip;
pub mod number;
pub mod text;

use std::{collections::HashMap, path::Path};

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;

use crate::config::dynamic_object::IndexedDynamicObjectFieldType;

use self::{
    boolean::BooleanFieldConfig,
    datetime::DateTimeFieldConfig,
    dynamic_object::DynamicObjectFieldConfig,
    ip::IpFieldConfig,
    number::NumberFieldConfig,
    text::{IndexedTextFieldType, TextFieldConfig},
};

const VERSION: u32 = 1;

fn default_version() -> u32 {
    VERSION
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastFieldNormalizerType {
    False,

    /// Chops the text on according to whitespace and
    /// punctuation, removes tokens that are too long, and lowercases
    /// tokens.
    True,

    /// Does not process nor tokenize the text.
    Raw,
}

impl From<FastFieldNormalizerType> for Option<&str> {
    fn from(value: FastFieldNormalizerType) -> Self {
        match value {
            FastFieldNormalizerType::False => None,
            FastFieldNormalizerType::True => Some("default"),
            FastFieldNormalizerType::Raw => Some("raw"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldTokenizerType {
    /// Chops the text on according to whitespace and
    /// punctuation, removes tokens that are too long, and lowercases
    /// tokens.
    Default,

    /// Does not process nor tokenize the text.
    Raw,

    /// Like `true`, but also applies stemming on the
    /// resulting tokens. Stemming can improve the recall of your
    /// search engine.
    EnStem,

    /// Splits the text on whitespaces.
    Whitespace,
}

impl From<FieldTokenizerType> for &str {
    fn from(value: FieldTokenizerType) -> Self {
        match value {
            FieldTokenizerType::Default => "default",
            FieldTokenizerType::Raw => "raw",
            FieldTokenizerType::EnStem => "en_stem",
            FieldTokenizerType::Whitespace => "whitespace",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text(TextFieldConfig),
    Number(NumberFieldConfig),
    Boolean(BooleanFieldConfig),
    Datetime(DateTimeFieldConfig),
    Ip(IpFieldConfig),
    DynamicObject(DynamicObjectFieldConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldsConfig {
    #[serde(rename = "type")]
    pub type_: FieldType,
}

impl FieldsConfig {
    pub fn is_indexed(&self) -> bool {
        use FieldType::*;
        match &self.type_ {
            Text(config) => !matches!(config.indexed, IndexedTextFieldType::False),
            Number(config) => config.indexed,
            Boolean(config) => config.indexed,
            Datetime(config) => config.indexed,
            Ip(config) => config.indexed,
            DynamicObject(config) => {
                !matches!(config.indexed, IndexedDynamicObjectFieldType::False)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSchema {
    #[serde(default)]
    pub fields: HashMap<String, FieldsConfig>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    time_field: Option<String>,
}

impl IndexSchema {
    pub fn get_indexed_fields(&self) -> Vec<String> {
        self.fields
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
