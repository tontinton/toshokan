pub mod boolean;
pub mod datetime;
pub mod dynamic_object;
pub mod ip;
pub mod number;
pub mod static_object;
pub mod text;

use std::{ops::Deref, path::Path, vec::IntoIter};

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
    static_object::StaticObjectFieldConfig,
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
    StaticObject(StaticObjectFieldConfig),
}

impl FieldType {
    pub fn is_indexed(&self) -> bool {
        use FieldType::*;
        match &self {
            Text(config) => !matches!(config.indexed, IndexedTextFieldType::False),
            Number(config) => config.indexed,
            Boolean(config) => config.indexed,
            Datetime(config) => config.indexed,
            Ip(config) => config.indexed,
            DynamicObject(config) => {
                !matches!(config.indexed, IndexedDynamicObjectFieldType::False)
            }
            StaticObject(_) => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub name: String,

    #[serde(default)]
    pub array: bool,

    #[serde(rename = "type")]
    pub type_: FieldType,
}

pub fn split_object_field_name(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;

    for (i, c) in s.char_indices().peekable() {
        if c == '.' && (i == 0 || (i > 0 && &s[i - 1..i] != "\\")) {
            result.push(&s[start..i]);
            start = i + 1;
        }
    }

    result.push(&s[start..]);
    result
}

fn escaped_field_name(name: &str) -> String {
    name.replace('.', "\\.")
}

pub fn unescaped_field_name(name: &str) -> String {
    name.replace("\\.", ".")
}

pub fn escaped_with_parent_name(name: &str, parent_name: Option<&str>) -> String {
    let escaped = escaped_field_name(name);
    if let Some(parent_name) = parent_name {
        format!("{}.{}", parent_name, escaped)
    } else {
        escaped
    }
}

impl FieldConfig {
    fn with_parent_name(self, parent_name: Option<&str>) -> Self {
        Self {
            name: escaped_with_parent_name(&self.name, parent_name),
            array: self.array,
            type_: self.type_,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FieldConfigs(Vec<FieldConfig>);

impl IntoIterator for FieldConfigs {
    type Item = FieldConfig;
    type IntoIter = IntoIter<FieldConfig>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Deref for FieldConfigs {
    type Target = Vec<FieldConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FieldConfigs {
    fn get_indexed_inner(&self, parent_name: Option<String>) -> Vec<FieldConfig> {
        let mut indexed_fields = self
            .iter()
            .filter(|field| field.type_.is_indexed())
            .cloned()
            .map(|field| field.with_parent_name(parent_name.as_deref()))
            .collect::<Vec<_>>();

        indexed_fields.extend(
            self.iter()
                .filter_map(|field| {
                    if let FieldType::StaticObject(config) = &field.type_ {
                        let name = escaped_with_parent_name(&field.name, parent_name.as_deref());
                        Some(config.fields.get_indexed_inner(Some(name)))
                    } else {
                        None
                    }
                })
                .flatten(),
        );

        indexed_fields
    }

    pub fn get_indexed(&self) -> Vec<FieldConfig> {
        self.get_indexed_inner(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSchema {
    #[serde(default)]
    pub fields: FieldConfigs,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    time_field: Option<String>,
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
