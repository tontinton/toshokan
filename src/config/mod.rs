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
            DynamicObject(config) => !matches!(config.indexed, IndexedTextFieldType::False),
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
