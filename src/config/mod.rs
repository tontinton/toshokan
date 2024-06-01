pub mod datetime;
pub mod text;

use std::{collections::HashMap, path::Path};

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;

use self::{
    datetime::DateTimeFieldConfig,
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