use serde::{Deserialize, Serialize};

use super::FieldConfigs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticObjectFieldConfig {
    #[serde(default)]
    pub fields: FieldConfigs,
}
