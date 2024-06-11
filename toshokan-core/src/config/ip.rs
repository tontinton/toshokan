use serde::{Deserialize, Serialize};
use tantivy::schema::IpAddrOptions;

use super::default_true;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,

    #[serde(default = "default_true")]
    pub indexed: bool,
}

impl From<IpFieldConfig> for IpAddrOptions {
    fn from(config: IpFieldConfig) -> Self {
        let mut options = IpAddrOptions::default();
        if config.stored {
            options = options.set_stored();
        }
        if config.indexed {
            options = options.set_indexed();
        }
        if config.fast {
            options = options.set_fast();
        }
        options
    }
}
