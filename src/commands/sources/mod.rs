mod buf_source;
mod kafka_source;

use async_trait::async_trait;
use color_eyre::{eyre::bail, Result};

use self::{
    buf_source::BufSource,
    kafka_source::{KafkaSource, KAFKA_PREFIX},
};

pub type JsonMap = serde_json::Map<String, serde_json::Value>;

pub enum SourceItem {
    /// A document to index.
    Document(JsonMap),

    /// The source is closed, can't read more from it.
    Close,

    /// The source decided to reload from the last checkpoint (example: kafka rebalance).
    Restart,
}

#[async_trait]
pub trait Source {
    async fn get_one(&mut self) -> Result<SourceItem>;
}

pub async fn connect_to_source(input: Option<&str>, stream: bool) -> Result<Box<dyn Source>> {
    Ok(match input {
        Some(url) if url.starts_with(KAFKA_PREFIX) => Box::new(KafkaSource::from_url(url, stream)?),
        Some(path) => {
            if stream {
                bail!("Streaming from a file is not currently supported.");
            }
            Box::new(BufSource::from_path(path).await?)
        }
        None => Box::new(BufSource::from_stdin()),
    })
}
