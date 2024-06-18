mod buf_source;
mod kafka_checkpoint;
mod kafka_source;

use async_trait::async_trait;
use color_eyre::{eyre::bail, Result};
use sqlx::PgPool;

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
    /// Get a document from the source.
    async fn get_one(&mut self) -> Result<SourceItem>;

    /// Called when an index file was just created to notify the source.
    /// Useful for implementing checkpoints for example.
    async fn on_index_created(&mut self) -> Result<()>;
}

pub async fn connect_to_source(
    input: Option<&str>,
    stream: bool,
    pool: &PgPool,
) -> Result<Box<dyn Source>> {
    Ok(match input {
        Some(url) if url.starts_with(KAFKA_PREFIX) => {
            Box::new(KafkaSource::from_url(url, stream, pool)?)
        }
        Some(path) => {
            if stream {
                bail!("Streaming from a file is not currently supported.");
            }
            Box::new(BufSource::from_path(path).await?)
        }
        None => Box::new(BufSource::from_stdin()),
    })
}
