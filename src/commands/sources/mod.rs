mod buf_source;
mod kafka_checkpoint;
pub mod kafka_source;

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

    /// If the source supports checkpointing, it creates a checkpoint commiter that stores a
    /// snapshot of the last read state. Once the indexer has successfuly commited and uploaded
    /// the new index file, it tells the checkpoint commiter to commit the snapshot.
    async fn get_checkpoint_commiter(&mut self) -> Option<Box<dyn CheckpointCommiter + Send>> {
        None
    }
}

#[async_trait]
pub trait CheckpointCommiter {
    /// Commit the stored state snapshot.
    async fn commit(&self) -> Result<()>;
}

pub async fn connect_to_source(
    input: Option<&str>,
    stream: bool,
    pool: &PgPool,
) -> Result<Box<dyn Source + Send + Sync>> {
    Ok(match input {
        Some(url) if url.starts_with(KAFKA_PREFIX) => {
            Box::new(KafkaSource::from_url(url, stream, pool).await?)
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
