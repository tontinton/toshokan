mod buf_source;
mod kafka_source;

use async_trait::async_trait;
use color_eyre::Result;

use self::{
    buf_source::BufSource,
    kafka_source::{KafkaSource, KAFKA_PREFIX},
};

type JsonMap = serde_json::Map<String, serde_json::Value>;

#[async_trait]
pub trait Source {
    async fn get_one(&mut self) -> Result<Option<JsonMap>>;
}

pub async fn connect_to_source(input: Option<&str>) -> Result<Box<dyn Source + Unpin>> {
    Ok(match input {
        Some(url) if url.starts_with(KAFKA_PREFIX) => Box::new(KafkaSource::from_url(url)?),
        Some(path) => Box::new(BufSource::from_path(path).await?),
        None => Box::new(BufSource::from_stdin()),
    })
}
