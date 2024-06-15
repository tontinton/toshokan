use std::time::Duration;

use async_trait::async_trait;
use color_eyre::{
    eyre::{eyre, Context, Report},
    Result,
};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer, ConsumerContext},
    error::KafkaError,
    ClientConfig, ClientContext, Message,
};
use tokio::{sync::mpsc, task::spawn_blocking};

use super::{JsonMap, Source};

pub const KAFKA_PREFIX: &str = "kafka://";
const CHANNEL_SIZE: usize = 10;
const POLL_DURATION: Duration = Duration::from_secs(1);

struct KafkaContext;

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {}

type KafkaConsumer = BaseConsumer<KafkaContext>;

pub struct KafkaSource {
    messages_rx: mpsc::Receiver<Result<Option<Vec<u8>>>>,
}

fn parse_url(url: &str) -> Result<(&str, &str)> {
    if !url.starts_with(KAFKA_PREFIX) {
        return Err(eyre!("'{}' does not start with {}", url, KAFKA_PREFIX));
    }

    let trimmed_input = &url[KAFKA_PREFIX.len()..];
    let parts: Vec<&str> = trimmed_input.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(eyre!(
            "'{}' needs to include a '/' to include the topic name",
            url
        ));
    }

    Ok((parts[0], parts[1]))
}

fn run_consumer_thread(consumer: KafkaConsumer, tx: mpsc::Sender<Result<Option<Vec<u8>>>>) {
    spawn_blocking(move || {
        while !tx.is_closed() {
            let Some(msg) = consumer.poll(POLL_DURATION) else {
                continue;
            };

            if let Err(KafkaError::PartitionEOF(partition)) = msg {
                debug!("Reached the end of kafka partition {}", partition);
                if let Err(e) = tx.blocking_send(Ok(None)) {
                    error!(
                        "Failed to send EOF message from kafka consumer thread: {}",
                        e
                    );
                }
                break;
            }

            if let Ok(ref x) = msg {
                if x.payload().is_none() {
                    debug!(
                        "Skipping empty message in partition {} offset {}",
                        x.partition(),
                        x.offset()
                    );
                    continue;
                }
            }

            let msg = msg
                .map(|x| Some(x.payload().unwrap().to_vec()))
                .map_err(|e| Report::new(e));

            if let Err(e) = tx.blocking_send(msg) {
                debug!("Failed to send message from kafka consumer thread: {}", e);
                break;
            }
        }
    });
}

impl KafkaSource {
    pub fn from_url(url: &str) -> Result<Self> {
        let (servers, topic) = parse_url(url)?;

        let log_level = if cfg!(debug_assertions) {
            RDKafkaLogLevel::Debug
        } else {
            RDKafkaLogLevel::Info
        };

        let consumer: KafkaConsumer = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set("session.timeout.ms", "6000") // Minimum allowed timeout.
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("group.id", topic) // Consumer group per topic for now.
            .set_log_level(log_level)
            .create_with_context(KafkaContext)
            .context("failed to create kafka consumer")?;

        consumer
            .subscribe(&[topic])
            .context("failed to subscribe to kafka topic")?;

        debug!("Reading from kafka '{}' (topic '{}')", servers, topic);

        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        run_consumer_thread(consumer, tx);

        Ok(Self { messages_rx: rx })
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn next(&mut self) -> Result<Option<JsonMap>> {
        let Some(result) = self.messages_rx.recv().await else {
            return Ok(None);
        };

        let maybe_msg = result?;
        let Some(msg) = maybe_msg else {
            return Ok(None);
        };

        Ok(serde_json::from_slice(&msg)?)
    }
}
