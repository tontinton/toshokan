use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use color_eyre::{
    eyre::{bail, eyre, Context, Report},
    Result,
};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance},
    error::KafkaError,
    ClientConfig, ClientContext, Message, Offset,
};
use sqlx::PgPool;
use tokio::{
    sync::{mpsc, oneshot},
    task::spawn_blocking,
};

use super::{kafka_checkpoint::Checkpoint, Source, SourceItem};

pub const KAFKA_PREFIX: &str = "kafka://";
const CONSUMER_THREAD_MESSAGES_CHANNEL_SIZE: usize = 10;
const POLL_DURATION: Duration = Duration::from_secs(1);

enum MessageFromConsumerThread {
    Payload {
        bytes: Vec<u8>,
        partition: i32,
        offset: i64,
    },
    Eof,
    PreRebalance,
    PostRebalance {
        partitions: Vec<i32>,
        checkpoint_tx: oneshot::Sender<Vec<(i32, Option<i64>)>>,
    },
}

struct KafkaContext {
    topic: String,
    messages_tx: mpsc::Sender<Result<MessageFromConsumerThread>>,
    ignore_rebalance: bool,
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Revoke(_tpl) => {
                if self.ignore_rebalance {
                    return;
                }

                if let Err(e) = self
                    .messages_tx
                    .blocking_send(Ok(MessageFromConsumerThread::PreRebalance))
                {
                    error!("Failed to send pre-rebalance event: {e}");
                }
            }
            Rebalance::Assign(tpl) => {
                if self.ignore_rebalance {
                    return;
                }

                // elements() panics when tpl is empty, so we check capacity.
                let partitions = if tpl.capacity() > 0 {
                    tpl.elements()
                        .iter()
                        .map(|x| {
                            assert_eq!(x.topic(), self.topic);
                            x.partition()
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };

                let (tx, rx) = oneshot::channel();
                let msg = MessageFromConsumerThread::PostRebalance {
                    partitions,
                    checkpoint_tx: tx,
                };
                if let Err(e) = self.messages_tx.blocking_send(Ok(msg)) {
                    error!("Failed to send post-rebalance event: {e}");
                    return;
                }

                let checkpoint_result = rx.blocking_recv();
                if let Err(e) = checkpoint_result {
                    error!("Failed to recv post-rebalance offsets: {e}");
                    return;
                }
                let partitions_and_offsets = checkpoint_result.unwrap();

                for (id, offset) in partitions_and_offsets {
                    let Some(mut partition) = tpl.find_partition(&self.topic, id) else {
                        warn!("Partition id '{id}' not found?");
                        continue;
                    };

                    let rdkafka_offset = if let Some(offset) = offset {
                        Offset::Offset(offset)
                    } else {
                        Offset::Beginning
                    };

                    if let Err(e) = partition.set_offset(rdkafka_offset) {
                        warn!("Failed to set offset to '{rdkafka_offset:?}' for partition id '{id}': {e}");
                    }
                }
            }
            Rebalance::Error(e) => {
                error!("Kafka rebalance error: {}", e);
            }
        }
    }
}

type KafkaConsumer = BaseConsumer<KafkaContext>;

pub struct KafkaSource {
    messages_rx: mpsc::Receiver<Result<MessageFromConsumerThread>>,
    checkpoint: Option<Checkpoint>,
    partition_to_offset: BTreeMap<i32, i64>,
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

fn run_consumer_thread(
    consumer: KafkaConsumer,
    tx: mpsc::Sender<Result<MessageFromConsumerThread>>,
) {
    spawn_blocking(move || {
        while !tx.is_closed() {
            let Some(msg) = consumer.poll(POLL_DURATION) else {
                continue;
            };

            if let Err(KafkaError::PartitionEOF(partition)) = msg {
                debug!("Reached the end of kafka partition {}", partition);
                if let Err(e) = tx.blocking_send(Ok(MessageFromConsumerThread::Eof)) {
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
                .map(|x| MessageFromConsumerThread::Payload {
                    bytes: x.payload().unwrap().to_vec(),
                    partition: x.partition(),
                    offset: x.offset(),
                })
                .map_err(|e| Report::new(e));

            if let Err(e) = tx.blocking_send(msg) {
                debug!("Failed to send message from kafka consumer thread: {}", e);
                break;
            }
        }
    });
}

impl KafkaSource {
    pub fn from_url(url: &str, stream: bool, pool: &PgPool) -> Result<Self> {
        let (servers, topic) = parse_url(url)?;

        let log_level = if cfg!(debug_assertions) {
            RDKafkaLogLevel::Debug
        } else {
            RDKafkaLogLevel::Info
        };

        let (tx, rx) = mpsc::channel(CONSUMER_THREAD_MESSAGES_CHANNEL_SIZE);

        let consumer: KafkaConsumer = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set("session.timeout.ms", "6000") // Minimum allowed timeout.
            .set(
                "auto.offset.reset",
                // Stream will seek to offset saved in checkpoint in the future.
                if stream { "latest" } else { "earliest" },
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", (!stream).to_string())
            // Consumer group per topic for now. Separate stream and batch.
            .set(
                "group.id",
                format!(
                    "toshokan_{}_{topic}",
                    if stream { "stream" } else { "batch" }
                ),
            )
            .set_log_level(log_level)
            .create_with_context(KafkaContext {
                topic: topic.to_string(),
                messages_tx: tx.clone(),
                ignore_rebalance: !stream,
            })
            .context("failed to create kafka consumer")?;

        consumer
            .subscribe(&[topic])
            .context("failed to subscribe to kafka topic")?;

        debug!("Reading from kafka '{}' (topic '{}')", servers, topic);

        run_consumer_thread(consumer, tx);

        let checkpoint = if stream {
            // Url is not a good identifier as a source id, but we'll live with it for now.
            Some(Checkpoint::new(url.to_string(), pool.clone()))
        } else {
            None
        };

        Ok(Self {
            messages_rx: rx,
            checkpoint,
            partition_to_offset: BTreeMap::new(),
        })
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn get_one(&mut self) -> Result<SourceItem> {
        Ok(loop {
            let Some(msg) = self.messages_rx.recv().await else {
                bail!("kafka consumer thread closed")
            };

            match msg? {
                MessageFromConsumerThread::Payload {
                    bytes,
                    partition,
                    offset,
                } => {
                    self.partition_to_offset.insert(partition, offset);
                    break SourceItem::Document(serde_json::from_slice(&bytes)?);
                }
                MessageFromConsumerThread::Eof => {
                    break SourceItem::Close;
                }
                MessageFromConsumerThread::PreRebalance => {
                    break SourceItem::Restart;
                }
                MessageFromConsumerThread::PostRebalance {
                    partitions,
                    checkpoint_tx,
                } => {
                    let Some(ref checkpoint) = self.checkpoint else {
                        continue;
                    };

                    let partitions_and_offsets = checkpoint
                        .load(&partitions)
                        .await
                        .context("failed to load checkpoint")?;
                    if checkpoint_tx.send(partitions_and_offsets).is_err() {
                        bail!("failed to respond with partition offsets, kafka consumer thread probably closed")
                    }

                    self.partition_to_offset.clear();
                }
            }
        })
    }

    async fn on_index_created(&mut self) -> Result<()> {
        let Some(ref checkpoint) = self.checkpoint else {
            return Ok(());
        };

        let flat = self
            .partition_to_offset
            .iter()
            .map(|(p, o)| (*p, *o))
            .collect::<Vec<_>>();
        checkpoint.save(&flat).await?;

        self.partition_to_offset.clear();

        Ok(())
    }
}
