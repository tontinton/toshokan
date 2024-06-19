mod common;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use clap::Parser;
use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use ctor::ctor;
use notify::{recommended_watcher, Event, EventKind, RecursiveMode, Watcher};
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::{Kafka as KafkaContainer, KAFKA_PORT};
use tokio::{
    fs::{create_dir_all, remove_dir_all},
    select,
    sync::mpsc,
};
use toshokan::{
    args::IndexArgs,
    commands::{create::run_create_from_config, index::run_index},
    config::IndexConfig,
};

use crate::common::{run_postgres, test_init};

#[ctor]
fn init() {
    test_init();
}

pub async fn watch_for_new_file(dir: &str) -> Result<PathBuf> {
    let (tx, mut rx) = mpsc::channel(1);

    let mut watcher = recommended_watcher(move |event| {
        tx.blocking_send(event).unwrap();
    })?;
    watcher.watch(&Path::new(dir), RecursiveMode::NonRecursive)?;

    loop {
        match rx.recv().await {
            Some(Ok(event)) => match event {
                Event {
                    kind: EventKind::Create(_),
                    paths,
                    ..
                } => {
                    if let Some(path) = paths.first() {
                        return Ok(path.clone());
                    }
                }
                _ => (),
            },
            Some(Err(e)) => {
                bail!("failed to watch '{dir}': {e}");
            }
            None => {
                panic!("failed to watch '{dir}'");
            }
        }
    }
}

#[tokio::test]
async fn test_kafka_index_stream() -> Result<()> {
    let postgres = run_postgres().await?;

    let kafka_container = KafkaContainer::default().start().await?;
    let kafka_port = kafka_container.get_host_port_ipv4(KAFKA_PORT).await?;
    let kafka_url = format!("kafka://127.0.0.1:{kafka_port}/test_topic");

    let mut config = IndexConfig::from_str(include_str!("../example_config.yaml"))?;
    config.path = "/tmp/toshokan_kafka_stream".to_string();

    // Just in case this path already exists, remove it.
    let _ = remove_dir_all(&config.path).await;
    create_dir_all(&config.path).await?;

    run_create_from_config(&config, &postgres.pool).await?;

    let index_stream_fut = run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &kafka_url,
            "--stream",
            "--commit-interval",
            "500ms",
        ]),
        &postgres.pool,
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{kafka_port}"))
        .set("message.timeout.ms", "5000")
        .create()
        .context("producer creation error")?;

    let logs = include_str!("test_files/hdfs-logs-multitenants-2.json");
    for log in logs.trim().lines() {
        producer
            .send(
                FutureRecord::to("test_topic")
                    .payload(log)
                    .key("key")
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
    producer.flush(Duration::from_secs(5))?;

    select! {
        result = watch_for_new_file(&config.path) => {
            let file_path = result?;
            assert!(file_path.to_string_lossy().trim_end().ends_with(".index"));
        }
        _ = index_stream_fut => {
            panic!("stream indexing should not exit before creating an index file");
        }
    }

    Ok(())
}
