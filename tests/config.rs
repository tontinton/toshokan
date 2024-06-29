mod common;

use std::{path::Path, str::FromStr};

use async_tempfile::TempFile;
use aws_sdk_s3::{
    config::{Credentials, Region, SharedCredentialsProvider},
    Client,
};
use clap::Parser;
use color_eyre::Result;
use ctor::ctor;
use lazy_static::lazy_static;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rstest::rstest;
use rstest_reuse::{self, *};
use testcontainers::{runners::AsyncRunner, ContainerAsync, RunnableImage};
use testcontainers_modules::localstack::LocalStack;
use tokio::{
    fs::{read_dir, remove_dir_all},
    io::AsyncWriteExt,
    sync::{mpsc, Mutex},
};
use toshokan::{
    args::{DropArgs, IndexArgs, SearchArgs},
    commands::{
        create::run_create_from_config, drop::run_drop, index::run_index,
        search::run_search_with_callback,
    },
    config::IndexConfig,
};

use crate::common::{run_postgres, test_init};

lazy_static! {
    static ref ENV_VAR_LOCK: Mutex<()> = Mutex::new(());
}

#[ctor]
fn init() {
    test_init();
}

async fn run_localstack(services: &str) -> Result<ContainerAsync<LocalStack>> {
    let image: RunnableImage<LocalStack> = LocalStack::default().into();
    let image = image.with_env_var(("SERVICES", services));
    let container = image.start().await?;
    Ok(container)
}

async fn get_number_of_files_in_dir<P: AsRef<Path>>(dir: P) -> std::io::Result<usize> {
    let mut file_count = 0;
    let mut entries = read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            file_count += 1;
        }
    }
    Ok(file_count)
}

#[template]
#[rstest]
#[case::example_config(
    include_str!("../example_config.yaml"),
    include_str!("test_files/hdfs-logs-multitenants-2.json"),
    "tenant_id:>50 AND severity_text:INFO",
    r#"{"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"severity_text":"INFO","tenant_id":58,"timestamp":"2016-04-13T06:46:53Z"}"#,
)]
#[case::array_of_u64(
    "
version: 1
name: array_test
path: /tmp/toshokan
schema:
  fields:
    - name: array
      array: true
      type: !number
        type: u64
",
    r#"{"array":[1,2,3,4]}"#,
    "*",
    r#"{"array":[1,2,3,4]}"#
)]
#[case::boolean(
    "
version: 1
name: boolean_test
path: /tmp/toshokan
schema:
  fields:
    - name: something
      type: !boolean
",
    r#"
    {"something":true}
    {"something":false}
    "#,
    "something:false",
    r#"{"something":false}"#
)]
#[case::ip(
    "
version: 1
name: ip_test
path: /tmp/toshokan
schema:
  fields:
    - name: something
      type: !ip
",
    r#"
    {"something":"127.0.0.1"}
    {"something":"2001:db8::ff00:42:8329"}
    {"something":"192.168.0.1"}
    "#,
    "something:[190.0.0.1 TO 195.200.10.1]",
    r#"{"something":"192.168.0.1"}"#
)]
#[case::parse_string(
    "
version: 1
name: parse_string_test
path: /tmp/toshokan
schema:
  fields:
    - name: negative
      type: !number
        type: i64
        parse_string: true
    - name: positive
      type: !number
        type: u64
        parse_string: true
    - name: float
      type: !number
        type: f64
        parse_string: true
    - name: boolean
      type: !boolean
        parse_string: true
",
    r#"
    {"negative": "-100", "positive": "100", "float": "25.52",  "boolean": "FaLsE"}
    {"negative": "100",  "positive": "500", "float": "-25.52", "boolean": "trUe"}
    "#,
    "negative:<0",
    r#"{"boolean":false,"float":25.52,"negative":-100,"positive":100}"#
)]
#[trace]
#[tokio::test]
async fn bunch_of_configs(
    #[case] raw_config: &str,
    #[case] index_input: &str,
    #[case] query: &str,
    #[case] expected_output: &str,
) {
}

#[apply(bunch_of_configs)]
async fn test_config_fs(
    #[case] raw_config: &str,
    #[case] index_input: &str,
    #[case] query: &str,
    #[case] expected_output: &str,
) -> Result<()> {
    let postgres = run_postgres().await?;
    let mut config = IndexConfig::from_str(raw_config)?;

    config.path = format!(
        "/tmp/{}",
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect::<String>()
    );

    // Just in case this path already exists, remove it.
    let _ = remove_dir_all(&config.path).await;

    run_create_from_config(&config, &postgres.pool).await?;

    let mut file_to_index = TempFile::new().await?;
    file_to_index
        .write_all(index_input.trim().as_bytes())
        .await?;

    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &file_to_index.file_path().to_string_lossy(),
        ]),
        &postgres.pool,
    )
    .await?;

    assert_eq!(get_number_of_files_in_dir(&config.path).await?, 1);

    let (tx, mut rx) = mpsc::channel(1);
    run_search_with_callback(
        SearchArgs::parse_from(["", &config.name, query, "--limit", "1"]),
        &postgres.pool,
        Box::new(move |doc| {
            tx.try_send(doc).unwrap();
        }),
    )
    .await?;

    assert_eq!(rx.recv().await.unwrap(), expected_output);

    run_drop(DropArgs::parse_from(["", &config.name]), &postgres.pool).await?;

    assert_eq!(get_number_of_files_in_dir(&config.path).await?, 0);

    Ok(())
}

#[apply(bunch_of_configs)]
async fn test_config_s3(
    #[case] raw_config: &str,
    #[case] index_input: &str,
    #[case] query: &str,
    #[case] expected_output: &str,
) -> Result<()> {
    let postgres = run_postgres().await?;
    let mut config = IndexConfig::from_str(raw_config)?;

    // Running in an isolated container, no need for random path.
    config.path = "s3://toshokan".to_string();

    let container = run_localstack("s3").await?;
    let s3_port = container.get_host_port_ipv4(4566).await?;
    let endpoint_url = format!("http://127.0.0.1:{s3_port}");

    let client = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .endpoint_url(&endpoint_url)
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test1", "test2", None, None, "local",
            )))
            .build(),
    );
    client.create_bucket().bucket("toshokan").send().await?;

    run_create_from_config(&config, &postgres.pool).await?;

    let mut file_to_index = TempFile::new().await?;
    file_to_index
        .write_all(index_input.trim().as_bytes())
        .await?;

    let _lock = ENV_VAR_LOCK.lock().await;
    std::env::set_var("S3_ENDPOINT", &endpoint_url);
    std::env::set_var("AWS_ACCESS_KEY_ID", "test1");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test2");
    std::env::set_var("AWS_REGION", "us-east-1");

    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &file_to_index.file_path().to_string_lossy(),
        ]),
        &postgres.pool,
    )
    .await?;

    let (tx, mut rx) = mpsc::channel(1);
    run_search_with_callback(
        SearchArgs::parse_from(["", &config.name, query, "--limit", "1"]),
        &postgres.pool,
        Box::new(move |doc| {
            tx.try_send(doc).unwrap();
        }),
    )
    .await?;

    assert_eq!(rx.recv().await.unwrap(), expected_output);

    run_drop(DropArgs::parse_from(["", &config.name]), &postgres.pool).await?;

    Ok(())
}
