mod common;

use std::{path::Path, str::FromStr};

use async_tempfile::TempFile;
use clap::Parser;
use color_eyre::Result;
use ctor::ctor;
use pretty_env_logger::formatted_timed_builder;
use rstest::rstest;
use tokio::{
    fs::{read_dir, remove_dir_all},
    io::AsyncWriteExt,
    sync::mpsc,
};
use toshokan::{
    args::{DropArgs, IndexArgs, SearchArgs},
    commands::{
        create::run_create_from_config, drop::run_drop, index::run_index,
        search::run_search_with_callback,
    },
    config::IndexConfig,
};

use crate::common::run_postgres;

#[ctor]
fn init() {
    color_eyre::install().unwrap();

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("toshokan=trace,opendal::services=info");
    log_builder.try_init().unwrap();
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

#[rstest]
#[case(
    include_str!("../example_config.yaml"),
    include_str!("test_files/hdfs-logs-multitenants-2.json"),
    "tenant_id:>50 AND severity_text:INFO",
    r#"{"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"severity_text":"INFO","tenant_id":58,"timestamp":"2016-04-13T06:46:53Z"}"#,
)]
#[case(
    "
version: 1
name: array_test
path: /tmp/toshokan_array
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
#[case(
    "
version: 1
name: boolean_test
path: /tmp/toshokan_boolean
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
#[case(
    "
version: 1
name: ip_test
path: /tmp/toshokan_ip
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
#[case(
    "
version: 1
name: parse_string_test
path: /tmp/toshokan_parse_string
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
async fn test_config(
    #[case] raw_config: &str,
    #[case] index_input: &str,
    #[case] query: &str,
    #[case] expected_output: &str,
) -> Result<()> {
    let postgres = run_postgres().await?;
    let config = IndexConfig::from_str(raw_config)?;

    // Just in case this path already exists, remove it.
    let _ = remove_dir_all(&config.path).await;

    run_create_from_config(&config, &postgres.pool).await?;

    let mut index_file = TempFile::new().await?;
    index_file.write_all(index_input.trim().as_bytes()).await?;

    run_index(
        IndexArgs::parse_from(["", &config.name, &index_file.file_path().to_string_lossy()]),
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
