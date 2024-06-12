mod common;

use std::str::FromStr;

use async_tempfile::TempFile;
use clap::Parser;
use color_eyre::Result;
use ctor::ctor;
use pretty_env_logger::formatted_timed_builder;
use rstest::rstest;
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, sync::mpsc};
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

#[rstest]
#[case(
    include_str!("../example_config.yaml"),
    include_str!("test_files/hdfs-logs-multitenants-2.json"),
    "tenant_id:>50 AND severity_text:INFO",
    r#"{"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"severity_text":"INFO","tenant_id":58,"timestamp":"2016-04-13T06:46:53Z"}"#,
)]
#[case(
    "
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

    run_create_from_config(&config, &postgres.pool).await?;

    let temp_build_dir = TempDir::new()?;
    let mut index_file = TempFile::new().await?;
    index_file.write_all(index_input.trim().as_bytes()).await?;

    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &index_file.file_path().to_string_lossy(),
            "--build-dir",
            &temp_build_dir.path().to_string_lossy(),
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
