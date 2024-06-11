mod common;

use std::str::FromStr;

use clap::Parser;
use color_eyre::Result;
use ctor::ctor;
use pretty_env_logger::formatted_timed_builder;
use tokio::sync::mpsc;
use toshokan::{
    args::{DropArgs, IndexArgs, SearchArgs},
    commands::{
        create::run_create_from_config, drop::run_drop, index::run_index,
        search::run_search_with_callback,
    },
    config::IndexConfig,
};

use crate::common::{get_test_file_path, run_postgres};

#[ctor]
fn init() {
    color_eyre::install().unwrap();

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("toshokan=trace,opendal::services=info");
    log_builder.try_init().unwrap();
}

#[tokio::test]
async fn test_example_config() -> Result<()> {
    let postgres = run_postgres().await?;
    let config = IndexConfig::from_str(include_str!("../example_config.yaml"))?;

    run_create_from_config(&config, &postgres.pool).await?;

    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &get_test_file_path("hdfs-logs-multitenants-2.json").to_string_lossy(),
        ]),
        &postgres.pool,
    )
    .await?;

    let (tx, mut rx) = mpsc::channel(1);
    run_search_with_callback(
        SearchArgs::parse_from([
            "",
            &config.name,
            "tenant_id:>50 AND severity_text:INFO",
            "--limit",
            "1",
        ]),
        &postgres.pool,
        Box::new(move |doc| {
            tx.try_send(doc).unwrap();
        }),
    )
    .await?;

    assert_eq!(
        rx.recv().await.unwrap(),
        r#"{"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"severity_text":"INFO","tenant_id":58,"timestamp":"2016-04-13T06:46:53Z"}"#
    );

    run_drop(DropArgs::parse_from(["", &config.name]), &postgres.pool).await?;

    Ok(())
}
