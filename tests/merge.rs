mod common;

use std::str::FromStr;

use async_tempfile::TempFile;
use clap::Parser;
use color_eyre::Result;
use ctor::ctor;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{fs::remove_dir_all, io::AsyncWriteExt};
use toshokan::{
    args::{DropArgs, IndexArgs, MergeArgs},
    commands::{
        create::run_create_from_config, drop::run_drop, index::run_index, merge::run_merge,
    },
    config::IndexConfig,
};

use crate::common::{get_number_of_files_in_dir, run_postgres, search_one, test_init};

#[ctor]
fn init() {
    test_init();
}

#[tokio::test]
async fn test_merge() -> Result<()> {
    let postgres = run_postgres().await?;
    let mut config = IndexConfig::from_str(include_str!("../example_config.yaml"))?;

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

    let mut file_to_index1 = TempFile::new().await?;
    file_to_index1
        .write_all(r#"{"timestamp":1460530013,"severity_text":"INFO","body":"PacketResponder: BP-108841162-10.10.34.11-1440074360971:blk_1074072698_331874, type=HAS_DOWNSTREAM_IN_PIPELINE terminating","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":58}"#.as_bytes())
        .await?;
    let mut file_to_index2 = TempFile::new().await?;
    file_to_index2
        .write_all(r#"{"timestamp":1460530014,"severity_text":"INFO","body":"Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072706_331882 src: /10.10.34.33:42666 dest: /10.10.34.11:50010","resource":{"service":"datanode/01"},"attributes":{"class":"org.apache.hadoop.hdfs.server.datanode.DataNode"},"tenant_id":46}"#.as_bytes())
        .await?;

    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &file_to_index1.file_path().to_string_lossy(),
        ]),
        &postgres.pool,
    )
    .await?;
    run_index(
        IndexArgs::parse_from([
            "",
            &config.name,
            &file_to_index2.file_path().to_string_lossy(),
        ]),
        &postgres.pool,
    )
    .await?;

    assert_eq!(get_number_of_files_in_dir(&config.path).await?, 2);

    // Search once before merge.
    search_one(
        &config.name,
        "tenant_id:>50 AND severity_text:INFO",
        &postgres.pool,
    )
    .await?;
    search_one(&config.name, "body:Receiving", &postgres.pool).await?;

    run_merge(MergeArgs::parse_from(["", &config.name]), &postgres.pool).await?;
    assert_eq!(get_number_of_files_in_dir(&config.path).await?, 1);

    // Search again after merge.
    search_one(
        &config.name,
        "tenant_id:>50 AND severity_text:INFO",
        &postgres.pool,
    )
    .await?;
    search_one(&config.name, "body:Receiving", &postgres.pool).await?;

    run_drop(DropArgs::parse_from(["", &config.name]), &postgres.pool).await?;
    assert_eq!(get_number_of_files_in_dir(&config.path).await?, 0);

    Ok(())
}
