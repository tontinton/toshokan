pub mod create;
pub mod drop;
mod field_parser;
pub mod index;
pub mod merge;
pub mod search;
pub mod sources;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use color_eyre::{eyre::bail, Result};
use futures::future::try_join_all;
use opendal::{layers::LoggingLayer, Operator};
use sqlx::{query, query_as, PgPool};
use tantivy::{directory::FileSlice, schema::IndexRecordOption, Index};
use tokio::{io::AsyncWriteExt, runtime::Builder, task::spawn_blocking};
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use crate::{
    config::{
        dynamic_object::{
            DynamicObjectFieldConfig, IndexedDynamicObjectFieldConfig,
            IndexedDynamicObjectFieldType,
        },
        FastFieldNormalizerType, FieldTokenizerType, IndexConfig,
    },
    opendal_file_handle::OpenDalFileHandle,
    unified_index::{
        file_cache::build_file_cache, unified_directory::UnifiedDirectory,
        writer::UnifiedIndexWriter,
    },
};

const DYNAMIC_FIELD_NAME: &str = "_dynamic";
const S3_PREFIX: &str = "s3://";

fn dynamic_field_config() -> DynamicObjectFieldConfig {
    DynamicObjectFieldConfig {
        stored: true,
        fast: FastFieldNormalizerType::False,
        indexed: IndexedDynamicObjectFieldType::Indexed(IndexedDynamicObjectFieldConfig {
            record: IndexRecordOption::Basic,
            tokenizer: FieldTokenizerType::Default,
        }),
        expand_dots: true,
    }
}

async fn get_index_config(name: &str, pool: &PgPool) -> Result<IndexConfig> {
    let (value,): (serde_json::Value,) = query_as("SELECT config FROM indexes WHERE name=$1")
        .bind(name)
        .fetch_one(pool)
        .await?;
    Ok(serde_json::from_value(value)?)
}

async fn get_index_path(name: &str, pool: &PgPool) -> Result<String> {
    let (value,): (serde_json::Value,) =
        query_as("SELECT config->'path' FROM indexes WHERE name=$1")
            .bind(name)
            .fetch_one(pool)
            .await?;
    Ok(serde_json::from_value(value)?)
}

async fn get_operator(path: &str) -> Result<Operator> {
    let op = if let Some(bucket) = path.strip_prefix(S3_PREFIX) {
        let mut unset_env_vars = Vec::new();
        for env_var in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"] {
            if let Err(e) = std::env::var(env_var) {
                debug!("Failed to check env var '{env_var}': {e}");
                unset_env_vars.push(env_var);
            }
        }

        if !unset_env_vars.is_empty() {
            bail!(
                "The following mandatory environment variables to use s3 are not set: {unset_env_vars:?}"
            );
        }

        let endpoint =
            std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());

        let mut s3 = opendal::services::S3::default();
        s3.endpoint(&endpoint).bucket(bucket);
        Operator::new(s3)?.layer(LoggingLayer::default()).finish()
    } else {
        let mut fs = opendal::services::Fs::default();
        fs.root(path);
        Operator::new(fs)?.layer(LoggingLayer::default()).finish()
    };

    Ok(op)
}

async fn open_unified_directories(
    index_path: &str,
    pool: &PgPool,
) -> Result<Vec<(String, UnifiedDirectory)>> {
    let op = get_operator(index_path).await?;

    let items = query!("SELECT id, file_name, len, footer_len FROM index_files")
        .fetch_all(pool)
        .await?;

    // Tantivy doesn't support async (tantivy::directory::FileHandle's read_bytes() is a sync fn),
    // and opendal's s3 service doesn't support sync...
    // To make them work together, we create a tokio runtime just for making opendal async calls
    // when tantivy requests a read from our opendal file.
    // The efficient solution is to make tantivy work in async rust, but that is a journey for
    // another time.
    let runtime = Arc::new(Builder::new_multi_thread().enable_all().build()?);

    let mut directories_args = Vec::with_capacity(items.len());
    for item in items {
        let reader = op.reader_with(&item.file_name).await?;
        let file_slice = FileSlice::new(Arc::new(OpenDalFileHandle::new(
            runtime.clone(),
            reader,
            item.len as usize,
        )));
        directories_args.push((item.id, file_slice, item.footer_len as usize))
    }

    let results = try_join_all(
        directories_args
            .into_iter()
            .map(|(id, file_slice, footer_len)| {
                spawn_blocking(move || -> Result<(String, UnifiedDirectory)> {
                    Ok((id, UnifiedDirectory::open_with_len(file_slice, footer_len)?))
                })
            }),
    )
    .await?;

    results.into_iter().collect::<Result<_>>()
}

async fn write_unified_index(
    id: &str,
    index: &Index,
    input_dir: &Path,
    index_name: &str,
    index_path: &str,
    pool: &PgPool,
) -> Result<()> {
    let op = get_operator(index_path).await?;

    let cloned_input_dir = PathBuf::from(input_dir);
    let file_cache = spawn_blocking(move || build_file_cache(&cloned_input_dir)).await??;

    let unified_index_writer =
        UnifiedIndexWriter::from_file_paths(input_dir, index.directory().list_managed_files())
            .await?;

    let file_name = format!("{}.index", id);
    let mut writer = op
        .writer_with(&file_name)
        .content_type("application/octet-stream")
        .chunk(5_000_000)
        .await?
        .into_futures_async_write()
        // Turn a futures::AsyncWrite into something that implements tokio::io::AsyncWrite.
        .compat_write();

    info!("Writing unified index file");
    let (total_len, footer_len) = unified_index_writer.write(&mut writer, file_cache).await?;
    writer.shutdown().await?;

    query(
        "INSERT INTO index_files (id, index_name, file_name, len, footer_len) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(index_name)
    .bind(&file_name)
    .bind(total_len as i64)
    .bind(footer_len as i64)
    .execute(pool)
    .await?;

    debug!(
        "Index file length: {}. Footer length: {}",
        total_len, footer_len
    );

    Ok(())
}
