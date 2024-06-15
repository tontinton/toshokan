pub mod create;
pub mod drop;
mod field_parser;
pub mod index;
pub mod merge;
pub mod search;
mod sources;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use color_eyre::Result;
use futures::future::try_join_all;
use opendal::{layers::LoggingLayer, BlockingOperator, Operator};
use sqlx::{query, query_as, PgPool};
use tantivy::{directory::FileSlice, schema::IndexRecordOption, Index};
use tokio::{io::AsyncWriteExt, task::spawn_blocking};
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

async fn open_unified_directories(
    index_dir: &str,
    pool: &PgPool,
) -> Result<Vec<(String, UnifiedDirectory)>> {
    let items = query!("SELECT id, file_name, footer_len FROM index_files")
        .fetch_all(pool)
        .await?;

    let mut builder = opendal::services::Fs::default();
    builder.root(index_dir);

    let op: BlockingOperator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish()
        .blocking();

    let mut directories_args = Vec::with_capacity(items.len());
    for item in items {
        let reader = op.reader_with(&item.file_name).call()?;
        let file_slice = FileSlice::new(Arc::new(
            OpenDalFileHandle::from_path(&Path::new(index_dir).join(&item.file_name), reader)
                .await?,
        ));

        directories_args.push((item.id, file_slice, item.footer_len))
    }

    let results = try_join_all(
        directories_args
            .into_iter()
            .map(|(id, file_slice, footer_len)| {
                spawn_blocking(move || -> Result<(String, UnifiedDirectory)> {
                    Ok((
                        id,
                        UnifiedDirectory::open_with_len(file_slice, footer_len as usize)?,
                    ))
                })
            }),
    )
    .await?;

    results.into_iter().collect::<Result<_>>()
}

async fn write_unified_index(
    index: &Index,
    input_dir: &str,
    index_name: &str,
    index_dir: &str,
    pool: &PgPool,
) -> Result<()> {
    let cloned_input_dir = PathBuf::from(input_dir);
    let file_cache = spawn_blocking(move || build_file_cache(&cloned_input_dir)).await??;

    let unified_index_writer = UnifiedIndexWriter::from_file_paths(
        Path::new(input_dir),
        index.directory().list_managed_files(),
    )
    .await?;

    let mut builder = opendal::services::Fs::default();
    builder.root(index_dir);

    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let id = uuid::Uuid::now_v7();
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
        "INSERT INTO index_files (id, index_name, file_name, footer_len) VALUES ($1, $2, $3, $4)",
    )
    .bind(&id.to_string())
    .bind(index_name)
    .bind(&file_name)
    .bind(footer_len as i64)
    .execute(pool)
    .await?;

    debug!(
        "Index file length: {}. Footer length: {}",
        total_len, footer_len
    );

    Ok(())
}
