mod args;
mod bincode;
mod merge_directory;
mod opendal_file_handle;
mod unified_index;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use args::{IndexArgs, MergeArgs, SearchArgs};
use color_eyre::eyre::Result;
use dotenvy::dotenv;
use futures::future::{try_join, try_join_all};
use opendal::{layers::LoggingLayer, BlockingOperator, Operator};
use pretty_env_logger::formatted_timed_builder;
use sqlx::{postgres::PgPoolOptions, query, PgPool};
use tantivy::{
    collector::TopDocs,
    directory::{DirectoryClone, FileSlice, MmapDirectory},
    indexer::NoMergePolicy,
    query::QueryParser,
    schema::{
        DateOptions, DateTimePrecision, JsonObjectOptions, Schema, FAST, INDEXED, STORED, STRING,
    },
    DateTime, Document, Index, IndexWriter, ReloadPolicy, TantivyDocument,
};
use tokio::{
    fs::{create_dir, create_dir_all, remove_file, File},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::Builder,
    spawn,
    sync::mpsc::channel,
    task::spawn_blocking,
};
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use unified_index::unified_directory::UnifiedDirectory;

use crate::{
    args::{parse_args, SubCommand},
    merge_directory::MergeDirectory,
    opendal_file_handle::OpenDalFileHandle,
    unified_index::{file_cache::build_file_cache, writer::UnifiedIndexWriter},
};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_DEBUG_LOG_LEVEL: &str = "toshokan=trace,opendal::services=info";
const DEFAULT_RELEASE_LOG_LEVEL: &str = "toshokan=info,opendal::services=info";

const MAX_DB_CONNECTIONS: u32 = 100;

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
    index: Index,
    input_dir: &str,
    output_dir: &str,
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
    builder.root(output_dir);

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

    query("INSERT INTO index_files (id, file_name, footer_len) VALUES ($1, $2, $3)")
        .bind(&id.to_string())
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

async fn index(args: IndexArgs, pool: PgPool, index_dir: &str) -> Result<()> {
    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(
        "_dynamic",
        JsonObjectOptions::from(STORED | STRING).set_expand_dots_enabled(),
    );
    let timestamp_field = schema_builder.add_date_field(
        "timestamp",
        DateOptions::from(INDEXED | STORED | FAST).set_precision(DateTimePrecision::Seconds),
    );

    let schema = schema_builder.build();

    let _ = create_dir_all(&args.build_dir).await;
    let index = Index::open_or_create(MmapDirectory::open(&args.build_dir)?, schema.clone())?;
    let mut index_writer: IndexWriter = index.writer(args.memory_budget)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut reader = BufReader::new(File::open(&args.input_path).await?);

    let mut line = String::new();
    let mut added = 0;

    loop {
        let len = reader.read_line(&mut line).await?;
        if len == 0 {
            break;
        }

        let mut doc = TantivyDocument::new();
        let mut json_obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&line)?;
        if let Some(timestamp_val) = json_obj.remove("timestamp") {
            if let Some(timestamp) = timestamp_val.as_i64() {
                doc.add_field_value(timestamp_field, DateTime::from_timestamp_secs(timestamp));
                doc.add_field_value(dynamic_field, json_obj);
                index_writer.add_document(doc)?;
                added += 1;
            }
        }

        line.clear();
    }

    info!("Commiting {added} documents");
    index_writer.prepare_commit()?.commit_future().await?;

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(index, &args.build_dir, index_dir, &pool).await?;

    Ok(())
}

async fn merge(args: MergeArgs, pool: PgPool, index_dir: &str) -> Result<()> {
    let (ids, directories): (Vec<_>, Vec<_>) = open_unified_directories(index_dir, &pool)
        .await?
        .into_iter()
        .map(|(id, dir)| (id, dir.box_clone()))
        .unzip();

    if directories.len() <= 1 {
        info!("Need at least 2 files in index directory to be able to merge");
        return Ok(());
    }

    let _ = create_dir(&args.merge_dir).await;
    let output_dir = MmapDirectory::open(&args.merge_dir)?;

    let index = Index::open(MergeDirectory::new(directories, output_dir.box_clone())?)?;
    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(index, &args.merge_dir, index_dir, &pool).await?;

    let delete_result = query("DELETE FROM index_files WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&pool)
        .await;

    for id in ids {
        let _ = remove_file(
            Path::new(index_dir)
                .join(format!("{}.index", id))
                .to_str()
                .expect("failed to build index path"),
        )
        .await;
    }

    delete_result?;

    Ok(())
}

async fn search(args: SearchArgs, directories: Vec<UnifiedDirectory>) -> Result<()> {
    if args.limit == 0 {
        return Ok(());
    }

    let (tx, mut rx) = channel(args.limit);
    let mut tx_handles = Vec::with_capacity(directories.len());

    // Should be chunked to never starve the thread pool (default in tokio is 500 threads).
    for directory in directories {
        let tx = tx.clone();
        let query = args.query.clone();

        // Should use rayon if search ends up being cpu bound (it seems io bound).
        tx_handles.push(spawn_blocking(move || -> Result<()> {
            if tx.is_closed() {
                return Ok(());
            }

            let index = Index::open(directory)?;
            let schema = index.schema();

            let dynamic_field = schema.get_field("_dynamic")?;
            let timestamp_field = schema.get_field("timestamp")?;

            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();

            let query_parser = QueryParser::for_index(&index, vec![dynamic_field, timestamp_field]);
            let query = query_parser.parse_query(&query)?;
            let docs = searcher.search(&query, &TopDocs::with_limit(args.limit))?;

            if tx.is_closed() {
                return Ok(());
            }

            for (_, doc_address) in docs {
                let doc: TantivyDocument = searcher.doc(doc_address)?;
                if tx.blocking_send(doc.to_json(&schema)).is_err() {
                    return Ok(());
                }
            }

            Ok(())
        }));
    }

    let rx_handle = spawn(async move {
        let mut i = 0;
        while let Some(doc) = rx.recv().await {
            println!("{}", doc);
            i += 1;
            if i == args.limit {
                break;
            }
        }
        rx.close();
    });

    try_join(try_join_all(tx_handles), rx_handle).await?;

    Ok(())
}

async fn open_db_pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(url)
        .await?)
}

async fn async_main() -> Result<()> {
    color_eyre::install()?;

    // Load vars inside .env into env vars, does nothing if the file does not exist.
    let _ = dotenv();

    let default_log_level = if cfg!(debug_assertions) {
        DEFAULT_DEBUG_LOG_LEVEL
    } else {
        DEFAULT_RELEASE_LOG_LEVEL
    };

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters(
        &std::env::var("RUST_LOG").unwrap_or_else(|_| default_log_level.to_string()),
    );
    log_builder.try_init()?;

    let args = parse_args();

    let pool = open_db_pool(&args.db.unwrap_or_else(|| {
        std::env::var("DATABASE_URL")
            .expect("database url must be provided using either --db or DATABASE_URL env var")
    }))
    .await?;

    match args.subcmd {
        SubCommand::Index(index_args) => {
            index(index_args, pool, &args.index_dir).await?;
        }
        SubCommand::Merge(merge_args) => {
            merge(merge_args, pool, &args.index_dir).await?;
        }
        SubCommand::Search(search_args) => {
            let directories = open_unified_directories(&args.index_dir, &pool)
                .await?
                .into_iter()
                .map(|(_, x)| x)
                .collect::<Vec<_>>();
            search(search_args, directories).await?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let runtime = Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_secs(20))
        .enable_all()
        .build()?;
    runtime.block_on(async_main())
}
