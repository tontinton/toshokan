mod args;
mod bincode;
mod opendal_file_handle;
mod unified_index;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use args::{IndexArgs, SearchArgs};
use color_eyre::eyre::Result;
use futures::future::{try_join, try_join_all};
use opendal::{layers::LoggingLayer, Operator};
use pretty_env_logger::formatted_timed_builder;
use tantivy::{
    collector::TopDocs,
    directory::{FileSlice, MmapDirectory},
    indexer::NoMergePolicy,
    query::QueryParser,
    schema::{
        DateOptions, DateTimePrecision, JsonObjectOptions, Schema, FAST, INDEXED, STORED, STRING,
    },
    DateTime, Document, Index, IndexWriter, ReloadPolicy, TantivyDocument,
};
use tokio::{
    fs::{create_dir, read_dir, read_to_string, write, File},
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
    opendal_file_handle::OpenDalFileHandle,
    unified_index::{file_cache::build_file_cache, writer::UnifiedIndexWriter},
};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_DEBUG_LOG_LEVEL: &str = "toshokan=trace,opendal::services=info";
const DEFAULT_RELEASE_LOG_LEVEL: &str = "toshokan=info,opendal::services=info";

async fn open_unified_directories(index_dir: &str) -> Result<Vec<UnifiedDirectory>> {
    let mut index_ids = HashSet::new();
    let mut dir_reader = read_dir(index_dir).await?;
    while let Some(entry) = dir_reader.next_entry().await? {
        if let Some(filename) = entry.file_name().to_str() {
            index_ids.insert(
                filename
                    .chars()
                    .take_while(|&c| c != '.')
                    .collect::<String>(),
            );
        }
    }

    let mut builder = opendal::services::Fs::default();
    builder.root(index_dir);

    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let mut directories_args = Vec::with_capacity(index_ids.len());
    for id in index_ids {
        let index_filename = format!("{}.index", id);
        let reader = op.reader_with(&index_filename).await?;
        let file_slice = FileSlice::new(Arc::new(
            OpenDalFileHandle::from_path(&Path::new(index_dir).join(&index_filename), reader)
                .await?,
        ));

        let footer_len = read_to_string(&Path::new(index_dir).join(&format!("{}.footer", id)))
            .await?
            .parse::<u64>()?;

        directories_args.push((file_slice, footer_len))
    }

    let results = try_join_all(
        directories_args
            .into_iter()
            .map(|(file_slice, footer_len)| {
                spawn_blocking(move || -> Result<UnifiedDirectory> {
                    UnifiedDirectory::open_with_len(file_slice, footer_len as usize)
                })
            }),
    )
    .await?;

    results.into_iter().collect::<Result<_>>()
}

async fn index(args: IndexArgs) -> Result<()> {
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

    let _ = create_dir(&args.build_dir).await;
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

    let build_dir_path = PathBuf::from(&args.build_dir);
    let file_cache = spawn_blocking(move || build_file_cache(&build_dir_path)).await??;

    let unified_index_writer = UnifiedIndexWriter::from_file_paths(
        Path::new(&args.build_dir),
        index.directory().list_managed_files(),
    )
    .await?;

    let mut builder = opendal::services::Fs::default();
    builder.root(&args.index_dir);

    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let id = uuid::Uuid::now_v7();
    let mut writer = op
        .writer_with(&format!("{}.index", id))
        .content_type("application/octet-stream")
        .chunk(5_000_000)
        .await?
        .into_futures_async_write()
        // Turn a futures::AsyncWrite into something that implements tokio::io::AsyncWrite.
        .compat_write();

    info!("Writing unified index file");
    let (total_len, footer_len) = unified_index_writer.write(&mut writer, file_cache).await?;
    writer.shutdown().await?;

    write(
        Path::new(&args.index_dir).join(format!("{}.footer", id)),
        footer_len.to_string(),
    )
    .await?;

    debug!(
        "Index file length: {}. Footer length: {}",
        total_len, footer_len
    );

    Ok(())
}

async fn search(args: SearchArgs) -> Result<()> {
    if args.limit == 0 {
        return Ok(());
    }

    let directories = open_unified_directories(&args.index_dir).await?;

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

async fn async_main() -> Result<()> {
    color_eyre::install()?;

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
    match args.subcmd {
        SubCommand::Index(index_args) => {
            index(index_args).await?;
        }
        SubCommand::Search(search_args) => {
            search(search_args).await?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let runtime = Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_secs(20))
        .build()?;
    runtime.block_on(async_main())
}
