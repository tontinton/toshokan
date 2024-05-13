mod args;
mod bincode;
mod opendal_reader;
mod unified_index;

use std::{
    fs::{create_dir, read_to_string, write, File},
    io::{BufRead, BufReader},
    path::Path,
    sync::Arc,
};

use args::{IndexArgs, SearchArgs};
use once_cell::sync::Lazy;
use opendal::{
    layers::{BlockingLayer, LoggingLayer},
    BlockingOperator, Operator,
};
use tantivy::{
    collector::TopDocs,
    directory::{FileSlice, MmapDirectory},
    query::QueryParser,
    schema::{
        DateOptions, DateTimePrecision, JsonObjectOptions, Schema, FAST, INDEXED, STORED, STRING,
    },
    DateTime, Document, Index, IndexWriter, ReloadPolicy, TantivyDocument,
};
use unified_index::directory::UnifiedDirectory;

use crate::{
    args::{parse_args, SubCommand},
    opendal_reader::OpenDalReader,
    unified_index::writer::UnifiedIndexWriter,
};

const FOOTER_FILE_PATH: &str = "/tmp/footer";

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

fn index(args: IndexArgs) -> anyhow::Result<()> {
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

    let _ = create_dir(&args.build_dir);
    let index = Index::open_or_create(MmapDirectory::open(&args.build_dir)?, schema.clone())?;
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let mut reader = BufReader::new(File::open(&args.input_path)?);

    let mut line = String::new();
    let mut i = 0;
    let mut added = 0;

    loop {
        let len = reader.read_line(&mut line)?;
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

        i += 1;
        if i % 1000 == 0 {
            println!("{i}");
        }
    }

    println!("Commiting {added} documents, after processing {i}");
    index_writer.commit()?;

    if args.merge {
        let segment_ids = index.searchable_segment_ids()?;
        println!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).wait()?;
    }

    println!("Joining merging threads");
    index_writer.wait_merging_threads()?;

    let unified_index_writer = UnifiedIndexWriter::from_file_paths(
        Path::new(&args.build_dir),
        index.directory().list_managed_files(),
    )?;

    let mut builder = opendal::services::Fs::default();
    builder.root("/");

    let _guard = RUNTIME.enter();
    let op: BlockingOperator = Operator::new(builder)?
        .layer(BlockingLayer::create()?)
        .layer(LoggingLayer::default())
        .finish()
        .blocking();

    let mut writer = op
        .writer_with(&args.output_path)
        .content_type("application/octet-stream")
        .buffer(5_000_000)
        .call()?
        .into_std_write();

    println!("Writing unified index file");
    let (total_len, footer_len) = unified_index_writer.write(&mut writer)?;
    writer.close()?;

    write(FOOTER_FILE_PATH, footer_len.to_string())?;

    println!(
        "Index file length: {}. Footer length: {}",
        total_len, footer_len
    );

    Ok(())
}

fn search(args: SearchArgs) -> anyhow::Result<()> {
    let mut builder = opendal::services::Fs::default();
    builder.root("/");

    let _guard = RUNTIME.enter();
    let op: BlockingOperator = Operator::new(builder)?
        .layer(BlockingLayer::create()?)
        .layer(LoggingLayer::default())
        .finish()
        .blocking();

    let reader = op.reader_with(&args.input_file).call()?;
    let file_slice = FileSlice::new(Arc::new(OpenDalReader::from_path(
        Path::new(&args.input_file),
        reader,
    )?));

    let footer = if let Some(f) = args.footer {
        f
    } else {
        read_to_string(FOOTER_FILE_PATH)?.parse::<u64>()?
    };

    let index = Index::open(UnifiedDirectory::open_with_len(
        file_slice,
        footer as usize,
    )?)?;
    let schema = index.schema();

    let dynamic_field = schema.get_field("_dynamic")?;
    let timestamp_field = schema.get_field("timestamp")?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![dynamic_field, timestamp_field]);
    let query = query_parser.parse_query(&args.query)?;
    let docs = searcher.search(&query, &TopDocs::with_limit(args.limit))?;

    for (_, doc_address) in docs {
        let doc: TantivyDocument = searcher.doc(doc_address)?;
        println!("{}", doc.to_json(&schema));
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = parse_args();
    match args.subcmd {
        SubCommand::Index(index_args) => {
            index(index_args)?;
        }
        SubCommand::Search(search_args) => {
            search(search_args)?;
        }
    }

    Ok(())
}
