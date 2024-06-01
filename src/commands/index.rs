use color_eyre::{eyre::eyre, Result};
use sqlx::PgPool;
use tantivy::{
    directory::MmapDirectory,
    indexer::NoMergePolicy,
    schema::{Field, JsonObjectOptions, OwnedValue, Schema, STORED, STRING},
    DateTime, Index, IndexWriter, TantivyDocument,
};
use tokio::{
    fs::{create_dir_all, File},
    io::{AsyncBufReadExt, BufReader},
    task::spawn_blocking,
};

use crate::{args::IndexArgs, index_config::FieldType};

use super::{get_index_config, write_unified_index, DYNAMIC_FIELD_NAME};

fn common_parse(value: serde_json::Value) -> Result<OwnedValue> {
    Ok(serde_json::from_value(value)?)
}

pub fn parse_timestamp(timestamp: i64) -> Result<DateTime> {
    // Minimum supported timestamp value in seconds (13 Apr 1972 23:59:55 GMT).
    const MIN_TIMESTAMP_SECONDS: i64 = 72_057_595;

    // Maximum supported timestamp value in seconds (16 Mar 2242 12:56:31 GMT).
    const MAX_TIMESTAMP_SECONDS: i64 = 8_589_934_591;

    const MIN_TIMESTAMP_MILLIS: i64 = MIN_TIMESTAMP_SECONDS * 1000;
    const MAX_TIMESTAMP_MILLIS: i64 = MAX_TIMESTAMP_SECONDS * 1000;
    const MIN_TIMESTAMP_MICROS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000;
    const MAX_TIMESTAMP_MICROS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000;
    const MIN_TIMESTAMP_NANOS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
    const MAX_TIMESTAMP_NANOS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000_000;

    match timestamp {
        MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS => {
            Ok(DateTime::from_timestamp_secs(timestamp))
        }
        MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS => {
            Ok(DateTime::from_timestamp_millis(timestamp))
        }
        MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS => {
            Ok(DateTime::from_timestamp_micros(timestamp))
        }
        MIN_TIMESTAMP_NANOS..=MAX_TIMESTAMP_NANOS => Ok(DateTime::from_timestamp_nanos(timestamp)),
        _ => Err(eyre!(
            "failed to parse unix timestamp `{timestamp}`. Supported timestamp ranges \
             from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`"
        )),
    }
}

pub async fn run_index(args: IndexArgs, pool: PgPool) -> Result<()> {
    let config = get_index_config(&args.name, &pool).await?;

    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(
        DYNAMIC_FIELD_NAME,
        JsonObjectOptions::from(STORED | STRING).set_expand_dots_enabled(),
    );

    let mut fields = Vec::<(String, Field, fn(serde_json::Value) -> Result<OwnedValue>)>::new();
    for (name, schema) in config.schema.mappings {
        match schema.type_ {
            FieldType::Text(options) => {
                let field = schema_builder.add_text_field(&name, options);
                fields.push((name, field, common_parse));
            }
            FieldType::Datetime(options) => {
                let field = schema_builder.add_date_field(&name, options);
                fields.push((name, field, |v| {
                    let timestamp: i64 = serde_json::from_value(v)?;
                    Ok(parse_timestamp(timestamp)?.into())
                }));
            }
        }
    }

    let schema = schema_builder.build();

    let _ = create_dir_all(&args.build_dir).await;
    let index = Index::open_or_create(MmapDirectory::open(&args.build_dir)?, schema)?;
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

        for (name, field, parse_fn) in &fields {
            if let Some(value) = json_obj.remove(name) {
                doc.add_field_value(*field, parse_fn(value)?);
            }
        }

        doc.add_field_value(dynamic_field, json_obj);
        index_writer.add_document(doc)?;
        added += 1;

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

    write_unified_index(index, &args.build_dir, &config.name, &config.path, &pool).await?;

    Ok(())
}
