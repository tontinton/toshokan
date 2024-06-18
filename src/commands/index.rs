use std::{path::Path, pin::Pin};

use color_eyre::Result;
use futures::{future::pending, Future};
use sqlx::PgPool;
use tantivy::{
    directory::MmapDirectory,
    indexer::NoMergePolicy,
    schema::{Field, Schema},
    Index, IndexWriter, TantivyDocument,
};
use tokio::{
    fs::{create_dir_all, remove_dir_all},
    select,
    task::spawn_blocking,
    time::sleep,
};

use crate::{
    args::IndexArgs,
    commands::{
        field_parser::build_parsers_from_field_configs,
        sources::{connect_to_source, Source, SourceItem},
    },
    config::IndexConfig,
};

use super::{
    dynamic_field_config, field_parser::FieldParser, get_index_config, write_unified_index,
    DYNAMIC_FIELD_NAME,
};

async fn pipe_source_to_index(
    source: &mut Box<dyn Source>,
    schema: Schema,
    field_parsers: &[FieldParser],
    dynamic_field: Field,
    args: &IndexArgs,
    config: &IndexConfig,
    pool: &PgPool,
) -> Result<bool> {
    let id = uuid::Uuid::now_v7().hyphenated().to_string();
    let index_dir = Path::new(&args.build_dir).join(&id);
    let _ = create_dir_all(&index_dir).await;
    let index = Index::open_or_create(MmapDirectory::open(&index_dir)?, schema)?;
    let mut index_writer: IndexWriter = index.writer(args.memory_budget)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut added = 0;
    let mut did_timeout = false;

    let mut commit_timeout_fut: Pin<Box<dyn Future<Output = ()>>> = if args.stream {
        Box::pin(sleep(args.commit_interval))
    } else {
        // Infinite timeout by waiting on a future that never resolves.
        Box::pin(pending::<()>())
    };

    debug!("Piping source -> index of id '{}'", &id);

    'reader_loop: loop {
        let item = select! {
            _ = &mut commit_timeout_fut => {
                did_timeout = true;
                break;
            }
            item = source.get_one() => {
                item?
            }
        };

        let mut json_obj = match item {
            SourceItem::Document(json_obj) => json_obj,
            SourceItem::Close => {
                debug!("Source closed for index of id '{}'", &id);
                break;
            }
            SourceItem::Restart => {
                debug!("Aborting index of id '{}' with {} documents", &id, added);
                if let Err(e) = remove_dir_all(&index_dir).await {
                    warn!("Failed to remove aborted index of id '{}': {}", &id, e);
                }
                return Ok(true);
            }
        };

        let mut doc = TantivyDocument::new();

        for field_parser in field_parsers {
            let name = &field_parser.name;
            let Some(json_value) = json_obj.remove(name) else {
                debug!("Field '{}' in schema but not found", &name);
                continue;
            };

            if let Err(e) = field_parser.add_parsed_field_value(&mut doc, json_value) {
                error!(
                    "Failed to parse '{}' (on {} iteration): {}",
                    &name, added, e
                );
                continue 'reader_loop;
            }
        }

        doc.add_field_value(dynamic_field, json_obj);
        index_writer.add_document(doc)?;
        added += 1;
    }

    if added == 0 {
        debug!("Not writing index: no documents added");
        return Ok(did_timeout);
    }

    info!("Commiting {added} documents");
    index_writer.prepare_commit()?.commit_future().await?;

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(&id, &index, &index_dir, &config.name, &config.path, pool).await?;

    source.on_index_created().await?;

    Ok(did_timeout)
}

pub async fn run_index(args: IndexArgs, pool: &PgPool) -> Result<()> {
    let config = get_index_config(&args.name, pool).await?;

    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(DYNAMIC_FIELD_NAME, dynamic_field_config());
    let field_parsers =
        build_parsers_from_field_configs(&config.schema.fields, &mut schema_builder)?;
    let schema = schema_builder.build();

    let mut source = connect_to_source(args.input.as_deref(), args.stream, pool).await?;

    while pipe_source_to_index(
        &mut source,
        schema.clone(),
        &field_parsers,
        dynamic_field,
        &args,
        &config,
        pool,
    )
    .await?
    {}

    Ok(())
}
