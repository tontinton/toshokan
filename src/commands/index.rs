use std::path::Path;

use color_eyre::Result;
use sqlx::PgPool;
use tantivy::{
    directory::MmapDirectory,
    indexer::NoMergePolicy,
    schema::{Field, Schema},
    Index, IndexWriter, TantivyDocument,
};
use tokio::{fs::create_dir_all, select, task::spawn_blocking, time::sleep};

use crate::{
    args::IndexArgs,
    commands::{
        field_parser::build_parsers_from_field_configs,
        sources::{connect_to_source, Source},
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
) -> Result<()> {
    let id = uuid::Uuid::now_v7().hyphenated().to_string();
    let index_dir = Path::new(&args.build_dir).join(&id);
    let _ = create_dir_all(&index_dir).await;
    let index = Index::open_or_create(MmapDirectory::open(&index_dir)?, schema)?;
    let mut index_writer: IndexWriter = index.writer(args.memory_budget)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut added = 0;

    let commit_timeout = sleep(args.commit_interval);
    tokio::pin!(commit_timeout);

    'reader_loop: loop {
        let mut json_obj = if args.stream {
            select! {
                _ = &mut commit_timeout => {
                    break;
                }
                maybe_json_obj = source.get_one() => {
                    let Some(json_obj) = maybe_json_obj? else {
                        break;
                    };
                    json_obj
                }
            }
        } else {
            let Some(json_obj) = source.get_one().await? else {
                break;
            };
            json_obj
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
        return Ok(());
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

    Ok(())
}

pub async fn run_index(args: IndexArgs, pool: &PgPool) -> Result<()> {
    let config = get_index_config(&args.name, pool).await?;

    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(DYNAMIC_FIELD_NAME, dynamic_field_config());
    let field_parsers =
        build_parsers_from_field_configs(&config.schema.fields, &mut schema_builder)?;
    let schema = schema_builder.build();

    let mut source = connect_to_source(args.input.as_deref(), args.stream).await?;

    if args.stream {
        loop {
            pipe_source_to_index(
                &mut source,
                schema.clone(),
                &field_parsers,
                dynamic_field,
                &args,
                &config,
                pool,
            )
            .await?;
        }
    } else {
        pipe_source_to_index(
            &mut source,
            schema,
            &field_parsers,
            dynamic_field,
            &args,
            &config,
            pool,
        )
        .await?;
    }

    Ok(())
}
