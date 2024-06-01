use std::collections::BTreeMap;

use color_eyre::Result;
use futures::future::{try_join, try_join_all};
use sqlx::PgPool;
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{OwnedValue, Schema},
    Document, Index, ReloadPolicy, TantivyDocument,
};
use tokio::{spawn, sync::mpsc::channel, task::spawn_blocking};

use crate::args::SearchArgs;

use super::{get_index_config, open_unified_directories, DYNAMIC_FIELD_NAME};

fn get_prettified_json(
    doc: TantivyDocument,
    schema: &Schema,
    field_names: &[String],
) -> Result<String> {
    let mut named_doc = doc.to_named_doc(schema);

    let mut prettified_field_map = BTreeMap::new();
    for field_name in field_names {
        if let Some(mut field_values) = named_doc.0.remove(field_name) {
            if let Some(value) = field_values.pop() {
                if let OwnedValue::Object(object) = value {
                    for (k, v) in object {
                        prettified_field_map.insert(k, v);
                    }
                } else {
                    prettified_field_map.insert(field_name.clone(), value);
                }
            }
        }
    }

    Ok(serde_json::to_string(&prettified_field_map)?)
}

pub async fn run_search(args: SearchArgs, pool: PgPool) -> Result<()> {
    if args.limit == 0 {
        return Ok(());
    }

    let config = get_index_config(&args.name, &pool).await?;

    let indexed_field_names = {
        let mut fields = config.schema.get_indexed_fields();
        fields.push(DYNAMIC_FIELD_NAME.to_string());
        fields
    };

    let directories = open_unified_directories(&config.path, &pool)
        .await?
        .into_iter()
        .map(|(_, x)| x)
        .collect::<Vec<_>>();

    let (tx, mut rx) = channel(args.limit);
    let mut tx_handles = Vec::with_capacity(directories.len());

    // Should be chunked to never starve the thread pool (default in tokio is 500 threads).
    for directory in directories {
        let tx = tx.clone();
        let query = args.query.clone();
        let indexed_field_names = indexed_field_names.clone();

        // Should use rayon if search ends up being cpu bound (it seems io bound).
        tx_handles.push(spawn_blocking(move || -> Result<()> {
            if tx.is_closed() {
                return Ok(());
            }

            let index = Index::open(directory)?;
            let schema = index.schema();

            let indexed_fields = indexed_field_names
                .iter()
                .map(|name| schema.get_field(name))
                .collect::<tantivy::Result<Vec<_>>>()?;

            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let searcher = reader.searcher();

            let query_parser = QueryParser::for_index(&index, indexed_fields);
            let query = query_parser.parse_query(&query)?;
            let docs = searcher.search(&query, &TopDocs::with_limit(args.limit))?;

            if tx.is_closed() {
                return Ok(());
            }

            for (_, doc_address) in docs {
                let doc: TantivyDocument = searcher.doc(doc_address)?;
                let doc_str = get_prettified_json(doc, &schema, &indexed_field_names)?;
                if tx.blocking_send(doc_str).is_err() {
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
