use std::{backtrace::Backtrace, collections::BTreeMap};

use color_eyre::{eyre::eyre, Result};
use futures::future::{join_all, select_all};
use rayon::{ThreadPool, ThreadPoolBuilder};
use sqlx::PgPool;
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{OwnedValue, Schema},
    Document, Index, ReloadPolicy, TantivyDocument,
};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};

use crate::{
    args::SearchArgs,
    config::{split_object_field_name, unescaped_field_name, FieldConfig, FieldType},
};

use super::{dynamic_field_config, get_index_config, open_unified_directories, DYNAMIC_FIELD_NAME};

fn get_prettified_json(
    doc: TantivyDocument,
    schema: &Schema,
    fields: &[FieldConfig],
) -> Result<String> {
    let mut named_doc = doc.to_named_doc(schema);

    let mut prettified_field_map = BTreeMap::new();
    for field in fields {
        let Some(mut field_values) = named_doc.0.remove(&field.name) else {
            continue;
        };

        if field.array {
            prettified_field_map.insert(field.name.clone(), OwnedValue::Array(field_values));
            continue;
        }

        let Some(value) = field_values.pop() else {
            continue;
        };

        if field.name == DYNAMIC_FIELD_NAME {
            let OwnedValue::Object(object) = value else {
                return Err(eyre!(
                    "expected {} field to be an object",
                    DYNAMIC_FIELD_NAME
                ));
            };

            for (k, v) in object {
                prettified_field_map.insert(k, v);
            }

            continue;
        }

        let names = split_object_field_name(&field.name)
            .into_iter()
            .map(unescaped_field_name)
            .collect::<Vec<_>>();
        if names.len() <= 1 {
            prettified_field_map.insert(field.name.clone(), value);
            continue;
        }

        // Prettify static object with inner fields like {"hello.world": 1}
        // to look like: {"hello": {"world": 1}}.

        let mut inner_map = prettified_field_map
            .entry(names[0].to_string())
            .or_insert(OwnedValue::Object(BTreeMap::new()));

        for name in &names[1..names.len() - 1] {
            let OwnedValue::Object(map) = inner_map else {
                panic!("invalid state, every map is an object");
            };

            inner_map = map
                .entry(name.to_string())
                .or_insert(OwnedValue::Object(BTreeMap::new()));
        }

        if let OwnedValue::Object(map) = inner_map {
            map.insert(names[names.len() - 1].to_string(), value);
        }
    }

    Ok(serde_json::to_string(&prettified_field_map)?)
}

fn run_on_thread_pool<F, R>(thread_pool: &ThreadPool, task: F) -> oneshot::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    thread_pool.spawn(move || {
        if tx.is_closed() {
            return;
        }
        let _ = tx.send(task());
    });
    rx
}

pub async fn run_search_with_callback(
    args: SearchArgs,
    pool: &PgPool,
    on_doc_fn: Box<dyn Fn(String) + Send>,
) -> Result<()> {
    if args.limit == 0 {
        return Ok(());
    }

    let thread_pool = ThreadPoolBuilder::new()
        .thread_name(|id| format!("search-{id}"))
        .panic_handler(|info| {
            let msg = if let Some(s) = info.downcast_ref::<&str>() {
                format!("Thread panic: {:?}", s)
            } else if let Some(s) = info.downcast_ref::<String>() {
                format!("Thread panic: {:?}", s)
            } else {
                "?".to_string()
            };

            error!("Task in search thread pool panicked: {}", msg);
            eprintln!("Backtrace: {}", Backtrace::capture());
        })
        .build()?;

    let config = get_index_config(&args.name, pool).await?;

    let indexed_field_names = {
        let mut fields = config.schema.fields.get_indexed();
        fields.push(FieldConfig {
            name: DYNAMIC_FIELD_NAME.to_string(),
            array: false,
            type_: FieldType::DynamicObject(dynamic_field_config()),
        });
        fields
    };

    let directories = open_unified_directories(&config.path, pool)
        .await?
        .into_iter()
        .map(|(_, x)| x)
        .collect::<Vec<_>>();

    if directories.is_empty() {
        info!("No index files for '{}'", &args.name);
        return Ok(());
    }

    let mut tx_handles = Vec::with_capacity(directories.len());
    let (tx, mut rx) = mpsc::channel(args.limit);

    for directory in directories {
        let tx = tx.clone();
        let query = args.query.clone();
        let indexed_field_names = indexed_field_names.clone();

        tx_handles.push(run_on_thread_pool(&thread_pool, move || -> Result<()> {
            if tx.is_closed() {
                return Ok(());
            }

            let index = Index::open(directory)?;
            let schema = index.schema();

            let indexed_fields = indexed_field_names
                .iter()
                .map(|field| schema.get_field(&field.name))
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

    let mut rx_handle = spawn(async move {
        let mut i = 0;
        while let Some(doc) = rx.recv().await {
            on_doc_fn(doc);
            i += 1;
            if i == args.limit {
                break;
            }
        }
        rx.close();
    });

    loop {
        select! {
            _ = &mut rx_handle => {
                // Wait for all tx threads to cleanly finish.
                let _ = join_all(tx_handles).await;
                break;
            }
            (result, i, _) = select_all(&mut tx_handles) => {
                if let Ok(Err(e)) = result {
                    error!("Error in search task({i}): {e}");
                }

                tx_handles.remove(i);
                if tx_handles.is_empty() {
                    // Wait for rx task to cleanly finish.
                    let _ = rx_handle.await;
                    break;
                }
            }
        }
    }

    Ok(())
}

pub async fn run_search(args: SearchArgs, pool: &PgPool) -> Result<()> {
    run_search_with_callback(
        args,
        pool,
        Box::new(|doc| {
            println!("{}", doc);
        }),
    )
    .await
}
