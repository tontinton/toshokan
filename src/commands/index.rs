use color_eyre::{eyre::eyre, Result};
use sqlx::PgPool;
use tantivy::{
    directory::MmapDirectory,
    indexer::NoMergePolicy,
    schema::{Field, OwnedValue, Schema, SchemaBuilder},
    Index, IndexWriter, TantivyDocument,
};
use tokio::{
    fs::{create_dir_all, File},
    io::{AsyncBufReadExt, BufReader},
    task::spawn_blocking,
};

use crate::{
    args::IndexArgs,
    config::{number::NumberFieldType, FieldConfig, FieldType},
};

use super::{dynamic_field_config, get_index_config, write_unified_index, DYNAMIC_FIELD_NAME};

enum ParsedValue {
    Single(OwnedValue),
    Multiple(Vec<OwnedValue>),
}

type SingleParseFn = Box<dyn Fn(serde_json::Value) -> Result<OwnedValue>>;
type ParseFn = Box<dyn Fn(serde_json::Value) -> Result<ParsedValue>>;

type FieldParser = (
    String,  // name
    Field,   // tantivy field in schema
    ParseFn, // parse function
);

fn common_parse(value: serde_json::Value) -> Result<OwnedValue> {
    Ok(serde_json::from_value(value)?)
}

fn get_field_parser(
    config: FieldConfig,
    schema_builder: &mut SchemaBuilder,
) -> Result<FieldParser> {
    let name = config.name;

    let (field, parse_single_fn): (Field, SingleParseFn) = match config.type_ {
        FieldType::Text(options) => {
            let field = schema_builder.add_text_field(&name, options);
            (field, Box::new(common_parse))
        }
        FieldType::Number(options) => {
            let field_type = options.type_.clone();
            let parse_string = options.parse_string;
            let field = match field_type {
                NumberFieldType::U64 => schema_builder.add_u64_field(&name, options),
                NumberFieldType::I64 => schema_builder.add_i64_field(&name, options),
                NumberFieldType::F64 => schema_builder.add_f64_field(&name, options),
            };

            (
                field,
                Box::new(move |value| {
                    if !parse_string {
                        return common_parse(value);
                    }

                    if let Ok(value_str) = serde_json::from_value::<String>(value.clone()) {
                        Ok(match field_type {
                            NumberFieldType::U64 => value_str.parse::<u64>()?.into(),
                            NumberFieldType::I64 => value_str.parse::<i64>()?.into(),
                            NumberFieldType::F64 => value_str.parse::<f64>()?.into(),
                        })
                    } else {
                        common_parse(value)
                    }
                }),
            )
        }
        FieldType::Boolean(options) => {
            let parse_string = options.parse_string;
            let field = schema_builder.add_bool_field(&name, options);
            (
                field,
                Box::new(move |value| {
                    if !parse_string {
                        return common_parse(value);
                    }

                    if let Ok(value_str) = serde_json::from_value::<String>(value.clone()) {
                        let trimmed = value_str.trim();
                        if trimmed.len() < 4 || trimmed.len() > 5 {
                            return Err(eyre!("cannot parse '{}' as boolean", trimmed));
                        }
                        let value_str = trimmed.to_lowercase();
                        match value_str.as_str() {
                            "true" => Ok(true.into()),
                            "false" => Ok(false.into()),
                            _ => Err(eyre!("cannot parse '{}' as boolean", trimmed)),
                        }
                    } else {
                        common_parse(value)
                    }
                }),
            )
        }
        FieldType::Datetime(options) => {
            let field = schema_builder.add_date_field(&name, options.clone());
            (
                field,
                Box::new(move |value| options.formats.try_parse(value)),
            )
        }
        FieldType::Ip(options) => {
            let field = schema_builder.add_ip_addr_field(&name, options);
            (field, Box::new(common_parse))
        }
        FieldType::DynamicObject(options) => {
            let field = schema_builder.add_json_field(&name, options);
            (field, Box::new(common_parse))
        }
    };

    let parse_fn: ParseFn = if config.array {
        Box::new(move |value| {
            let array: Vec<serde_json::Value> = serde_json::from_value(value)?;
            Ok(ParsedValue::Multiple(
                array
                    .into_iter()
                    .map(&parse_single_fn)
                    .collect::<Result<Vec<_>>>()?,
            ))
        })
    } else {
        Box::new(move |value| Ok(ParsedValue::Single(parse_single_fn(value)?)))
    };

    Ok((name, field, parse_fn))
}

pub async fn run_index(args: IndexArgs, pool: PgPool) -> Result<()> {
    let config = get_index_config(&args.name, &pool).await?;

    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(DYNAMIC_FIELD_NAME, dynamic_field_config());
    let field_parsers = config
        .schema
        .fields
        .into_iter()
        .map(|field| get_field_parser(field, &mut schema_builder))
        .collect::<Result<Vec<FieldParser>>>()?;

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

        for (name, field, parse_fn) in &field_parsers {
            if let Some(json_value) = json_obj.remove(name) {
                match parse_fn(json_value) {
                    Ok(ParsedValue::Single(value)) => {
                        doc.add_field_value(*field, value);
                    }
                    Ok(ParsedValue::Multiple(values)) => {
                        for value in values {
                            doc.add_field_value(*field, value);
                        }
                    }
                    Err(e) => {
                        error!("failed to parse '{}': {}", &name, e);
                    }
                }
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
