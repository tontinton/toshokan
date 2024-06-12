use async_trait::async_trait;
use color_eyre::Result;
use sqlx::PgPool;
use tantivy::{
    directory::MmapDirectory, indexer::NoMergePolicy, schema::Schema, Index, IndexWriter,
    TantivyDocument,
};
use tokio::{
    fs::{create_dir_all, File},
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
    task::spawn_blocking,
};

use crate::{args::IndexArgs, commands::field_parser::build_parsers_from_field_configs};

use super::{dynamic_field_config, get_index_config, write_unified_index, DYNAMIC_FIELD_NAME};

type JsonMap = serde_json::Map<String, serde_json::Value>;
type AsyncBufReader = BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>;

#[async_trait]
trait JsonReader {
    async fn next(&mut self) -> Result<Option<JsonMap>>;
}

struct BufJsonReader {
    reader: AsyncBufReader,
    line: String,
}

impl BufJsonReader {
    async fn from_path(path: &str) -> std::io::Result<Self> {
        debug!("Reading from '{}'", path);
        Ok(Self::from_buf_reader(BufReader::new(Box::new(
            File::open(&path).await?,
        ))))
    }

    fn from_stdin() -> Self {
        debug!("Reading from stdin");
        Self::from_buf_reader(BufReader::new(Box::new(stdin())))
    }

    fn from_buf_reader(reader: AsyncBufReader) -> Self {
        Self {
            reader,
            line: String::new(),
        }
    }
}

#[async_trait]
impl JsonReader for BufJsonReader {
    async fn next(&mut self) -> Result<Option<JsonMap>> {
        let len = self.reader.read_line(&mut self.line).await?;
        if len == 0 {
            return Ok(None);
        }

        let map = serde_json::from_str(&self.line)?;
        self.line.clear();
        return Ok(map);
    }
}

async fn build_json_reader(input: Option<&str>) -> Result<Box<dyn JsonReader + Unpin>> {
    Ok(match input {
        Some(path) => Box::new(BufJsonReader::from_path(path).await?),
        None => Box::new(BufJsonReader::from_stdin()),
    })
}

pub async fn run_index(args: IndexArgs, pool: &PgPool) -> Result<()> {
    let config = get_index_config(&args.name, pool).await?;

    let mut schema_builder = Schema::builder();
    let dynamic_field = schema_builder.add_json_field(DYNAMIC_FIELD_NAME, dynamic_field_config());
    let field_parsers =
        build_parsers_from_field_configs(config.schema.fields, &mut schema_builder)?;

    let schema = schema_builder.build();

    let _ = create_dir_all(&args.build_dir).await;
    let index = Index::open_or_create(MmapDirectory::open(&args.build_dir)?, schema)?;
    let mut index_writer: IndexWriter = index.writer(args.memory_budget)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let mut reader = build_json_reader(args.input.as_deref()).await?;
    let mut added = 0;

    'reader_loop: loop {
        let Some(mut json_obj) = reader.next().await? else {
            break;
        };

        let mut doc = TantivyDocument::new();

        for field_parser in &field_parsers {
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

    info!("Commiting {added} documents");
    index_writer.prepare_commit()?.commit_future().await?;

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(&index, &args.build_dir, &config.name, &config.path, pool).await?;

    Ok(())
}
