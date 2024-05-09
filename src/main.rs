use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use tantivy::{
    directory::MmapDirectory,
    schema::{JsonObjectOptions, Schema, FAST, INDEXED, STORED},
    DateOptions, DateTime, DateTimePrecision, Index, IndexWriter, TantivyDocument,
};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    schema_builder.add_json_field(
        "_dynamic",
        JsonObjectOptions::from(STORED)
            .set_fast(Some("raw"))
            .set_expand_dots_enabled(),
    );
    schema_builder.add_date_field(
        "timestamp",
        DateOptions::from(INDEXED | STORED | FAST).set_precision(DateTimePrecision::Seconds),
    );

    let schema = schema_builder.build();

    let index = Index::open_or_create(MmapDirectory::open("/home/tony/test")?, schema.clone())?;
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let dynamic_field = schema.get_field("_dynamic")?;
    let timestamp_field = schema.get_field("timestamp")?;

    let mut reader = BufReader::new(File::open("/home/tony/hdfs-logs-multitenants-10000.json")?);

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

    Ok(())
}
