use color_eyre::{eyre::eyre, Result};
use tantivy::{
    schema::{Field, OwnedValue, SchemaBuilder},
    TantivyDocument,
};

use crate::config::{number::NumberFieldType, FieldConfig, FieldType};

type ParseFn = Box<dyn Fn(serde_json::Value) -> Result<OwnedValue>>;

pub struct FieldParser {
    pub name: String,
    field: Field,
    parse_fn: ParseFn,
    is_array: bool,
}

impl FieldParser {
    /// Parses the `serde_json::Value` into a `tantivy::OwnedValue` and adds
    /// the result into the tantivy document.
    pub fn add_parsed_field_value(
        &self,
        doc: &mut TantivyDocument,
        json_value: serde_json::Value,
    ) -> Result<()> {
        if self.is_array {
            let values: Vec<serde_json::Value> = serde_json::from_value(json_value)?;
            for value in values {
                doc.add_field_value(self.field, (self.parse_fn)(value)?);
            }
        } else {
            let value = (self.parse_fn)(json_value)?;
            doc.add_field_value(self.field, value);
        }

        Ok(())
    }
}

fn common_parse(value: serde_json::Value) -> Result<OwnedValue> {
    Ok(serde_json::from_value(value)?)
}

fn build_parser_from_field_config(
    config: FieldConfig,
    schema_builder: &mut SchemaBuilder,
) -> Result<FieldParser> {
    let name = config.name;

    let (field, parse_fn): (Field, ParseFn) = match config.type_ {
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

    Ok(FieldParser {
        name,
        field,
        parse_fn,
        is_array: config.array,
    })
}

pub fn build_parsers_from_fields_config(
    fields: Vec<FieldConfig>,
    schema_builder: &mut SchemaBuilder,
) -> Result<Vec<FieldParser>> {
    fields
        .into_iter()
        .map(|field| build_parser_from_field_config(field, schema_builder))
        .collect::<Result<Vec<_>>>()
}
