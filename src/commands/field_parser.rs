use std::net::IpAddr;

use color_eyre::{eyre::eyre, Result};
use tantivy::{
    schema::{Field, OwnedValue, SchemaBuilder},
    TantivyDocument,
};

use crate::config::{
    escaped_with_parent_name, number::NumberFieldType, FieldConfig, FieldConfigs, FieldType,
};

type ParseFn = Box<dyn Fn(serde_json::Value) -> Result<OwnedValue>>;

enum FieldParserVariation {
    Value { field: Field, parse_fn: ParseFn },
    Object(Vec<FieldParser>),
}

pub struct FieldParser {
    /// The field name. Example: "world".
    pub name: String,

    /// The tantivy name flattened and escaped. Example: "hello.world".
    /// Only used for a debug log.
    full_name: String,

    /// Whether the field is a tantivy field or an object of parsers.
    variation: FieldParserVariation,

    /// Whether the field is an array.
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
        match &self.variation {
            FieldParserVariation::Value { field, parse_fn } => {
                if self.is_array {
                    let values: Vec<serde_json::Value> = serde_json::from_value(json_value)?;
                    for value in values {
                        doc.add_field_value(*field, parse_fn(value)?);
                    }
                } else {
                    let value = parse_fn(json_value)?;
                    doc.add_field_value(*field, value);
                }
            }
            FieldParserVariation::Object(parsers) => {
                let mut json_obj: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_value(json_value)?;

                for parser in parsers {
                    let Some(json_value) = json_obj.remove(parser.name.as_str()) else {
                        debug!("Field '{}' in schema but not found", &parser.full_name);
                        continue;
                    };

                    parser.add_parsed_field_value(doc, json_value)?;
                }
            }
        }

        Ok(())
    }
}

fn common_parse(value: serde_json::Value) -> Result<OwnedValue> {
    Ok(serde_json::from_value(value)?)
}

fn build_parser_from_field_config(
    config: FieldConfig,
    full_name: String,
    schema_builder: &mut SchemaBuilder,
) -> Result<FieldParser> {
    let (field, parse_fn): (Field, ParseFn) = match config.type_ {
        FieldType::Text(options) => {
            let field = schema_builder.add_text_field(&full_name, options);
            (field, Box::new(common_parse))
        }
        FieldType::Number(options) => {
            let field_type = options.type_.clone();
            let parse_string = options.parse_string;
            let field = match field_type {
                NumberFieldType::U64 => schema_builder.add_u64_field(&full_name, options),
                NumberFieldType::I64 => schema_builder.add_i64_field(&full_name, options),
                NumberFieldType::F64 => schema_builder.add_f64_field(&full_name, options),
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
            let field = schema_builder.add_bool_field(&full_name, options);
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
            let field = schema_builder.add_date_field(&full_name, options.clone());
            (
                field,
                Box::new(move |value| options.formats.try_parse(value)),
            )
        }
        FieldType::Ip(options) => {
            let field = schema_builder.add_ip_addr_field(&full_name, options);
            (
                field,
                Box::new(move |value| {
                    let ip_str = serde_json::from_value::<String>(value)?;
                    let addr = ip_str.parse::<IpAddr>()?;
                    let ipv6 = match addr {
                        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
                        IpAddr::V6(ip) => ip,
                    };
                    Ok(OwnedValue::IpAddr(ipv6))
                }),
            )
        }
        FieldType::DynamicObject(options) => {
            let field = schema_builder.add_json_field(&full_name, options);
            (field, Box::new(common_parse))
        }
        FieldType::StaticObject(options) => {
            let parsers = build_parsers_from_field_configs_inner(
                options.fields,
                schema_builder,
                Some(full_name.clone()),
            )?;
            return Ok(FieldParser {
                name: config.name,
                full_name,
                variation: FieldParserVariation::Object(parsers),
                is_array: config.array,
            });
        }
    };

    Ok(FieldParser {
        name: config.name,
        full_name,
        variation: FieldParserVariation::Value { field, parse_fn },
        is_array: config.array,
    })
}

fn build_parsers_from_field_configs_inner(
    fields: FieldConfigs,
    schema_builder: &mut SchemaBuilder,
    parent_name: Option<String>,
) -> Result<Vec<FieldParser>> {
    fields
        .into_iter()
        .map(|field| {
            let name = escaped_with_parent_name(&field.name, parent_name.as_deref());
            build_parser_from_field_config(field, name, schema_builder)
        })
        .collect::<Result<Vec<_>>>()
}

pub fn build_parsers_from_field_configs(
    fields: &FieldConfigs,
    schema_builder: &mut SchemaBuilder,
) -> Result<Vec<FieldParser>> {
    build_parsers_from_field_configs_inner(fields.clone(), schema_builder, None)
}
