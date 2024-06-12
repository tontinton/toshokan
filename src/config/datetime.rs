use std::ops::Deref;

use color_eyre::{eyre::eyre, Result};
use serde::{Deserialize, Serialize};
use tantivy::{
    schema::OwnedValue,
    time::{
        format_description::well_known::{Iso8601, Rfc2822, Rfc3339},
        OffsetDateTime,
    },
    DateOptions, DateTime, DateTimePrecision,
};

use super::default_true;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateTimeFormatType {
    Iso8601,
    Rfc2822,
    Rfc3339,
    Timestamp,
}

impl DateTimeFormatType {
    fn try_parse(&self, value: serde_json::Value) -> Result<OwnedValue> {
        match self {
            DateTimeFormatType::Iso8601 => {
                let timestamp: String = serde_json::from_value(value)?;
                Ok(OffsetDateTime::parse(&timestamp, &Iso8601::DEFAULT)
                    .map(DateTime::from_utc)?
                    .into())
            }
            DateTimeFormatType::Rfc2822 => {
                let timestamp: String = serde_json::from_value(value)?;
                Ok(OffsetDateTime::parse(&timestamp, &Rfc2822)
                    .map(DateTime::from_utc)?
                    .into())
            }
            DateTimeFormatType::Rfc3339 => {
                let timestamp: String = serde_json::from_value(value)?;
                Ok(OffsetDateTime::parse(&timestamp, &Rfc3339)
                    .map(DateTime::from_utc)?
                    .into())
            }
            DateTimeFormatType::Timestamp => {
                let timestamp: i64 = serde_json::from_value(value)?;
                Ok(parse_timestamp(timestamp)?.into())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTimeFormats(Vec<DateTimeFormatType>);

impl Default for DateTimeFormats {
    fn default() -> Self {
        Self(vec![
            DateTimeFormatType::Rfc3339,
            DateTimeFormatType::Timestamp,
        ])
    }
}

impl Deref for DateTimeFormats {
    type Target = Vec<DateTimeFormatType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DateTimeFormats {
    pub fn try_parse(&self, value: serde_json::Value) -> Result<OwnedValue> {
        for format in &self.0 {
            match format.try_parse(value.clone()) {
                Ok(x) => {
                    return Ok(x);
                }
                Err(e) => {
                    debug!(
                        "Failed to parse using the {:?} datetime format: {}",
                        format, e
                    );
                }
            }
        }
        Err(eyre!("None of the datetime formats was able to parse"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DateTimeFastPrecisionType {
    #[default]
    False,
    True,
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl From<DateTimeFastPrecisionType> for Option<DateTimePrecision> {
    fn from(value: DateTimeFastPrecisionType) -> Self {
        use DateTimeFastPrecisionType::*;
        match value {
            False => None,
            True | Seconds => Some(DateTimePrecision::Seconds),
            Milliseconds => Some(DateTimePrecision::Milliseconds),
            Microseconds => Some(DateTimePrecision::Microseconds),
            Nanoseconds => Some(DateTimePrecision::Nanoseconds),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DateTimeFieldConfig {
    #[serde(default = "default_true")]
    pub stored: bool,

    #[serde(default = "default_true")]
    pub indexed: bool,

    #[serde(default)]
    pub fast: DateTimeFastPrecisionType,

    #[serde(default)]
    pub formats: DateTimeFormats,
}

impl From<DateTimeFieldConfig> for DateOptions {
    fn from(config: DateTimeFieldConfig) -> Self {
        let mut options = DateOptions::default();
        if config.stored {
            options = options.set_stored();
        }
        if config.indexed {
            options = options.set_indexed();
        }
        if let Some(precision) = config.fast.into() {
            options = options.set_fast().set_precision(precision);
        }
        options
    }
}
