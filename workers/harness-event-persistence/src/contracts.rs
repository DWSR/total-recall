#![allow(clippy::useless_borrows_in_formatting)]

use chrono::DateTime;
use pbjson_types::{ListValue, Struct, Value as ProtoValue};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

include!(concat!(env!("OUT_DIR"), "/total_recall.harness.v1.rs"));
include!(concat!(
    env!("OUT_DIR"),
    "/total_recall.harness.v1.serde.rs"
));

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ContractError {
    #[error("queued event must be a JSON object")]
    InvalidEnvelope,
    #[error("queued event has an invalid event_type")]
    InvalidDiscriminator,
    #[error("{event_type} event has an invalid field set")]
    InvalidFields { event_type: &'static str },
    #[error("{event_type} event has an empty session ID")]
    EmptySessionId { event_type: &'static str },
    #[error("{event_type} event has an invalid timestamp")]
    InvalidTimestamp { event_type: &'static str },
    #[error("observation event data must be a JSON object")]
    InvalidObservationData,
    #[error("observation event contains an unrepresentable number")]
    InvalidObservationNumber,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PersistableEvent {
    SessionStart {
        event: SessionStartEvent,
        source_timestamp_epoch_millis: i64,
    },
    Observation {
        event: ObservationEvent,
        source_timestamp_epoch_millis: i64,
    },
    SessionEnd {
        event: SessionEndEvent,
        source_timestamp_epoch_millis: i64,
    },
}

impl PersistableEvent {
    pub fn source_timestamp(&self) -> &str {
        match self {
            Self::SessionStart { event, .. } => &event.timestamp,
            Self::Observation { event, .. } => &event.timestamp,
            Self::SessionEnd { event, .. } => &event.timestamp,
        }
    }

    pub fn source_timestamp_epoch_millis(&self) -> i64 {
        match self {
            Self::SessionStart {
                source_timestamp_epoch_millis,
                ..
            }
            | Self::Observation {
                source_timestamp_epoch_millis,
                ..
            }
            | Self::SessionEnd {
                source_timestamp_epoch_millis,
                ..
            } => *source_timestamp_epoch_millis,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "event_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueuedHarnessEventInput {
    SessionStart {
        #[serde(deserialize_with = "deserialize_session_id")]
        session_id: String,
        project_name: String,
        #[serde(deserialize_with = "deserialize_timestamp")]
        timestamp: String,
        current_working_directory: String,
    },
    Observation {
        hook_type: String,
        project_name: String,
        current_working_directory: String,
        #[serde(deserialize_with = "deserialize_timestamp")]
        timestamp: String,
        #[serde(deserialize_with = "deserialize_session_id")]
        session_id: String,
        #[serde(deserialize_with = "deserialize_observation_data")]
        data: BTreeMap<String, Value>,
    },
    SessionEnd {
        #[serde(deserialize_with = "deserialize_session_id")]
        session_id: String,
        project_name: String,
        #[serde(deserialize_with = "deserialize_timestamp")]
        timestamp: String,
        current_working_directory: String,
    },
}

impl TryFrom<Value> for QueuedHarnessEventInput {
    type Error = ContractError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let event_kind = EventKind::from_value(&value)?;
        validate_queued_event(&value, event_kind)?;
        serde_json::from_value(value).map_err(|_| ContractError::InvalidFields {
            event_type: event_kind.as_str(),
        })
    }
}

impl QueuedHarnessEventInput {
    pub fn into_persistable_event(self) -> Result<PersistableEvent, ContractError> {
        self.try_into()
    }
}

impl TryFrom<QueuedHarnessEventInput> for PersistableEvent {
    type Error = ContractError;

    fn try_from(input: QueuedHarnessEventInput) -> Result<Self, Self::Error> {
        match input {
            QueuedHarnessEventInput::SessionStart {
                session_id,
                project_name,
                timestamp,
                current_working_directory,
            } => {
                let source_timestamp_epoch_millis =
                    validate_common_fields(EventKind::SessionStart, &session_id, &timestamp)?;
                Ok(Self::SessionStart {
                    event: SessionStartEvent {
                        event_type: EventKind::SessionStart.as_str().to_owned(),
                        session_id,
                        project_name,
                        timestamp,
                        current_working_directory,
                    },
                    source_timestamp_epoch_millis,
                })
            }
            QueuedHarnessEventInput::Observation {
                hook_type,
                project_name,
                current_working_directory,
                timestamp,
                session_id,
                data,
            } => {
                let source_timestamp_epoch_millis =
                    validate_common_fields(EventKind::Observation, &session_id, &timestamp)?;
                Ok(Self::Observation {
                    event: ObservationEvent {
                        event_type: EventKind::Observation.as_str().to_owned(),
                        hook_type,
                        project_name,
                        current_working_directory,
                        timestamp,
                        session_id,
                        data: Some(json_object_to_struct(data)?),
                    },
                    source_timestamp_epoch_millis,
                })
            }
            QueuedHarnessEventInput::SessionEnd {
                session_id,
                project_name,
                timestamp,
                current_working_directory,
            } => {
                let source_timestamp_epoch_millis =
                    validate_common_fields(EventKind::SessionEnd, &session_id, &timestamp)?;
                Ok(Self::SessionEnd {
                    event: SessionEndEvent {
                        event_type: EventKind::SessionEnd.as_str().to_owned(),
                        session_id,
                        project_name,
                        timestamp,
                        current_working_directory,
                    },
                    source_timestamp_epoch_millis,
                })
            }
        }
    }
}

pub fn adapt_queued_event(value: Value) -> Result<PersistableEvent, ContractError> {
    QueuedHarnessEventInput::try_from(value)?.into_persistable_event()
}

#[derive(Clone, Copy)]
enum EventKind {
    SessionStart,
    Observation,
    SessionEnd,
}

impl EventKind {
    fn from_value(value: &Value) -> Result<Self, ContractError> {
        let fields = value.as_object().ok_or(ContractError::InvalidEnvelope)?;
        match fields.get("event_type").and_then(Value::as_str) {
            Some("session_start") => Ok(Self::SessionStart),
            Some("observation") => Ok(Self::Observation),
            Some("session_end") => Ok(Self::SessionEnd),
            _ => Err(ContractError::InvalidDiscriminator),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::SessionStart => "session_start",
            Self::Observation => "observation",
            Self::SessionEnd => "session_end",
        }
    }

    const fn required_fields(self) -> &'static [&'static str] {
        match self {
            Self::SessionStart | Self::SessionEnd => &[
                "event_type",
                "session_id",
                "project_name",
                "timestamp",
                "current_working_directory",
            ],
            Self::Observation => &[
                "event_type",
                "hook_type",
                "project_name",
                "current_working_directory",
                "timestamp",
                "session_id",
                "data",
            ],
        }
    }

    const fn string_fields(self) -> &'static [&'static str] {
        match self {
            Self::SessionStart | Self::SessionEnd => &[
                "session_id",
                "project_name",
                "timestamp",
                "current_working_directory",
            ],
            Self::Observation => &[
                "hook_type",
                "project_name",
                "current_working_directory",
                "timestamp",
                "session_id",
            ],
        }
    }
}

fn validate_queued_event(value: &Value, event_kind: EventKind) -> Result<(), ContractError> {
    let fields = value.as_object().ok_or(ContractError::InvalidEnvelope)?;
    let required_fields = event_kind.required_fields();
    if fields.len() != required_fields.len()
        || required_fields
            .iter()
            .any(|field| !fields.contains_key(*field))
        || event_kind
            .string_fields()
            .iter()
            .any(|field| !fields.get(*field).is_some_and(Value::is_string))
    {
        return Err(ContractError::InvalidFields {
            event_type: event_kind.as_str(),
        });
    }

    if matches!(event_kind, EventKind::Observation)
        && !fields.get("data").is_some_and(Value::is_object)
    {
        return Err(ContractError::InvalidObservationData);
    }

    let session_id =
        fields
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or(ContractError::InvalidFields {
                event_type: event_kind.as_str(),
            })?;
    let timestamp =
        fields
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or(ContractError::InvalidFields {
                event_type: event_kind.as_str(),
            })?;
    validate_common_fields(event_kind, session_id, timestamp).map(|_| ())
}

fn validate_common_fields(
    event_kind: EventKind,
    session_id: &str,
    timestamp: &str,
) -> Result<i64, ContractError> {
    if session_id.is_empty() {
        return Err(ContractError::EmptySessionId {
            event_type: event_kind.as_str(),
        });
    }

    timestamp_epoch_millis(timestamp).map_err(|_| ContractError::InvalidTimestamp {
        event_type: event_kind.as_str(),
    })
}

fn deserialize_session_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(serde::de::Error::custom("session_id must not be empty"));
    }

    Ok(value)
}

fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    timestamp_epoch_millis(&value).map_err(|_| serde::de::Error::custom("timestamp is invalid"))?;
    Ok(value)
}

fn deserialize_observation_data<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Object(fields) => Ok(fields.into_iter().collect()),
        _ => Err(serde::de::Error::custom("data must be a JSON object")),
    }
}

fn json_object_to_struct(
    fields: impl IntoIterator<Item = (String, Value)>,
) -> Result<Struct, ContractError> {
    fields
        .into_iter()
        .map(|(key, value)| json_value_to_proto(value).map(|value| (key, value)))
        .collect()
}

fn json_value_to_proto(value: Value) -> Result<ProtoValue, ContractError> {
    match value {
        Value::Null => Ok(().into()),
        Value::Bool(value) => Ok(value.into()),
        Value::Number(value) => value
            .as_f64()
            .ok_or(ContractError::InvalidObservationNumber)
            .map(Into::into),
        Value::String(value) => Ok(value.into()),
        Value::Array(values) => values
            .into_iter()
            .map(json_value_to_proto)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| ListValue::from(values).into()),
        Value::Object(fields) => json_object_to_struct(fields).map(Into::into),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TimestampError;

fn timestamp_epoch_millis(value: &str) -> Result<i64, TimestampError> {
    let bytes = value.as_bytes();
    let is_utc = bytes.len() == 24 && matches!(bytes[23], b'Z' | b'z');
    let is_offset = bytes.len() == 29 && matches!(bytes[23], b'+' | b'-') && bytes[26] == b':';

    if !(is_utc || is_offset)
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || !digits(bytes, 0, 4)
        || !digits(bytes, 5, 7)
        || !digits(bytes, 8, 10)
        || !digits(bytes, 11, 13)
        || !digits(bytes, 14, 16)
        || !digits(bytes, 17, 19)
        || !digits(bytes, 20, 23)
    {
        return Err(TimestampError);
    }

    if is_offset {
        if !digits(bytes, 24, 26) || !digits(bytes, 27, 29) {
            return Err(TimestampError);
        }

        let offset_hours = value[24..26].parse::<u8>().map_err(|_| TimestampError)?;
        let offset_minutes = value[27..29].parse::<u8>().map_err(|_| TimestampError)?;
        if offset_hours > 23 || offset_minutes > 59 {
            return Err(TimestampError);
        }
    }

    let mut normalized = bytes.to_vec();
    normalized[10] = b'T';
    if is_utc {
        normalized[23] = b'Z';
    }
    let normalized = String::from_utf8(normalized).map_err(|_| TimestampError)?;
    DateTime::parse_from_rfc3339(&normalized)
        .map(|timestamp| timestamp.timestamp_millis())
        .map_err(|_| TimestampError)
}

fn digits(bytes: &[u8], start: usize, end: usize) -> bool {
    bytes[start..end].iter().all(|byte| byte.is_ascii_digit())
}
