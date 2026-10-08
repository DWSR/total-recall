#![allow(clippy::useless_borrows_in_formatting)]

use chrono::DateTime;
use pbjson_types::{ListValue, Struct, Value as ProtoValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};
use thiserror::Error;

include!(concat!(env!("OUT_DIR"), "/total_recall.harness.v1.rs"));
include!(concat!(
    env!("OUT_DIR"),
    "/total_recall.harness.v1.serde.rs"
));

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("timestamp must be RFC 3339 with exactly three fractional digits")]
pub struct TimestampError;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ContractError {
    #[error("session_id must not be empty")]
    EmptySessionId,
    #[error("timestamp is invalid")]
    InvalidTimestamp,
    #[error("observation data contains an unrepresentable number")]
    InvalidObservationNumber,
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
pub struct HarnessTimestamp(String);

impl HarnessTimestamp {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    fn validate(&self) -> Result<(), TimestampError> {
        validate_timestamp(&self.0)
    }
}

impl<'de> Deserialize<'de> for HarnessTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        validate_timestamp(&value).map_err(serde::de::Error::custom)?;
        Ok(Self(value))
    }
}

impl TryFrom<String> for HarnessTimestamp {
    type Error = TimestampError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_timestamp(&value)?;
        Ok(Self(value))
    }
}

impl From<HarnessTimestamp> for String {
    fn from(value: HarnessTimestamp) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SessionStartInput {
    #[serde(deserialize_with = "deserialize_session_id")]
    pub session_id: String,
    pub project_name: String,
    pub timestamp: HarnessTimestamp,
    pub current_working_directory: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SessionEndInput {
    #[serde(deserialize_with = "deserialize_session_id")]
    pub session_id: String,
    pub project_name: String,
    pub timestamp: HarnessTimestamp,
    pub current_working_directory: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObservationInput {
    pub hook_type: String,
    pub project_name: String,
    pub current_working_directory: String,
    pub timestamp: HarnessTimestamp,
    #[serde(deserialize_with = "deserialize_session_id")]
    pub session_id: String,
    #[serde(deserialize_with = "deserialize_observation_data")]
    pub data: BTreeMap<String, serde_json::Value>,
}

impl TryFrom<SessionStartInput> for SessionStartRequest {
    type Error = ContractError;

    fn try_from(input: SessionStartInput) -> Result<Self, Self::Error> {
        validate_input(&input.session_id, &input.timestamp)?;
        Ok(Self {
            session_id: input.session_id,
            project_name: input.project_name,
            timestamp: input.timestamp.into_inner(),
            current_working_directory: input.current_working_directory,
        })
    }
}

impl TryFrom<SessionEndInput> for SessionEndRequest {
    type Error = ContractError;

    fn try_from(input: SessionEndInput) -> Result<Self, Self::Error> {
        validate_input(&input.session_id, &input.timestamp)?;
        Ok(Self {
            session_id: input.session_id,
            project_name: input.project_name,
            timestamp: input.timestamp.into_inner(),
            current_working_directory: input.current_working_directory,
        })
    }
}

impl ObservationInput {
    pub fn into_request(self) -> Result<ObservationRequest, ContractError> {
        validate_input(&self.session_id, &self.timestamp)?;
        let data = json_object_to_struct(self.data)?;
        Ok(ObservationRequest {
            hook_type: self.hook_type,
            project_name: self.project_name,
            current_working_directory: self.current_working_directory,
            timestamp: self.timestamp.into_inner(),
            session_id: self.session_id,
            data: Some(data),
        })
    }
}

fn json_object_to_struct(
    fields: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> Result<Struct, ContractError> {
    fields
        .into_iter()
        .map(|(key, value)| json_value_to_proto(value).map(|value| (key, value)))
        .collect()
}

fn json_value_to_proto(value: serde_json::Value) -> Result<ProtoValue, ContractError> {
    match value {
        serde_json::Value::Null => Ok(().into()),
        serde_json::Value::Bool(value) => Ok(value.into()),
        serde_json::Value::Number(value) => value
            .as_f64()
            .ok_or(ContractError::InvalidObservationNumber)
            .map(Into::into),
        serde_json::Value::String(value) => Ok(value.into()),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_value_to_proto)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| ListValue::from(values).into()),
        serde_json::Value::Object(fields) => json_object_to_struct(fields).map(Into::into),
    }
}

fn deserialize_session_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_session_id(&value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_observation_data<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Object(object) => Ok(object.into_iter().collect()),
        _ => Err(serde::de::Error::custom("data must be a JSON object")),
    }
}

fn validate_input(session_id: &str, timestamp: &HarnessTimestamp) -> Result<(), ContractError> {
    validate_session_id(session_id).map_err(|_| ContractError::EmptySessionId)?;
    timestamp
        .validate()
        .map_err(|_| ContractError::InvalidTimestamp)
}

fn validate_session_id(value: &str) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError::EmptySessionId);
    }

    Ok(())
}

fn validate_timestamp(value: &str) -> Result<(), TimestampError> {
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
        .map(|_| ())
        .map_err(|_| TimestampError)
}

fn digits(bytes: &[u8], start: usize, end: usize) -> bool {
    bytes[start..end].iter().all(|byte| byte.is_ascii_digit())
}

impl fmt::Display for HarnessTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
