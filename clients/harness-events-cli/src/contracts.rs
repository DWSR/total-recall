#![allow(clippy::useless_borrows_in_formatting)]

use std::io::Read;

use pbjson_types::{ListValue, Struct, Value as ProtoValue};
use serde::Deserialize;

use crate::cli::{EventCommand, LifecycleArgs, ObservationArgs};

include!(concat!(env!("OUT_DIR"), "/total_recall.harness.v1.rs"));
include!(concat!(
    env!("OUT_DIR"),
    "/total_recall.harness.v1.serde.rs"
));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestionFunction {
    SessionStart,
    Observation,
    SessionEnd,
}

impl IngestionFunction {
    pub const ALL: [Self; 3] = [Self::SessionStart, Self::Observation, Self::SessionEnd];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionStart => "harness::session_start",
            Self::Observation => "harness::observation",
            Self::SessionEnd => "harness::session_end",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Submission {
    pub function_id: IngestionFunction,
    pub payload: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("observation input could not be read")]
    ObservationInputRead,
    #[error("observation data is not valid JSON")]
    InvalidObservationJson,
    #[error("observation data contains trailing content")]
    TrailingObservationData,
    #[error("observation data must be a JSON object")]
    NonObjectObservationData,
    #[error("observation data contains an unrepresentable number")]
    InvalidObservationNumber,
    #[error("generated ProtoJSON serialization failed")]
    ProtoJsonSerialization(#[from] serde_json::Error),
}

pub fn prepare(command: EventCommand, stdin: impl Read) -> Result<Submission, InputError> {
    match command {
        EventCommand::SessionStart(args) => prepare_session_start(args),
        EventCommand::Observation(args) => prepare_observation(args, stdin),
        EventCommand::SessionEnd(args) => prepare_session_end(args),
    }
}

pub fn prepare_session_start(args: LifecycleArgs) -> Result<Submission, InputError> {
    Ok(Submission {
        function_id: IngestionFunction::SessionStart,
        payload: serde_json::to_value(SessionStartRequest {
            session_id: args.session_id,
            project_name: args.project_name,
            timestamp: args.timestamp,
            current_working_directory: args.current_working_directory,
        })?,
    })
}

pub fn prepare_session_end(args: LifecycleArgs) -> Result<Submission, InputError> {
    Ok(Submission {
        function_id: IngestionFunction::SessionEnd,
        payload: serde_json::to_value(SessionEndRequest {
            session_id: args.session_id,
            project_name: args.project_name,
            timestamp: args.timestamp,
            current_working_directory: args.current_working_directory,
        })?,
    })
}

fn prepare_observation(args: ObservationArgs, stdin: impl Read) -> Result<Submission, InputError> {
    let data = read_observation_object(stdin)?;
    let data = json_object_to_struct(data)?;

    Ok(Submission {
        function_id: IngestionFunction::Observation,
        payload: serde_json::to_value(ObservationRequest {
            hook_type: args.hook_type,
            project_name: args.project_name,
            current_working_directory: args.current_working_directory,
            timestamp: args.timestamp,
            session_id: args.session_id,
            data: Some(data),
        })?,
    })
}

fn read_observation_object(
    mut stdin: impl Read,
) -> Result<serde_json::Map<String, serde_json::Value>, InputError> {
    let mut bytes = Vec::new();
    stdin
        .read_to_end(&mut bytes)
        .map_err(|_| InputError::ObservationInputRead)?;

    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = serde_json::Value::deserialize(&mut deserializer)
        .map_err(|_| InputError::InvalidObservationJson)?;
    deserializer
        .end()
        .map_err(|_| InputError::TrailingObservationData)?;

    match value {
        serde_json::Value::Object(fields) => Ok(fields),
        _ => Err(InputError::NonObjectObservationData),
    }
}

fn json_object_to_struct(
    fields: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> Result<Struct, InputError> {
    fields
        .into_iter()
        .map(|(key, value)| json_value_to_proto(value).map(|value| (key, value)))
        .collect()
}

fn json_value_to_proto(value: serde_json::Value) -> Result<ProtoValue, InputError> {
    match value {
        serde_json::Value::Null => Ok(().into()),
        serde_json::Value::Bool(value) => Ok(value.into()),
        serde_json::Value::Number(value) => value
            .as_f64()
            .ok_or(InputError::InvalidObservationNumber)
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

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    use crate::cli::{EventCommand, LifecycleArgs, ObservationArgs};

    use super::{
        IngestionFunction, InputError, SessionEndRequest, SessionStartRequest, Submission, prepare,
        prepare_session_end, prepare_session_start,
    };

    #[derive(Clone, Copy)]
    struct PanicReader;

    impl Read for PanicReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("lifecycle preparation must not read standard input")
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("sensitive observation data"))
        }
    }

    fn observation_command() -> EventCommand {
        EventCommand::Observation(ObservationArgs {
            hook_type: "PostToolUse".to_owned(),
            project_name: "example-project".to_owned(),
            current_working_directory: "/workspace/example".to_owned(),
            timestamp: "2026-09-19T20:14:33.123Z".to_owned(),
            session_id: "command-session".to_owned(),
        })
    }

    #[test]
    fn exposes_the_complete_authoritative_ingestion_function_set() {
        assert_eq!(
            IngestionFunction::ALL.map(IngestionFunction::as_str),
            [
                "harness::session_start",
                "harness::observation",
                "harness::session_end",
            ]
        );
    }

    #[test]
    fn prepares_session_start_with_exact_identifier_and_protojson_payload() {
        let args = LifecycleArgs {
            session_id: "  start-session  ".to_owned(),
            project_name: String::new(),
            current_working_directory: " /workspace/start ".to_owned(),
            timestamp: " not-a-timestamp ".to_owned(),
        };
        let expected_payload = serde_json::to_value(SessionStartRequest {
            session_id: args.session_id.clone(),
            project_name: args.project_name.clone(),
            timestamp: args.timestamp.clone(),
            current_working_directory: args.current_working_directory.clone(),
        })
        .expect("generated session-start request serializes");

        assert_eq!(
            expected_payload,
            serde_json::json!({
                "session_id": "  start-session  ",
                "project_name": "",
                "timestamp": " not-a-timestamp ",
                "current_working_directory": " /workspace/start ",
            })
        );
        let submission: Result<Submission, InputError> = prepare_session_start(args);

        assert_eq!(
            submission.expect("generated session-start request serializes"),
            Submission {
                function_id: IngestionFunction::SessionStart,
                payload: expected_payload,
            }
        );
    }

    #[test]
    fn prepares_session_end_with_exact_identifier_and_protojson_payload() {
        let args = LifecycleArgs {
            session_id: String::new(),
            project_name: "  end-project  ".to_owned(),
            current_working_directory: String::new(),
            timestamp: "\tend-timestamp\t".to_owned(),
        };
        let expected_payload = serde_json::to_value(SessionEndRequest {
            session_id: args.session_id.clone(),
            project_name: args.project_name.clone(),
            timestamp: args.timestamp.clone(),
            current_working_directory: args.current_working_directory.clone(),
        })
        .expect("generated session-end request serializes");

        assert_eq!(
            expected_payload,
            serde_json::json!({
                "session_id": "",
                "project_name": "  end-project  ",
                "timestamp": "\tend-timestamp\t",
                "current_working_directory": "",
            })
        );
        let submission: Result<Submission, InputError> = prepare_session_end(args);

        assert_eq!(
            submission.expect("generated session-end request serializes"),
            Submission {
                function_id: IngestionFunction::SessionEnd,
                payload: expected_payload,
            }
        );
    }

    #[test]
    fn prepares_observation_with_exact_identifier_and_opaque_nested_protojson_payload() {
        let submission = prepare(
            observation_command(),
            br#"{
                "arbitrary.key/with spaces": {
                    "nested key": [null, true, "value", {"flag": false}],
                    "empty": {}
                }
            }"#
            .as_slice(),
        )
        .expect("observation data is prepared");

        assert_eq!(
            submission,
            Submission {
                function_id: IngestionFunction::Observation,
                payload: serde_json::json!({
                    "hook_type": "PostToolUse",
                    "project_name": "example-project",
                    "current_working_directory": "/workspace/example",
                    "timestamp": "2026-09-19T20:14:33.123Z",
                    "session_id": "command-session",
                    "data": {
                        "arbitrary.key/with spaces": {
                            "nested key": [null, true, "value", {"flag": false}],
                            "empty": {}
                        }
                    }
                }),
            }
        );
    }

    #[test]
    fn preserves_an_empty_observation_object_with_trailing_whitespace() {
        let submission = prepare(observation_command(), b"{} \t\n".as_slice())
            .expect("empty observation object is prepared");

        assert_eq!(submission.payload["data"], serde_json::json!({}));
    }

    #[test]
    fn rounds_large_observation_integers_to_protobuf_doubles() {
        let submission = prepare(
            observation_command(),
            br#"{"counter": 9007199254740993}"#.as_slice(),
        )
        .expect("large integer is converted to a protobuf double");

        assert_eq!(
            submission.payload["data"]["counter"].as_f64(),
            Some(9_007_199_254_740_992.0)
        );
    }

    #[test]
    fn rejects_every_invalid_observation_input_during_preparation() {
        for (description, input) in [
            ("empty input", ""),
            ("whitespace-only input", " \t\n"),
            ("malformed JSON", "{\"value\":"),
        ] {
            let error = prepare(observation_command(), input.as_bytes())
                .expect_err("invalid JSON must not create a submission");

            assert_eq!(error.to_string(), "observation data is not valid JSON");
            assert!(
                matches!(error, InputError::InvalidObservationJson),
                "{description} must return InvalidObservationJson"
            );
        }

        for (description, input) in [("a scalar", "null"), ("an array", "[]")] {
            let error = prepare(observation_command(), input.as_bytes())
                .expect_err("a non-object must not create a submission");

            assert_eq!(error.to_string(), "observation data must be a JSON object");
            assert!(
                matches!(error, InputError::NonObjectObservationData),
                "{description} must return NonObjectObservationData"
            );
        }

        for (description, input) in [
            ("trailing junk", "{} trailing"),
            ("a second JSON value", "{} {}"),
        ] {
            let error = prepare(observation_command(), input.as_bytes())
                .expect_err("trailing content must not create a submission");

            assert_eq!(
                error.to_string(),
                "observation data contains trailing content"
            );
            assert!(
                matches!(error, InputError::TrailingObservationData),
                "{description} must return TrailingObservationData"
            );
        }
    }

    #[test]
    fn rejects_unrepresentable_observation_numbers_during_preparation() {
        let error = prepare(observation_command(), br#"{"number": 1e999}"#.as_slice())
            .expect_err("an unrepresentable number must not create a submission");

        assert_eq!(
            error.to_string(),
            "observation data contains an unrepresentable number"
        );
        assert!(
            matches!(error, InputError::InvalidObservationNumber),
            "unrepresentable numbers must return InvalidObservationNumber"
        );
    }

    #[test]
    fn rejects_observation_input_read_failures_before_creating_a_submission() {
        let error = prepare(observation_command(), FailingReader)
            .expect_err("a read failure must not create a submission");

        assert_eq!(error.to_string(), "observation input could not be read");
        assert!(
            matches!(error, InputError::ObservationInputRead),
            "read failures must return ObservationInputRead"
        );
    }

    #[test]
    fn dispatcher_never_reads_standard_input_for_lifecycle_commands() {
        let session_start = LifecycleArgs {
            session_id: "start-session".to_owned(),
            project_name: "start-project".to_owned(),
            current_working_directory: "/workspace/start".to_owned(),
            timestamp: "start-time".to_owned(),
        };
        let session_end = LifecycleArgs {
            session_id: "end-session".to_owned(),
            project_name: "end-project".to_owned(),
            current_working_directory: "/workspace/end".to_owned(),
            timestamp: "end-time".to_owned(),
        };

        assert_eq!(
            prepare(
                EventCommand::SessionStart(session_start.clone()),
                PanicReader
            )
            .expect("session-start preparation succeeds"),
            prepare_session_start(session_start).expect("session-start builder succeeds"),
        );
        assert_eq!(
            prepare(EventCommand::SessionEnd(session_end.clone()), PanicReader)
                .expect("session-end preparation succeeds"),
            prepare_session_end(session_end).expect("session-end builder succeeds"),
        );
    }
}
