use clap::{CommandFactory as _, Parser as _, error::ErrorKind};
use harness_events_cli::cli::{Cli, EventCommand, LifecycleArgs, ObservationArgs};

const SESSION_ID: &str = "session-1";
const PROJECT_NAME: &str = "example-project";
const CURRENT_WORKING_DIRECTORY: &str = "/workspace/example-project";
const TIMESTAMP: &str = "2026-09-19T12:34:56.000Z";

fn lifecycle_arguments(command: &'static str) -> Vec<&'static str> {
    vec![
        "harness-events",
        command,
        "--session-id",
        SESSION_ID,
        "--project-name",
        PROJECT_NAME,
        "--current-working-directory",
        CURRENT_WORKING_DIRECTORY,
        "--timestamp",
        TIMESTAMP,
    ]
}

fn observation_arguments() -> Vec<&'static str> {
    vec![
        "harness-events",
        "observation",
        "--hook-type",
        "after-tool",
        "--project-name",
        PROJECT_NAME,
        "--current-working-directory",
        CURRENT_WORKING_DIRECTORY,
        "--timestamp",
        TIMESTAMP,
        "--session-id",
        SESSION_ID,
    ]
}

fn assert_usage_error(arguments: &[&str], expected_kind: ErrorKind) {
    let error = Cli::try_parse_from(arguments).expect_err("arguments must be rejected");

    assert_eq!(error.kind(), expected_kind);
    assert_eq!(error.exit_code(), 2);
}

fn assert_usage_exit(arguments: &[&str]) {
    let error = Cli::try_parse_from(arguments).expect_err("arguments must be rejected");

    assert_eq!(error.exit_code(), 2);
}

#[test]
fn parses_session_start_with_exact_fields() {
    let parsed = Cli::try_parse_from(lifecycle_arguments("session-start"))
        .expect("session-start arguments parse");

    assert_eq!(
        parsed.command,
        EventCommand::SessionStart(LifecycleArgs {
            session_id: SESSION_ID.to_owned(),
            project_name: PROJECT_NAME.to_owned(),
            current_working_directory: CURRENT_WORKING_DIRECTORY.to_owned(),
            timestamp: TIMESTAMP.to_owned(),
        })
    );
}

#[test]
fn exposes_only_the_three_event_subcommands_and_enables_claps_builtin_help() {
    let command = Cli::command();
    assert!(!command.is_disable_help_subcommand_set());
    let commands = command
        .get_subcommands()
        .map(|command| command.get_name())
        .collect::<Vec<_>>();

    assert_eq!(commands, ["session-start", "observation", "session-end"]);
}

#[test]
fn parses_observation_with_exact_fields() {
    let parsed = Cli::try_parse_from(observation_arguments()).expect("observation arguments parse");

    assert_eq!(
        parsed.command,
        EventCommand::Observation(ObservationArgs {
            hook_type: "after-tool".to_owned(),
            project_name: PROJECT_NAME.to_owned(),
            current_working_directory: CURRENT_WORKING_DIRECTORY.to_owned(),
            timestamp: TIMESTAMP.to_owned(),
            session_id: SESSION_ID.to_owned(),
        })
    );
}

#[test]
fn parses_session_end_with_exact_fields() {
    let parsed = Cli::try_parse_from(lifecycle_arguments("session-end"))
        .expect("session-end arguments parse");

    assert_eq!(
        parsed.command,
        EventCommand::SessionEnd(LifecycleArgs {
            session_id: SESSION_ID.to_owned(),
            project_name: PROJECT_NAME.to_owned(),
            current_working_directory: CURRENT_WORKING_DIRECTORY.to_owned(),
            timestamp: TIMESTAMP.to_owned(),
        })
    );
}

#[test]
fn rejects_every_missing_lifecycle_option() {
    for command in ["session-start", "session-end"] {
        for required_option in [
            "--session-id",
            "--project-name",
            "--current-working-directory",
            "--timestamp",
        ] {
            let mut arguments = lifecycle_arguments(command);
            let option_index = arguments
                .iter()
                .position(|argument| *argument == required_option)
                .expect("test arguments include the required option");
            arguments.drain(option_index..=option_index + 1);

            assert_usage_error(&arguments, ErrorKind::MissingRequiredArgument);
        }
    }
}

#[test]
fn rejects_every_missing_observation_option() {
    for required_option in [
        "--hook-type",
        "--project-name",
        "--current-working-directory",
        "--timestamp",
        "--session-id",
    ] {
        let mut arguments = observation_arguments();
        let option_index = arguments
            .iter()
            .position(|argument| *argument == required_option)
            .expect("test arguments include the required option");
        arguments.drain(option_index..=option_index + 1);

        assert_usage_error(&arguments, ErrorKind::MissingRequiredArgument);
    }
}

#[test]
fn rejects_options_without_values_with_a_usage_exit() {
    for arguments in [
        vec![
            "harness-events",
            "session-start",
            "--session-id",
            SESSION_ID,
            "--project-name",
            PROJECT_NAME,
            "--current-working-directory",
            CURRENT_WORKING_DIRECTORY,
            "--timestamp",
        ],
        vec![
            "harness-events",
            "observation",
            "--hook-type",
            "after-tool",
            "--project-name",
            PROJECT_NAME,
            "--current-working-directory",
            CURRENT_WORKING_DIRECTORY,
            "--timestamp",
            TIMESTAMP,
            "--session-id",
        ],
        vec![
            "harness-events",
            "session-end",
            "--session-id",
            SESSION_ID,
            "--project-name",
            PROJECT_NAME,
            "--current-working-directory",
            CURRENT_WORKING_DIRECTORY,
            "--timestamp",
        ],
    ] {
        let error = Cli::try_parse_from(arguments).expect_err("option without a value is rejected");

        assert_eq!(error.exit_code(), 2);
        assert!(error.to_string().contains("a value is required"));
    }
}

#[test]
fn rejects_a_missing_subcommand_with_a_usage_exit() {
    assert_usage_exit(&["harness-events"]);
}

#[test]
fn rejects_unknown_commands_with_a_usage_exit() {
    let error = Cli::try_parse_from(["harness-events", "unexpected-command"])
        .expect_err("unknown command is rejected");

    assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
    assert_eq!(error.exit_code(), 2);
    assert!(error.to_string().contains("unexpected-command"));
}

#[test]
fn rejects_unknown_options_for_each_command() {
    for mut arguments in [
        lifecycle_arguments("session-start"),
        observation_arguments(),
        lifecycle_arguments("session-end"),
    ] {
        arguments.extend(["--unexpected-option", "value"]);

        assert_usage_error(&arguments, ErrorKind::UnknownArgument);
    }
}

#[test]
fn rejects_snake_case_option_aliases() {
    for (command, kebab_case, snake_case) in [
        ("session-start", "--session-id", "--session_id"),
        ("session-start", "--project-name", "--project_name"),
        (
            "session-start",
            "--current-working-directory",
            "--current_working_directory",
        ),
        ("observation", "--hook-type", "--hook_type"),
        ("observation", "--project-name", "--project_name"),
        (
            "observation",
            "--current-working-directory",
            "--current_working_directory",
        ),
        ("observation", "--session-id", "--session_id"),
        ("session-end", "--project-name", "--project_name"),
    ] {
        let mut arguments = if command == "observation" {
            observation_arguments()
        } else {
            lifecycle_arguments(command)
        };
        let option_index = arguments
            .iter()
            .position(|argument| *argument == kebab_case)
            .expect("test arguments include the kebab-case option");
        arguments[option_index] = snake_case;

        assert_usage_error(&arguments, ErrorKind::UnknownArgument);
    }
}

#[test]
fn rejects_positional_arguments_for_each_event_command() {
    for mut arguments in [
        lifecycle_arguments("session-start"),
        observation_arguments(),
        lifecycle_arguments("session-end"),
    ] {
        arguments.insert(2, SESSION_ID);

        assert_usage_error(&arguments, ErrorKind::UnknownArgument);
    }
}

#[test]
fn generated_root_help_subcommand_lists_commands_with_a_zero_exit() {
    let root_help = Cli::try_parse_from(["harness-events", "help"])
        .expect_err("root help is returned as a Clap display error");

    assert_eq!(root_help.kind(), ErrorKind::DisplayHelp);
    assert_eq!(root_help.exit_code(), 0);
    let root_help = root_help.to_string();
    assert!(root_help.contains("Usage: harness-events <COMMAND>"));
    assert!(root_help.contains("Commands:"));
    for command in ["session-start", "observation", "session-end"] {
        assert!(root_help.contains(command));
    }
}

#[test]
fn generated_help_lists_commands_and_required_options_with_a_zero_exit() {
    let root_help = Cli::try_parse_from(["harness-events", "--help"])
        .expect_err("help is returned as a Clap display error");

    assert_eq!(root_help.kind(), ErrorKind::DisplayHelp);
    assert_eq!(root_help.exit_code(), 0);
    let root_help = root_help.to_string();
    for command in ["session-start", "observation", "session-end"] {
        assert!(root_help.contains(command));
    }

    let help_cases: [(&str, &[&str]); 3] = [
        (
            "session-start",
            &[
                "--session-id",
                "--project-name",
                "--current-working-directory",
                "--timestamp",
            ],
        ),
        (
            "observation",
            &[
                "--hook-type",
                "--project-name",
                "--current-working-directory",
                "--timestamp",
                "--session-id",
            ],
        ),
        (
            "session-end",
            &[
                "--session-id",
                "--project-name",
                "--current-working-directory",
                "--timestamp",
            ],
        ),
    ];

    for (command, required_options) in help_cases {
        let command_help = Cli::try_parse_from(["harness-events", command, "--help"])
            .expect_err("subcommand help is returned as a Clap display error");

        assert_eq!(command_help.kind(), ErrorKind::DisplayHelp);
        assert_eq!(command_help.exit_code(), 0);
        let command_help = command_help.to_string();
        for option in required_options {
            assert!(command_help.contains(option));
        }
    }
}
