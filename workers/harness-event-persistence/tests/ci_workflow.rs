use serde::Deserialize;
use serde_yaml::Value;

const CI_WORKFLOW: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../.github/workflows/ci.yml"
));
const DIRECT_POSTGRESQL_BUILD_PREFIX: [&str; 4] =
    ["nix", "build", "--print-build-logs", "--no-link"];
const REQUIRED_POSTGRESQL_OUTPUTS: [&str; 13] = [
    ".#${{ matrix.package_prefix }}-search-verify",
    ".#${{ matrix.package_prefix }}-search-fixture-verify",
    ".#${{ matrix.package_prefix }}-memory-storage-verify",
    ".#${{ matrix.package_prefix }}-memory-search-heads-verify",
    ".#${{ matrix.package_prefix }}-memory-embedding-lifecycle-verify",
    ".#${{ matrix.package_prefix }}-memory-embedding-work-verify",
    ".#${{ matrix.package_prefix }}-memory-version-heads-verify",
    ".#${{ matrix.package_prefix }}-memory-bm25-verify",
    ".#${{ matrix.package_prefix }}-memory-vector-verify",
    ".#${{ matrix.package_prefix }}-session-post-processing-verify",
    ".#${{ matrix.package_prefix }}-knowledge-graph-schema-verify",
    ".#${{ matrix.package_prefix }}-knowledge-graph-mutation-verify",
    ".#${{ matrix.package_prefix }}-knowledge-graph-traversal-verify",
];

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct PostgresqlMatrixEntry {
    major: u8,
    package_prefix: String,
}

fn yaml_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value
        .as_mapping()
        .and_then(|mapping| mapping.get(Value::String(field.to_owned())))
}

fn parsed_ci_workflow(workflow: &str) -> Value {
    serde_yaml::from_str::<Value>(workflow).expect("CI workflow must be valid YAML")
}

fn postgresql_direct_smoke_job(workflow: &Value) -> Result<&Value, String> {
    yaml_field(workflow, "jobs")
        .and_then(|jobs| yaml_field(jobs, "postgresql-direct-smoke"))
        .ok_or_else(|| "CI must define postgresql-direct-smoke".to_owned())
}

fn ci_workflow_mutation(needle: &str, replacement: &str) -> String {
    let (before, after) = CI_WORKFLOW
        .split_once(needle)
        .expect("CI mutation fixture must match the checked-in workflow");

    format!("{before}{replacement}{after}")
}

fn is_direct_postgresql_build(run: &str) -> bool {
    run.split_whitespace()
        .collect::<Vec<_>>()
        .starts_with(&DIRECT_POSTGRESQL_BUILD_PREFIX)
}

fn direct_postgresql_build_arguments(run: &str) -> Vec<String> {
    run.replace("${{ matrix.package_prefix }}", "${{matrix.package_prefix}}")
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn is_literal_boolean(value: &Value, expected: bool) -> bool {
    matches!(value, Value::Bool(actual) if *actual == expected)
}

fn is_unconditionally_enabled(value: &Value) -> bool {
    match yaml_field(value, "if") {
        None => true,
        Some(condition) => is_literal_boolean(condition, true),
    }
}

fn propagates_failure(value: &Value) -> bool {
    match yaml_field(value, "continue-on-error") {
        None => true,
        Some(continue_on_error) => is_literal_boolean(continue_on_error, false),
    }
}

fn require_unconditionally_enabled(value: &Value, name: &str) -> Result<(), String> {
    if is_unconditionally_enabled(value) {
        Ok(())
    } else {
        Err(format!(
            "{name} must be unconditionally enabled; `if` may be absent or a literal true boolean"
        ))
    }
}

fn require_failure_propagation(value: &Value, name: &str) -> Result<(), String> {
    if propagates_failure(value) {
        Ok(())
    } else {
        Err(format!(
            "{name} must propagate failures; `continue-on-error` may be absent or a literal false boolean"
        ))
    }
}

fn require_no_default_shell(value: &Value, name: &str) -> Result<(), String> {
    let Some(defaults) = yaml_field(value, "defaults") else {
        return Ok(());
    };
    let defaults = defaults
        .as_mapping()
        .ok_or_else(|| format!("{name} defaults must be a mapping"))?;
    let Some(run) = defaults.get(Value::String("run".to_owned())) else {
        return Ok(());
    };
    let run = run
        .as_mapping()
        .ok_or_else(|| format!("{name} defaults.run must be a mapping"))?;

    if run.contains_key(Value::String("shell".to_owned())) {
        Err(format!("{name} must not configure defaults.run.shell"))
    } else {
        Ok(())
    }
}

fn require_no_explicit_shell(value: &Value, name: &str) -> Result<(), String> {
    if yaml_field(value, "shell").is_some() {
        Err(format!("{name} must not configure shell"))
    } else {
        Ok(())
    }
}

fn direct_postgresql_build_step(workflow: &Value) -> Result<&Value, String> {
    require_no_default_shell(workflow, "CI workflow")?;
    let job = postgresql_direct_smoke_job(workflow)?;
    require_no_default_shell(job, "postgresql-direct-smoke")?;
    require_unconditionally_enabled(job, "postgresql-direct-smoke")?;
    require_failure_propagation(job, "postgresql-direct-smoke")?;

    let build_steps = yaml_field(job, "steps")
        .and_then(Value::as_sequence)
        .ok_or_else(|| "postgresql-direct-smoke must define steps".to_owned())?
        .iter()
        .filter(|step| {
            yaml_field(step, "run")
                .and_then(Value::as_str)
                .is_some_and(is_direct_postgresql_build)
        })
        .collect::<Vec<_>>();
    let [build_step] = build_steps.as_slice() else {
        return Err(format!(
            "postgresql-direct-smoke must contain exactly one `nix build --print-build-logs --no-link` step; found {}",
            build_steps.len()
        ));
    };

    require_unconditionally_enabled(build_step, "the direct PostgreSQL Nix build step")?;
    require_failure_propagation(build_step, "the direct PostgreSQL Nix build step")?;
    require_no_default_shell(build_step, "the direct PostgreSQL Nix build step")?;
    require_no_explicit_shell(build_step, "the direct PostgreSQL Nix build step")?;

    let build_run = yaml_field(build_step, "run")
        .and_then(Value::as_str)
        .ok_or_else(|| "the direct PostgreSQL Nix build step must define run".to_owned())?;
    let arguments = direct_postgresql_build_arguments(build_run);
    let required_outputs = REQUIRED_POSTGRESQL_OUTPUTS
        .iter()
        .map(|output| output.replace("${{ matrix.package_prefix }}", "${{matrix.package_prefix}}"))
        .collect::<Vec<_>>();
    let unexpected_arguments = arguments[DIRECT_POSTGRESQL_BUILD_PREFIX.len()..]
        .iter()
        .filter(|argument| {
            !required_outputs
                .iter()
                .any(|required| required == *argument)
        })
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !unexpected_arguments.is_empty() {
        return Err(format!(
            "the direct PostgreSQL Nix build step must contain only required flake outputs after `nix build --print-build-logs --no-link`; unexpected arguments: {}",
            unexpected_arguments.join(" ")
        ));
    }

    Ok(*build_step)
}

const SESSION_POST_PROCESSING_RELEASE_BUILD: &str =
    "nix develop --command cargo build --locked --release --package session-post-processing";
const SESSION_POST_PROCESSING_PROTOCOL_FAKE: &str = "nix develop --command cargo run --locked --release --package session-post-processing-engine-fake -- --manifest \"$PWD/workers/session-post-processing/iii.worker.yaml\"";

fn normalized_workflow(workflow: &str) -> String {
    workflow.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn ci_named_job<'a>(workflow: &'a str, name: &str) -> &'a str {
    let start = format!("\n  {name}:\n");
    workflow
        .split_once(&start)
        .map(|(_, workflow)| {
            let next_job = workflow.match_indices("\n  ").find_map(|(index, _)| {
                (workflow.as_bytes().get(index + 3) != Some(&b' ')).then_some(index)
            });
            next_job.map_or(workflow, |index| &workflow[..index])
        })
        .unwrap_or_else(|| panic!("CI is missing job {name:?}"))
}

fn ci_named_step<'a>(job: &'a str, name: &str) -> &'a str {
    let start = format!("\n      - name: {name}\n");
    job.split_once(&start)
        .map(|(_, step)| step.split_once("\n      - ").map_or(step, |(step, _)| step))
        .unwrap_or_else(|| panic!("CI job is missing step {name:?}"))
}

#[derive(Debug)]
struct CiRun<'a> {
    source: &'a str,
    normalized: String,
}

fn ci_step_mappings(job: &str) -> Vec<&str> {
    let starts = job
        .match_indices("\n      - ")
        .map(|(index, _)| index + 1)
        .collect::<Vec<_>>();

    starts
        .iter()
        .enumerate()
        .map(|(index, start)| {
            let end = starts.get(index + 1).copied().unwrap_or(job.len());
            &job[*start..end]
        })
        .collect()
}

fn ci_step_run(step: &str) -> Option<CiRun<'_>> {
    let run_start = step
        .match_indices("        run:")
        .find_map(|(index, _)| (index == 0 || step[..index].ends_with('\n')).then_some(index))?;
    let line_end = step[run_start..]
        .find('\n')
        .map(|offset| run_start + offset)
        .unwrap_or(step.len());
    let indicator = step[run_start + "        run:".len()..line_end].trim();

    if matches!(indicator, "|" | "|-" | "|+" | ">" | ">-" | ">+") {
        let mut values = Vec::new();
        let mut cursor = if line_end == step.len() {
            line_end
        } else {
            line_end + 1
        };
        let mut scalar_end = cursor;

        while cursor < step.len() {
            let next_line_end = step[cursor..]
                .find('\n')
                .map(|offset| cursor + offset)
                .unwrap_or(step.len());
            let line = &step[cursor..next_line_end];

            if line.is_empty() {
                values.push("");
            } else if let Some(value) = line.strip_prefix("          ") {
                values.push(value);
            } else {
                break;
            }

            scalar_end = next_line_end;
            cursor = if next_line_end == step.len() {
                next_line_end
            } else {
                next_line_end + 1
            };
        }

        return (!values.is_empty()).then(|| CiRun {
            source: &step[run_start..scalar_end],
            normalized: normalized_workflow(&values.join("\n")),
        });
    }

    (!indicator.is_empty()).then(|| CiRun {
        source: &step[run_start..line_end],
        normalized: normalized_workflow(indicator),
    })
}

fn required_ci_step_run<'a>(step: &'a str, name: &str) -> CiRun<'a> {
    ci_step_run(step)
        .unwrap_or_else(|| panic!("CI step {name:?} must define a non-empty run scalar"))
}

fn ci_job_run_commands(job: &str) -> Vec<String> {
    ci_step_mappings(job)
        .into_iter()
        .filter_map(ci_step_run)
        .map(|run| run.normalized)
        .collect()
}

fn ci_mapping_weakening_directives(mapping: &str, property_indent: usize) -> Vec<&str> {
    let property_prefix = " ".repeat(property_indent);

    mapping
        .lines()
        .filter_map(|line| line.strip_prefix(&property_prefix))
        .filter(|line| !line.starts_with(' '))
        .filter(|line| line.starts_with("if:") || line.starts_with("continue-on-error:"))
        .map(str::trim)
        .collect()
}

fn assert_no_weakening_directives(mapping: &str, property_indent: usize, requirement: &str) {
    let weakening_directives = ci_mapping_weakening_directives(mapping, property_indent);
    assert!(
        weakening_directives.is_empty(),
        "{requirement}:\n{}",
        weakening_directives.join("\n")
    );
}

fn assert_no_path_flake_references(job: &str, job_name: &str) {
    let path_flake_commands = ci_job_run_commands(job)
        .into_iter()
        .filter(|command| command.contains("path:."))
        .collect::<Vec<_>>();
    assert!(
        path_flake_commands.is_empty(),
        "{job_name} run commands must use only Git-backed flake references:\n{}",
        path_flake_commands.join("\n")
    );
}

fn assert_session_post_processing_ci_contract(workflow: &str) {
    let test_job = ci_named_job(workflow, "test");
    let test_run_commands = ci_job_run_commands(test_job);
    assert!(
        test_run_commands.iter().any(|command| {
            command.as_str()
                == "nix develop --command cargo-clippy --workspace --all-targets --locked -- -D warnings"
        }),
        "test must run cargo-clippy from the Nix toolchain"
    );
    assert!(
        test_run_commands
            .iter()
            .all(|command| !command.contains("nix develop --command cargo clippy")),
        "CI must use cargo-clippy from the Nix toolchain"
    );

    let release_build = ci_named_step(test_job, "Build session post-processing release worker");
    let protocol_fake = ci_named_step(test_job, "Run session post-processing protocol fake");
    let release_build_run = required_ci_step_run(
        release_build,
        "Build session post-processing release worker",
    );
    let protocol_fake_run =
        required_ci_step_run(protocol_fake, "Run session post-processing protocol fake");
    assert!(
        release_build_run.normalized == SESSION_POST_PROCESSING_RELEASE_BUILD,
        "test must run the session post-processing release build in its named release-build step"
    );
    assert!(
        protocol_fake_run.normalized == SESSION_POST_PROCESSING_PROTOCOL_FAKE,
        "test must run the session post-processing protocol fake in its named protocol-fake step"
    );

    assert_no_weakening_directives(
        test_job,
        4,
        "test must remain unconditional and failure-propagating",
    );
    assert_no_weakening_directives(
        release_build,
        8,
        "test must remain unconditional and failure-propagating",
    );
    assert_no_weakening_directives(
        protocol_fake,
        8,
        "test must remain unconditional and failure-propagating",
    );

    let release_build_position = test_job
        .find("\n      - name: Build session post-processing release worker\n")
        .expect("test is missing the session post-processing release build step");
    let protocol_fake_position = test_job
        .find("\n      - name: Run session post-processing protocol fake\n")
        .expect("test is missing the session post-processing protocol fake step");
    assert!(
        release_build_position < protocol_fake_position,
        "test must build the session post-processing release worker before the session protocol fake"
    );
}

fn with_named_job_directive(workflow: &str, name: &str, directive: &str) -> String {
    let job = format!("\n  {name}:\n");
    workflow.replacen(&job, &format!("{job}    {directive}\n"), 1)
}

fn with_named_step_directive(workflow: &str, name: &str, directive: &str) -> String {
    let step = format!("\n      - name: {name}\n");
    workflow.replacen(&step, &format!("{step}        {directive}\n"), 1)
}

fn with_named_step_run(workflow: &str, job_name: &str, name: &str, run: &str) -> String {
    let job = ci_named_job(workflow, job_name);
    let step = ci_named_step(job, name);
    let existing_run = required_ci_step_run(step, name);

    workflow.replacen(existing_run.source, &format!("        run: {run}"), 1)
}

fn with_test_step_run(workflow: &str, name: &str, run: &str) -> String {
    with_named_step_run(workflow, "test", name, run)
}

fn with_unrelated_test_step_path(workflow: &str) -> String {
    workflow.replacen(
        "        uses: actions/checkout@v4\n",
        "        uses: actions/checkout@v4\n        with:\n          path: checked-out-repository\n",
        1,
    )
}

fn with_session_post_processing_steps_reversed(workflow: &str) -> String {
    let job = ci_named_job(workflow, "test");
    let release_build_step = format!(
        "\n      - name: Build session post-processing release worker\n{}",
        ci_named_step(job, "Build session post-processing release worker")
    );
    let protocol_fake_step = format!(
        "\n      - name: Run session post-processing protocol fake\n{}",
        ci_named_step(job, "Run session post-processing protocol fake")
    );

    workflow
        .replacen(&protocol_fake_step, &release_build_step, 1)
        .replacen(&release_build_step, &protocol_fake_step, 1)
}

#[test]
fn ci_runs_the_full_infrastructure_free_worker_verification_contract() {
    assert_full_infrastructure_free_worker_verification_contract(CI_WORKFLOW);
}

fn assert_full_infrastructure_free_worker_verification_contract(workflow: &str) {
    let normalized = normalized_workflow(workflow);
    let required_commands = [
        "nix flake check",
        "nix develop --command cargo test --workspace --locked",
        "nix develop --command cargo test --locked --package knowledge-graph-store",
        "nix develop --command cargo fmt --all --check",
        "nix develop --command actionlint",
        "nix develop --command cargo-clippy --workspace --all-targets --locked -- -D warnings",
        "nix develop --command cargo deny --locked check licenses advisories",
        "nix develop --command cargo build --locked --release --package harness-ingestion",
        "nix develop --command cargo build --locked --release --package harness-event-persistence",
        "nix develop --command cargo build --locked --release --package mcp-worker",
        "nix develop --command cargo build --locked --release --package memory-embedding",
        "nix develop --command cargo run --locked --release --package harness-engine-fake -- --manifest \"$PWD/workers/harness-ingestion/iii.worker.yaml\"",
        "nix develop --command cargo run --locked --release --package harness-persistence-engine-fake -- --manifest \"$PWD/workers/harness-event-persistence/iii.worker.yaml\"",
        "nix develop --command cargo run --locked --release --package mcp-worker-fake -- --worker \"$PWD/target/release/mcp-worker\"",
        "nix develop --command cargo run --locked --release --package memory-embedding-engine-fake -- --manifest \"$PWD/workers/memory-embedding/iii.worker.yaml\"",
        "nix develop --command cargo run --locked --release --package knowledge-graph-database-engine-fake",
    ];

    let missing = required_commands
        .iter()
        .filter(|command| !normalized.contains(**command))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "CI is missing required verification commands:\n{}",
        missing.join("\n")
    );
    assert_no_path_flake_references(ci_named_job(workflow, "test"), "test");
}

fn assert_direct_postgresql_ci_contract(workflow: &str) {
    let postgresql_job = ci_named_job(workflow, "postgresql-direct-smoke");
    let normalized_postgresql_job = postgresql_job
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let postgresql_verifier = ci_named_step(postgresql_job, "Run direct PostgreSQL verifiers");
    let postgresql_verifier_run =
        required_ci_step_run(postgresql_verifier, "Run direct PostgreSQL verifiers");
    let required_matrix_fragments = [
        "major: 17 package_prefix: postgresql17",
        "major: 18 package_prefix: postgresql18",
    ];
    let missing_matrix_fragments = required_matrix_fragments
        .iter()
        .filter(|fragment| !normalized_postgresql_job.contains(**fragment))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        missing_matrix_fragments.is_empty(),
        "postgresql-direct-smoke is missing required matrix pairs:\n{}",
        missing_matrix_fragments.join("\n")
    );

    let required_verifier_fragments = [
        ".#${{ matrix.package_prefix }}-search-verify",
        ".#${{ matrix.package_prefix }}-search-fixture-verify",
        ".#${{ matrix.package_prefix }}-memory-storage-verify",
        ".#${{ matrix.package_prefix }}-memory-search-heads-verify",
        ".#${{ matrix.package_prefix }}-memory-embedding-lifecycle-verify",
        ".#${{ matrix.package_prefix }}-memory-version-heads-verify",
        ".#${{ matrix.package_prefix }}-memory-bm25-verify",
        ".#${{ matrix.package_prefix }}-memory-vector-verify",
        ".#${{ matrix.package_prefix }}-session-post-processing-verify",
        ".#${{ matrix.package_prefix }}-knowledge-graph-schema-verify",
        ".#${{ matrix.package_prefix }}-knowledge-graph-mutation-verify",
        ".#${{ matrix.package_prefix }}-knowledge-graph-traversal-verify",
    ];
    let missing_fragments = required_verifier_fragments
        .iter()
        .filter(|fragment| !postgresql_verifier_run.normalized.contains(**fragment))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing_fragments.is_empty(),
        "postgresql-direct-smoke is missing required verification fragments:\n{}",
        missing_fragments.join("\n")
    );
    assert_no_weakening_directives(
        postgresql_job,
        4,
        "postgresql-direct-smoke must remain unconditional and failure-propagating",
    );
    assert_no_weakening_directives(
        postgresql_verifier,
        8,
        "postgresql-direct-smoke must remain unconditional and failure-propagating",
    );
    assert_no_path_flake_references(postgresql_job, "postgresql-direct-smoke");
}

#[test]
fn ci_enforces_session_post_processing_release_and_direct_postgresql_gates() {
    assert_session_post_processing_ci_contract(CI_WORKFLOW);
    assert_direct_postgresql_ci_contract(CI_WORKFLOW);
}

#[test]
#[should_panic(expected = "test must remain unconditional and failure-propagating")]
fn ci_rejects_a_conditional_test_job() {
    let workflow = with_named_job_directive(CI_WORKFLOW, "test", "if: false");

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
#[should_panic(expected = "test must remain unconditional and failure-propagating")]
fn ci_rejects_a_failure_ignoring_session_protocol_fake() {
    let workflow = with_named_step_directive(
        CI_WORKFLOW,
        "Run session post-processing protocol fake",
        "continue-on-error: true",
    );

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
#[should_panic(expected = "test must remain unconditional and failure-propagating")]
fn ci_rejects_a_conditional_session_release_build() {
    let workflow = with_named_step_directive(
        CI_WORKFLOW,
        "Build session post-processing release worker",
        "if: false",
    );

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
#[should_panic(expected = "test must run the session post-processing release build")]
fn ci_rejects_an_echo_of_the_session_release_build_command() {
    let workflow = with_test_step_run(
        CI_WORKFLOW,
        "Build session post-processing release worker",
        &format!("echo '{SESSION_POST_PROCESSING_RELEASE_BUILD}'"),
    );

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
#[should_panic(expected = "test must run the session post-processing protocol fake")]
fn ci_rejects_a_comment_of_the_session_protocol_fake_command() {
    let workflow = with_test_step_run(
        CI_WORKFLOW,
        "Run session post-processing protocol fake",
        &format!("# {SESSION_POST_PROCESSING_PROTOCOL_FAKE}"),
    );

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
fn ci_allows_weakening_directives_on_unrelated_test_steps() {
    let workflow = with_named_step_directive(
        CI_WORKFLOW,
        "Check out repository",
        "continue-on-error: true",
    );
    let workflow = with_named_step_directive(&workflow, "Install Nix", "if: always()");

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
#[should_panic(
    expected = "postgresql-direct-smoke must remain unconditional and failure-propagating"
)]
fn ci_rejects_a_conditional_direct_postgresql_job() {
    let workflow = with_named_job_directive(CI_WORKFLOW, "postgresql-direct-smoke", "if: false");

    assert_direct_postgresql_ci_contract(&workflow);
}

#[test]
#[should_panic(
    expected = "postgresql-direct-smoke must remain unconditional and failure-propagating"
)]
fn ci_rejects_a_failure_ignoring_direct_postgresql_verifiers() {
    let workflow = with_named_step_directive(
        CI_WORKFLOW,
        "Run direct PostgreSQL verifiers",
        "continue-on-error: true",
    );

    assert_direct_postgresql_ci_contract(&workflow);
}

#[test]
#[should_panic(
    expected = "postgresql-direct-smoke run commands must use only Git-backed flake references"
)]
fn ci_rejects_a_path_flake_reference_in_direct_postgresql_verifiers() {
    let workflow = with_named_step_run(
        CI_WORKFLOW,
        "postgresql-direct-smoke",
        "Run direct PostgreSQL verifiers",
        "nix build path:.#${{ matrix.package_prefix }}-search-verify .#${{ matrix.package_prefix }}-search-fixture-verify .#${{ matrix.package_prefix }}-memory-storage-verify .#${{ matrix.package_prefix }}-memory-search-heads-verify .#${{ matrix.package_prefix }}-memory-embedding-lifecycle-verify .#${{ matrix.package_prefix }}-memory-version-heads-verify .#${{ matrix.package_prefix }}-memory-bm25-verify .#${{ matrix.package_prefix }}-memory-vector-verify .#${{ matrix.package_prefix }}-session-post-processing-verify .#${{ matrix.package_prefix }}-knowledge-graph-schema-verify .#${{ matrix.package_prefix }}-knowledge-graph-mutation-verify .#${{ matrix.package_prefix }}-knowledge-graph-traversal-verify",
    );

    assert_direct_postgresql_ci_contract(&workflow);
}

#[test]
fn ci_allows_an_unrelated_checkout_path() {
    let workflow = with_unrelated_test_step_path(CI_WORKFLOW);

    assert_full_infrastructure_free_worker_verification_contract(&workflow);
}

#[test]
#[should_panic(expected = "test must build the session post-processing release worker before")]
fn ci_rejects_session_post_processing_steps_in_the_wrong_order() {
    let workflow = with_session_post_processing_steps_reversed(CI_WORKFLOW);

    assert_session_post_processing_ci_contract(&workflow);
}

#[test]
fn ci_runs_all_direct_postgresql_verifiers_from_git_backed_flakes() {
    let workflow = parsed_ci_workflow(CI_WORKFLOW);
    let job =
        postgresql_direct_smoke_job(&workflow).expect("CI must define postgresql-direct-smoke");
    let matrix_entries = yaml_field(job, "strategy")
        .and_then(|strategy| yaml_field(strategy, "matrix"))
        .and_then(|matrix| yaml_field(matrix, "include"))
        .and_then(Value::as_sequence)
        .expect("postgresql-direct-smoke must define a matrix include list")
        .iter()
        .cloned()
        .map(serde_yaml::from_value::<PostgresqlMatrixEntry>)
        .collect::<Result<Vec<_>, _>>()
        .expect("postgresql-direct-smoke matrix entries must define a major and package prefix");
    let required_matrix_entries = [
        PostgresqlMatrixEntry {
            major: 17,
            package_prefix: "postgresql17".to_owned(),
        },
        PostgresqlMatrixEntry {
            major: 18,
            package_prefix: "postgresql18".to_owned(),
        },
    ];
    let missing_matrix_entries = required_matrix_entries
        .iter()
        .filter(|entry| !matrix_entries.contains(entry))
        .map(|entry| {
            format!(
                "major: {} / package_prefix: {}",
                entry.major, entry.package_prefix
            )
        })
        .collect::<Vec<_>>();

    assert!(
        missing_matrix_entries.is_empty(),
        "PostgreSQL smoke is missing required matrix entries:\n{}",
        missing_matrix_entries.join("\n")
    );

    let build_step = direct_postgresql_build_step(&workflow).expect(
        "postgresql-direct-smoke must contain exactly one `nix build --print-build-logs --no-link` step",
    );
    let build_run = yaml_field(build_step, "run")
        .and_then(Value::as_str)
        .expect("the direct PostgreSQL build step must define run");
    let normalized_build_run = build_run.split_whitespace().collect::<String>();
    let missing = REQUIRED_POSTGRESQL_OUTPUTS
        .iter()
        .filter(|output| {
            !normalized_build_run.contains(&output.split_whitespace().collect::<String>())
        })
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "PostgreSQL smoke build is missing required flake outputs:\n{}",
        missing.join("\n")
    );
    assert!(
        !build_run
            .split_whitespace()
            .any(|argument| argument.starts_with("path:.#")),
        "PostgreSQL smoke build must use Git-backed flake references"
    );
}

#[test]
fn ci_rejects_skipped_or_nonblocking_direct_postgresql_smoke() {
    let fixtures = [
        (
            "job conditional skip",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    if: false\n",
            ),
        ),
        (
            "job quoted if",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    if: \"true\"\n",
            ),
        ),
        (
            "job expression if",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    if: ${{ true }}\n",
            ),
        ),
        (
            "build step conditional skip",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        if: ${{ false }}\n        run: >-",
            ),
        ),
        (
            "build step quoted if",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        if: \"true\"\n        run: >-",
            ),
        ),
        (
            "build step expression if",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        if: ${{ true }}\n        run: >-",
            ),
        ),
        (
            "job continue-on-error",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    continue-on-error: true\n",
            ),
        ),
        (
            "job quoted continue-on-error",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    continue-on-error: \"false\"\n",
            ),
        ),
        (
            "job expression continue-on-error",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    continue-on-error: ${{ false }}\n",
            ),
        ),
        (
            "build step continue-on-error",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        continue-on-error: true\n        run: >-",
            ),
        ),
        (
            "build step quoted continue-on-error",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        continue-on-error: \"false\"\n        run: >-",
            ),
        ),
        (
            "build step expression continue-on-error",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        continue-on-error: ${{ false }}\n        run: >-",
            ),
        ),
        (
            "workflow custom shell",
            ci_workflow_mutation(
                "permissions:\n",
                "defaults:\n  run:\n    shell: bash\n\npermissions:\n",
            ),
        ),
        (
            "job custom shell",
            ci_workflow_mutation(
                "  postgresql-direct-smoke:\n",
                "  postgresql-direct-smoke:\n    defaults:\n      run:\n        shell: bash\n",
            ),
        ),
        (
            "build step custom shell",
            ci_workflow_mutation(
                "      - name: Run direct PostgreSQL verifiers\n        run: >-",
                "      - name: Run direct PostgreSQL verifiers\n        shell: bash\n        run: >-",
            ),
        ),
        (
            "build shell || true",
            ci_workflow_mutation(
                "          .#${{ matrix.package_prefix }}-memory-vector-verify\n",
                "          .#${{ matrix.package_prefix }}-memory-vector-verify || true\n",
            ),
        ),
        (
            "build shell ; true",
            ci_workflow_mutation(
                "          .#${{ matrix.package_prefix }}-memory-vector-verify\n",
                "          .#${{ matrix.package_prefix }}-memory-vector-verify; true\n",
            ),
        ),
    ];
    let accepted = fixtures
        .into_iter()
        .filter_map(|(name, workflow)| {
            let workflow = parsed_ci_workflow(&workflow);
            direct_postgresql_build_step(&workflow)
                .is_ok()
                .then_some(name)
        })
        .collect::<Vec<_>>();

    assert!(
        accepted.is_empty(),
        "CI direct PostgreSQL smoke contract accepts unsafe workflow mutations:\n{}",
        accepted.join("\n")
    );
}

#[test]
fn ci_runs_the_pi_node_lts_package_and_live_e2e_contract() {
    let job = CI_WORKFLOW
        .split_once("\n  pi-node-lts:\n")
        .and_then(|(_, workflow)| workflow.split_once("\n  postgresql-direct-smoke:\n"))
        .map(|(job, _)| job.split_whitespace().collect::<Vec<_>>().join(" "))
        .expect("CI must define pi-node-lts before the PostgreSQL jobs");
    let required_fragments = [
        "name: Pi Node ${{ matrix.node }} LTS",
        "runs-on: ubuntu-latest",
        "timeout-minutes: 20",
        "RUST_BACKTRACE: \"1\"",
        "NODE_MAJOR: ${{ matrix.node }}",
        "fail-fast: false",
        "node: [24]",
        "nix develop \".#node${NODE_MAJOR}\" --command bash",
        "node_path=\"$(command -v node)\"",
        "iii_path=\"$(command -v iii)\"",
        "npm ci",
        "npm run check",
        "npm pack --json --ignore-scripts --pack-destination \"$stage_dir\"",
        "JSON.parse(pack_output)",
        "npm install --offline --ignore-scripts --legacy-peer-deps --no-package-lock --no-audit --no-fund \"$archive_path\"",
        "extension_root=\"$(cd \"$consumer_dir/node_modules/@dwsr/pi-harness-events\" && pwd -P)\"",
        "cargo build --locked --release --package harness-events-cli",
        "PI_HARNESS_EVENTS_E2E_NODE=\"$node_path\"",
        "PI_HARNESS_EVENTS_E2E_PI_CLI=\"$pi_cli_path\"",
        "PI_HARNESS_EVENTS_E2E_III=\"$iii_path\"",
        "PI_HARNESS_EVENTS_E2E_HARNESS_EVENTS=\"$harness_events_path\"",
        "PI_HARNESS_EVENTS_E2E_EXTENSION_ROOT=\"$extension_root\"",
        "PI_HARNESS_EVENTS_E2E_SCRIPTED_PROVIDER=\"$scripted_provider_path\"",
        "cargo test --locked --package pi-harness-events-e2e --test live -- --ignored --test-threads=1",
    ];

    let missing = required_fragments
        .iter()
        .filter(|fragment| !job.contains(**fragment))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "pi-node-lts is missing required contract fragments:\n{}",
        missing.join("\n")
    );
    assert!(
        !job.contains("npx"),
        "pi-node-lts must invoke the locked local Pi CLI directly"
    );
}

#[test]
fn ci_runs_package_quality_and_serial_live_matrix_after_release_artifacts() {
    let workflow = normalized_workflow(CI_WORKFLOW);
    let parsed_workflow = parsed_ci_workflow(CI_WORKFLOW);
    let required_fragments = [
        "working-directory: integrations/opencode-harness-events",
        "nix develop --command bun install --frozen-lockfile",
        "nix develop --command bun run check",
        "nix develop --command bun run pack",
        "export III_BIN=\"\\$(command -v iii)\"",
        "export OPENCODE_V1_BIN=\"\\$(command -v opencode-v1)\"",
        "export OPENCODE_V2_BIN=\"\\$(command -v opencode-v2)\"",
        "export HARNESS_EVENTS_BIN=\"\\$PWD/target/release/harness-events\"",
        "export PLUGIN_TARBALL=\"\\$PWD/integrations/opencode-harness-events/opencode-harness-events-0.1.0.tgz\"",
        "test -f \"\\$III_BIN\"",
        "test -f \"\\$OPENCODE_V1_BIN\"",
        "test -f \"\\$OPENCODE_V2_BIN\"",
        "test -f \"\\$HARNESS_EVENTS_BIN\"",
        "test -f \"\\$PLUGIN_TARBALL\"",
        "cargo test --locked --package opencode-harness-events-e2e --test live -- --ignored --test-threads=1",
    ];

    let missing = required_fragments
        .iter()
        .filter(|fragment| !workflow.contains(**fragment))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "CI is missing package/live verification contract fragments:\n{}",
        missing.join("\n")
    );

    let test_job = yaml_field(&parsed_workflow, "jobs")
        .and_then(|jobs| yaml_field(jobs, "test"))
        .expect("CI must define the test job");
    assert_eq!(
        yaml_field(test_job, "timeout-minutes").and_then(Value::as_u64),
        Some(40),
        "test job timeout must allow cold release builds and live verification"
    );

    let ordered_steps = [
        "nix develop --command bun install --frozen-lockfile",
        "nix develop --command bun run check",
        "nix develop --command bun run pack",
        "nix develop --command cargo build --locked --release --package harness-events-cli",
        "export III_BIN=\"\\$(command -v iii)\"",
        "cargo test --locked --package opencode-harness-events-e2e --test live -- --ignored --test-threads=1",
    ];
    let mut previous = 0;
    for step in ordered_steps {
        let position = workflow
            .find(step)
            .unwrap_or_else(|| panic!("CI is missing ordered step: {step}"));
        assert!(position >= previous, "CI step appears out of order: {step}");
        previous = position;
    }
}
