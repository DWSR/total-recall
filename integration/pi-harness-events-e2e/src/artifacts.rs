use std::{
    env,
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use thiserror::Error;

pub const NODE_ENV: &str = "PI_HARNESS_EVENTS_E2E_NODE";
pub const PI_CLI_ENV: &str = "PI_HARNESS_EVENTS_E2E_PI_CLI";
pub const III_ENV: &str = "PI_HARNESS_EVENTS_E2E_III";
pub const HARNESS_EVENTS_ENV: &str = "PI_HARNESS_EVENTS_E2E_HARNESS_EVENTS";
pub const EXTENSION_ROOT_ENV: &str = "PI_HARNESS_EVENTS_E2E_EXTENSION_ROOT";
pub const SCRIPTED_PROVIDER_ENV: &str = "PI_HARNESS_EVENTS_E2E_SCRIPTED_PROVIDER";

const EXTENSION_PACKAGE_NAME: &str = "@dwsr/pi-harness-events";
const EXTENSION_ENTRYPOINT: &str = "./src/index.ts";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactRole {
    Node,
    PiCli,
    Iii,
    HarnessEvents,
    ExtensionRoot,
    ScriptedProvider,
}

impl ArtifactRole {
    pub const ALL: [Self; 6] = [
        Self::Node,
        Self::PiCli,
        Self::Iii,
        Self::HarnessEvents,
        Self::ExtensionRoot,
        Self::ScriptedProvider,
    ];

    pub const fn environment_key(self) -> &'static str {
        match self {
            Self::Node => NODE_ENV,
            Self::PiCli => PI_CLI_ENV,
            Self::Iii => III_ENV,
            Self::HarnessEvents => HARNESS_EVENTS_ENV,
            Self::ExtensionRoot => EXTENSION_ROOT_ENV,
            Self::ScriptedProvider => SCRIPTED_PROVIDER_ENV,
        }
    }
}

impl fmt::Display for ArtifactRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Node => "node",
            Self::PiCli => "pi CLI",
            Self::Iii => "iii",
            Self::HarnessEvents => "harness-events",
            Self::ExtensionRoot => "staged extension",
            Self::ScriptedProvider => "scripted provider",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactInputs {
    pub node: Option<PathBuf>,
    pub pi_cli: Option<PathBuf>,
    pub iii: Option<PathBuf>,
    pub harness_events: Option<PathBuf>,
    pub extension_root: Option<PathBuf>,
    pub scripted_provider: Option<PathBuf>,
}

impl ArtifactInputs {
    pub fn from_environment(environment: &impl ArtifactEnvironment) -> Self {
        Self {
            node: environment.get(NODE_ENV).map(PathBuf::from),
            pi_cli: environment.get(PI_CLI_ENV).map(PathBuf::from),
            iii: environment.get(III_ENV).map(PathBuf::from),
            harness_events: environment.get(HARNESS_EVENTS_ENV).map(PathBuf::from),
            extension_root: environment.get(EXTENSION_ROOT_ENV).map(PathBuf::from),
            scripted_provider: environment.get(SCRIPTED_PROVIDER_ENV).map(PathBuf::from),
        }
    }

    pub fn from_process_environment() -> Self {
        Self::from_environment(&ProcessEnvironment)
    }
}

pub trait ArtifactEnvironment {
    fn get(&self, key: &str) -> Option<OsString>;
}

pub struct ProcessEnvironment;

impl ArtifactEnvironment for ProcessEnvironment {
    fn get(&self, key: &str) -> Option<OsString> {
        env::var_os(key)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ArtifactError {
    #[error("missing {0} artifact")]
    Missing(ArtifactRole),
    #[error("empty {0} artifact input")]
    Empty(ArtifactRole),
    #[error("wrong artifact type for {0}")]
    WrongType(ArtifactRole),
    #[error("invalid extension manifest")]
    InvalidExtensionManifest,
    #[error("wrong extension package identity")]
    WrongExtensionPackage,
    #[error("wrong extension entrypoint")]
    WrongExtensionEntrypoint,
    #[error("missing extension source entrypoint")]
    MissingExtensionEntrypoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPaths {
    pub node: PathBuf,
    pub pi_cli: PathBuf,
    pub iii: PathBuf,
    pub harness_events: PathBuf,
    pub extension_root: PathBuf,
    pub scripted_provider: PathBuf,
}

impl ArtifactPaths {
    pub fn from_environment() -> Result<Self, ArtifactError> {
        Self::from_inputs(&ArtifactInputs::from_process_environment())
    }

    pub fn from_inputs(inputs: &ArtifactInputs) -> Result<Self, ArtifactError> {
        let node = required_file(&inputs.node, ArtifactRole::Node)?;
        let pi_cli = required_file(&inputs.pi_cli, ArtifactRole::PiCli)?;
        let iii = required_file(&inputs.iii, ArtifactRole::Iii)?;
        let harness_events = required_file(&inputs.harness_events, ArtifactRole::HarnessEvents)?;
        let extension_root =
            required_directory(&inputs.extension_root, ArtifactRole::ExtensionRoot)?;
        validate_extension_root(&extension_root)?;
        let scripted_provider =
            required_file(&inputs.scripted_provider, ArtifactRole::ScriptedProvider)?;

        Ok(Self {
            node,
            pi_cli,
            iii,
            harness_events,
            extension_root,
            scripted_provider,
        })
    }
}

fn required_file(input: &Option<PathBuf>, role: ArtifactRole) -> Result<PathBuf, ArtifactError> {
    let path = required_input(input, role)?;
    if path.is_file() {
        return Ok(path);
    }

    if path.exists() {
        Err(ArtifactError::WrongType(role))
    } else {
        Err(ArtifactError::Missing(role))
    }
}

fn required_directory(
    input: &Option<PathBuf>,
    role: ArtifactRole,
) -> Result<PathBuf, ArtifactError> {
    let path = required_input(input, role)?;
    if path.is_dir() {
        return Ok(path);
    }

    if path.exists() {
        Err(ArtifactError::WrongType(role))
    } else {
        Err(ArtifactError::Missing(role))
    }
}

fn required_input(input: &Option<PathBuf>, role: ArtifactRole) -> Result<PathBuf, ArtifactError> {
    let path = input.clone().ok_or(ArtifactError::Missing(role))?;
    if path.as_os_str().is_empty() {
        Err(ArtifactError::Empty(role))
    } else {
        Ok(path)
    }
}

fn validate_extension_root(root: &Path) -> Result<(), ArtifactError> {
    let manifest = fs::read_to_string(root.join("package.json"))
        .ok()
        .and_then(|contents| serde_json::from_str::<Value>(&contents).ok())
        .ok_or(ArtifactError::InvalidExtensionManifest)?;

    if manifest.get("name").and_then(Value::as_str) != Some(EXTENSION_PACKAGE_NAME) {
        return Err(ArtifactError::WrongExtensionPackage);
    }

    let Some(extensions) = manifest
        .get("pi")
        .and_then(Value::as_object)
        .and_then(|pi| pi.get("extensions"))
        .and_then(Value::as_array)
    else {
        return Err(ArtifactError::WrongExtensionEntrypoint);
    };
    if extensions.len() != 1 || extensions[0].as_str() != Some(EXTENSION_ENTRYPOINT) {
        return Err(ArtifactError::WrongExtensionEntrypoint);
    }

    if root.join("src/index.ts").is_file() {
        Ok(())
    } else {
        Err(ArtifactError::MissingExtensionEntrypoint)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{
        ArtifactEnvironment, ArtifactError, ArtifactInputs, ArtifactPaths, ArtifactRole,
        EXTENSION_ROOT_ENV, HARNESS_EVENTS_ENV, III_ENV, NODE_ENV, PI_CLI_ENV,
        SCRIPTED_PROVIDER_ENV,
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    struct TemporaryDirectory {
        path: PathBuf,
    }

    impl TemporaryDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "pi-harness-events-e2e-artifacts-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct ArtifactFixture {
        directory: TemporaryDirectory,
        inputs: ArtifactInputs,
    }

    impl ArtifactFixture {
        fn new() -> Self {
            let directory = TemporaryDirectory::new();
            let node = write_file(directory.path().join("node"));
            let pi_cli = write_file(directory.path().join("pi-cli.mjs"));
            let iii = write_file(directory.path().join("iii"));
            let harness_events = write_file(directory.path().join("harness-events"));
            let scripted_provider = write_file(directory.path().join("scripted-provider.ts"));
            let extension_root = write_extension_root(directory.path().join("extension"));

            Self {
                directory,
                inputs: ArtifactInputs {
                    node: Some(node),
                    pi_cli: Some(pi_cli),
                    iii: Some(iii),
                    harness_events: Some(harness_events),
                    extension_root: Some(extension_root),
                    scripted_provider: Some(scripted_provider),
                },
            }
        }
    }

    struct TestEnvironment(BTreeMap<String, OsString>);

    impl ArtifactEnvironment for TestEnvironment {
        fn get(&self, key: &str) -> Option<OsString> {
            self.0.get(key).cloned()
        }
    }

    #[test]
    fn reads_artifacts_from_an_injected_environment() {
        let environment = TestEnvironment(BTreeMap::from([
            (NODE_ENV.to_owned(), OsString::from("node")),
            (PI_CLI_ENV.to_owned(), OsString::from("pi-cli.mjs")),
            (III_ENV.to_owned(), OsString::from("iii")),
            (
                HARNESS_EVENTS_ENV.to_owned(),
                OsString::from("harness-events"),
            ),
            (EXTENSION_ROOT_ENV.to_owned(), OsString::from("extension")),
            (
                SCRIPTED_PROVIDER_ENV.to_owned(),
                OsString::from("scripted-provider.ts"),
            ),
        ]));

        assert_eq!(
            ArtifactInputs::from_environment(&environment),
            ArtifactInputs {
                node: Some(PathBuf::from("node")),
                pi_cli: Some(PathBuf::from("pi-cli.mjs")),
                iii: Some(PathBuf::from("iii")),
                harness_events: Some(PathBuf::from("harness-events")),
                extension_root: Some(PathBuf::from("extension")),
                scripted_provider: Some(PathBuf::from("scripted-provider.ts")),
            }
        );
    }

    #[test]
    fn rejects_every_missing_artifact_input() {
        let actual = ArtifactRole::ALL.map(|role| {
            let mut fixture = ArtifactFixture::new();
            set_input(&mut fixture.inputs, role, None);
            ArtifactPaths::from_inputs(&fixture.inputs).err()
        });

        assert_eq!(
            actual,
            ArtifactRole::ALL.map(|role| Some(ArtifactError::Missing(role)))
        );
    }

    #[test]
    fn rejects_every_empty_artifact_input() {
        let actual = ArtifactRole::ALL.map(|role| {
            let mut fixture = ArtifactFixture::new();
            set_input(&mut fixture.inputs, role, Some(PathBuf::new()));
            ArtifactPaths::from_inputs(&fixture.inputs).err()
        });

        assert_eq!(
            actual,
            ArtifactRole::ALL.map(|role| Some(ArtifactError::Empty(role)))
        );
    }

    #[test]
    fn rejects_every_missing_artifact_path() {
        let actual = ArtifactRole::ALL.map(|role| {
            let mut fixture = ArtifactFixture::new();
            let missing = fixture.directory.path().join(format!("missing-{role}"));
            set_input(&mut fixture.inputs, role, Some(missing));
            ArtifactPaths::from_inputs(&fixture.inputs).err()
        });

        assert_eq!(
            actual,
            ArtifactRole::ALL.map(|role| Some(ArtifactError::Missing(role)))
        );
    }

    #[test]
    fn rejects_every_wrong_artifact_type() {
        let actual = ArtifactRole::ALL.map(|role| {
            let mut fixture = ArtifactFixture::new();
            let wrong_type = fixture.directory.path().join(format!("wrong-{role}"));
            if role == ArtifactRole::ExtensionRoot {
                write_file(wrong_type.clone());
            } else {
                fs::create_dir(&wrong_type).unwrap();
            }
            set_input(&mut fixture.inputs, role, Some(wrong_type));
            ArtifactPaths::from_inputs(&fixture.inputs).err()
        });

        assert_eq!(
            actual,
            ArtifactRole::ALL.map(|role| Some(ArtifactError::WrongType(role)))
        );
    }

    #[test]
    fn rejects_invalid_extension_manifest() {
        for manifest in [None, Some("not JSON")] {
            let fixture = ArtifactFixture::new();
            let manifest_path = fixture
                .inputs
                .extension_root
                .as_ref()
                .unwrap()
                .join("package.json");
            match manifest {
                Some(contents) => fs::write(manifest_path, contents).unwrap(),
                None => fs::remove_file(manifest_path).unwrap(),
            }

            assert_eq!(
                ArtifactPaths::from_inputs(&fixture.inputs).unwrap_err(),
                ArtifactError::InvalidExtensionManifest
            );
        }
    }

    #[test]
    fn rejects_a_staged_extension_with_the_wrong_package_identity() {
        let fixture = ArtifactFixture::new();
        write_manifest(
            fixture.inputs.extension_root.as_ref().unwrap(),
            "@dwsr/wrong-package",
            "./src/index.ts",
        );

        assert_eq!(
            ArtifactPaths::from_inputs(&fixture.inputs).unwrap_err(),
            ArtifactError::WrongExtensionPackage
        );
    }

    #[test]
    fn rejects_a_staged_extension_with_the_wrong_entrypoint() {
        for entrypoints in [
            r#"["./src/wrong.ts"]"#,
            r#"["./src/index.ts", "./src/extra.ts"]"#,
        ] {
            let fixture = ArtifactFixture::new();
            fs::write(
                fixture
                    .inputs
                    .extension_root
                    .as_ref()
                    .unwrap()
                    .join("package.json"),
                format!(
                    r#"{{"name":"@dwsr/pi-harness-events","pi":{{"extensions":{entrypoints}}}}}"#
                ),
            )
            .unwrap();

            assert_eq!(
                ArtifactPaths::from_inputs(&fixture.inputs).unwrap_err(),
                ArtifactError::WrongExtensionEntrypoint
            );
        }
    }

    #[test]
    fn rejects_a_staged_extension_without_its_installed_source_entrypoint() {
        let fixture = ArtifactFixture::new();
        fs::remove_file(
            fixture
                .inputs
                .extension_root
                .as_ref()
                .unwrap()
                .join("src/index.ts"),
        )
        .unwrap();

        assert_eq!(
            ArtifactPaths::from_inputs(&fixture.inputs).unwrap_err(),
            ArtifactError::MissingExtensionEntrypoint
        );
    }

    #[test]
    fn accepts_a_complete_artifact_contract() {
        let fixture = ArtifactFixture::new();

        assert!(ArtifactPaths::from_inputs(&fixture.inputs).is_ok());
    }

    #[test]
    fn errors_are_bounded_and_do_not_expose_input_paths() {
        let mut fixture = ArtifactFixture::new();
        let sentinel = "artifact-path-private-sentinel";
        let missing = fixture.directory.path().join(sentinel);
        set_input(&mut fixture.inputs, ArtifactRole::Node, Some(missing));

        let error = ArtifactPaths::from_inputs(&fixture.inputs).unwrap_err();

        assert_eq!(error, ArtifactError::Missing(ArtifactRole::Node));
        assert!(!error.to_string().contains(sentinel));
        assert!(!format!("{error:?}").contains(sentinel));
    }

    fn write_file(path: PathBuf) -> PathBuf {
        fs::write(&path, "fixture").unwrap();
        path
    }

    fn write_extension_root(path: PathBuf) -> PathBuf {
        fs::create_dir_all(path.join("src")).unwrap();
        write_manifest(&path, "@dwsr/pi-harness-events", "./src/index.ts");
        write_file(path.join("src/index.ts"));
        path
    }

    fn write_manifest(root: &Path, name: &str, entrypoint: &str) {
        fs::write(
            root.join("package.json"),
            format!(r#"{{"name":"{name}","pi":{{"extensions":["{entrypoint}"]}}}}"#),
        )
        .unwrap();
    }

    fn set_input(inputs: &mut ArtifactInputs, role: ArtifactRole, value: Option<PathBuf>) {
        match role {
            ArtifactRole::Node => inputs.node = value,
            ArtifactRole::PiCli => inputs.pi_cli = value,
            ArtifactRole::Iii => inputs.iii = value,
            ArtifactRole::HarnessEvents => inputs.harness_events = value,
            ArtifactRole::ExtensionRoot => inputs.extension_root = value,
            ArtifactRole::ScriptedProvider => inputs.scripted_provider = value,
        }
    }
}
