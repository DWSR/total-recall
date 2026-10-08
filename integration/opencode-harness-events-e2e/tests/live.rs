use std::path::PathBuf;

use opencode_harness_events_e2e::{
    ArtifactKind, ArtifactPathError, ArtifactPaths, run_compatibility_matrix,
};

fn regular_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
}

fn valid_artifact_paths() -> ArtifactPaths {
    let path = regular_file();
    ArtifactPaths {
        iii: path.clone(),
        opencode_v1: path.clone(),
        opencode_v2: path.clone(),
        harness_events: path.clone(),
        plugin_tarball: path,
    }
}

#[test]
fn artifact_paths_accept_existing_regular_files() {
    assert_eq!(valid_artifact_paths().validate(), Ok(()));
}

#[test]
fn artifact_paths_reject_blank_entries() {
    let paths = ArtifactPaths {
        iii: PathBuf::from("   "),
        opencode_v1: PathBuf::from("opencode-v1"),
        opencode_v2: PathBuf::from("opencode-v2"),
        harness_events: PathBuf::from("harness-events"),
        plugin_tarball: PathBuf::from("plugin.tgz"),
    };

    assert!(paths.validate().is_err());
}

#[test]
fn artifact_paths_reject_missing_files() {
    let mut paths = valid_artifact_paths();
    paths.iii = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("missing-iii-artifact");

    assert!(matches!(
        paths.validate(),
        Err(ArtifactPathError::Unusable {
            artifact: ArtifactKind::Iii,
            ..
        })
    ));
}

#[test]
fn artifact_paths_reject_directories() {
    let mut paths = valid_artifact_paths();
    paths.plugin_tarball = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    assert!(matches!(
        paths.validate(),
        Err(ArtifactPathError::Unusable {
            artifact: ArtifactKind::PluginTarball,
            ..
        })
    ));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicit Bun, iii, OpenCode v1 and v2, CLI, and plugin artifacts"]
async fn real_opencode_serial_compatibility_matrix_is_provider_free() {
    let paths = ArtifactPaths::from_env().expect("all live artifact paths must be explicit");
    run_compatibility_matrix(&paths)
        .await
        .expect("serial v1 and v2 compatibility matrix completes");
}
