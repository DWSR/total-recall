use std::process::Command;

#[cfg(unix)]
use std::{
    io::ErrorKind,
    net::TcpListener,
    process::{Child, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use session_post_processing::config::{
    III_URL_ENV, TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV, TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV,
};
#[cfg(unix)]
use session_post_processing::config::{
    TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV, TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
    TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
    TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV,
};

const TOKEN_SENTINEL: &str = "process-provider-token-secret-sentinel";
const ENGINE_SENTINEL: &str = "process-engine-url-secret-sentinel";
#[cfg(unix)]
const SOURCE_SESSION_SENTINEL: &str = "process-session-content-secret-sentinel";
#[cfg(unix)]
const MEMORY_SENTINEL: &str = "process-memory-content-secret-sentinel";

#[test]
fn invalid_configuration_exits_before_readiness_without_secret_diagnostics() {
    let output = Command::new(env!("CARGO_BIN_EXE_session-post-processing"))
        .env_clear()
        .env(TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV, "openai")
        .env(
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
            TOKEN_SENTINEL,
        )
        .env(
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
            "process-model",
        )
        .env(III_URL_ENV, ENGINE_SENTINEL)
        .output()
        .expect("worker executable should start");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr,
        "worker startup configuration failed: invalid configuration: TOTAL_RECALL_POST_PROCESSING_DATABASE (missing)\n"
    );
    for protected in [TOKEN_SENTINEL, ENGINE_SENTINEL, "worker ready"] {
        assert!(
            !stderr.contains(protected),
            "invalid startup diagnostics leaked protected content: {stderr}"
        );
    }
}

#[cfg(unix)]
#[test]
fn sigterm_during_stalled_iii_startup_honors_drain_deadline_and_redacts_diagnostics() {
    let (engine_url, startup_attempted, listener_stop, listener) = start_stalled_listener();
    let mut child = valid_worker_command(&engine_url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("worker executable should start");

    if startup_attempted
        .recv_timeout(Duration::from_secs(3))
        .is_err()
    {
        reap_after_failed_assertion(&mut child);
        stop_listener(&listener_stop, listener);
        panic!("worker did not attempt the stalled iii connection");
    }

    let shutdown_started = Instant::now();
    send_sigterm(&child);
    let output = match wait_for_exit(&mut child, Duration::from_millis(1500)) {
        Some(_) => child
            .wait_with_output()
            .expect("worker output should be available after normal exit"),
        None => {
            let elapsed = shutdown_started.elapsed();
            reap_after_failed_assertion(&mut child);
            stop_listener(&listener_stop, listener);
            panic!(
                "worker did not exit within the configured 1 second shutdown drain: {elapsed:?}"
            );
        }
    };
    stop_listener(&listener_stop, listener);

    assert!(
        shutdown_started.elapsed() <= Duration::from_millis(1500),
        "worker shutdown exceeded the bounded observation window"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "worker should handle SIGTERM and exit through its startup failure path"
    );
    assert!(
        output.stdout.is_empty(),
        "worker lifecycle diagnostics must not write to stdout"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr,
        "worker termination signal received during startup readiness\n"
    );
    for protected in [
        TOKEN_SENTINEL,
        SOURCE_SESSION_SENTINEL,
        MEMORY_SENTINEL,
        ENGINE_SENTINEL,
        engine_url.as_str(),
        "iii-connection",
    ] {
        assert!(
            !stderr.contains(protected),
            "signal shutdown diagnostics leaked protected content: {stderr}"
        );
    }
}

#[cfg(unix)]
fn valid_worker_command(engine_url: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_session-post-processing"));
    command
        .env_clear()
        .env(
            TOTAL_RECALL_POST_PROCESSING_DATABASE_ENV,
            SOURCE_SESSION_SENTINEL,
        )
        .env(
            TOTAL_RECALL_POST_PROCESSING_MEMORY_DATABASE_ENV,
            MEMORY_SENTINEL,
        )
        .env(TOTAL_RECALL_POST_PROCESSING_PROVIDER_ENV, "openai")
        .env(
            TOTAL_RECALL_POST_PROCESSING_OPENAI_BASE_URL_ENV,
            "http://127.0.0.1:9/provider-content-secret-sentinel/",
        )
        .env(
            TOTAL_RECALL_POST_PROCESSING_OPENAI_API_TOKEN_ENV,
            TOKEN_SENTINEL,
        )
        .env(
            TOTAL_RECALL_POST_PROCESSING_OPENAI_MODEL_ENV,
            "process-model",
        )
        .env(TOTAL_RECALL_POST_PROCESSING_SHUTDOWN_DRAIN_SECONDS_ENV, "1")
        .env(III_URL_ENV, engine_url);
    command
}

#[cfg(unix)]
fn start_stalled_listener() -> (String, Receiver<()>, Arc<AtomicBool>, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .expect("stalled iii listener should bind a loopback port");
    listener
        .set_nonblocking(true)
        .expect("stalled iii listener should support nonblocking accept");
    let engine_url = format!(
        "ws://{}/{ENGINE_SENTINEL}",
        listener
            .local_addr()
            .expect("stalled iii listener should expose its local address")
    );
    let (startup_sender, startup_attempted) = mpsc::sync_channel(1);
    let stop = Arc::new(AtomicBool::new(false));
    let listener_stop = Arc::clone(&stop);
    let listener = thread::spawn(move || {
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if listener_stop.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        };
        let _ = startup_sender.send(());
        while !listener_stop.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(10));
        }
        drop(stream);
    });

    (engine_url, startup_attempted, stop, listener)
}

#[cfg(unix)]
fn send_sigterm(child: &Child) {
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("signal command should run");
    assert!(status.success(), "signal command should target the worker");
}

#[cfg(unix)]
fn wait_for_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match child
            .try_wait()
            .expect("worker status should be readable during shutdown")
        {
            Some(status) => return Some(status),
            None if Instant::now() >= deadline => return None,
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}

#[cfg(unix)]
fn reap_after_failed_assertion(child: &mut Child) {
    if child
        .try_wait()
        .expect("worker status should be readable during cleanup")
        .is_none()
    {
        child
            .kill()
            .expect("timed-out worker should be terminable during test cleanup");
    }
    child
        .wait()
        .expect("timed-out worker should be reaped during test cleanup");
}

#[cfg(unix)]
fn stop_listener(stop: &AtomicBool, listener: JoinHandle<()>) {
    stop.store(true, Ordering::SeqCst);
    listener
        .join()
        .expect("stalled iii listener should not panic");
}
