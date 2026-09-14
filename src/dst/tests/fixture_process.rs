#![cfg(test)]
//! One owner for isolated subprocess selection, evidence, deadlines and reaping.

use crate::config::{Environment, ProcessEnvironment};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const PLAN_ENV: &str = "STREAMS_RELIABILITY_CHILD";
const EVIDENCE_ENV: &str = "STREAMS_RELIABILITY_EVIDENCE";
pub(super) const CRASH_EXIT: i32 = 73;

pub(super) fn child_plan<T: DeserializeOwned>() -> Option<T> {
    ProcessEnvironment
        .get(PLAN_ENV)
        .map(|raw| serde_json::from_str(&raw).unwrap())
}

pub(super) fn witness(plan: &impl Serialize) {
    let evidence = ProcessEnvironment.get(EVIDENCE_ENV).unwrap();
    std::fs::write(
        PathBuf::from(evidence).join("child.witness"),
        serde_json::to_vec(plan).unwrap(),
    )
    .unwrap();
}

/// Exit bypasses Rust destructors and Tokio shutdown, including final DB flushes.
pub(super) fn crash(plan: &impl Serialize) -> ! {
    witness(plan);
    std::process::exit(CRASH_EXIT);
}

/// Every child is reaped. File-backed diagnostics cannot block on a full pipe.
pub(super) async fn run_child(
    evidence: &Path,
    test: &str,
    plan: &impl Serialize,
    expected_exit: i32,
) {
    std::fs::create_dir_all(evidence).unwrap();
    let log_path = evidence.join("child.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let encoded = serde_json::to_string(plan).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env_clear()
        .env(PLAN_ENV, &encoded)
        .env(EVIDENCE_ENV, evidence)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("child timed out; log: {}", log_path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let log = std::fs::read_to_string(log_path).unwrap();
    assert_eq!(
        status.code(),
        Some(expected_exit),
        "evidence at {}:\n{log}",
        evidence.display()
    );
    assert_eq!(
        std::fs::read_to_string(evidence.join("child.witness")).unwrap(),
        encoded,
        "the exact selected child must reach its asserted cut"
    );
}
