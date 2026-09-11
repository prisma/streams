#![cfg(test)]

mod synchronization {
    pub(super) use loom::sync::Mutex;
    pub(super) use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
}

#[path = "../src/bin/pilot/generator/membership.rs"]
mod membership;

#[test]
fn quality_loom_final_membership_acquires_all_accounting_and_prevents_late_work() {
    use loom::sync::Arc;
    use synchronization::{AtomicU64, Ordering};
    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.check(|| {
        let members = Arc::new(membership::Membership::new());
        let accepted = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let members = members.clone();
            let accepted = accepted.clone();
            let completed = completed.clone();
            workers.push(loom::thread::spawn(move || {
                account_once(&members, &accepted, &completed);
            }));
        }
        members.close();
        let (closed, active) = members.snapshot();
        assert!(closed);
        let observed_completed = completed.load(Ordering::Relaxed);
        if active == 0 {
            assert_eq!(
                accepted.load(Ordering::Relaxed),
                completed.load(Ordering::Relaxed)
            );
            assert!(!members.reserve());
        }
        for worker in workers {
            worker.join().unwrap();
        }
        if active == 0 {
            assert_eq!(
                completed.load(Ordering::Relaxed),
                observed_completed,
                "final snapshots prohibit later accounting"
            );
        }
        assert_eq!(members.snapshot(), (true, 0));
        assert_eq!(
            accepted.load(Ordering::Relaxed),
            completed.load(Ordering::Relaxed)
        );
        assert!(!members.reserve());
    });
}

fn account_once(
    members: &membership::Membership,
    accepted: &synchronization::AtomicU64,
    completed: &synchronization::AtomicU64,
) {
    if !members.reserve() {
        return;
    }
    accepted.fetch_add(1, synchronization::Ordering::Relaxed);
    completed.fetch_add(1, synchronization::Ordering::Relaxed);
    members.release();
}

#[test]
fn generator_cli_rejects_missing_credentials_before_starting_work() {
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_pilot"))
        .env_clear()
        .env("MODE", "gen")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("AUTH_TOKEN required"));
}
