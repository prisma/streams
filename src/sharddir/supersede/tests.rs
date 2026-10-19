//! The supersession's decisions (#179): the verdict of a read of the boot
//! record, the bounded read, registration and its retries, the wake that only
//! an engine death sends, and a yielded directory's refusal and readiness.
#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt};

use super::super::open_gate_tests::open_engine;
use super::{
    BOOT_RECORD, BootLineage, Checker, MARK_WAIT_MS, RECORD_BOUND, REPLACED_RECORD, SUPERSEDED,
    Verdict, mark_decision, start, verdict,
};
use crate::shard_directory::{
    Adoption, OpenTiming, ResolveError, RetireOutcome, RetirementReason, ShardDirectory,
};
use crate::sharddir::{EngineIncarnation, OpenFn};
use crate::tasks::{TaskOutcome, TaskSupervisor};

const PREFIX: &str = "0";

fn record(boot: &str) -> bytes::Bytes {
    bytes::Bytes::from(format!(r#"{{"bootId":"{boot}","atMs":1700000000000}}"#))
}

/// A directory over one prefix whose opener opens a real engine and counts.
fn directory() -> (ShardDirectory, Arc<AtomicUsize>) {
    let opens = Arc::new(AtomicUsize::new(0));
    let counted = opens.clone();
    let opener: OpenFn = Box::new(move |prefix: String, _incarnation: EngineIncarnation| {
        counted.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(open_engine(&prefix).await) })
    });
    let dir = ShardDirectory::new(
        vec![PREFIX.to_string()],
        crate::ownership::OwnershipService::new(""),
        OpenTiming {
            open_deadline: Duration::from_secs(60),
            open_wait: Duration::from_secs(30),
        },
        |_notifier| opener,
    );
    (dir, opens)
}

fn checker(dir: &ShardDirectory, store: &Arc<dyn ObjectStore>, own: &str) -> Checker {
    Checker {
        supersession: dir.supersession(),
        store: store.clone(),
        own: own.to_string(),
        at_ms: 1_700_000_000_000,
    }
}

/// The boot the record names, read by its literal key.
async fn named(store: &Arc<dyn ObjectStore>) -> Option<String> {
    let found = store
        .get(&object_store::path::Path::from(BOOT_RECORD))
        .await
        .ok()?;
    let body = found.bytes().await.ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&body).ok()?;
    doc["bootId"].as_str().map(str::to_string)
}

/// Only a record that names another boot supersedes; one that names this
/// boot is the newest, and anything unread decides nothing.
#[test]
fn only_a_record_naming_another_boot_supersedes() {
    let not_found = object_store::Error::NotFound {
        path: BOOT_RECORD.into(),
        source: "absent".into(),
    };
    let failed = object_store::Error::Generic {
        store: "rig",
        source: "unavailable".into(),
    };
    let cases = [
        (Some(Ok(record("a"))), Verdict::Newest),
        (
            Some(Ok(record("b"))),
            Verdict::Superseded {
                newest: "b".to_string(),
            },
        ),
        (Some(Ok(bytes::Bytes::from_static(b"{"))), Verdict::Unknown),
        (
            Some(Ok(bytes::Bytes::from_static(br#"{"atMs":1}"#))),
            Verdict::Unknown,
        ),
        (Some(Err(not_found)), Verdict::Unknown),
        (Some(Err(failed)), Verdict::Unknown),
        (None, Verdict::Unknown),
    ];
    for (read, want) in cases {
        let shown = format!("{read:?}");
        assert_eq!(verdict("a", read), want, "{shown}");
    }
}

/// A record read slower than its bound answers nothing at the bound: the
/// verdict is in before the 3 s base holdoff could let an open through.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_read_slower_than_its_bound_answers_nothing_at_the_bound() {
    let started = tokio::time::Instant::now();
    assert_eq!(super::bounded(std::future::pending::<()>()).await, None);
    assert_eq!(started.elapsed(), RECORD_BOUND);
    assert!(RECORD_BOUND < super::super::HOLDOFF_BASE);
    assert_eq!(super::bounded(async { 7 }).await, Some(7));
}

/// Each boot's registration is read back at once: the first boot is the
/// newest until the second registers, then the second is and the first is
/// superseded by it. Nothing registered reads as unknown.
#[tokio::test]
async fn the_last_registered_boot_is_the_newest() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (dir_a, _) = directory();
    let (dir_b, _) = directory();
    let (a, b) = (
        checker(&dir_a, &store, "boot-a"),
        checker(&dir_b, &store, "boot-b"),
    );
    let tasks = TaskSupervisor::new();
    let cancel = tasks.cancellation();
    assert_eq!(a.check().await, Verdict::Unknown, "nothing registered yet");
    assert!(a.register(&cancel).await);
    assert_eq!(named(&store).await.as_deref(), Some("boot-a"));
    assert_eq!(a.check().await, Verdict::Newest);
    assert!(b.register(&cancel).await);
    assert_eq!(named(&store).await.as_deref(), Some("boot-b"));
    assert_eq!(
        a.check().await,
        Verdict::Superseded {
            newest: "boot-b".to_string()
        }
    );
    assert_eq!(b.check().await, Verdict::Newest);
}

/// A boot whose record cannot be written retries after 1, 2, 4 and 8 s and
/// then gives up, never yielding; a stop during a retry ends it at once.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_registration_that_keeps_failing_gives_up_after_its_retries() {
    let failing: Arc<dyn ObjectStore> = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        7,
        crate::dst::FaultPlan {
            error_pct: 100,
            ..crate::dst::FaultPlan::CLEAN
        },
    );
    let (dir, _) = directory();
    let unregistered = checker(&dir, &failing, "boot-a");
    let tasks = TaskSupervisor::new();
    let started = tokio::time::Instant::now();
    assert!(!unregistered.register(&tasks.cancellation()).await);
    assert_eq!(started.elapsed(), Duration::from_secs(15));
    let stopping = TaskSupervisor::new();
    stopping.cancel();
    let started = tokio::time::Instant::now();
    assert!(!unregistered.register(&stopping.cancellation()).await);
    assert_eq!(started.elapsed(), Duration::ZERO, "a stop ends the retries");
}

/// Only an engine that died wakes the checker: a retirement does not, and
/// neither does a late close of an engine already gone.
#[tokio::test]
async fn only_an_engine_that_died_wakes_the_checker() {
    let (dir, _) = directory();
    let supersession = dir.supersession();
    let hash = [0u8; 16];
    dir.resolve(&hash, Adoption::External).await.unwrap();
    assert!(matches!(
        dir.retire(PREFIX, RetirementReason::SweepEviction, |_, _| true),
        RetireOutcome::Retired(_)
    ));
    assert!(
        !supersession.woken_within(Duration::from_millis(50)).await,
        "a retirement is no engine death"
    );
    dir.clear_holdoff(PREFIX);
    dir.resolve(&hash, Adoption::External).await.unwrap();
    let incarnation = dir.resident_incarnation(PREFIX).unwrap();
    assert!(dir.close_notifier().closed(PREFIX, incarnation));
    assert!(supersession.woken_within(Duration::from_secs(5)).await);
    assert!(!dir.close_notifier().closed(PREFIX, incarnation));
    assert!(
        !supersession.woken_within(Duration::from_millis(50)).await,
        "a late close of an evicted engine is no second death"
    );
}

/// A yielded directory opens nothing: a prefix without a resident answers
/// `shard_closing` with `Retry-After: 1` and its opener is never called, its
/// resident keeps serving, and readiness names the supersession.
#[tokio::test]
async fn a_yielded_directory_refuses_every_open_and_reports_superseded() {
    let (dir, opens) = directory();
    assert_eq!(dir.unready_reason(), None);
    let served = dir.resolve(&[0u8; 16], Adoption::External).await.unwrap();
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    dir.supersession().yield_to_newer();
    assert_eq!(dir.unready_reason().as_deref(), Some(SUPERSEDED));
    let still = dir.resolve(&[0u8; 16], Adoption::External).await.unwrap();
    assert!(Arc::ptr_eq(&served, &still), "the resident keeps serving");
    assert!(matches!(
        dir.retire(PREFIX, RetirementReason::SweepEviction, |_, _| true),
        RetireOutcome::Retired(_)
    ));
    dir.clear_holdoff(PREFIX);
    let refused = dir.resolve(&[0u8; 16], Adoption::External).await;
    assert_eq!(
        refused.err(),
        Some(ResolveError::Opening {
            prefix: PREFIX.to_string(),
            code: "shard_closing",
            retry_after_secs: 1,
        })
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1, "no open after the yield");
}

/// Polls `ready` every 10 ms for up to 5 s.
async fn eventually(mut ready: impl FnMut() -> bool) -> bool {
    for _ in 0..500 {
        if ready() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ready()
}

/// Whether the record names `own` within 5 s.
async fn registered_as(store: &Arc<dyn ObjectStore>, own: &str) -> bool {
    for _ in 0..500 {
        if named(store).await.as_deref() == Some(own) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

fn runtime(seed: u64) -> crate::runtime::RuntimeCaps {
    crate::runtime::RuntimeCaps::with_sources(
        Arc::new(crate::runtime::ManualClock::at(1_700_000_000_000)),
        &crate::runtime::SeededEntropy::domain(seed, "runtime-identity"),
        Arc::new(crate::runtime::SeededEntropy::domain(seed, "stream-epoch")),
        "supersede-rig",
    )
}

/// Two boots of one cell, started as bootstrap starts them with fleet off:
/// each registers; the older yields at its engine's death, the newer does not
/// at its own; both checkers end with the stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_older_boot_yields_at_an_engine_death_and_the_newest_does_not() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut boots = HashMap::new();
    for seed in [1, 2] {
        let (dir, _) = directory();
        let (caps, tasks) = (runtime(seed), TaskSupervisor::new());
        assert!(!start(&dir, &store, &caps, &tasks), "no fleet loop started");
        let own = caps.identity.boot_id.clone();
        assert!(registered_as(&store, &own).await, "boot {seed} registered");
        boots.insert(seed, (dir, tasks));
    }
    let (older, newer) = (&boots[&1].0, &boots[&2].0);
    newer.supersession().engine_died();
    older.supersession().engine_died();
    assert!(eventually(|| older.unready_reason().is_some()).await);
    assert_eq!(older.unready_reason().as_deref(), Some(SUPERSEDED));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(newer.unready_reason(), None, "the newest never yields");
    for (_, tasks) in boots.values() {
        let report = tasks.shutdown(Duration::from_secs(5)).await;
        assert_eq!(
            report.outcomes,
            vec![("supersession", TaskOutcome::Finished)]
        );
    }
}

/// The mark decides a predecessor superseded only when it names that boot; one
/// that names another, or none, is undecided until the wait passes and then
/// a no. A mark found after the wait is still a yes.
#[test]
fn the_mark_decides_yes_at_once_and_no_only_after_the_wait() {
    let failed = || object_store::Error::Generic {
        store: "rig",
        source: "unavailable".into(),
    };
    let cases = [
        (Some(Ok(record("a"))), 0, Some(true)),
        (Some(Ok(record("a"))), MARK_WAIT_MS, Some(true)),
        (Some(Ok(record("b"))), 0, None),
        (Some(Ok(record("b"))), MARK_WAIT_MS - 1, None),
        (Some(Ok(record("b"))), MARK_WAIT_MS, Some(false)),
        (Some(Err(failed())), MARK_WAIT_MS - 1, None),
        (Some(Err(failed())), MARK_WAIT_MS, Some(false)),
        (None, MARK_WAIT_MS - 1, None),
        (None, MARK_WAIT_MS, Some(false)),
    ];
    for (read, waited, want) in cases {
        let shown = format!("{read:?} after {waited} ms");
        assert_eq!(mark_decision("a", read, waited), want, "{shown}");
    }
}

/// The record a boot reads before it writes its own is its predecessor; the
/// first boot of a store has none, and a boot never names itself.
#[tokio::test]
async fn a_registering_boot_learns_the_boot_the_record_named() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let tasks = TaskSupervisor::new();
    let cancel = tasks.cancellation();
    let (dir_a, _) = directory();
    let (dir_b, _) = directory();
    let (a, b) = (
        checker(&dir_a, &store, "boot-a"),
        checker(&dir_b, &store, "boot-b"),
    );
    let none = BootLineage {
        predecessor: None,
        predecessor_superseded: None,
    };
    assert_eq!(dir_a.supersession().lineage(&store, 0).await, none);
    assert!(a.register(&cancel).await);
    assert_eq!(dir_a.supersession().lineage(&store, 0).await, none);
    assert!(b.register(&cancel).await);
    let supersession = dir_b.supersession();
    let at = |ms| supersession.lineage(&store, ms);
    let undecided = BootLineage {
        predecessor: Some("boot-a".to_string()),
        predecessor_superseded: None,
    };
    assert_eq!(at(1_700_000_000_000).await, undecided);
    let gone = BootLineage {
        predecessor: Some("boot-a".to_string()),
        predecessor_superseded: Some(false),
    };
    assert_eq!(at(1_700_000_000_000 + MARK_WAIT_MS).await, gone);
}

/// A replaced boot's mark names it, written once a newer boot is registered
/// and not before; the next boot's answer is a yes, and a decided answer is
/// kept without another read.
#[tokio::test]
async fn a_replaced_boot_marks_itself_and_the_next_boot_keeps_the_answer() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let tasks = TaskSupervisor::new();
    let cancel = tasks.cancellation();
    let (dir_a, _) = directory();
    let (dir_b, _) = directory();
    let (a, b) = (
        checker(&dir_a, &store, "boot-a"),
        checker(&dir_b, &store, "boot-b"),
    );
    let mark = || async {
        let found = store
            .get(&object_store::path::Path::from(REPLACED_RECORD))
            .await
            .ok()?;
        let doc: serde_json::Value = serde_json::from_slice(&found.bytes().await.ok()?).ok()?;
        doc["bootId"].as_str().map(str::to_string)
    };
    assert!(a.register(&cancel).await);
    a.mark_if_replaced().await;
    assert_eq!(mark().await, None, "the newest marks nothing");
    assert!(b.register(&cancel).await);
    b.mark_if_replaced().await;
    assert_eq!(mark().await, None, "the newest marks nothing");
    a.mark_if_replaced().await;
    assert_eq!(mark().await.as_deref(), Some("boot-a"));
    let yes = BootLineage {
        predecessor: Some("boot-a".to_string()),
        predecessor_superseded: Some(true),
    };
    assert_eq!(dir_b.supersession().lineage(&store, 0).await, yes);
    store
        .delete(&object_store::path::Path::from(REPLACED_RECORD))
        .await
        .unwrap();
    assert_eq!(dir_b.supersession().lineage(&store, 0).await, yes, "kept");
}

/// An unreadable record leaves nothing to mark: a boot that cannot tell it was
/// replaced writes no mark, as a crashed one wrote none.
#[tokio::test]
async fn an_unread_record_leaves_no_mark() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (dir, _) = directory();
    checker(&dir, &store, "boot-a").mark_if_replaced().await;
    assert!(
        store
            .get(&object_store::path::Path::from(REPLACED_RECORD))
            .await
            .is_err()
    );
}
