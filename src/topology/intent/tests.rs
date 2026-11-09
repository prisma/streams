#![cfg(test)]
//! The intent's store protocol and its clocks (S4-DESIGN §4).

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use super::{Intent, IntentError, LEASE, Lapse, Lease, Outcome, SKEW, State};
use crate::runtime::{Clock, ManualClock};

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

fn split(op_id: &str) -> Intent {
    Intent {
        kind: "split".to_string(),
        op_id: op_id.to_string(),
        sources: vec![("1".to_string(), 1)],
        children: vec![2, 3],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 10_000,
        started_ms: 0,
        outcome: None,
    }
}

fn ended(mut intent: Intent, state: State, result: &str) -> Intent {
    intent.state = state;
    intent.outcome = Some(Outcome {
        result: result.to_string(),
        reason: "test".to_string(),
    });
    intent
}

/// **A live intent refuses a second operation; a terminal one is written over.** The first
/// create lands; a second operation's meets `Busy` naming the live one, and the stored intent
/// is unchanged; once the first is `Reclaimed`, the second's create replaces it with its own
/// `op_id`.
#[tokio::test(crate = "crate::test_rt")]
async fn a_live_intent_refuses_a_second_operation_and_a_terminal_one_is_written_over() {
    let store = store();
    let first = super::create(&*store, &split("op-1")).await.unwrap();
    let refused = super::create(&*store, &split("op-2")).await;
    assert!(
        matches!(&refused, Err(IntentError::Busy(live)) if live.op_id == "op-1"),
        "{refused:?}"
    );
    let stored = super::read(&*store).await.unwrap().unwrap();
    assert_eq!(stored, first, "the live intent is unchanged");
    let done = ended(first.intent.clone(), State::Reclaimed, "published");
    super::replace(&*store, &first, done).await.unwrap();
    let second = super::create(&*store, &split("op-2")).await.unwrap();
    let stored = super::read(&*store).await.unwrap().unwrap();
    assert_eq!(
        (stored.intent.op_id.as_str(), stored.intent.state),
        ("op-2", State::Preparing)
    );
    assert_eq!(stored, second);
}

/// **A write over an intent that moved loses, and a terminal intent stays.** A renewal lands
/// and moves the ETag; a stale actor's terminal write over the old ETag is `Lost` and changes
/// nothing; the current holder's terminal write lands, and the intent is still there, terminal,
/// with its outcome: nothing deletes it.
#[tokio::test(crate = "crate::test_rt")]
async fn a_write_over_a_moved_intent_loses_and_a_terminal_intent_stays() {
    let store = store();
    let held = super::create(&*store, &split("op-1")).await.unwrap();
    let mut renewed = held.intent.clone();
    renewed.lease_until_ms += 3_300;
    let current = super::replace(&*store, &held, renewed).await.unwrap();
    let stale = ended(held.intent.clone(), State::RolledBack, "rolled_back");
    let lost = super::replace(&*store, &held, stale).await;
    assert!(matches!(lost, Err(IntentError::Lost)), "{lost:?}");
    assert_eq!(super::read(&*store).await.unwrap().unwrap(), current);
    let done = ended(current.intent.clone(), State::Reclaimed, "published");
    super::replace(&*store, &current, done.clone())
        .await
        .unwrap();
    let stored = super::read(&*store).await.unwrap().unwrap();
    assert_eq!(stored.intent, done);
    assert!(stored.intent.is_terminal());
}

/// **The lease and the lapse are measured on the observer's own monotonic clock.** An actor
/// owns its intent for less than the lease plus the skew since its last successful renewal. A
/// claimant sees a lapse only once the ETag has stayed the same across that span of its own
/// observations, and an ETag that moved starts the span again; the wall clock never takes
/// part (a jump of an hour changes nothing).
#[test]
fn the_lease_and_the_lapse_are_measured_on_the_observers_monotonic_clock() {
    let clock = ManualClock::at(0);
    let span = LEASE + SKEW;
    let lease = Lease::renewed_at(clock.monotonic());
    let mut lapse = Lapse::first("e1", clock.monotonic());
    clock.jump_wall(3_600_000);
    clock.advance_monotonic(span - Duration::from_millis(1));
    assert!(lease.owned(clock.monotonic()), "owned until the span");
    assert_eq!(
        lapse.observe("e1", clock.monotonic()),
        None,
        "not yet lapsed"
    );
    clock.advance_monotonic(Duration::from_millis(1));
    assert!(!lease.owned(clock.monotonic()), "the span passed");
    assert_eq!(lapse.observe("e1", clock.monotonic()), Some(Duration::ZERO));
    clock.advance_monotonic(Duration::from_secs(4));
    assert_eq!(
        lapse.observe("e1", clock.monotonic()),
        Some(Duration::from_secs(4))
    );
    assert_eq!(
        lapse.observe("e2", clock.monotonic()),
        None,
        "a moved ETag starts again"
    );
    clock.advance_monotonic(span - Duration::from_millis(1));
    assert_eq!(lapse.observe("e2", clock.monotonic()), None);
}
