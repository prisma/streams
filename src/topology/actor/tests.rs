#![cfg(test)]
//! The actor's re-claim of its own lapsed lease (S4-DESIGN §4.3).

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use super::Actor;
use crate::runtime::ManualClock;
use crate::topology::intent::{self, Intent, State};

fn split() -> Intent {
    Intent {
        kind: "split".to_string(),
        op_id: "op-a".to_string(),
        sources: vec![("0".to_string(), 0)],
        children: vec![2, 3],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    }
}

/// **An actor whose own clock passed its lease, while no claimant took the intent, re-claims it
/// and runs on.** Its renewals did not land for 12 s (past the lease and its skew): `owned` fails,
/// but `still_owned` CASes the intent at the next epoch, under the same owner, and the actor owns
/// it again. Had a claimant taken the intent first, the re-claim loses and the intent stays the
/// claimant's.
#[tokio::test(crate = "crate::test_rt")]
async fn an_actor_past_its_lease_reclaims_its_own_intent_unless_a_claimant_took_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let clock = ManualClock::at(1_700_000_000_000);
    let held = intent::create(&*store, &split()).await.unwrap();
    let actor = Actor::new(&*store, &clock, held);
    clock.advance(Duration::from_secs(12));
    assert!(
        actor.owned("the clones").is_err(),
        "lapsed by its own clock"
    );
    actor.still_owned("the clones").await.expect("re-claimed");
    let stored = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(
        (stored.lease_owner.as_str(), stored.lease_epoch),
        ("inst-a", 2)
    );
    assert!(actor.owned("'T'").is_ok(), "owned again");

    clock.advance(Duration::from_secs(12));
    let current = intent::read(&*store).await.unwrap().unwrap();
    let mut claimed = current.intent.clone();
    claimed.lease_owner = "opener:0@3".to_string();
    claimed.lease_epoch += 1;
    intent::replace(&*store, &current, claimed).await.unwrap();
    let lost = actor.still_owned("'T'").await.unwrap_err();
    assert!(lost.contains("lease"), "{lost}");
    let stored = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(
        (stored.lease_owner.as_str(), stored.lease_epoch),
        ("opener:0@3", 3)
    );
}

/// The intent written behind the actor's back, its lease moved and `change` applied: what a
/// write of the actor's own whose reply was lost, or a claimant's, leaves in the store.
async fn written_behind(store: &dyn ObjectStore, change: fn(&mut Intent)) {
    let current = intent::read(store).await.unwrap().unwrap();
    let mut next = current.intent.clone();
    next.lease_until_ms += 1;
    change(&mut next);
    intent::replace(store, &current, next).await.unwrap();
}

/// The writes a renewal or an end can lose its CAS to, and whether the actor adopts each: only
/// its own (the same epoch, owner and operation, still live) is adopted; any other ends it.
/// A write behind the actor: what it is, the change it makes, and whether the actor adopts it.
type Behind = (&'static str, fn(&mut Intent), bool);

const BEHIND: [Behind; 5] = [
    ("its own write", |_| {}, true),
    ("another epoch", |i| i.lease_epoch += 1, false),
    (
        "another owner",
        |i| i.lease_owner = "opener:0@2".to_string(),
        false,
    ),
    ("another operation", |i| i.op_id = "op-b".to_string(), false),
    ("a terminal intent", |i| i.state = State::RolledBack, false),
];

/// **A renewal whose CAS lost to a write of the actor's own adopts it and the actor runs on; one
/// lost to any other write ends the actor's ownership.** Each case writes behind a fresh actor,
/// then renews once: the actor still owns its intent exactly when the write was its own, and
/// then holds the stored intent.
#[tokio::test(crate = "crate::test_rt")]
async fn a_renewal_lost_to_its_own_write_adopts_it_and_lost_to_any_other_ends_ownership() {
    for (what, change, adopted) in BEHIND {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let clock = ManualClock::at(1_700_000_000_000);
        let held = intent::create(&*store, &split()).await.unwrap();
        let actor = Actor::new(&*store, &clock, held);
        written_behind(&*store, change).await;
        actor.renew_once().await;
        assert_eq!(actor.owned("the next step").is_ok(), adopted, "{what}");
        let stored = intent::read(&*store).await.unwrap().unwrap();
        assert_eq!(actor.held() == stored, adopted, "{what}: the held intent");
    }
}

/// **An end whose CAS lost to a write of the actor's own retries over it and ends the intent; one
/// lost to any other write leaves the stored intent as it is.**
#[tokio::test(crate = "crate::test_rt")]
async fn an_end_lost_to_its_own_write_ends_the_intent_and_lost_to_any_other_leaves_it() {
    for (what, change, adopted) in BEHIND {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let clock = ManualClock::at(1_700_000_000_000);
        let held = intent::create(&*store, &split()).await.unwrap();
        let actor = Actor::new(&*store, &clock, held);
        written_behind(&*store, change).await;
        let behind = intent::read(&*store).await.unwrap().unwrap();
        actor.end(State::RolledBack, "rolled_back").await;
        let stored = intent::read(&*store).await.unwrap().unwrap();
        if adopted {
            assert_eq!(stored.intent.state, State::RolledBack, "{what}");
            assert_eq!(
                stored.intent.lease_until_ms, behind.intent.lease_until_ms,
                "{what}"
            );
        } else {
            assert_eq!(stored, behind, "{what}: left as it was");
        }
    }
}
