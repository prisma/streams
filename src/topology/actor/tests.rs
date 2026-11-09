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
