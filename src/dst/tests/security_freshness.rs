//! V01: logical freshness boundaries use an owned clock, never scheduler timing.
use super::fixture_auth::{RIG_SCOPES, auth_rig, rig_policy};
use super::fixture_failpoints::gap_lock;
use crate::auth::{AuthLease, AuthService, LeaseInvalidReason};
use crate::project_policy::{CredentialGrant, CredentialStatus, GrantSnapshot, PolicySnapshot};
use crate::runtime::{Clock, ManualClock};
use crate::sse::auth::{LeaseWatch, SseLease, TerminateOnce};
use std::{sync::Arc, time::Duration};

struct LeaseRig {
    service: Arc<AuthService>,
    state: Arc<crate::http::AppState>,
    clock: ManualClock,
    lease: AuthLease,
    watch: LeaseWatch,
    version: u64,
}
impl LeaseRig {
    async fn new() -> Self {
        // Initialize the real owner under its ordinary production window.
        // Every short-window decision below then uses only this owned clock.
        let (service, state, _) = auth_rig("proj-v01", "ws-v01", &["c-v01"], None).await;
        let clock = ManualClock::at(crate::shard::now_ms());
        let lease = AuthLease {
            project_id: crate::tenant::ProjectId::new("proj-v01").unwrap(),
            credential_id: Arc::from("c-v01"),
            ownership_version: 1,
            grant_version: 1,
            expires_at: clock.now().ms() / 1000 + 600,
        };
        let watch = LeaseWatch::new_checked(
            &state,
            SseLease::Customer(lease.clone()),
            Arc::new(TerminateOnce::default()),
        )
        .unwrap();
        service.set_staleness_max_secs(3);
        let mut rig = Self {
            service,
            state,
            clock,
            lease,
            watch,
            version: 1,
        };
        rig.publish(true, true);
        assert!(!rig.revoked());
        rig
    }
    fn now(&self) -> i64 {
        self.clock.now().ms() / 1000
    }
    fn advance(&self, seconds: u64) {
        self.clock.advance(Duration::from_secs(seconds));
    }
    fn revoked(&mut self) -> bool {
        self.watch.revoked_with_clock(&self.state, &self.clock)
    }
    #[expect(
        clippy::fn_params_excessive_bools,
        reason = "LeaseRig::publish; the fixture publishes the policy and the grants as two independent switches the scenarios toggle separately; an enum would restate two booleans"
    )]
    fn publish(&mut self, policy: bool, grants: bool) {
        self.version += 1;
        if policy {
            let policy = rig_policy("proj-v01", "ws-v01", 1, 1);
            self.service
                .publish_policies(PolicySnapshot {
                    projects: [(policy.project_id.clone(), policy)].into(),
                    fetched_at_unix: self.now(),
                    feed_version: self.version,
                })
                .unwrap();
        }
        if grants {
            let credential_id: Arc<str> = Arc::from("c-v01");
            self.service
                .publish_grants(GrantSnapshot {
                    credentials: [(
                        credential_id.clone(),
                        CredentialGrant {
                            credential_id,
                            project_id: self.lease.project_id.clone(),
                            grant_version: 1,
                            status: CredentialStatus::Active,
                            scopes: crate::tenant::ScopeSet::parse(RIG_SCOPES).0,
                            grant: crate::tenant::StreamGrant::All,
                            expires_at: None,
                        },
                    )]
                    .into(),
                    fetched_at_unix: self.now(),
                    feed_version: self.version,
                })
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v01_owned_time_refreshes_survive_every_preceding_deadline() {
    let _serial = gap_lock().lock().await;
    let mut rig = LeaseRig::new().await;
    let original_deadline = rig.watch.next_deadline;
    for round in 0..4 {
        let previous_deadline = rig.watch.next_deadline;
        let generation = rig.watch.last_gen;
        rig.advance(2);
        assert!(
            rig.now() < previous_deadline,
            "refresh-before-deadline precondition"
        );
        rig.publish(true, true);
        assert!(!rig.revoked());
        assert_eq!(rig.watch.last_gen, generation + 2);
        assert_eq!(rig.watch.next_deadline, rig.now() + 3);
        if round > 0 {
            assert!(rig.now() > original_deadline);
        }
        // Move exactly to the old mandatory recheck point. Publication moved
        // the actual deadline, so this same watcher must remain authorized.
        rig.advance(1);
        assert_eq!(rig.now(), previous_deadline);
        assert!(!rig.revoked());
        // Establish the next round's explicitly owned three-second window.
        if round < 3 {
            let next_deadline = rig.watch.next_deadline;
            rig.publish(true, true);
            assert!(!rig.revoked());
            assert!(rig.watch.next_deadline > next_deadline);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v01_owned_time_staleness_refuses_each_unrefreshed_feed_at_the_boundary() {
    let _serial = gap_lock().lock().await;
    for missing_policy in [true, false] {
        let mut rig = LeaseRig::new().await;
        let start = rig.now();
        rig.advance(2);
        rig.publish(!missing_policy, missing_policy);
        assert!(!rig.revoked());
        rig.advance(1);
        assert_eq!(rig.now(), start + 3);
        // lease_deadline is a mandatory recheck; the predicate intentionally
        // admits age == window and rejects age > window.
        assert!(!rig.revoked(), "exactly at the window remains fresh");
        rig.advance(1);
        let reason = if missing_policy {
            LeaseInvalidReason::PolicyStale
        } else {
            LeaseInvalidReason::GrantsStale
        };
        assert_eq!(rig.service.lease_check(&rig.lease, rig.now()), Err(reason));
        assert!(
            rig.revoked(),
            "the unchanged generation cannot bypass elapsed freshness"
        );
        assert!(
            !rig.watch.term.record_once(reason),
            "termination was already recorded by the watcher"
        );
    }
}
