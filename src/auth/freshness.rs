//! §7.1 freshness refusals, and the refresh each one asks for
//! (shared-cells R2).
//!
//! Every request-path and lease-path read of a feed snapshot refuses here
//! when the snapshot is past its window, and the refusal wakes the
//! refresher. Freshness is measured on the wall clock, but the refresher's
//! cadence is a monotonic tokio interval. A cell that slept while its wall
//! clock moved on therefore wakes with stale feeds and no tick due, and it
//! used to answer `503` until that tick. Now its first refusal starts the
//! pass that ends the refusals. The wake is `request_kid_refresh`'s nudge:
//! at most one per 30 s, shared with the unknown-kid sighting, so a storm
//! of refusals costs one out-of-cadence pass.

use std::sync::Arc;

use arc_swap::Guard;

use super::{AuthError, AuthService, JWKS_STALENESS_MAX_SECS, JwksSnapshot, feed_stale};
use crate::project_policy::{GrantSnapshot, PolicySnapshot};

impl AuthService {
    /// The published key set, unless it is past its window.
    pub(super) fn fresh_jwks(&self, now: i64) -> Result<Guard<Arc<JwksSnapshot>>, AuthError> {
        let snap = self.jwks.load();
        let window = JWKS_STALENESS_MAX_SECS;
        self.refuse_stale(snap.fetched_at_unix, window, now, AuthError::KeysStale)?;
        Ok(snap)
    }

    /// The published policy snapshot, unless it is past its window.
    pub(super) fn fresh_policies(&self, now: i64) -> Result<Guard<Arc<PolicySnapshot>>, AuthError> {
        let snap = self.projects.load();
        let window = self.staleness_max_secs();
        self.refuse_stale(snap.fetched_at_unix, window, now, AuthError::PolicyStale)?;
        Ok(snap)
    }

    /// The published grant snapshot, unless it is past its window.
    pub(super) fn fresh_grants(&self, now: i64) -> Result<Guard<Arc<GrantSnapshot>>, AuthError> {
        let snap = self.credentials.load();
        let window = self.staleness_max_secs();
        self.refuse_stale(snap.fetched_at_unix, window, now, AuthError::GrantsStale)?;
        Ok(snap)
    }

    /// Refuse with `stale` when a snapshot stamped `fetched_at_unix` is
    /// past `window_secs` at `now`, and wake the refresher when refusing.
    pub(super) fn refuse_stale<E>(
        &self,
        fetched_at_unix: i64,
        window_secs: i64,
        now: i64,
        stale: E,
    ) -> Result<(), E> {
        if feed_stale(fetched_at_unix, window_secs, now) {
            self.request_kid_refresh();
            return Err(stale);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures_util::FutureExt;

    use super::super::tests::{NOW, service};
    use super::super::{AuthError, AuthLease, AuthService, LeaseInvalidReason};
    use super::super::{JWKS_STALENESS_MAX_SECS, JwksSnapshot, POLICY_STALENESS_MAX_SECS};
    use crate::tenant::ProjectId;

    /// Whether the refresher was woken since the last look: the nudge
    /// stores one permit, and this takes it.
    fn woken(svc: &AuthService) -> bool {
        svc.kid_wakeup.notified().now_or_never().is_some()
    }

    /// The fixture service with one feed (keys, policies or grants)
    /// republished one second past its window at `NOW`.
    fn stale(feed: &str) -> AuthService {
        let svc = service();
        let policy_window = NOW - POLICY_STALENESS_MAX_SECS - 1;
        match feed {
            "keys" => svc.publish_jwks(JwksSnapshot {
                keys: HashMap::new(),
                fetched_at_unix: NOW - JWKS_STALENESS_MAX_SECS - 1,
                feed_version: 2,
            }),
            "policies" => {
                let mut snap = (**svc.projects.load()).clone();
                snap.fetched_at_unix = policy_window;
                svc.publish_policies(snap)
            }
            _ => {
                let mut snap = (**svc.credentials.load()).clone();
                snap.fetched_at_unix = policy_window;
                svc.publish_grants(snap)
            }
        }
        .unwrap();
        svc
    }

    /// The fixture credential's lease, as verification would grant it.
    fn lease() -> AuthLease {
        AuthLease {
            project_id: ProjectId::new("proj_456").unwrap(),
            credential_id: Arc::from("strcred_123"),
            ownership_version: 12,
            grant_version: 7,
            expires_at: NOW + 600,
        }
    }

    /// Shared-cells R2: a fresh read never wakes the refresher.
    #[test]
    fn a_fresh_read_does_not_wake_the_refresher() {
        let svc = service();
        let pid = ProjectId::new("proj_456").unwrap();
        let reads = (
            svc.fresh_jwks(NOW).is_ok(),
            svc.fresh_policies(NOW).is_ok(),
            svc.fresh_grants(NOW).is_ok(),
            svc.status_and_quotas(&pid, NOW).is_ok(),
            svc.lease_check(&lease(), NOW),
        );
        assert_eq!(reads, (true, true, true, true, Ok(())));
        assert!(!woken(&svc), "a fresh read must not wake the refresher");
    }

    /// Shared-cells R2: every path that refuses on a stale feed wakes the
    /// refresher (verification's three feeds, a capability's status and a
    /// live lease's two feeds), and a second refusal inside the nudge's
    /// 30 s does not wake it again.
    #[test]
    fn every_stale_refusal_wakes_the_refresher_once() {
        let pid = ProjectId::new("proj_456").unwrap();
        let (keys, policies, grants) = (stale("keys"), stale("policies"), stale("grants"));
        let capability = stale("policies");
        let requests = [
            (keys.fresh_jwks(NOW).err(), woken(&keys)),
            (policies.fresh_policies(NOW).err(), woken(&policies)),
            (grants.fresh_grants(NOW).err(), woken(&grants)),
            (
                capability.status_and_quotas(&pid, NOW).err(),
                woken(&capability),
            ),
            (policies.fresh_policies(NOW).err(), woken(&policies)),
        ];
        let refused = |e: AuthError, woke: bool| (Some(e), woke);
        assert_eq!(
            requests,
            [
                refused(AuthError::KeysStale, true),
                refused(AuthError::PolicyStale, true),
                refused(AuthError::GrantsStale, true),
                refused(AuthError::PolicyStale, true),
                refused(AuthError::PolicyStale, false),
            ]
        );
        let (policies, grants) = (stale("policies"), stale("grants"));
        let leases = [
            (policies.lease_check(&lease(), NOW), woken(&policies)),
            (grants.lease_check(&lease(), NOW), woken(&grants)),
        ];
        assert_eq!(
            leases,
            [
                (Err(LeaseInvalidReason::PolicyStale), true),
                (Err(LeaseInvalidReason::GrantsStale), true),
            ]
        );
    }
}
