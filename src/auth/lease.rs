//! The live-subscription lease (review V4, round 3 F1): the facts a
//! long-lived subscription keeps re-proving, why it stops being valid, and
//! the next instant it must re-prove. It is a child of `auth` because it
//! reads the same snapshots request verification reads.

use std::sync::Arc;

use super::{AuthService, feed_fresh_until, feed_stale};
use crate::tenant::ProjectId;

/// Review V4: compact authorization lease for LONG-LIVED
/// subscriptions. A live SSE connection re-checks this whenever the
/// auth snapshot generation changes (bounded by the heartbeat
/// cadence) and terminates no later than token expiry — an old owner
/// must not keep receiving records through a connection opened
/// before a transfer, suspension, revocation, or expiry.
#[derive(Clone, Debug)]
pub(crate) struct AuthLease {
    pub project_id: ProjectId,
    pub credential_id: Arc<str>,
    pub ownership_version: u64,
    pub grant_version: u64,
    pub expires_at: i64,
}

/// Review round 3 F1: the reasons a live subscription's lease stops
/// being valid. Exported as termination counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseInvalidReason {
    TokenExpired,
    PolicyStale,
    GrantsStale,
    ProjectMissing,
    ProjectNotActive,
    OwnershipChanged,
    CredentialMissing,
    CredentialInactive,
    GrantChanged,
    CredentialExpired,
}

// Every reason sits in `ALL` at its own discriminant, so `index` is a
// slot in any array sized by `ALL`: an omission, a reorder or a reason
// inserted mid-enum fails to compile here.
const _: () = {
    let mut slot = 0;
    while slot < LeaseInvalidReason::ALL.len() {
        assert!(LeaseInvalidReason::ALL[slot] as usize == slot);
        slot += 1;
    }
};

impl LeaseInvalidReason {
    /// Its length is spelled from the last reason, so a reason appended
    /// after `CredentialExpired` must move it (and then the check above
    /// demands its slot).
    pub(crate) const ALL: [LeaseInvalidReason; Self::CredentialExpired as usize + 1] = [
        Self::TokenExpired,
        Self::PolicyStale,
        Self::GrantsStale,
        Self::ProjectMissing,
        Self::ProjectNotActive,
        Self::OwnershipChanged,
        Self::CredentialMissing,
        Self::CredentialInactive,
        Self::GrantChanged,
        Self::CredentialExpired,
    ];
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TokenExpired => "token_expired",
            Self::PolicyStale => "policy_stale",
            Self::GrantsStale => "grants_stale",
            Self::ProjectMissing => "project_missing",
            Self::ProjectNotActive => "project_not_active",
            Self::OwnershipChanged => "ownership_changed",
            Self::CredentialMissing => "credential_missing",
            Self::CredentialInactive => "credential_inactive",
            Self::GrantChanged => "grant_changed",
            Self::CredentialExpired => "credential_expired",
        }
    }
    /// The reason's slot in `sse::auth::LEASE_TERMINATIONS`: its
    /// discriminant, which is its position in `ALL`, so no search can
    /// miss and count one reason as another.
    pub(crate) fn index(self) -> usize {
        self as usize
    }
}

impl AuthService {
    /// Review round 3 F1: why a lease stopped being valid — exported
    /// as a termination counter and used to pick the next deadline.
    pub(crate) fn lease_check(
        &self,
        l: &AuthLease,
        now_unix: i64,
    ) -> Result<(), LeaseInvalidReason> {
        use LeaseInvalidReason as R;
        if now_unix >= l.expires_at {
            return Err(R::TokenExpired);
        }
        let w = self.staleness_max_secs();
        let pols = self.projects.load();
        // Fail closed at the SAME window new requests use: an
        // established subscription must never outlive the feed truth.
        if feed_stale(pols.fetched_at_unix, w, now_unix) {
            return Err(R::PolicyStale);
        }
        // §8.1: absent from this cell's snapshot, or placed on another cell.
        let Some(p) = self.served_policy(&pols, &l.project_id) else {
            return Err(R::ProjectMissing);
        };
        if p.status != crate::project_policy::ProjectStatus::Active {
            return Err(R::ProjectNotActive);
        }
        if p.ownership_version != l.ownership_version {
            return Err(R::OwnershipChanged);
        }
        let creds = self.credentials.load();
        if feed_stale(creds.fetched_at_unix, w, now_unix) {
            return Err(R::GrantsStale);
        }
        let Some(c) = creds.credentials.get(&l.credential_id) else {
            return Err(R::CredentialMissing);
        };
        if c.status != crate::project_policy::CredentialStatus::Active {
            return Err(R::CredentialInactive);
        }
        if c.grant_version != l.grant_version || c.project_id != l.project_id {
            return Err(R::GrantChanged);
        }
        if let Some(e) = c.expires_at
            && now_unix >= e
        {
            return Err(R::CredentialExpired);
        }
        Ok(())
    }

    /// Review round 3 F1: the next instant at which this lease MUST be
    /// re-proved even if no feed publishes — the earliest of token
    /// expiry, credential expiry, and each feed's staleness boundary.
    /// A clean refresh (even an identical replay) moves it forward.
    pub(crate) fn lease_deadline(&self, l: &AuthLease) -> i64 {
        let w = self.staleness_max_secs();
        let mut d = l.expires_at;
        d = d.min(feed_fresh_until(self.projects.load().fetched_at_unix, w));
        let creds = self.credentials.load();
        d = d.min(feed_fresh_until(creds.fetched_at_unix, w));
        if let Some(e) = creds
            .credentials
            .get(&l.credential_id)
            .and_then(|c| c.expires_at)
        {
            d = d.min(e);
        }
        d
    }
}
