//! Monotonic authentication feed publication.
//!
//! Retained version history, full snapshots and subscriber generations commit
//! under one lock. Every check runs before the first mutation; rejected feeds
//! cannot change authorization, refresh its age, or announce a generation.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{AuthService, JwksKey, JwksSnapshot};
use crate::project_policy::{
    CredentialGrant, CredentialStatus, GrantSnapshot, PolicySnapshot, ProjectPolicy,
};

const HIGH_WATER_MAX: usize = 65_536;

/// Per-project retained feed history (SR-4 + SR2 finding 2).
#[derive(Clone, Debug)]
struct ProjHw {
    /// Max (ownership_version, policy_version) ever seen.
    o_hw: u64,
    p_hw: u64,
    /// Round-4 finding 3: the workspace bound to `o_hw`. The
    /// direct-transition check compares a new policy against a project
    /// still PRESENT in the current snapshot — an omit-and-reintroduce
    /// sequence hid behind that, moving the workspace at an UNCHANGED
    /// ownership_version (no transfer event, no credential revocation,
    /// no billing split). The workspace at the high-water ownership
    /// version survives omissions, so the coupling holds across them.
    workspace_at_o_hw: crate::tenant::WorkspaceId,
    /// Version pair at the moment the project was last OMITTED from a
    /// full snapshot. Reintroduction requires strictly exceeding one
    /// of them — a replayed pre-omission snapshot cannot resurrect it.
    omitted_at: Option<(u64, u64)>,
    /// Semantic fingerprint of the last content seen at (o_hw, p_hw):
    /// an EQUAL version pair must carry identical content — versions
    /// pin bytes, and a same-version status or quota flip is a
    /// publisher defect, refused.
    fp_at: (u64, u64),
    fp: [u8; 32],
}

/// Per-credential retained feed history (SR-4 + SR2 finding 2).
#[derive(Clone, Debug)]
struct CredHw {
    /// Max grant_version ever seen.
    v_hw: u64,
    /// Max version ever seen NOT Active (revoked/disabled/expired).
    dead: Option<u64>,
    /// grant_version at the last omission from a full snapshot.
    omitted_at: Option<u64>,
    /// Semantic fingerprint of the content at v_hw.
    fp_at: u64,
    fp: [u8; 32],
}

/// PROCESS-LOCAL, BOUNDED (`HIGH_WATER_MAX` FIFO). The durable
/// security guarantee is the Control-Plane feed contract (versions
/// only move forward; full snapshots); this table is defense in depth
/// against a MISBEHAVING PUBLISHER within one process lifetime — a
/// restart or FIFO eviction resets it, deliberately and documented.
#[derive(Default, Debug)]
pub(super) struct HighWater {
    projects: std::collections::HashMap<crate::tenant::ProjectId, ProjHw>,
    p_order: std::collections::VecDeque<crate::tenant::ProjectId>,
    // mt-lint: allow(name-keyed-map): credential id (feed high-water table)
    credentials: std::collections::HashMap<Arc<str>, CredHw>,
    c_order: std::collections::VecDeque<Arc<str>>,
    /// SR3-3: every kid EVER seen, with its material fingerprint and
    /// whether it has been retired (omitted from a full snapshot). A
    /// retired kid never returns, at ANY generation; a known kid never
    /// changes material.
    // mt-lint: allow(name-keyed-map): JWKS key id (kid), not stream identity
    kids: std::collections::HashMap<String, KidHw>,
    k_order: std::collections::VecDeque<String>,
    /// SR3-3: per-feed (generation, canonical digest) of the LAST
    /// accepted snapshot — the same generation must always carry the
    /// same digest (catches an entry ADDED under a published
    /// generation, which per-ID checks cannot see).
    jwks_gen: Option<(u64, [u8; 32])>,
    policy_gen: Option<(u64, [u8; 32])>,
    grant_gen: Option<(u64, [u8; 32])>,
}

#[derive(Clone, Debug)]
struct KidHw {
    fp: [u8; 32],
    alg_dbg: String,
    retired: bool,
}

/// Canonical semantic fingerprint. Debug formatting is deterministic
/// for these derive(Debug) scalar/vec types, and the table never
/// crosses a process boundary, so the encoding cannot skew between
/// writer and checker.
fn feed_fp(debug: impl std::fmt::Debug) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(format!("{debug:?}").as_bytes());
    h.finalize().into()
}

impl ProjHw {
    fn check(&self, np: &ProjectPolicy) -> Result<(), &'static str> {
        if np.ownership_version < self.o_hw {
            return Err("ownership_version below high-water");
        }
        if np.project_policy_version < self.p_hw {
            return Err("project_policy_version below high-water");
        }
        // Round-4 finding 3: the workspace bound to the
        // HIGH-WATER ownership version survives omissions. A
        // workspace change at an unchanged ownership_version is
        // refused even when the current snapshot no longer
        // contains the project — omit-and-reintroduce must not
        // bypass the transfer coupling.
        if np.ownership_version == self.o_hw && np.workspace_id != self.workspace_at_o_hw {
            return Err("workspace changed without ownership_version increment");
        }
        if let Some((oo, op)) = self.omitted_at
            && np.ownership_version <= oo
            && np.project_policy_version <= op
        {
            return Err("omitted project reintroduced without a newer version (SR2 tombstone)");
        }
        if (np.ownership_version, np.project_policy_version) == self.fp_at && feed_fp(np) != self.fp
        {
            return Err("same project version with different content");
        }
        Ok(())
    }

    fn observe(&mut self, np: &ProjectPolicy) {
        let vpair = (np.ownership_version, np.project_policy_version);
        if np.ownership_version > self.o_hw {
            self.o_hw = np.ownership_version;
            // Round-4 finding 3: the workspace travels with
            // the ownership high-water.
            self.workspace_at_o_hw = np.workspace_id.clone();
        }
        self.p_hw = self.p_hw.max(np.project_policy_version);
        if vpair >= self.fp_at {
            self.fp_at = vpair;
            self.fp = feed_fp(np);
        }
        // A strictly newer version clears the tombstone.
        if let Some((oo, op)) = self.omitted_at
            && (np.ownership_version > oo || np.project_policy_version > op)
        {
            self.omitted_at = None;
        }
    }
}

fn check_projects_transition(op: &ProjectPolicy, np: &ProjectPolicy) -> Result<(), &'static str> {
    if np.ownership_version < op.ownership_version {
        return Err("ownership_version regressed");
    }
    if np.project_policy_version < op.project_policy_version {
        return Err("project_policy_version regressed");
    }
    // Review round 3 F3: the contract ties a workspace
    // (owner) change to an ownership_version increment. A
    // higher policy version must not smuggle an owner
    // change past the lease/transfer machinery.
    if np.workspace_id != op.workspace_id && np.ownership_version <= op.ownership_version {
        return Err("workspace changed without ownership_version increment");
    }
    Ok(())
}

impl CredHw {
    fn check(&self, nc: &CredentialGrant) -> Result<(), &'static str> {
        if nc.grant_version < self.v_hw {
            return Err("grant_version below high-water");
        }
        if nc.status == CredentialStatus::Active && self.dead.is_some_and(|d| nc.grant_version <= d)
        {
            return Err("revoked credential reactivated without a newer grant_version");
        }
        if self.omitted_at.is_some_and(|o| nc.grant_version <= o) {
            return Err(
                "omitted credential reintroduced without a newer grant_version (SR2 tombstone)",
            );
        }
        if nc.grant_version == self.fp_at && feed_fp(nc) != self.fp {
            return Err("same grant_version with different content");
        }
        Ok(())
    }

    fn observe(&mut self, nc: &CredentialGrant) {
        let dead = !matches!(nc.status, CredentialStatus::Active);
        self.v_hw = self.v_hw.max(nc.grant_version);
        if dead {
            self.dead = Some(
                self.dead
                    .map_or(nc.grant_version, |d| d.max(nc.grant_version)),
            );
        }
        if nc.grant_version >= self.fp_at {
            self.fp_at = nc.grant_version;
            self.fp = feed_fp(nc);
        }
        if self.omitted_at.is_some_and(|o| nc.grant_version > o) {
            self.omitted_at = None;
        }
    }
}

fn check_credentials_transition(
    oc: &CredentialGrant,
    nc: &CredentialGrant,
) -> Result<(), &'static str> {
    if nc.grant_version < oc.grant_version {
        return Err("grant_version regressed");
    }
    let was_dead = matches!(
        oc.status,
        CredentialStatus::Revoked | CredentialStatus::Disabled
    );
    if was_dead && nc.status == CredentialStatus::Active && nc.grant_version <= oc.grant_version {
        // Un-revocation is an explicit act, never a replay:
        // it must arrive under a STRICTLY newer version.
        return Err("revoked credential reactivated without a newer grant_version");
    }
    Ok(())
}

impl KidHw {
    fn check(&self, key: &JwksKey) -> Result<(), &'static str> {
        if self.retired {
            return Err("retired kid reintroduced (SR3 tombstone)");
        }
        if self.fp != key.fp || self.alg_dbg != format!("{:?}", key.alg) {
            return Err("kid rebound to different key material");
        }
        Ok(())
    }
}

fn jwks_digest(snapshot: &JwksSnapshot) -> [u8; 32] {
    use sha2::Digest;
    let mut kids: Vec<_> = snapshot
        .keys
        .iter()
        .map(|(k, v)| (k.clone(), format!("{:?}", v.alg), v.fp))
        .collect();
    kids.sort();
    let mut h = sha2::Sha256::new();
    for (k, a, fp) in &kids {
        h.update(k.as_bytes());
        h.update([0u8]);
        h.update(a.as_bytes());
        h.update([0u8]);
        h.update(fp);
    }
    let out: [u8; 32] = h.finalize().into();
    out
}

impl HighWater {
    fn remember_project(&mut self, pid: &crate::tenant::ProjectId, np: &ProjectPolicy) {
        if let Some(entry) = self.projects.get_mut(pid) {
            entry.observe(np);
            return;
        }
        let vpair = (np.ownership_version, np.project_policy_version);
        if self.projects.len() >= HIGH_WATER_MAX
            && let Some(old) = self.p_order.pop_front()
        {
            self.projects.remove(&old);
        }
        self.projects.insert(
            pid.clone(),
            ProjHw {
                o_hw: np.ownership_version,
                p_hw: np.project_policy_version,
                workspace_at_o_hw: np.workspace_id.clone(),
                omitted_at: None,
                fp_at: vpair,
                fp: feed_fp(np),
            },
        );
        self.p_order.push_back(pid.clone());
    }

    fn remember_credential(&mut self, id: &Arc<str>, nc: &CredentialGrant) {
        if let Some(entry) = self.credentials.get_mut(id) {
            entry.observe(nc);
            return;
        }
        let dead = !matches!(nc.status, CredentialStatus::Active);
        if self.credentials.len() >= HIGH_WATER_MAX
            && let Some(old) = self.c_order.pop_front()
        {
            self.credentials.remove(&old);
        }
        self.credentials.insert(
            id.clone(),
            CredHw {
                v_hw: nc.grant_version,
                dead: dead.then_some(nc.grant_version),
                omitted_at: None,
                fp_at: nc.grant_version,
                fp: feed_fp(nc),
            },
        );
        self.c_order.push_back(id.clone());
    }
}

impl AuthService {
    #[expect(
        clippy::unwrap_used,
        reason = "AuthService publication; poisoned history may be partially changed; recovering could restore revoked authority"
    )]
    pub(crate) fn publish_jwks(&self, snapshot: JwksSnapshot) -> Result<(), &'static str> {
        let mut hw = self.high_water.lock().unwrap();
        let cur = self.jwks.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("jwks feed_version regressed");
        }
        // SR3-3 (round-3 finding 3): signing-key lifecycle rules —
        //   * a kid names ONE algorithm and ONE public key forever;
        //   * once omitted from a full snapshot, a kid is RETIRED and
        //     never returns, at ANY later generation;
        //   * the same generation always carries the same canonical
        //     digest (identical replay ok; content drift refused).
        // ALL checks run before ANY mutation.
        let digest = jwks_digest(&snapshot);
        if let Some((g, d)) = hw.jwks_gen
            && snapshot.feed_version == g
            && digest != d
        {
            return Err("same jwks generation with a different key set");
        }
        for (kid, key) in &snapshot.keys {
            if let Some(k) = hw.kids.get(kid.as_str()) {
                k.check(key)?;
            }
        }
        for (kid, key) in &snapshot.keys {
            if hw.kids.contains_key(kid.as_str()) {
                continue;
            }
            if hw.kids.len() >= HIGH_WATER_MAX
                && let Some(old) = hw.k_order.pop_front()
            {
                hw.kids.remove(&old);
            }
            hw.kids.insert(
                kid.clone(),
                KidHw {
                    fp: key.fp,
                    alg_dbg: format!("{:?}", key.alg),
                    retired: false,
                },
            );
            hw.k_order.push_back(kid.clone());
        }
        // Retire every kid the FULL snapshot dropped.
        for kid in cur.keys.keys() {
            if !snapshot.keys.contains_key(kid)
                && let Some(k) = hw.kids.get_mut(kid.as_str())
            {
                k.retired = true;
            }
        }
        hw.jwks_gen = Some((snapshot.feed_version, digest));
        self.jwks.store(Arc::new(snapshot));
        self.unknown_kid_seen.store(0, Ordering::Relaxed);
        self.published();
        drop(hw);
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "AuthService publication; poisoned history may be partially changed; recovering could restore revoked authority"
    )]
    pub(crate) fn publish_policies(&self, snapshot: PolicySnapshot) -> Result<(), &'static str> {
        let mut hw = self.high_water.lock().unwrap();
        let cur = self.projects.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("policy feed_version regressed");
        }
        // SR-4 + SR2 finding 2: versions are compared against the
        // retained HIGH-WATER marks; an OMISSION from a full snapshot
        // tombstones the entry at its last version pair (strictly
        // newer to reintroduce); an EQUAL version pair must carry
        // IDENTICAL content. ALL checks run before ANY mutation — a
        // refused snapshot leaves no trace.
        // SR3-3: the same policy generation must carry the same
        // canonical digest — a NEW project slipped under an
        // already-published generation is invisible to per-ID checks.
        let digest = {
            let mut rows: Vec<_> = snapshot
                .projects
                .values()
                .map(|p| format!("{p:?}"))
                .collect();
            rows.sort();
            feed_fp(&rows)
        };
        if let Some((g, d)) = hw.policy_gen
            && snapshot.feed_version == g
            && digest != d
        {
            return Err("same policy generation with different content");
        }
        for (pid, np) in &snapshot.projects {
            if let Some(entry) = hw.projects.get(pid) {
                entry.check(np)?;
            }
            if let Some(op) = cur.projects.get(pid) {
                check_projects_transition(op, np)?;
            }
        }
        for (pid, np) in &snapshot.projects {
            hw.remember_project(pid, np);
        }
        // Tombstone every project the FULL snapshot dropped.
        for (pid, op) in &cur.projects {
            if !snapshot.projects.contains_key(pid)
                && let Some(e) = hw.projects.get_mut(pid)
            {
                let (ownership, policy) = e.omitted_at.unwrap_or((0, 0));
                e.omitted_at = Some((
                    ownership.max(op.ownership_version),
                    policy.max(op.project_policy_version),
                ));
            }
        }
        hw.policy_gen = Some((snapshot.feed_version, digest));
        self.projects.store(Arc::new(snapshot));
        self.published();
        drop(hw);
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "AuthService publication; poisoned history may be partially changed; recovering could restore revoked authority"
    )]
    pub(crate) fn publish_grants(&self, snapshot: GrantSnapshot) -> Result<(), &'static str> {
        let mut hw = self.high_water.lock().unwrap();
        let cur = self.credentials.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("grant feed_version regressed");
        }
        // SR-4: high-water checks survive snapshot omission — a revoked
        // credential removed from the feed and later reintroduced
        // Active at an old (or the same) version is refused; only a
        // STRICTLY newer grant_version than any version it was ever
        // seen dead at can reactivate it.
        // SR3-3: generation digest, as for policies.
        let digest = {
            let mut rows: Vec<_> = snapshot
                .credentials
                .values()
                .map(|c| format!("{c:?}"))
                .collect();
            rows.sort();
            feed_fp(&rows)
        };
        if let Some((g, d)) = hw.grant_gen
            && snapshot.feed_version == g
            && digest != d
        {
            return Err("same grant generation with different content");
        }
        for (id, nc) in &snapshot.credentials {
            if let Some(entry) = hw.credentials.get(id) {
                entry.check(nc)?;
            }
            if let Some(oc) = cur.credentials.get(id) {
                check_credentials_transition(oc, nc)?;
            }
        }
        for (id, nc) in &snapshot.credentials {
            hw.remember_credential(id, nc);
        }
        // Tombstone every credential the FULL snapshot dropped.
        for (id, oc) in &cur.credentials {
            if !snapshot.credentials.contains_key(id)
                && let Some(e) = hw.credentials.get_mut(id.as_ref())
            {
                e.omitted_at = Some(
                    e.omitted_at
                        .map_or(oc.grant_version, |o| o.max(oc.grant_version)),
                );
            }
        }
        hw.grant_gen = Some((snapshot.feed_version, digest));
        self.credentials.store(Arc::new(snapshot));
        self.published();
        drop(hw);
        Ok(())
    }

    /// Keep the cheap atomic generation and the retained watch value together,
    /// including publications while there are no receivers.
    fn published(&self) {
        let generation = self.generation.fetch_add(1, Ordering::Release) + 1;
        self.gen_tx.send_replace(generation);
    }
}

#[cfg(test)]
mod tests;
