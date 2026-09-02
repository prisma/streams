//! Ring ownership (WP-02 / PR 6-A): which instance serves which shard
//! prefix. Extracted from `http::AppState` — the transport used to be
//! the service locator for ring state — into a concrete owner with
//! narrow methods. One per runtime, no statics: two rigs in one
//! process never share a ring.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// Rendezvous pick: FNV-1a over `"<shard> <instance>"`, highest wins.
/// Every instance computes the same answer from the same active set.
pub fn ring_pick(shard: &str, instances: &[String]) -> usize {
    let mut best = 0usize;
    let mut best_score = 0u32;
    for (i, name) in instances.iter().enumerate() {
        let key = format!("{shard} {name}");
        let mut h: u32 = 2166136261;
        for b in key.bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(16777619);
        }
        if i == 0 || h > best_score {
            best_score = h;
            best = i;
        }
    }
    best
}

/// The ownership view a parked session compares against (Round-11.4:
/// when it changes, every parked SSE session re-checks its source).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipView {
    pub active: Vec<String>,
    // mt-lint: allow(name-keyed-map): shard prefix -> owning instance (an ordered snapshot of the override map)
    pub overrides: BTreeMap<String, String>,
}

/// Who serves which shard prefix: the fleet-published active set, the
/// rebalancer's overrides, and this instance's own name.
#[derive(Clone, Debug)]
pub struct OwnershipService {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// This instance's name. Empty = the ownership check is disabled
    /// (fleet mode off or bootstrapping): everyone may serve everything.
    instance: String,
    /// Fresh + healthy instances by heartbeat, as last observed by the
    /// fleet loop (its single writer).
    ring_active: RwLock<Vec<String>>,
    /// Rebalancer shard-move overrides (fleet/overrides.json, CAS'd):
    /// shard prefix -> instance. Consulted before the rendezvous pick; an
    /// override whose target is not in the active set is ignored.
    // mt-lint: allow(name-keyed-map): shard prefix -> owning instance
    ring_overrides: RwLock<HashMap<String, String>>,
}

impl OwnershipService {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                instance: instance.into(),
                ring_active: RwLock::new(Vec::new()),
                ring_overrides: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub fn instance(&self) -> &str {
        &self.inner.instance
    }

    /// Ring ownership for a shard prefix: the rebalancer override if its
    /// target is active, else the rendezvous pick. None when no ring is
    /// configured (single instance) — then everyone may serve everything.
    pub fn effective_owner(&self, prefix: &str) -> Option<String> {
        let active = self.inner.ring_active.read().unwrap().clone();
        if active.is_empty() || self.inner.instance.is_empty() {
            return None;
        }
        if let Some(t) = self.inner.ring_overrides.read().unwrap().get(prefix)
            && active.iter().any(|a| a == t)
        {
            return Some(t.clone());
        }
        Some(active[ring_pick(prefix, &active)].clone())
    }

    /// `Some(owner)` iff the ring assigns `prefix` to ANOTHER instance —
    /// the redirect target. None = serve it here (ours, or no ring).
    pub fn foreign_owner(&self, prefix: &str) -> Option<String> {
        self.effective_owner(prefix)
            .filter(|o| *o != self.inner.instance)
    }

    pub fn is_mine(&self, prefix: &str) -> bool {
        self.foreign_owner(prefix).is_none()
    }

    pub fn ring_active(&self) -> Vec<String> {
        self.inner.ring_active.read().unwrap().clone()
    }

    pub fn set_ring_active(&self, active: Vec<String>) {
        *self.inner.ring_active.write().unwrap() = active;
    }

    pub fn overrides(&self) -> HashMap<String, String> {
        self.inner.ring_overrides.read().unwrap().clone()
    }

    /// Replace the whole override map (the fleet loop mirrors
    /// fleet/overrides.json on every tick).
    pub fn set_overrides(&self, map: HashMap<String, String>) {
        *self.inner.ring_overrides.write().unwrap() = map;
    }

    /// One override, as the rebalancer installs it the moment its CAS
    /// wins (the mirror catches up next tick).
    pub fn set_override(&self, prefix: &str, to: &str) {
        self.inner
            .ring_overrides
            .write()
            .unwrap()
            .insert(prefix.to_string(), to.to_string());
    }

    pub fn view(&self) -> OwnershipView {
        OwnershipView {
            active: self.ring_active(),
            overrides: self.overrides().into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// No ring, or an unnamed instance: nobody owns anything, so
    /// everything is served locally.
    #[test]
    fn no_ring_means_everyone_serves_everything() {
        let o = OwnershipService::new("a");
        assert_eq!(o.effective_owner("00"), None);
        assert!(o.is_mine("00"));
        let unnamed = OwnershipService::new("");
        unnamed.set_ring_active(names(&["a", "b"]));
        assert_eq!(unnamed.effective_owner("00"), None);
    }

    /// The rendezvous pick is a pure function of (prefix, active set):
    /// two services with the same view agree, and a shard picked for
    /// the other instance is foreign here.
    #[test]
    fn rendezvous_pick_is_shared_and_foreign_is_relative() {
        let a = OwnershipService::new("a");
        let b = OwnershipService::new("b");
        for o in [&a, &b] {
            o.set_ring_active(names(&["a", "b"]));
        }
        for prefix in ["00", "01", "10", "11"] {
            assert_eq!(a.effective_owner(prefix), b.effective_owner(prefix));
            let owner = a.effective_owner(prefix).unwrap();
            assert_eq!(a.foreign_owner(prefix).is_none(), owner == "a");
            assert_eq!(b.foreign_owner(prefix).is_none(), owner == "b");
        }
    }

    /// An override wins only while its target is active; a stale
    /// override to a departed instance falls back to the pick.
    #[test]
    fn override_honored_only_for_an_active_target() {
        let o = OwnershipService::new("a");
        o.set_ring_active(names(&["a", "b"]));
        let picked = o.effective_owner("00").unwrap();
        let other = if picked == "a" { "b" } else { "a" };
        o.set_override("00", other);
        assert_eq!(o.effective_owner("00").as_deref(), Some(other));
        o.set_override("00", "gone");
        assert_eq!(o.effective_owner("00").as_deref(), Some(picked.as_str()));
        o.set_overrides(HashMap::new());
        assert_eq!(o.effective_owner("00").as_deref(), Some(picked.as_str()));
        let v = o.view();
        assert_eq!(v.active, names(&["a", "b"]));
        assert!(v.overrides.is_empty());
    }
}
