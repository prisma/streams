//! Ownership-scoped maintenance backlog: the source of truth for
//! admission decisions.
//!
//! R24-A. The first version of maintenance backpressure derived backlog
//! from two process-lifetime atomics, `INGEST_BYTES_TOTAL` minus
//! `ABSORB_BYTES_TOTAL`. That is a process-local heuristic wearing the
//! costume of a safety boundary, and it fails in four distinct ways:
//!
//! 1. **Phantom backlog.** The ingest counter is incremented while the
//!    commit group is being assembled, before `write_with_options`
//!    succeeds. A failed group write leaves bytes the absorber can never
//!    retire, so the instance can enter permanent `maintenance_backpressure`
//!    for records that were never committed and that no customer holds.
//! 2. **Restart hides backlog.** Both counters reset to zero. A shard
//!    holding hundreds of MB of durable unabsorbed data reports zero and
//!    admits writes freely. Then, as the old data drains,
//!    `ABSORB > INGEST` and `saturating_sub` pins the reading at zero
//!    until new ingest overtakes the historical absorbed count — the
//!    exact opposite of the intended restart safety.
//! 3. **Ownership movement misinforms both sides.** The counters are
//!    process-wide, not keyed by shard. After moving a loaded shard from
//!    A to B, A keeps its deficit and may shed streams it no longer
//!    owns, while B inherits the durable backlog with no history and
//!    admits freely.
//! 4. **The per-shard figure measured the wrong thing** — it read
//!    policy-deferred sparse-absorption bytes, so a shard could carry a
//!    large active backlog while reporting nearly zero.
//!
//! The fix: each physical shard owns a durable maintenance row, updated
//! atomically with the batch that creates or retires the work. This
//! module is the in-memory MIRROR of those rows for fast admission — the
//! durable row is authoritative on restart and handoff, and the mirror
//! is rebuilt from it when a shard opens and dropped when it departs.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::shard::StreamMaintenance;

fn shards() -> &'static Mutex<HashMap<String, StreamMaintenance>> {
    static M: OnceLock<Mutex<HashMap<String, StreamMaintenance>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Install a shard's durable maintenance row on open. This is what makes
/// restart honest: a shard with pre-existing backlog reports it from the
/// first admission decision, before any new traffic arrives.
pub fn install(prefix: &str, m: StreamMaintenance) {
    shards()
        .lock()
        .unwrap()
        .insert(prefix.to_string(), m);
}

/// Drop a shard on departure, so a former owner stops counting backlog
/// it handed away.
pub fn remove(prefix: &str) {
    shards().lock().unwrap().remove(prefix);
}

/// Apply a committed delta. `added` is bytes newly appended; `retired` is
/// bytes the absorber has durably taken. Only ever called AFTER the
/// batch carrying that work has committed.
pub fn apply_delta(prefix: &str, added: u64, retired: u64, now_ms: i64) -> StreamMaintenance {
    let mut g = shards().lock().unwrap();
    let e = g.entry(prefix.to_string()).or_default();
    e.unabsorbed_bytes = e.unabsorbed_bytes.saturating_add(added).saturating_sub(retired);
    if e.unabsorbed_bytes == 0 {
        // Nothing outstanding: the age claim is retired with the work.
        e.oldest_unabsorbed_ms = 0;
    } else if e.oldest_unabsorbed_ms == 0 && added > 0 {
        // First outstanding bytes since the last drain start the clock.
        // Only set here, never moved forward while work remains, so the
        // age can overstate but never reset — a conservative bound is
        // the only safe direction for a safety threshold.
        e.oldest_unabsorbed_ms = now_ms;
    }
    *e
}

/// One shard's current backlog.
pub fn for_shard(prefix: &str) -> StreamMaintenance {
    shards()
        .lock()
        .unwrap()
        .get(prefix)
        .copied()
        .unwrap_or_default()
}

/// Aggregate across CURRENTLY OWNED shards only.
///
/// Returns (total bytes, largest single shard's bytes, oldest age in
/// seconds). Every threshold is derived from this, so an instance can
/// only ever be judged on work it actually holds.
pub fn aggregate(now_ms: i64) -> (u64, u64, u64) {
    let g = shards().lock().unwrap();
    let mut total = 0u64;
    let mut max_shard = 0u64;
    let mut oldest_ms = 0i64;
    for m in g.values() {
        total = total.saturating_add(m.unabsorbed_bytes);
        max_shard = max_shard.max(m.unabsorbed_bytes);
        if m.oldest_unabsorbed_ms > 0 && (oldest_ms == 0 || m.oldest_unabsorbed_ms < oldest_ms) {
            oldest_ms = m.oldest_unabsorbed_ms;
        }
    }
    let age_secs = if oldest_ms > 0 {
        ((now_ms - oldest_ms).max(0) / 1000) as u64
    } else {
        0
    };
    (total, max_shard, age_secs)
}

/// Shards currently over a per-shard byte bound. Admission uses this to
/// shed ONLY the offending shard's appends instead of latching the whole
/// process — one noisy tenant must not reject every other customer's
/// writes on the same instance.
pub fn shards_over(limit: u64) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }
    shards()
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, m)| m.unabsorbed_bytes > limit)
        .map(|(p, _)| p.clone())
        .collect()
}

pub fn snapshot_json() -> serde_json::Value {
    let g = shards().lock().unwrap();
    serde_json::json!({
        "owned_shards": g.len(),
        "shards": g.iter().map(|(p, m)| serde_json::json!({
            "prefix": p,
            "unabsorbed_bytes": m.unabsorbed_bytes,
            "oldest_unabsorbed_ms": m.oldest_unabsorbed_ms,
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
pub fn reset_for_tests() {
    shards().lock().unwrap().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These tests mutate PROCESS-GLOBAL shard state, so they must not
    /// run concurrently with each other. Without this the suite passes
    /// or fails on thread-scheduling luck — which is worse than no test,
    /// because a green run proves nothing. (Caught exactly that way: the
    /// full suite passed while a filtered run failed.)
    fn fresh() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        let g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_for_tests();
        g
    }

    /// The defect that made the old bound fictional: a group write that
    /// fails must leave NO backlog behind. The delta is applied only
    /// after the batch commits, so a failed write simply never calls in.
    #[test]
    fn a_failed_write_creates_no_backlog() {
        let _g = fresh();
        // Successful commit of 1000 bytes.
        apply_delta("s1", 1000, 0, 10_000);
        assert_eq!(aggregate(10_000).0, 1000);
        // A failed write contributes nothing — no apply_delta call at
        // all — so the backlog is unchanged.
        assert_eq!(aggregate(10_000).0, 1000, "failed write must not add backlog");
        // And the absorber retiring it returns to zero.
        apply_delta("s1", 0, 1000, 11_000);
        assert_eq!(aggregate(11_000).0, 0);
    }

    /// Restart must reflect pre-existing durable backlog immediately,
    /// not report zero until new ingest overtakes history.
    #[test]
    fn restart_reflects_pre_existing_backlog() {
        let _g = fresh();
        // A fresh process installs what the durable row says.
        install(
            "s1",
            StreamMaintenance {
                unabsorbed_bytes: 500_000_000,
                oldest_unabsorbed_ms: 1_000,
                ..Default::default()
            },
        );
        let (total, max_shard, age) = aggregate(61_000);
        assert_eq!(total, 500_000_000, "restart must see durable backlog at once");
        assert_eq!(max_shard, 500_000_000);
        assert_eq!(age, 60, "age derives from the durable timestamp");

        // Absorbing MORE than was ingested this process lifetime must
        // not underflow into a false zero-then-negative reading.
        apply_delta("s1", 0, 500_000_000, 62_000);
        assert_eq!(aggregate(62_000).0, 0);
    }

    /// Ownership movement: the contribution must leave A and arrive at
    /// B, so neither side answers for work it does not hold.
    #[test]
    fn ownership_move_transfers_the_contribution() {
        let _g = fresh();
        install(
            "shardA",
            StreamMaintenance {
                unabsorbed_bytes: 800,
                oldest_unabsorbed_ms: 5,
                ..Default::default()
            },
        );
        install("other", StreamMaintenance { unabsorbed_bytes: 10, ..Default::default() });
        assert_eq!(aggregate(1000).0, 810);

        // A hands shardA away.
        remove("shardA");
        assert_eq!(
            aggregate(1000).0,
            10,
            "a former owner must stop counting backlog it handed away"
        );

        // B opens it and installs the same durable row.
        reset_for_tests();
        install(
            "shardA",
            StreamMaintenance {
                unabsorbed_bytes: 800,
                oldest_unabsorbed_ms: 5,
                ..Default::default()
            },
        );
        assert_eq!(aggregate(1000).0, 800, "the new owner inherits it");
    }

    /// One hot shard must not latch the whole instance.
    #[test]
    fn per_shard_bound_names_only_the_offender() {
        let _g = fresh();
        install("hot", StreamMaintenance { unabsorbed_bytes: 5_000, ..Default::default() });
        install("calm", StreamMaintenance { unabsorbed_bytes: 5, ..Default::default() });
        let over = shards_over(1_000);
        assert_eq!(over, vec!["hot".to_string()]);
        assert!(shards_over(0).is_empty(), "a zero limit disables the bound");
    }

    /// Age is conservative: it may overstate, never reset while work
    /// remains outstanding.
    #[test]
    fn age_never_moves_forward_while_work_remains() {
        let _g = fresh();
        apply_delta("s1", 100, 0, 1_000);
        let first = for_shard("s1").oldest_unabsorbed_ms;
        assert_eq!(first, 1_000);
        // More bytes arrive later; the clock must NOT restart.
        apply_delta("s1", 100, 0, 9_000);
        assert_eq!(
            for_shard("s1").oldest_unabsorbed_ms,
            1_000,
            "a later append must not make the backlog look younger"
        );
        // Fully draining retires the claim.
        apply_delta("s1", 0, 200, 9_500);
        assert_eq!(for_shard("s1").oldest_unabsorbed_ms, 0);
    }
}
