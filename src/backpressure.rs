//! Maintenance backpressure: a hard bound on unabsorbed work.
//!
//! The chaos campaign measured absorption running ~2.3x BELOW ingest at
//! the tested load, so the hot tier grew without bound and absorb lag
//! climbed monotonically. Nothing lost data — everything is durable in
//! the shard log — but there was no bounded steady state either, and
//! unbounded hot-tier bytes turn into unbounded replay time, unbounded
//! storage cost, and unbounded recovery.
//!
//! The rule this module enforces:
//!
//!   never accept writes indefinitely faster than we can either absorb
//!   them or safely reject them.
//!
//! When maintenance falls far enough behind, new appends are refused
//! with a RETRYABLE 503 until the backlog drains below a low watermark.
//! That converts an unbounded, eventually-fatal condition into a
//! bounded, observable, client-visible one.
//!
//! Three properties matter more than the thresholds themselves:
//!
//! 1. **Only appends shed.** Reads, consumer pull/settle, and every
//!    control-plane operation stay admitted. Shedding a consumer would
//!    stop the drain; shedding the control plane would make the overload
//!    unrecoverable — the operator could not delete a stream, move
//!    ownership, or run cleanup precisely when they most need to.
//! 2. **Hysteresis.** Engaging at the high mark and releasing at the low
//!    mark stops the flapping that a single threshold produces when the
//!    backlog hovers at the line.
//! 3. **The decision is precomputed.** A background tick evaluates the
//!    snapshot; the request path reads one atomic. Walking the lag map
//!    per request would put the overload on the hot path.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// Thresholds. Zero disables an individual bound.
///
/// SEMANTICS (R26-6, documented decision): these are RESIDENT-SAFETY
/// bounds. The instance aggregate covers the engines this process
/// currently has OPEN — the memory, commit pipelines, and replay work
/// resident right now — because a closed shard consumes no process
/// resources. An owned-but-cold shard with durable backlog is NOT in
/// the aggregate; it is protected individually the moment anything
/// opens it, because the engine loads its durable ledger before
/// serving and the per-shard gate evaluates on first access. These
/// limits are therefore not contractual bounds on total owned hot-tier
/// storage or fleet-wide recovery backlog; ownership-wide accounting
/// (an owned-shard index summing one-row maintenance reads without
/// opening engines) is pre-fleet-GA work, tracked separately. The old
/// MAX_REPLAY_BYTES bound was deleted for exactly this honesty: it was
/// the same open-engine sum as the instance bound under a name that
/// implied ownership-wide replay projection.
#[derive(Clone, Copy, Debug, Default)]
pub struct Limits {
    pub unabsorbed_bytes_instance: u64,
    pub unabsorbed_bytes_shard: u64,
    pub absorb_lag_secs: u64,
    /// Release threshold as a percentage of the engage threshold.
    /// 75 means "engage at the limit, release at 75% of it".
    pub release_pct: u64,
}

impl Limits {
    pub fn from_env() -> Self {
        fn v(k: &str, d: u64) -> u64 {
            std::env::var(k)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(d)
        }
        Self {
            // Defaults are deliberately generous: this is a safety net
            // against unbounded growth, not a throughput throttle. An
            // instance in a healthy steady state must never touch it.
            unabsorbed_bytes_instance: v("MAX_UNABSORBED_BYTES_PER_INSTANCE", 512 * 1024 * 1024),
            unabsorbed_bytes_shard: v("MAX_UNABSORBED_BYTES_PER_SHARD", 256 * 1024 * 1024),
            absorb_lag_secs: v("MAX_ABSORB_LAG_SECS", 900),
            release_pct: v("MAINT_BACKPRESSURE_RELEASE_PCT", 75).min(100),
        }
    }

    fn any_enabled(&self) -> bool {
        self.unabsorbed_bytes_instance > 0
            || self.unabsorbed_bytes_shard > 0
            || self.absorb_lag_secs > 0
    }
}

/// What the evaluator saw. Kept as plain data so the decision is a pure
/// function that can be tested without an engine.
///
/// R27-1: deliberately NO per-shard field. The global machine and the
/// per-engine shard machine are separate; a shard quantity in this
/// struct is how the masking bug happened (see `next_state`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub unabsorbed_bytes_instance: u64,
    pub absorb_lag_secs: u64,
}

/// Which bound tripped. Ordered by how actionable it is for an operator.
/// `ShardBytes` is produced ONLY by the per-engine latch in `admit()`;
/// the global machine can never store it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    InstanceBytes,
    ShardBytes,
    LagSecs,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::InstanceBytes => "unabsorbed bytes on this instance",
            Cause::ShardBytes => "unabsorbed bytes on one shard",
            Cause::LagSecs => "absorb lag",
        }
    }
    fn code(self) -> u8 {
        match self {
            Cause::InstanceBytes => 1,
            Cause::ShardBytes => 2,
            Cause::LagSecs => 3,
        }
    }
    fn from_code(c: u8) -> Option<Self> {
        Some(match c {
            1 => Cause::InstanceBytes,
            2 => Cause::ShardBytes,
            3 => Cause::LagSecs,
            _ => return None,
        })
    }
}

/// The GLOBAL state machine, as a pure function — instance bytes and
/// no-progress lag ONLY (R27-1).
///
/// Engage when ANY enabled bound is exceeded. Release only when EVERY
/// enabled bound is back under its release threshold — a backlog that
/// drains on one axis while another stays pinned is still a backlog.
///
/// Shard bytes were REMOVED from this machine: when one shard was over
/// its byte line while the instance was over its lag line, the single
/// cause slot stored ShardBytes, and the read side filtered ShardBytes
/// out (a hot shard must not shed the whole process) — which silently
/// disabled the LagSecs bound for every other shard. The same slot
/// also pinned RELEASE: a shard above its own release line kept the
/// global cause at ShardBytes while the public state read "released".
/// Two conditions, two machines: this one, and the per-engine latch in
/// `admit()`.
pub fn next_state(engaged: bool, s: &Snapshot, l: &Limits) -> (bool, Option<Cause>) {
    if !l.any_enabled() {
        return (false, None);
    }
    let pairs = [
        (Cause::InstanceBytes, s.unabsorbed_bytes_instance, l.unabsorbed_bytes_instance),
        (Cause::LagSecs, s.absorb_lag_secs, l.absorb_lag_secs),
    ];
    if !engaged {
        for (cause, got, limit) in pairs {
            if limit > 0 && got > limit {
                return (true, Some(cause));
            }
        }
        return (false, None);
    }
    // Engaged: hold until everything is under the release line.
    for (cause, got, limit) in pairs {
        if limit == 0 {
            continue;
        }
        let release = limit.saturating_mul(l.release_pct) / 100;
        if got > release {
            return (true, Some(cause));
        }
    }
    (false, None)
}

/// The instance-wide latch (R27-1): one INSTANCE per AppState instead
/// of process statics, so every test rig owns an isolated latch and no
/// global-engagement test can shed an unrelated rig's appends. Holds
/// exactly the global machine's state — the per-shard machine lives on
/// each engine (`maintenance_shard_shed`).
#[derive(Default)]
pub struct GlobalLatch {
    /// One word: 0 = released, otherwise the engaged Cause's code
    /// (1 = instance bytes, 3 = lag). R28 review: two independent
    /// relaxed atomics let a reader observe engaged=true with a stale
    /// zero cause and misread an engaged latch as released; a single
    /// state word makes that torn read structurally impossible.
    state: AtomicU8,
    /// Times the latch went from released to engaged (not per-request).
    pub engage_count: AtomicU64,
    /// Appends refused while engaged (global + per-shard sheds alike).
    pub shed_count: AtomicU64,
    /// Last observed snapshot fields, for /v1/debug/load.
    last_unabsorbed: AtomicU64,
    last_lag_secs: AtomicU64,
}

impl GlobalLatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Some(cause)` when the WHOLE instance is over a process-level
    /// bound. Only InstanceBytes and LagSecs can appear here — the
    /// global machine no longer evaluates shard quantities, so the old
    /// read-side ShardBytes filter (which masked a simultaneous global
    /// violation) is structurally unnecessary.
    pub fn engaged(&self) -> Option<Cause> {
        Cause::from_code(self.state.load(Ordering::Relaxed))
    }

    pub fn note_shed(&self) {
        self.shed_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply an evaluated snapshot. Returns the new engaged state.
    pub fn apply(&self, s: &Snapshot, l: &Limits) -> bool {
        let was = self.state.load(Ordering::Relaxed) != 0;
        let (now, cause) = next_state(was, s, l);
        self.last_unabsorbed
            .store(s.unabsorbed_bytes_instance, Ordering::Relaxed);
        self.last_lag_secs.store(s.absorb_lag_secs, Ordering::Relaxed);
        self.state.store(
            if now { cause.map(Cause::code).unwrap_or(0) } else { 0 },
            Ordering::Relaxed,
        );
        if now && !was {
            self.engage_count.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                unabsorbed = s.unabsorbed_bytes_instance,
                lag_secs = s.absorb_lag_secs,
                "maintenance backpressure ENGAGED ({}); new appends will be \
                 refused with a retryable 503 until the backlog drains",
                cause.map(Cause::as_str).unwrap_or("unknown"),
            );
        } else if !now && was {
            tracing::info!(
                unabsorbed = s.unabsorbed_bytes_instance,
                lag_secs = s.absorb_lag_secs,
                "maintenance backpressure released; accepting appends again",
            );
        }
        now
    }

    pub fn stats_json(&self) -> serde_json::Value {
        let cause = self.engaged();
        serde_json::json!({
            // Instance-machine state. `engaged`/`cause` keep their names
            // for existing scripts; the instance_* aliases make the
            // two-machine split explicit next to shards_engaged (which
            // /v1/debug/load emits from the per-engine flags).
            "engaged": cause.is_some(),
            "cause": cause.map(Cause::as_str),
            "instance_engaged": cause.is_some(),
            "instance_cause": cause.map(Cause::as_str),
            "engage_count": self.engage_count.load(Ordering::Relaxed),
            "appends_shed": self.shed_count.load(Ordering::Relaxed),
            "unabsorbed_bytes": self.last_unabsorbed.load(Ordering::Relaxed),
            "absorb_lag_secs": self.last_lag_secs.load(Ordering::Relaxed),
        })
    }
}

/// Per-shard admission, evaluated AFTER the request's owner and shard are
/// resolved (R24-B).
///
/// Two properties this placement buys, which a global middleware check
/// could not:
///
///   * A NON-OWNER never sheds. The maintenance mirror only holds shards
///     this instance currently owns, so a shard we do not hold reports no
///     backlog and the request falls through to the normal ownership
///     replay — instead of a stale 503 about a backlog that belongs to
///     someone else.
///   * Only the OFFENDING shard's appends are refused. Streams on other
///     shards of the same instance keep being served.
///
/// R27-1: two INDEPENDENT machines compose here — the caller's global
/// latch (instance bytes / lag) first, then this engine's shard latch.
/// A shard over its byte line can therefore never mask a simultaneous
/// global violation, and a shard holding above its release line cannot
/// pin the global state.
pub fn admit(
    engine: &crate::shard::ShardEngine,
    global: &GlobalLatch,
    l: &Limits,
) -> Option<Cause> {
    // The instance-wide latch applies to everything we own.
    if let Some(c) = global.engaged() {
        return Some(c);
    }
    if l.unabsorbed_bytes_shard == 0 {
        return None;
    }
    // R25-C: the per-shard latch lives ON THE ENGINE, with the same
    // high/low hysteresis as the instance latch. No prefix-keyed global
    // map: ownership removal is automatic when the engine leaves
    // state.shards, and a former owner cannot latch a shard it no
    // longer holds.
    let m = engine.maintenance_snapshot();
    let release = l.unabsorbed_bytes_shard.saturating_mul(l.release_pct) / 100;
    let was = engine.maintenance_shard_shed.load(Ordering::Relaxed);
    let now = if was {
        m.unabsorbed_frame_bytes > release
    } else {
        m.unabsorbed_frame_bytes > l.unabsorbed_bytes_shard
    };
    if now != was {
        engine.maintenance_shard_shed.store(now, Ordering::Relaxed);
    }
    now.then_some(Cause::ShardBytes)
}

/// Snapshot the RESIDENT engines' maintenance state (R25-C, semantics
/// pinned in R26-6).
///
/// This iterates `state.shards` — the engines currently OPEN in this
/// process — which is exactly the resident-safety scope the limits
/// document: open engines are what consume this process's memory and
/// pipelines. An owned-but-cold shard is absent here BY DESIGN; its
/// durable ledger is loaded before it ever serves, so the per-shard
/// gate covers it on first access. Each engine's state leaves this
/// aggregate the moment the engine leaves `state.shards` — no
/// process-global map to go stale.
pub fn snapshot(state: &crate::http::AppState) -> Snapshot {
    let engines: Vec<std::sync::Arc<crate::shard::ShardEngine>> =
        state.shards.read().unwrap().values().cloned().collect();
    let now = crate::shard::now_ms();
    let mut total = 0u64;
    let mut max_stall = 0u64;
    for engine in engines {
        let m = engine.maintenance_snapshot();
        total = total.saturating_add(m.unabsorbed_frame_bytes);
        max_stall = max_stall.max(m.no_progress_secs(now));
    }
    Snapshot {
        unabsorbed_bytes_instance: total,
        // Time since durable maintenance PROGRESS, not oldest-record
        // age — under continuous traffic an oldest-record clock stays
        // permanently old even while absorption keeps up.
        absorb_lag_secs: max_stall,
    }
}

/// The process's configured limits, resolved once at startup.
pub fn limits() -> Limits {
    static L: std::sync::OnceLock<Limits> = std::sync::OnceLock::new();
    *L.get_or_init(Limits::from_env)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            unabsorbed_bytes_instance: 1000,
            unabsorbed_bytes_shard: 800,
            absorb_lag_secs: 100,
            release_pct: 75,
        }
    }

    #[test]
    fn engages_on_any_bound_and_releases_only_when_all_are_clear() {
        let l = limits();
        let quiet = Snapshot::default();
        assert_eq!(next_state(false, &quiet, &l), (false, None));

        // Any single bound engages.
        let hot = Snapshot {
            unabsorbed_bytes_instance: 1001,
            ..Default::default()
        };
        assert_eq!(next_state(false, &hot, &l), (true, Some(Cause::InstanceBytes)));
        let laggy = Snapshot {
            absorb_lag_secs: 101,
            ..Default::default()
        };
        assert_eq!(next_state(false, &laggy, &l), (true, Some(Cause::LagSecs)));

        // Engaged: dropping below the LIMIT is not enough, it must reach
        // the release line. This is the hysteresis that stops flapping.
        let between = Snapshot {
            unabsorbed_bytes_instance: 900, // < 1000 but > 750
            ..Default::default()
        };
        assert_eq!(
            next_state(true, &between, &l),
            (true, Some(Cause::InstanceBytes)),
            "released too early — this flaps at the threshold"
        );
        let clear = Snapshot {
            unabsorbed_bytes_instance: 700,
            ..Default::default()
        };
        assert_eq!(next_state(true, &clear, &l), (false, None));

        // One axis clear while another is pinned keeps it engaged.
        let mixed = Snapshot {
            unabsorbed_bytes_instance: 100,
            absorb_lag_secs: 99, // under the limit, over the 75 release
            ..Default::default()
        };
        assert_eq!(next_state(true, &mixed, &l), (true, Some(Cause::LagSecs)));
    }

    #[test]
    fn a_zero_limit_disables_that_bound_and_all_zero_disables_the_feature() {
        let l = Limits {
            unabsorbed_bytes_instance: 0,
            unabsorbed_bytes_shard: 0,
            absorb_lag_secs: 100,
            release_pct: 75,
        };
        let huge = Snapshot {
            unabsorbed_bytes_instance: u64::MAX,
            absorb_lag_secs: 1,
        };
        assert_eq!(
            next_state(false, &huge, &l),
            (false, None),
            "a disabled bound must not trip on any value"
        );

        let off = Limits {
            release_pct: 75,
            ..Default::default()
        };
        assert_eq!(next_state(false, &huge, &off), (false, None));
        assert_eq!(
            next_state(true, &huge, &off),
            (false, None),
            "turning every bound off must release, not latch forever"
        );
    }

    #[test]
    fn release_pct_100_still_releases() {
        let l = Limits {
            unabsorbed_bytes_instance: 1000,
            release_pct: 100,
            ..Default::default()
        };
        let at = Snapshot {
            unabsorbed_bytes_instance: 1000,
            ..Default::default()
        };
        assert_eq!(next_state(true, &at, &l), (false, None));
    }

    /// R27-1 regression: the global machine evaluates ONLY the global
    /// bounds. Before the split, a shard over its byte line while the
    /// instance was over its LAG line stored ShardBytes in the single
    /// cause slot, and the read side filtered ShardBytes to None — the
    /// LagSecs violation was silently masked for every other shard.
    /// With no shard quantity in the machine, the lag cause wins by
    /// construction, and the latch (isolated per instance) reports it.
    #[test]
    fn shard_pressure_cannot_mask_a_global_violation() {
        let l = limits();
        let latch = GlobalLatch::new();
        // Global lag violated. (In the old machine, a simultaneous
        // shard-over-bytes condition produced ShardBytes here.)
        let s = Snapshot {
            unabsorbed_bytes_instance: 0,
            absorb_lag_secs: 101,
        };
        assert!(latch.apply(&s, &l));
        assert_eq!(
            latch.engaged(),
            Some(Cause::LagSecs),
            "the global latch must report the GLOBAL cause, unmasked"
        );
        // Simultaneous instance-bytes violation: also never masked.
        let latch2 = GlobalLatch::new();
        let s2 = Snapshot {
            unabsorbed_bytes_instance: 1001,
            absorb_lag_secs: 0,
        };
        assert!(latch2.apply(&s2, &l));
        assert_eq!(latch2.engaged(), Some(Cause::InstanceBytes));
    }

    /// R27-1 regression (release side): a shard holding above ITS OWN
    /// release line must not pin the global latch. The global machine
    /// releases on its own bounds alone; the shard's hysteresis lives
    /// on the engine and is invisible here.
    #[test]
    fn release_is_not_pinned_by_shard_state() {
        let l = limits();
        let latch = GlobalLatch::new();
        latch.apply(
            &Snapshot {
                absorb_lag_secs: 101,
                ..Default::default()
            },
            &l,
        );
        assert!(latch.engaged().is_some());
        // Lag drains under the release line. (Any shard-over-release
        // condition has no representation here — that is the point.)
        let cleared = Snapshot {
            unabsorbed_bytes_instance: 0,
            absorb_lag_secs: 10,
        };
        assert!(!latch.apply(&cleared, &l));
        assert_eq!(latch.engaged(), None, "global latch pinned after its own bounds cleared");
        // And the inverse: lag still high keeps it engaged regardless
        // of anything shard-shaped happening elsewhere.
        let latch2 = GlobalLatch::new();
        latch2.apply(
            &Snapshot {
                absorb_lag_secs: 101,
                ..Default::default()
            },
            &l,
        );
        assert!(latch2.apply(
            &Snapshot {
                unabsorbed_bytes_instance: 0,
                absorb_lag_secs: 99, // under the limit, over the 75% release
            },
            &l
        ));
        assert_eq!(latch2.engaged(), Some(Cause::LagSecs));
    }
}
