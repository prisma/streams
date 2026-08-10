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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

/// Thresholds. Zero disables an individual bound.
#[derive(Clone, Copy, Debug, Default)]
pub struct Limits {
    pub unabsorbed_bytes_instance: u64,
    pub unabsorbed_bytes_shard: u64,
    pub absorb_lag_secs: u64,
    pub replay_bytes: u64,
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
            replay_bytes: v("MAX_REPLAY_BYTES", 512 * 1024 * 1024),
            release_pct: v("MAINT_BACKPRESSURE_RELEASE_PCT", 75).min(100),
        }
    }

    fn any_enabled(&self) -> bool {
        self.unabsorbed_bytes_instance > 0
            || self.unabsorbed_bytes_shard > 0
            || self.absorb_lag_secs > 0
            || self.replay_bytes > 0
    }
}

/// What the evaluator saw. Kept as plain data so the decision is a pure
/// function that can be tested without an engine.
#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub unabsorbed_bytes_instance: u64,
    pub unabsorbed_bytes_max_shard: u64,
    pub absorb_lag_secs: u64,
    pub replay_bytes: u64,
}

/// Which bound tripped. Ordered by how actionable it is for an operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    InstanceBytes,
    ShardBytes,
    LagSecs,
    ReplayBytes,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::InstanceBytes => "unabsorbed bytes on this instance",
            Cause::ShardBytes => "unabsorbed bytes on one shard",
            Cause::LagSecs => "absorb lag",
            Cause::ReplayBytes => "projected replay bytes",
        }
    }
    fn code(self) -> u8 {
        match self {
            Cause::InstanceBytes => 1,
            Cause::ShardBytes => 2,
            Cause::LagSecs => 3,
            Cause::ReplayBytes => 4,
        }
    }
    fn from_code(c: u8) -> Option<Self> {
        Some(match c {
            1 => Cause::InstanceBytes,
            2 => Cause::ShardBytes,
            3 => Cause::LagSecs,
            4 => Cause::ReplayBytes,
            _ => return None,
        })
    }
}

/// The state machine, as a pure function.
///
/// Engage when ANY enabled bound is exceeded. Release only when EVERY
/// enabled bound is back under its release threshold — a backlog that
/// drains on one axis while another stays pinned is still a backlog.
pub fn next_state(engaged: bool, s: &Snapshot, l: &Limits) -> (bool, Option<Cause>) {
    if !l.any_enabled() {
        return (false, None);
    }
    let pairs = [
        (Cause::InstanceBytes, s.unabsorbed_bytes_instance, l.unabsorbed_bytes_instance),
        (Cause::ShardBytes, s.unabsorbed_bytes_max_shard, l.unabsorbed_bytes_shard),
        (Cause::LagSecs, s.absorb_lag_secs, l.absorb_lag_secs),
        (Cause::ReplayBytes, s.replay_bytes, l.replay_bytes),
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

static ENGAGED: AtomicBool = AtomicBool::new(false);
static CAUSE: AtomicU8 = AtomicU8::new(0);
/// Times the latch went from released to engaged (not per-request).
pub static ENGAGE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Appends refused while engaged.
pub static SHED_COUNT: AtomicU64 = AtomicU64::new(0);
/// Last observed snapshot fields, for /v1/debug/load.
pub static LAST_UNABSORBED: AtomicU64 = AtomicU64::new(0);
pub static LAST_LAG_SECS: AtomicU64 = AtomicU64::new(0);

/// Cheap hot-path read: `Some(cause)` when appends must be refused.
pub fn engaged() -> Option<Cause> {
    if !ENGAGED.load(Ordering::Relaxed) {
        return None;
    }
    Cause::from_code(CAUSE.load(Ordering::Relaxed))
}

pub fn note_shed() {
    SHED_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Apply an evaluated snapshot. Returns the new engaged state.
pub fn apply(s: &Snapshot, l: &Limits) -> bool {
    let was = ENGAGED.load(Ordering::Relaxed);
    let (now, cause) = next_state(was, s, l);
    LAST_UNABSORBED.store(s.unabsorbed_bytes_instance, Ordering::Relaxed);
    LAST_LAG_SECS.store(s.absorb_lag_secs, Ordering::Relaxed);
    CAUSE.store(cause.map(Cause::code).unwrap_or(0), Ordering::Relaxed);
    ENGAGED.store(now, Ordering::Relaxed);
    if now && !was {
        ENGAGE_COUNT.fetch_add(1, Ordering::Relaxed);
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

/// Read the live counters into a snapshot.
pub fn observe() -> Snapshot {
    let ingest = crate::history::INGEST_BYTES_TOTAL.load(Ordering::Relaxed);
    let absorbed = crate::history::ABSORB_BYTES_TOTAL.load(Ordering::Relaxed);
    let unabsorbed = ingest.saturating_sub(absorbed);
    let lag = crate::usage::absorb_lag_all()
        .into_iter()
        .map(|(_, s)| s)
        .max()
        .unwrap_or(0);
    Snapshot {
        unabsorbed_bytes_instance: unabsorbed,
        // Per-shard attribution uses the absorber's own pending summary
        // where it exists; the instance total is the conservative
        // fallback, since one shard can never hold more than all of them.
        unabsorbed_bytes_max_shard: crate::usage::max_shard_deferred_bytes().max(0),
        absorb_lag_secs: lag,
        // Replay cost is dominated by what must be re-read from the
        // shard log on open, which is exactly the unabsorbed tail.
        replay_bytes: unabsorbed,
    }
}

pub fn stats_json() -> serde_json::Value {
    let cause = engaged();
    serde_json::json!({
        "engaged": cause.is_some(),
        "cause": cause.map(Cause::as_str),
        "engage_count": ENGAGE_COUNT.load(Ordering::Relaxed),
        "appends_shed": SHED_COUNT.load(Ordering::Relaxed),
        "unabsorbed_bytes": LAST_UNABSORBED.load(Ordering::Relaxed),
        "absorb_lag_secs": LAST_LAG_SECS.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
pub fn reset_for_tests() {
    ENGAGED.store(false, Ordering::Relaxed);
    CAUSE.store(0, Ordering::Relaxed);
    ENGAGE_COUNT.store(0, Ordering::Relaxed);
    SHED_COUNT.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            unabsorbed_bytes_instance: 1000,
            unabsorbed_bytes_shard: 800,
            absorb_lag_secs: 100,
            replay_bytes: 0, // disabled
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
            replay_bytes: 0,
            release_pct: 75,
        };
        let huge = Snapshot {
            unabsorbed_bytes_instance: u64::MAX,
            unabsorbed_bytes_max_shard: u64::MAX,
            absorb_lag_secs: 1,
            replay_bytes: u64::MAX,
            ..Default::default()
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

    /// The blast radius of backpressure. Shedding the wrong request
    /// class turns a recoverable overload into an unrecoverable one:
    /// no consumer drain, no delete, no ownership move.
    #[test]
    fn only_record_appends_are_refused() {
        use axum::http::Method;
        let is = crate::http::is_append_request;

        // Refused: the three ways to append new records.
        assert!(is(&Method::POST, "/v1/streams/orders/records"));
        assert!(is(&Method::POST, "/v1/streams/orders:batch"));
        assert!(is(&Method::POST, "/v1/stream/orders"));
        assert!(is(&Method::POST, "/v1/streams/a.b-c_d/records"));

        // Admitted: consumers must keep draining.
        assert!(!is(&Method::POST, "/v1/streams/orders:pull"));
        assert!(!is(&Method::POST, "/v1/streams/orders:settle"));

        // Admitted: control plane must stay reachable to REPAIR the
        // overload — this is the property that keeps it recoverable.
        assert!(!is(&Method::PUT, "/v1/streams/orders"));
        assert!(!is(&Method::DELETE, "/v1/streams/orders"));
        assert!(!is(&Method::POST, "/v1/streams/orders:seal"));

        // Admitted: reads, scans, operator and platform surfaces.
        assert!(!is(&Method::GET, "/v1/streams/orders/records"));
        assert!(!is(&Method::GET, "/v1/streams/orders:scan"));
        assert!(!is(&Method::GET, "/health"));
        assert!(!is(&Method::POST, "/v1/segments"));
        assert!(!is(&Method::POST, "/operator/anything"));
        assert!(!is(&Method::POST, "/v1/debug/absorb"));

        // A colon name inside a stream name must not read as a verb.
        assert!(is(&Method::POST, "/v1/streams/a:b/records"));
        // Raw route with a sub-path is not the raw append.
        assert!(!is(&Method::POST, "/v1/stream/orders/records"));
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
}
