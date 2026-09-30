//! The scaler's shared test fixtures: a stored descriptor, segment sketches
//! at a chosen heat, and the stream names one evaluation chose. They live
//! beside the tests because src/scaler3.rs is at its 1,000-line ceiling.
#![cfg(test)]

use crate::registry::StreamDesc;
use crate::scaler3::{Decisions, ScalePolicy, SegSketch, State};
use crate::sketch::KeyDistribution;

pub(super) fn test_desc(name: &str) -> StreamDesc {
    crate::registry::PersistedDescriptor {
        seal_gen_counter: 0,
        account_id: None,
        project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
        name: name.into(),
        stream_epoch: "00000000000000000000000000000000".into(),
        key_fingerprint: "fp".into(),
        created_ms: 1,
        expires_at_ms: None,
        deleted: false,
        content_type: "application/json".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
    .try_into()
    .expect("valid descriptor fixture")
}

pub(super) fn sketch(epoch: &str, now: i64, hot: bool, key: u8) -> SegSketch {
    let mut dist = KeyDistribution::new(0, u64::MAX, ScalePolicy::default().rate_window_secs);
    dist.note(
        now,
        1,
        [key; 16],
        if hot { 1_000_000_000_000 } else { 1 },
        1,
    );
    SegSketch {
        epoch: epoch.into(),
        dist,
        hot_streak: ScalePolicy::default().hot_evals,
        cold_streak: 0,
        last_fed_ms: now,
    }
}

/// A hot segment the split rule accepts: keys on both sides of the median.
pub(super) fn splittable(epoch: &str, now: i64) -> SegSketch {
    let mut sk = sketch(epoch, now, true, 1);
    sk.dist
        .note(now, u64::MAX / 4 * 3, [2; 16], 1_000_000_000_000, 1);
    sk
}

/// Two cold segments of `name` whose streaks sit `short` evaluations
/// under the merge patience.
pub(super) fn cold_pair(
    s: &mut State,
    name: &crate::tenant::TenantStreamRef,
    now: i64,
    short: u32,
) {
    for seg in [0, 1] {
        let mut sk = sketch("epoch", now, false, 1);
        sk.cold_streak = ScalePolicy::default().hot_evals * 4 - short;
        s.sketches.insert((name.clone(), seg), sk);
    }
}

/// The streams one evaluation chose to split and to merge.
pub(super) fn chosen(d: &Decisions) -> (Vec<String>, Vec<String>) {
    let name = |r: &crate::tenant::TenantStreamRef| r.name().as_str().to_owned();
    let splits = d.0.iter().map(|x| name(&x.0)).collect();
    (splits, d.1.iter().map(|x| name(&x.0)).collect())
}
