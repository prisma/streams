//! The scaler's bookkeeping outside an evaluation: what its debug rendering
//! shows, that an append under a stream's current incarnation leaves its
//! heat and hot key alone after one scan of each table, that appends
//! accumulate in one sketch, and that retiring segments drops exactly their
//! sketches.
#![cfg(test)]

use std::sync::Arc;

use super::{sketch, test_desc};
use crate::crypto::RoutingKeyHash;
use crate::scaler3::{ScalePolicy, Scaler, State};

fn scaler() -> Scaler {
    Scaler::new(
        &ScalePolicy::default(),
        &crate::config::AdmissionConfig::default(),
        Arc::new(crate::runtime::ManualClock::at(1_000)),
    )
}

/// The rendering shows the policy the scaler runs and elides the rest.
#[test]
fn the_debug_rendering_names_the_policy() {
    assert_eq!(
        format!("{:?}", scaler()),
        format!("Scaler {{ policy: {:?}, .. }}", ScalePolicy::default())
    );
}

/// An append under the incarnation a stream's sketches already carry
/// changes nothing: its heat, its hot key and its cooldown stay, and
/// deciding so scans each table once (every sketch, since none matches,
/// and every cooldown).
#[test]
fn an_unchanged_incarnation_keeps_its_heat_and_hot_key_after_one_scan_of_each_table() {
    let [name, other] = ["steady", "other"].map(|n| test_desc(n).sref());
    let mut s = State::default();
    for n in [&name, &other] {
        s.sketches
            .insert((n.clone(), 0), sketch("epoch", 1, true, 1));
        s.last_transition_ms.insert((n.clone(), "epoch".into()), 1);
    }
    s.hot_keys.insert(name.clone(), RoutingKeyHash([1; 16]));
    s.forget_previous_incarnation(&name, "epoch");
    assert_eq!(s.hot_keys.get(&name), Some(&RoutingKeyHash([1; 16])));
    assert_eq!(s.sketches.len(), 2);
    assert_eq!(s.last_transition_ms.len(), 2);
    assert_eq!(
        s.incarnation_scan_entries, 4,
        "two sketches and two cooldowns, each visited once"
    );
}

/// Appends to one segment of one incarnation accumulate in its one sketch.
#[test]
fn appends_to_one_segment_accumulate_in_its_one_sketch() {
    let scaler = scaler();
    let desc = test_desc("steady");
    let seg = desc.resolve_segment("key");
    scaler.note_append(&desc, &seg, 100, 1);
    scaler.note_append(&desc, &seg, 100, 1);
    let state = scaler.state.lock().unwrap();
    let keys: Vec<_> = state.sketches.keys().cloned().collect();
    assert_eq!(keys, [(desc.sref(), seg.seg_id)]);
    assert_eq!(
        state.sketches[&(desc.sref(), seg.seg_id)]
            .dist
            .top_keys
            .total,
        200
    );
}

/// Retiring segments drops exactly their sketches: the stream's other
/// segments and other streams keep theirs.
#[test]
fn retiring_segments_drops_exactly_their_sketches() {
    let scaler = scaler();
    let [name, other] = ["retiring", "other"].map(|n| test_desc(n).sref());
    {
        let mut state = scaler.state.lock().unwrap();
        for seg in 0..3 {
            state
                .sketches
                .insert((name.clone(), seg), sketch("epoch", 1, true, 1));
        }
        state
            .sketches
            .insert((other.clone(), 0), sketch("epoch", 1, true, 1));
    }
    scaler.retire_segments(&name, &[0, 2]);
    let mut left: Vec<_> = scaler
        .state
        .lock()
        .unwrap()
        .sketches
        .keys()
        .cloned()
        .collect();
    left.sort_by(|a, b| (a.0.name().as_str(), a.1).cmp(&(b.0.name().as_str(), b.1)));
    assert_eq!(left, [(other, 0), (name, 1)]);
}
