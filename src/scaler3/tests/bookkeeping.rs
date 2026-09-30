//! The scaler's bookkeeping outside an evaluation: what its debug rendering
//! shows, that an append under a stream's current incarnation leaves its
//! heat and hot key alone after one scan of each table, that appends
//! accumulate in one sketch while a fork chain's are never sketched, that
//! retiring segments drops exactly their sketches, and what it reports: its
//! hot keys in tenant order and the stats `/v1/debug/load` serves under
//! `scaler`.
#![cfg(test)]

use std::sync::Arc;

use super::{sketch, test_desc};
use crate::crypto::RoutingKeyHash;
use crate::registry::StreamDesc;
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

/// A fork, and a stream that has forks, stay single-segment: their appends
/// are never sketched, so no evaluation can split them. The same append to
/// an unforked stream is sketched.
#[test]
fn appends_to_a_fork_or_to_a_stream_with_forks_are_never_sketched() {
    let scaler = scaler();
    let plain = test_desc("plain");
    let mut fork = test_desc("fork").to_persisted();
    fork.forked_from = Some(crate::registry::ForkRef {
        source: "plain".into(),
        source_epoch: plain.stream_epoch.clone(),
        fork_offset: 0,
        fork_sub: 0,
        fork_id: "fork-1".into(),
    });
    let mut parent = test_desc("parent").to_persisted();
    parent.fork_children = vec!["fork-1".into()];
    let [fork, parent] = [fork, parent].map(|d| StreamDesc::try_from(d).unwrap());
    for desc in [&fork, &parent, &plain] {
        scaler.note_append(desc, &desc.resolve_segment("key"), 100, 1);
    }
    let keys: Vec<_> = scaler
        .state
        .lock()
        .unwrap()
        .sketches
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, [(plain.sref(), plain.resolve_segment("key").seg_id)]);
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

/// Hot keys come back one per stream in tenant order: by project, then by
/// name.
#[test]
fn hot_keys_are_listed_one_per_stream_by_project_then_name() {
    let scaler = scaler();
    let in_project = |project: &str, name: &str| {
        let mut desc = test_desc(name).to_persisted();
        desc.project_id = crate::tenant::ProjectId::new(project).unwrap();
        StreamDesc::try_from(desc).unwrap().sref()
    };
    let b_alpha = in_project("proj-b", "alpha");
    let a_zeta = in_project("proj-a", "zeta");
    let a_beta = in_project("proj-a", "beta");
    {
        let mut state = scaler.state.lock().unwrap();
        for (name, byte) in [(&b_alpha, 1), (&a_zeta, 2), (&a_beta, 3)] {
            state
                .hot_keys
                .insert(name.clone(), RoutingKeyHash([byte; 16]));
        }
    }
    assert_eq!(
        scaler.hot_keys_all(),
        [
            (a_beta, RoutingKeyHash([3; 16])),
            (a_zeta, RoutingKeyHash([2; 16])),
            (b_alpha, RoutingKeyHash([1; 16])),
        ]
    );
}

/// What `/v1/debug/load` serves under `scaler` (src/http.rs `debug_load`;
/// bench/costab/split-driver.py gates on its `segment_splits`): the six
/// process-wide counters by name, the sketch population, and each hot key
/// as `project/name:` and its first four bytes in hex, in tenant order.
/// Tests running beside this one move the counters, so each is pinned
/// between two reads of its own counter.
#[test]
fn the_stats_name_every_counter_the_sketch_count_and_each_hot_key() {
    use crate::scaler3::{
        INEFFECTIVE_SPLIT_AVOIDED, SEGMENT_MAP_REFRESHES, SEGMENT_MERGES, SEGMENT_SPLITS,
        SKETCH_EVICTIONS, UNTRACKED_APPENDS,
    };
    use std::sync::atomic::Ordering::Relaxed;
    let scaler = scaler();
    let [hot, busy] = ["hot", "busy"].map(|n| test_desc(n).sref());
    {
        let mut state = scaler.state.lock().unwrap();
        for seg in 0..3 {
            state
                .sketches
                .insert((busy.clone(), seg), sketch("epoch", 1, true, 1));
        }
        let mut key = [9; 16];
        key[..4].copy_from_slice(&[0xab, 0xcd, 0x01, 0x02]);
        state.hot_keys.insert(hot, RoutingKeyHash(key));
        state.hot_keys.insert(busy, RoutingKeyHash([0x10; 16]));
    }
    let counters = [
        ("segment_splits", &SEGMENT_SPLITS),
        ("segment_merges", &SEGMENT_MERGES),
        ("ineffective_split_avoided", &INEFFECTIVE_SPLIT_AVOIDED),
        ("segment_map_refreshes", &SEGMENT_MAP_REFRESHES),
        ("sketch_evictions", &SKETCH_EVICTIONS),
        ("untracked_appends", &UNTRACKED_APPENDS),
    ];
    let before = counters.map(|(_, counter)| counter.load(Relaxed));
    let stats = scaler.stats_json();
    let after = counters.map(|(_, counter)| counter.load(Relaxed));
    let mut want = serde_json::Map::new();
    want.insert("sketches".into(), 3.into());
    let hot_keys = vec!["proj-test/busy:10101010", "proj-test/hot:abcd0102"];
    want.insert("hot_keys".into(), hot_keys.into());
    for (i, (name, _)) in counters.iter().enumerate() {
        let read = stats[*name]
            .as_u64()
            .unwrap_or_else(|| panic!("{name} is not a counter in {stats}"));
        assert!(
            (before[i]..=after[i]).contains(&read),
            "{name} reads {read}; its counter read {} then {}",
            before[i],
            after[i]
        );
        want.insert((*name).into(), read.into());
    }
    assert_eq!(stats, serde_json::Value::Object(want));
}
