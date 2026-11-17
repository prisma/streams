#![cfg(test)]
//! A split's start (S4-DESIGN §6.2 steps 0.1 and 0.2): its refusals, the version it splits from and
//! the home its intent names, and the engines its publish hands back. `refuse` answers before
//! `begin` reserves homes, so a refused start leaves the stored topology as it was.

use super::fixture_http::engine_shutdown;
use super::fixture_split::{Cell, cell};

/// **A split of a split's child starts from the version the first published and names the child's
/// own home, and each published child is the engine of its own prefix.** Leaf `00` splits into
/// `000` and `001` at version 2, its intent ended `Reclaimed`; a terminal intent holds nothing, so
/// a split of `000` starts: it answers version 2, and its intent, `Preparing`, names `000` at the
/// home the first split gave it (never home 0) and the version it saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_of_a_child_starts_from_the_published_version_at_the_childs_home() {
    let Cell { state, parent, .. } = cell().await;
    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-1", "inst-a", &*clock)
            .await
            .expect("the first split publishes");
    assert_eq!(published.version, 2);
    for (prefix, _, child) in &published.children {
        assert_eq!(&child.prefix, prefix, "the engine published for {prefix}");
    }
    let (_, home, child) = &published.children[0];
    assert!(*home > 0, "an operation-born home: {home}");
    let store = state.data_store.clone();
    let first = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(first.state, crate::topology::intent::State::Reclaimed);
    let started = crate::topology::split::start(&state.shards, child, "op-2", "inst-a")
        .await
        .map_err(|refused| refused.to_string())
        .expect("a split of the child starts");
    assert_eq!(started.version(), 2, "the version it splits from");
    let second = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        (second.op_id.as_str(), second.state, second.version_seen),
        ("op-2", crate::topology::intent::State::Preparing, 2)
    );
    assert_eq!(second.sources, [("000".to_string(), *home)]);
    drop(started);
    engine_shutdown(&state).await;
}

/// **A split under a live intent is refused `topology_busy` (`intent`) before it reserves homes.**
/// One split of `00` has started and holds the cell's intent, `Preparing`; a second start of the
/// same leaf is refused `Busy("intent")`, the stored topology is unchanged (no home CAS: its
/// `next_home` did not move), and the intent is still the first's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_under_a_live_intent_is_refused_busy() {
    let Cell { state, parent, .. } = cell().await;
    let first = crate::topology::split::start(&state.shards, &parent, "op-live", "inst-a")
        .await
        .map_err(|refused| refused.to_string())
        .expect("the first split starts");
    let store = state.data_store.clone();
    let (before, _) = crate::topology::store::read(&*store).await.unwrap();
    let refused = crate::topology::split::start(&state.shards, &parent, "op-late", "inst-a")
        .await
        .err()
        .expect("the second start is refused");
    assert!(
        matches!(refused, crate::topology::split::Refused::Busy("intent")),
        "{refused}"
    );
    let (after, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(after, before, "a refused start reserves no homes");
    let held = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    assert_eq!(
        (held.op_id.as_str(), held.state),
        ("op-live", crate::topology::intent::State::Preparing)
    );
    drop(first);
    engine_shutdown(&state).await;
}

/// **A split started with a birth names its actor for both children in the fleet's overrides and
/// its view** (GATE-DECISION-f2-children-on-actor A2; PR #158's survivor split.rs:292:13, a
/// `born_by` that drops its birth). Leaf `00` is split by `inst-a` with a birth over a fleet store
/// of its own: once the split published, the overrides document's entries are exactly `000` and
/// `001`, each naming `inst-a`, and the birth's ownership view holds the same two overrides. A
/// split whose birth was dropped leaves the document without an entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_started_with_a_birth_names_its_actor_for_both_children() {
    let Cell { state, parent, .. } = cell().await;
    let fleet_store = super::fixture_storage::mem();
    let fleet = crate::fleet::FleetRepository::new(Some(fleet_store));
    let ownership = fleet.ownership("inst-a".to_string());
    let birth =
        crate::topology::split::Birth::new(fleet.clone(), ownership.clone(), "inst-a".to_string());
    let started = crate::topology::split::start(&state.shards, &parent, "op-born", "inst-a")
        .await
        .map_err(|refused| refused.to_string())
        .expect("the split starts");
    let clock = state.runtime.clock.clone();
    let published = started
        .born_by(birth)
        .finish(&*clock)
        .await
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let children: Vec<String> = published
        .children
        .iter()
        .map(|(p, _, _)| p.clone())
        .collect();
    assert_eq!(children, ["000", "001"]);
    let (doc, _) = fleet.read_overrides().await.unwrap();
    let mut named: Vec<(String, String)> =
        doc.entries.into_iter().map(|(p, e)| (p, e.to)).collect();
    named.sort();
    let view: Vec<(String, String)> = ownership.view().overrides.into_iter().collect();
    let born = [("000", "inst-a"), ("001", "inst-a")].map(|(p, o)| (p.to_owned(), o.to_owned()));
    assert_eq!(
        (named, view),
        (born.to_vec(), born.to_vec()),
        "the children's entries"
    );
    engine_shutdown(&state).await;
}

/// **A published split carries its phases in order** (GATE-DECISION-meas149-rerun-10000-2026-11-17,
/// M). Leaf `00` split by `inst-a`: the published split's phases are the pause's, in order
/// (`flush`, `checkpoint`, the log reads' `snapshot`, `sentinel` and `intervals`, `plan`, `lists`,
/// `clones`, `'T'`, `births`, `cas`), then both children's opens (as each finished) and `drain`;
/// the pause the split reports is at least the sum of the pause's phases, and the published split
/// notes the rows and queues the pause read (F5's counts), which its log line prints after
/// the phases.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_published_split_carries_its_phases_in_order() {
    let Cell { state, parent, .. } = cell().await;
    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-ph", "inst-a", &*clock)
            .await
            .map_err(|refused| refused.to_string())
            .expect("the split publishes");
    let names: Vec<&str> = published.phases.iter().map(|(n, _)| n.as_str()).collect();
    let pause = [
        "flush",
        "checkpoint",
        "snapshot",
        "sentinel",
        "intervals",
        "plan",
        "lists",
        "clones",
        "'T'",
        "births",
        "cas",
    ];
    assert_eq!(names.len(), pause.len() + 3, "{names:?}");
    assert_eq!(names[..pause.len()], pause, "the pause's phases");
    let mut opens = names[pause.len()..pause.len() + 2].to_vec();
    opens.sort_unstable();
    assert_eq!(opens, ["open 000", "open 001"], "the children's opens");
    assert_eq!(names[pause.len() + 2], "drain");
    let paused: u64 = published.phases[..pause.len()]
        .iter()
        .map(|(_, ms)| ms)
        .sum();
    let reported = u64::try_from(published.pause.as_millis()).unwrap();
    assert!(
        reported + 1 >= paused,
        "pause {reported} ms, its phases {paused} ms"
    );
    let notes: Vec<&str> = published.notes.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(notes, ["rows", "heads"], "the pause's read counts");
    let shown = crate::topology::split::shown_phases(&published);
    let heads = format!(" heads={}", published.notes[1].1);
    assert!(
        shown.starts_with("flush=") && shown.contains(" drain=") && shown.ends_with(&heads),
        "the log line's phases then counts: {shown}"
    );
    assert!(
        published.notes[0].1 > 0,
        "the sentinel rows read: {:?}",
        published.notes
    );
    engine_shutdown(&state).await;
}
