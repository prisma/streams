#![cfg(test)]
//! The join (§22 step 5; GATE-DECISION-s22-addendum-2026-11-19 C5-C8, C13, §9; GATE-DECISION-
//! tla049-r1-r3-2026-11-20 R1-R3, H1): a scale-out moves nothing. The newcomer enters owning
//! nothing, by one CAS of the overrides document once every leaf it would win is pinned to its
//! holder; the holders pin; each pinned leaf then moves to its new home giver first (J5, C8). These
//! are the addendum's DSTs 1, 3, 4, 5 and 9, test 7 as replaced, CE2 to CE4, 2.1's restart after a
//! planned drain (R1's boot ids), the idle fleet's write count (C16), and the join ruling's
//! Addendum 6 (a leaf is returned only by its holder).
//!
//! Every case asserts the same net: no engine fenced ahead on any member (`fenced_ahead`), no
//! planned move ended by a fence (`handoffs_fenced`), and each member's held leaves left it only by
//! a handoff (the giver's last `handoff` line moved it).

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::fault_store::{FaultProfile, FaultStore};
use crate::dst::trace_store::{StoreTraceEvent, TraceOutcome, TraceStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// How long a fleet step (a view, a handoff, a serve) may take: several 2 s ticks.
pub(super) const BOUND: Duration = Duration::from_secs(30);

/// How long a wait for a join into a running cell to land may take (GATE-DECISION-s22-join-2026-11-20
/// Addendum 38 and its ruling): at least three times the worst such landing the gate's own suite
/// run showed, 6026 ms (6021 ms under a CPU hog), so 3.3x. Not [`BOUND`]: a decision that keeps
/// every joiner out (#183's TIMEOUT mutants: `r3` as `false`, `uncovered`'s `!=` as `==`,
/// `of_joiner` as `Default`) fails each of these waits, and at `BOUND` the join DSTs together
/// outlasted cargo-mutants' limit, while the handoff and serve waits keep `BOUND`'s slack. A fresh
/// cell's first join (both members booting) is a cold join and waits [`BOUND`]: its worst was
/// 10.1 s, so 3.0x; the cold join over a slow store (`fleet_join_boot`) keeps its own bound.
pub(super) const JOIN_BOUND: Duration = Duration::from_secs(20);

#[path = "fleet_join_foreign.rs"]
mod foreign;

/// The cell's leaves: four, so every member of up to five wins some.
pub(super) const LEAVES: [&str; 4] = ["00", "01", "10", "11"];

/// Only the test moves the count: no shrink before an hour, and no growth at all. The controller
/// sizes the count from the cores its members report, which in a DST are the loaded test host's,
/// and it grows at once on that (no sustain window): a rise makes the holders pin for ordinals the
/// scenario never boots (the join's C7). A maximum of one bounds every rise; the counts the test
/// writes stand, since a count above the maximum is lowered only by a shrink, held for an hour.
fn held(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
    cli.fleet_max = 1;
    // No idle merge: the topology policy would co-locate and merge the quiet pairs, whose
    // co-location entries are writes of the overrides document no membership step makes.
    cli.shard_merge_sustain_secs = 3_600;
}

/// Member `name` over `data` and `fleet`, its fleet started.
pub(super) async fn member(
    data: &Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
) -> HttpRig {
    member_parked(data, fleet, (name, runtime), None).await
}

/// [`member`], its shard opener parked on `park` while the test holds it (the rig's `open_park`).
async fn member_parked(
    data: &Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    (name, runtime): (&str, RigRuntime),
    park: Option<Arc<tokio::sync::Mutex<()>>>,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        runtime,
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some(name.to_string()),
            cli: held,
            prefixes: LEAVES.iter().map(|leaf| (*leaf).to_string()).collect(),
            open_park: park,
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

pub(super) async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Publish desired.json with `count` at `epoch`, as a member's tick would.
pub(super) async fn desired(fleet: &Arc<dyn ObjectStore>, count: u64, epoch: u64) {
    let body = format!(r#"{{"count":{count},"epoch":{epoch},"reason":"test","computed_at_ms":0}}"#);
    let path = Path::from("fleet/desired.json");
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
}

/// The stored document: its members as (name, boot id) and its entries as (leaf, to).
pub(super) async fn stored(
    fleet: &Arc<dyn ObjectStore>,
) -> (Vec<(String, Option<String>)>, Vec<(String, String)>) {
    let Ok(got) = fleet.get(&Path::from("fleet/overrides.json")).await else {
        return (Vec::new(), Vec::new());
    };
    let doc: crate::fleet::Overrides = serde_json::from_slice(&got.bytes().await.unwrap()).unwrap();
    let members = doc.members.unwrap_or_default();
    let mut entries: Vec<(String, String)> = doc
        .entries
        .into_iter()
        .map(|(leaf, e)| (leaf, e.to))
        .collect();
    entries.sort();
    (
        members.into_iter().map(|m| (m.name, m.boot_id)).collect(),
        entries,
    )
}

/// The names listed.
pub(super) async fn listed(fleet: &Arc<dyn ObjectStore>) -> Vec<String> {
    stored(fleet)
        .await
        .0
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Wait, bounded by [`BOUND`], until `done` holds; `what` names the step.
pub(super) async fn until(what: &str, done: impl AsyncFnMut() -> bool) {
    within(BOUND, what, done).await;
}

/// Wait, bounded by [`JOIN_BOUND`], until a join lands (`done` holds); `what` names it.
pub(super) async fn until_joined(what: &str, done: impl AsyncFnMut() -> bool) {
    within(JOIN_BOUND, what, done).await;
}

async fn within(bound: Duration, what: &str, mut done: impl AsyncFnMut() -> bool) {
    let t0 = tokio::time::Instant::now();
    while !done().await {
        assert!(t0.elapsed() < bound, "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Open every leaf `rig` owns, as a loaded member would have them.
pub(super) async fn open_owned(rig: &HttpRig) {
    for leaf in LEAVES {
        if rig.state.ownership.effective_owner(leaf).as_deref()
            == Some(rig.state.config.cli.instance_name.as_str())
        {
            let opened = rig
                .state
                .shards
                .open_or_wait(leaf, Duration::from_secs(5))
                .await;
            assert!(
                matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
                "{leaf}"
            );
        }
    }
}

/// Ask `rig` to open each leaf it owns now, as a routed request would; an open that must wait
/// (a move's hold) is asked again on the next call.
async fn touch_owned(rig: &HttpRig) {
    let me = rig.state.config.cli.instance_name.as_str();
    let owned = LEAVES
        .iter()
        .filter(|leaf| rig.state.ownership.effective_owner(leaf).as_deref() == Some(me));
    for leaf in owned {
        drop(
            rig.state
                .shards
                .open_or_wait(leaf, Duration::from_millis(200))
                .await,
        );
    }
}

/// The net every case asserts: nothing fenced ahead, no planned move ended by a fence, on any of
/// `rigs`.
pub(super) fn nothing_fenced(rigs: &[&HttpRig]) -> Vec<(u64, u64)> {
    rigs.iter()
        .map(|rig| {
            (
                rig.state.shards.fenced_ahead(),
                rig.state.fleet.standing().handoffs_fenced(),
            )
        })
        .collect()
}

/// A holder's tick yields, counted (the join ruling, Addendum 27): the directory's retirements of a
/// shard because its ownership moved (`RetirementReason::OwnershipMoved`, logged "possession yields
/// at tick") on `rig` since the watch began, of any leaf. A planned move closes its engine itself
/// and is not one. (It judged by the overrides document's events before, an outbox the fleet
/// drains, so a clean handoff sampled after a drain read as a yield.)
struct YieldWatch {
    start: u64,
}

impl YieldWatch {
    fn of(rig: &HttpRig) -> Self {
        Self {
            start: rig.state.shards.tick_yields(),
        }
    }

    /// The tick yields on `rig` since the watch began.
    fn yields(&self, rig: &HttpRig) -> u64 {
        rig.state.shards.tick_yields() - self.start
    }

    /// The count and `rig`'s planned moves, for a failing assertion.
    fn report(&self, rig: &HttpRig) -> String {
        let standing = rig.state.fleet.standing();
        format!(
            "{} tick yields; handoffs recorded {}, ended by a fence {}, kept by H1 {}; the last {:?}",
            self.yields(rig),
            standing.handoffs_recorded(),
            standing.handoffs_fenced(),
            standing.h1_kept(),
            standing.last_handoff()
        )
    }
}

/// **A scale-out moves its shards without fencing** (the addendum's DST 1). `streams-1` alone,
/// listed, holds every leaf open; the count goes 1 to 2 and `streams-2` boots. `streams-1` pins
/// the leaves `streams-2` would win; `streams-2` joins owning nothing (its boot id listed); then
/// each pinned leaf moves to it giver first (J5) and `streams-2` serves it. Throughout, no engine
/// is fenced. Red with returns by a plain CAS (R-y, the join ruling, Addendum 29): `streams-2`
/// opens a won leaf while `streams-1` still holds it, and `streams-1`'s writer is fenced ahead.
/// That no leaf is yielded at a tick is
/// [`a_scale_out_to_a_slow_peer_moves_its_shards_without_a_yield`]'s, where a red can reach it
/// (Addendum 30): here the peer's open fences the holder before the holder's tick could yield.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_scale_out_moves_its_shards_without_fencing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    open_owned(&a).await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let won: Vec<&str> = LEAVES
        .iter()
        .copied()
        .filter(|leaf| both[crate::ownership::ring_pick(leaf, &both)] == "streams-2")
        .collect();
    assert!(!won.is_empty(), "streams-2 wins a leaf streams-1 holds");
    desired(&fleet, 2, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let t0 = tokio::time::Instant::now();
    while !won.iter().all(|leaf| b.state.shards.is_open(leaf)) {
        assert!(t0.elapsed() < BOUND, "streams-2 serves what it won");
        touch_owned(&b).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let boot = stored(&fleet).await.0[1].1.clone();
    assert_eq!(
        (boot.as_deref(), nothing_fenced(&[&a, &b])),
        (
            Some(b.state.runtime.identity.boot_id.as_str()),
            vec![(0, 0), (0, 0)]
        ),
        "(R1a: its boot id, fences)"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A scale-out to a slow peer moves its shards without a yield** (the join ruling, Addendum
/// 30): DST 1 with `streams-2`'s shard opens parked (the rig's `open_park`) until `streams-1`'s
/// tick has run twice after the won leaves left the overrides document. Each won leaf is handed off
/// giver first, so `streams-1` closed it before the document named `streams-2`, and its tick finds
/// nothing open to yield; nothing is fenced. Red with returns by a plain CAS (R-y): the entries
/// are dropped while `streams-1` still holds the leaves open, and its tick, the ring now giving
/// them to `streams-2`, yields them ("possession yields at tick"), the peer's opens still parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_scale_out_to_a_slow_peer_moves_its_shards_without_a_yield() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    open_owned(&a).await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let won: Vec<&str> = LEAVES
        .iter()
        .copied()
        .filter(|leaf| both[crate::ownership::ring_pick(leaf, &both)] == "streams-2")
        .collect();
    let watch = YieldWatch::of(&a);
    let park = Arc::new(tokio::sync::Mutex::new(()));
    let parked = park.clone().lock_owned().await;
    desired(&fleet, 2, 2).await;
    let peer = ("streams-2", RigRuntime::incarnation(1));
    let b = member_parked(&data, fleet.clone(), peer, Some(park)).await;
    until(
        "streams-2 is listed and the won leaves left the document",
        async || {
            let (members, entries) = stored(&fleet).await;
            let entered = |(leaf, _): &(String, String)| won.contains(&leaf.as_str());
            members.len() == 2 && !entries.iter().any(entered)
        },
    )
    .await;
    // Two of `streams-1`'s ticks with the peer's opens still parked.
    tokio::time::sleep(Duration::from_secs(5)).await;
    drop(parked);
    let t0 = tokio::time::Instant::now();
    while !won.iter().all(|leaf| b.state.shards.is_open(leaf)) {
        assert!(t0.elapsed() < BOUND, "streams-2 serves what it won");
        touch_owned(&b).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        (watch.yields(&a), nothing_fenced(&[&a, &b])),
        (0, vec![(0, 0), (0, 0)]),
        "(tick yields on streams-1, fences) with streams-2's opens parked: {}",
        watch.report(&a)
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A joiner waits for a holder that has not pinned** (DST 3). `streams-1`'s publication is held
/// between its read and its view (`pause_view_publication`), so it cannot pin. `streams-2`, within
/// the count, stays unlisted and out of every ring while the hold lasts; once released, `streams-1`
/// pins and `streams-2` joins. The model's `Witness_JoinerWaitsOnUnpinnedHolder`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_joiner_waits_for_a_holder_that_has_not_pinned() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    a.state
        .fleet
        .standing()
        .pause_view_publication(crate::topology::gate::PausePoint { reached, go });
    reached_rx.await.expect("streams-1's pass is held");
    desired(&fleet, 2, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let held = (listed(&fleet).await, b.state.ownership.ring_active());
    assert!(go_tx.send(()).is_ok());
    until_joined("streams-2 joins once pinned", async || {
        listed(&fleet).await.len() == 2
    })
    .await;
    assert_eq!(
        held,
        (vec!["streams-1".to_string()], vec!["streams-1".to_string()]),
        "(the listing, streams-2's ring) while streams-1 had not pinned"
    );
    assert_eq!(nothing_fenced(&[&a, &b]), [(0, 0), (0, 0)]);
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A joiner past its phase joins, and the unpinned move is counted** (DST 4). `streams-1`'s
/// writes of the overrides document fail on every pass (its fleet store refuses the PUT), so it
/// never pins. `streams-2` waits; its `JOIN_PHASE` clock is backdated past 139 s; it then joins
/// anyway, `joins_unpinned` is 1, and the leaves it took are crash class (E5): counted, not
/// asserted unfenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_joiner_past_its_phase_joins_and_the_unpinned_move_is_counted() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let a_fleet = FaultStore::new(fleet.clone(), 0x5224, FaultProfile::clean());
    let a = member(&data, a_fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    // streams-1's pin CAS never lands: its PUT of the document is held (its pass then waits).
    let refused = a_fleet.hold_class_under(
        StoreOp::Put,
        ObjClass::Fleet,
        "fleet/overrides.json",
        u64::MAX,
    );
    desired(&fleet, 2, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until("streams-2 finds something uncovered", async || {
        b.state.fleet.standing().join_phase_since().is_some()
    })
    .await;
    b.state
        .fleet
        .standing()
        .backdate_join_phase(crate::fleet::JOIN_PHASE);
    // The leaves streams-2 then takes are crash class (E5), counted: not asserted unfenced.
    until_joined("streams-2 joins past its phase", async || {
        listed(&fleet).await.len() == 2
    })
    .await;
    assert_eq!(b.state.fleet.standing().joins_unpinned(), 1);
    assert!(
        refused.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "streams-1 tried to pin"
    );
    a_fleet.release_hold();
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A restart in place rejoins without pins** (DST 5, C13, R1a). Both listed with their boot ids;
/// `streams-2` crashes (its last beat stays live, not withdrawn) and boots again within 12 s. Its previous beat is young, so it replaces its
/// boot id in one CAS: no entry is written, no forward hop, and it serves its leaves again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_restart_in_place_rejoins_without_pins() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until("both listed stamped", async || {
        let members = stored(&fleet).await.0;
        members.len() == 2 && members.iter().all(|(_, boot)| boot.is_some())
    })
    .await;
    // Setup only (the join ruling, Addendum 23 §2 (a)): the cold boot's pins return before the
    // entries are read, so the restart is measured against the converged document.
    super::fixture_members::joined(&fleet, &[&a, &b], BOUND).await;
    let entries = stored(&fleet).await.1;
    stop(b).await;
    // A crash, not an orderly stop: the stopped boot's last beat stays live, not withdrawn.
    previous_beat(&fleet, "streams-2", 500).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(2),
    )
    .await;
    let boot = b.state.runtime.identity.boot_id.clone();
    until("its boot id replaced in place", async || {
        listed_boot(&fleet, "streams-2").await == Some(Some(boot.clone()))
    })
    .await;
    assert_eq!(stored(&fleet).await.1, entries, "no pin written");
    assert_eq!(nothing_fenced(&[&a, &b]), [(0, 0), (0, 0)]);
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A count that drops while a member joins moves nothing** (test 7 as replaced). `streams-2`'s
/// publication is held between its coverage read and its CAS; the count drops to one; the CAS
/// lands (the document did not change) and lists `streams-2` beyond the count; the next pass of
/// `streams-1` unlists it (the interim's removal half). It owned nothing, so nothing moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_count_that_drops_while_a_member_joins_moves_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1", "streams-2"] || listed(&fleet).await == ["streams-1"]
    })
    .await;
    open_owned(&a).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    b.state
        .fleet
        .standing()
        .pause_view_publication(crate::topology::gate::PausePoint { reached, go });
    reached_rx.await.expect("streams-2's pass is held");
    desired(&fleet, 1, 2).await;
    assert!(go_tx.send(()).is_ok());
    until("streams-2 is unlisted", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    assert_eq!(nothing_fenced(&[&a, &b]), [(0, 0), (0, 0)]);
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A scale-out by three pins each joiner apart** (DST 9; CE3's count). The count goes 2 to 5 in
/// one publish; three members boot. Each joins owning nothing, in any order, and every move of a
/// pinned leaf is a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn scale_out_by_three_pins_each_joiner_apart() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("two listed", async || listed(&fleet).await.len() == 2).await;
    open_owned(&a).await;
    open_owned(&b).await;
    desired(&fleet, 5, 2).await;
    let mut rigs = vec![a, b];
    for n in 3..=5 {
        let name = format!("streams-{n}");
        rigs.push(member(&data, fleet.clone(), &name, RigRuntime::incarnation(n)).await);
    }
    until_joined("all five listed", async || listed(&fleet).await.len() == 5).await;
    let all: Vec<&HttpRig> = rigs.iter().collect();
    assert!(nothing_fenced(&all).iter().all(|net| *net == (0, 0)));
    for rig in rigs {
        stop(rig).await;
    }
}

/// **A peer that refreshes entries before its next tick opens nothing** (CE4, C2). `streams-1`
/// returns a pinned leaf to the joiner `streams-2` giver first; `streams-3`'s forward refreshes the
/// document before its own next tick. Its refresh pairs the new document's entries with that
/// document's ring (`Standing::ring_of`), so in its view the leaf is `streams-2`'s and it opens
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_peer_that_refreshes_before_its_tick_opens_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let c = member(
        &data,
        fleet.clone(),
        "streams-3",
        RigRuntime::incarnation(3),
    )
    .await;
    until("streams-1 listed", async || {
        listed(&fleet).await.contains(&"streams-1".to_string())
    })
    .await;
    open_owned(&a).await;
    desired(&fleet, 3, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("streams-2 joins", async || {
        listed(&fleet).await.contains(&"streams-2".to_string())
    })
    .await;
    crate::fleet::view::refreshed(&c.state)
        .await
        .expect("streams-3 refreshes");
    let opened = LEAVES
        .iter()
        .filter(|leaf| c.state.shards.is_open(leaf))
        .count();
    assert_eq!(
        (opened, nothing_fenced(&[&a, &b, &c])),
        (0, vec![(0, 0); 3])
    );
    for rig in [a, b, c] {
        stop(rig).await;
    }
}

/// **A restart after a planned drain rejoins as a joiner, and a count drop inside its peers'
/// re-read interval moves nothing** (GATE-DECISION-tla049-r1-r3 §2.1, R1's boot ids). `streams-2`
/// drains (removed from `members`), stops, and boots again: a joiner. It joins with its new boot
/// id while `streams-1` still holds its draining beat of the old boot; the count then drops. That
/// held beat is of another boot, so it is no evidence against the new listing: `streams-1` keeps
/// `streams-2` in its ring, drops no pin as a no-op, and `streams-2` serves what it was handed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_restart_after_a_planned_drain_rejoins_and_a_count_drop_moves_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    // Both joined with their own boot ids, not only listed: a fresh cell's first boot lists every
    // counted ordinal unstamped (C17), so a count of two names would pass before any join, and a
    // drain of a boot that never joined runs out its whole budget (#183's TIMEOUTs).
    super::fixture_members::joined(&fleet, &[&a, &b], BOUND).await;
    open_owned(&a).await;
    // A handoff's bound, not the drain's whole budget (as `fleet_drain`'s drains): a drain that
    // cannot hand off fails here within [`BOUND`] (#183's `in_ring` TIMEOUTs ran out the budget).
    let drained = crate::fleet::drain::drain(&b.state, "streams-2", BOUND);
    assert!(
        matches!(drained.await, crate::fleet::drain::DrainOutcome::HandedOff),
        "a planned drain"
    );
    stop(b).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(2),
    )
    .await;
    until_joined("streams-2 rejoins", async || {
        listed(&fleet).await.len() == 2
    })
    .await;
    desired(&fleet, 1, 2).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(nothing_fenced(&[&a, &b]), [(0, 0), (0, 0)]);
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **An idle fleet's write count is unchanged** (C16). Two settled members, idle for ten passes:
/// no PUT of the overrides document lands (a pass writes only a pin, a stamp, a decision or a
/// join), and the only extra read is each boot's one previous-beat GET.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_idle_fleet_writes_no_membership() {
    let (data, fleet) = (mem(), TraceStore::verbatim(mem()));
    let fleet_store: Arc<dyn ObjectStore> = fleet.clone();
    desired(&fleet_store, 2, 1).await;
    let (first, second) = (RigRuntime::first(), RigRuntime::incarnation(1));
    // Setup only (the join ruling, Addendum 23 §2 (b)): both listed with their boot ids before
    // they boot, so the cell starts converged; the cold join's writes are
    // `fleet_join_boot::a_cold_join_over_a_20_ms_store_converges`'s.
    let seeded = [("streams-1", &first), ("streams-2", &second)];
    super::fixture_members::seed_members(&fleet_store, &seeded).await;
    let a = member(&data, fleet_store.clone(), "streams-1", first).await;
    let b = member(&data, fleet_store.clone(), "streams-2", second).await;
    until("both listed", async || {
        listed(&fleet_store).await.len() == 2
    })
    .await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let landed = |events: &[StoreTraceEvent]| {
        let put = |event: &&StoreTraceEvent| {
            event.op == StoreOp::Put
                && event.path == "fleet/overrides.json"
                && matches!(event.outcome, TraceOutcome::Ok)
        };
        events.iter().filter(put).count()
    };
    let before = landed(&fleet.events());
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert_eq!(
        landed(&fleet.events()),
        before,
        "an idle pass writes nothing"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// Seed `name`'s previous beat, `age_ms` old, as the process before this boot left it.
pub(super) async fn previous_beat(fleet: &Arc<dyn ObjectStore>, name: &str, age_ms: i64) {
    let ts = crate::shard::now_ms() - age_ms;
    let body = format!(
        r#"{{"instance":"{name}","boot_id":"before","ts_ms":{ts},"rps":0.0,"owned_shards":[],"draining":false}}"#
    );
    let path = Path::from(format!("fleet/{name}.json"));
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
}

/// **A split child's no-op entry, then a scale-out** (CE2). `streams-1` alone holds every leaf and
/// leaf `x`'s entry names it, a no-op past the return window. The window is reached by the entry's
/// age (its stamp is an hour old), not by `REBALANCE_RETURN_SECS`: the return window is one
/// comparison of an entry's age against that setting, met here from the age's side (the join
/// ruling, Addendum 1). The count goes to two and `streams-2`
/// boots; over the ring with both ordinals `x`'s home is `streams-2`, so the entry is no no-op any
/// more (C6(b) counts the ordinals) and stays: it is `x`'s pin, `streams-2` joins covered, and `x`
/// moves to it by handoff, with no fence. Red with C6(b) over the ring alone (rev 1's no-op drop):
/// the entry is dropped, and `streams-2` is never listed; and with returns by a plain CAS (R-y, the
/// join ruling, Addendum 29): `streams-1` is fenced ahead. (It asserted no tick yield too; no red
/// makes this scenario yield, so that clause is gone: Addendum 29.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_childs_no_op_entry_then_a_scale_out_moves_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 1, 1).await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let x = LEAVES
        .iter()
        .find(|leaf| both[crate::ownership::ring_pick(leaf, &both)] == "streams-2")
        .expect("a leaf streams-2 wins");
    let a = member(&data, fleet.clone(), "streams-1", RigRuntime::first()).await;
    until("streams-1 is listed", async || {
        listed(&fleet).await == ["streams-1"]
    })
    .await;
    let (doc, version) = a.state.fleet.read_overrides().await.unwrap();
    let mut next = doc.clone();
    let aged = crate::shard::now_ms() - 3_600_000;
    next.entries.insert(
        (*x).to_string(),
        crate::fleet::OverrideEntry {
            to: "streams-1".into(),
            ms: aged,
        },
    );
    let body = serde_json::to_vec(&next).unwrap();
    let document = crate::fleet::FleetDocument::Overrides;
    assert!(
        a.state
            .fleet
            .replace_document(document, body, version)
            .await
    );
    open_owned(&a).await;
    desired(&fleet, 2, 2).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("streams-2 joins", async || listed(&fleet).await.len() == 2).await;
    // `x`'s return home drops its entry, so only a routed request opens it at `streams-2`.
    let t0 = tokio::time::Instant::now();
    while !b.state.shards.is_open(x) {
        assert!(t0.elapsed() < BOUND, "x moves to streams-2");
        touch_owned(&b).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        nothing_fenced(&[&a, &b]),
        [(0, 0), (0, 0)],
        "fences, as x moves to streams-2"
    );
    for rig in [a, b] {
        stop(rig).await;
    }
}

/// **A return held at its CAS during a scale-out by three** (CE3). Count 2 to 5; `streams-1`'s
/// next PUT of the overrides document (a pin, or the return of a pin to its home) is held while
/// the three join, then released. A return CAS that lands after another joiner's join was computed
/// against an older version and fails; the pin stays until a later pass returns it, giver first.
/// Every joiner joins owning nothing and nothing is fenced. Rev 1 removed the pin and a second
/// joiner's flip gave the leaf to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_return_held_at_its_cas_during_a_scale_out_by_three_moves_nothing() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 2, 1).await;
    let a_fleet = FaultStore::new(fleet.clone(), 0xCE3, FaultProfile::clean());
    let a = member(&data, a_fleet.clone(), "streams-1", RigRuntime::first()).await;
    let b = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
    )
    .await;
    until_joined("two listed", async || listed(&fleet).await.len() == 2).await;
    open_owned(&a).await;
    open_owned(&b).await;
    let engaged =
        a_fleet.hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides.json", 1);
    desired(&fleet, 5, 2).await;
    let mut rigs = vec![a, b];
    for n in 3..=5 {
        let name = format!("streams-{n}");
        rigs.push(member(&data, fleet.clone(), &name, RigRuntime::incarnation(n)).await);
    }
    until("streams-1's CAS is held", async || {
        engaged.load(std::sync::atomic::Ordering::SeqCst) > 0
    })
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    a_fleet.release_hold();
    until_joined("all five listed", async || listed(&fleet).await.len() == 5).await;
    let all: Vec<&HttpRig> = rigs.iter().collect();
    assert!(nothing_fenced(&all).iter().all(|net| *net == (0, 0)));
    for rig in rigs {
        stop(rig).await;
    }
}

/// Rewrite `leaf`'s entry in `rig`'s fleet document to name `to`, stamped `ms`, retrying a lost
/// CAS (the members' pins write the same document).
async fn entry_set(rig: &HttpRig, leaf: &str, to: &str, ms: i64) {
    let entry = crate::fleet::OverrideEntry { to: to.into(), ms };
    let document = crate::fleet::FleetDocument::Overrides;
    for _ in 0..50 {
        let (mut next, version) = rig.state.fleet.read_overrides().await.unwrap();
        next.entries.insert(leaf.to_string(), entry.clone());
        let body = serde_json::to_vec(&next).unwrap();
        if rig
            .state
            .fleet
            .replace_document(document, body, version)
            .await
        {
            return;
        }
    }
    panic!("the entry for {leaf} is written");
}

/// **A leaf is returned only by its holder** (the join ruling, Addendum 6; C6(a), C8). The count
/// is three, `streams-3` not yet booted; `streams-1` and `streams-2` join. Leaf `10` is
/// `streams-2`'s and its entry names it, an hour old, as a split child's entry names its actor:
/// over the ring with `streams-3`, `10`'s home is `streams-3`. `streams-2` holds `10` open and its
/// overrides PUTs are held; `streams-3` boots and joins, and `streams-1` and `streams-3` run their
/// passes for 6 s more. The entry stands throughout. Released, `streams-2` returns `10` to
/// `streams-3` by a handoff, giver first, and nothing is fenced. Red with C6(a)'s gate reverted
/// (R-g): a member the entry does not name drops it by a plain CAS (`streams-1`'s return home, or
/// `streams-3`'s no-op), so the entry does not stand and `10` is not handed off; and with returns
/// by a plain CAS (R-y, the join ruling, Addendum 29): `streams-2` is fenced. (It asserted no tick
/// yield too; no red makes this scenario yield, so that clause is gone: Addendum 29.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaf_named_by_an_entry_is_returned_only_by_its_holder() {
    let (data, fleet) = (mem(), mem());
    desired(&fleet, 3, 1).await;
    let b_fleet = FaultStore::new(fleet.clone(), 0xA6, FaultProfile::clean());
    let (first, second) = (RigRuntime::first(), RigRuntime::incarnation(1));
    // Setup only (the join ruling, Addendum 23 §2 (b)): `streams-1` and `streams-2` are listed with
    // their boot ids before they boot, so they start converged; `streams-3` joins as before.
    let seeded = [("streams-1", &first), ("streams-2", &second)];
    super::fixture_members::seed_members(&fleet, &seeded).await;
    let a = member(&data, fleet.clone(), "streams-1", first).await;
    let b = member(&data, b_fleet.clone(), "streams-2", second).await;
    // The count names `streams-3`, so `10`'s pin for it (C7) stands: wait for the listing and both
    // views, not for the join's convergence.
    super::fixture_members::published(&fleet, &[&a, &b], BOUND).await;
    let x = "10";
    entry_set(&b, x, "streams-2", crate::shard::now_ms() - 3_600_000).await;
    open_owned(&a).await;
    open_owned(&b).await;
    assert!(b.state.shards.is_open(x), "streams-2 holds 10");
    let before = b.state.fleet.standing().handoffs_recorded();
    let path = "fleet/overrides.json";
    b_fleet.hold_class_under(StoreOp::Put, ObjClass::Fleet, path, u64::MAX);
    let c = member(
        &data,
        fleet.clone(),
        "streams-3",
        RigRuntime::incarnation(2),
    )
    .await;
    until_joined("streams-3 joins", async || listed(&fleet).await.len() == 3).await;
    let named = (x.to_string(), "streams-2".to_string());
    let window = tokio::time::Instant::now() + Duration::from_secs(6);
    let mut stood = true;
    while tokio::time::Instant::now() < window {
        stood &= stored(&fleet).await.1.contains(&named);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    b_fleet.release_hold();
    let t0 = tokio::time::Instant::now();
    while !c.state.shards.is_open(x) {
        assert!(t0.elapsed() < BOUND, "streams-3 serves 10");
        touch_owned(&c).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let returned = b.state.fleet.standing().handoffs_recorded() > before;
    assert_eq!(
        (stood, returned, nothing_fenced(&[&a, &b, &c])),
        (true, true, vec![(0, 0); 3]),
        "(the entry stood while its holder could not write, its holder handed 10 off, fences)"
    );
    for rig in [a, b, c] {
        stop(rig).await;
    }
}

/// The boot id of `name`'s beat object as stored, if any.
pub(super) async fn beat_boot(fleet: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    let got = fleet
        .get(&Path::from(format!("fleet/{name}.json")))
        .await
        .ok()?;
    let beat: crate::fleet::Heartbeat = serde_json::from_slice(&got.bytes().await.ok()?).ok()?;
    Some(beat.boot_id)
}

/// `name`'s listed boot id in `fleet`'s document, if it is listed.
pub(super) async fn listed_boot(
    fleet: &Arc<dyn ObjectStore>,
    name: &str,
) -> Option<Option<String>> {
    let members = stored(fleet).await.0;
    members
        .into_iter()
        .find(|(listed, _)| listed == name)
        .map(|(_, boot)| boot)
}
