#![cfg(test)]
//! A stopped ordinal boots again while its peer still holds its old beat (GATE-DECISION-meas-full-
//! forward-wedge-2026-11-19, Addendum 20 (a); §22). The 3a diagnostic leg's 20,000 scale-out:
//! `streams-2`, the ordinal stopped at the scale-in before, booted again and saw itself at its
//! first tick, while `streams-1` went on judging the draining beat it held (a held member is read
//! again on one read in `PEER_READ_TICKS`). The two views disagreed: each member opened the shards
//! its own view gave it and fenced the other, whose view named itself, with no yield.
//!
//! §22's join turns it around: the newcomer owns nothing until coverage and its CAS, so this pins
//! both halves of that interleaving gone (no fence on either member, no yield at the tick).

use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::topology_policy::store_topology;
use crate::dst::fault_store::{FaultProfile, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// How long a fleet step (a view, an open, a fence) may take: several 2 s ticks.
const BOUND: Duration = Duration::from_secs(20);

/// The cell's leaves: a ring of both members gives both to `streams-2`.
const LEAVES: [&str; 2] = ["0", "1"];

/// No shrink is published before an hour, so the count stays at two.
fn two_members(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
}

/// Member `name` of the cell over `data` and `fleet`, its fleet started.
async fn member(
    data: &Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        runtime,
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some(name.to_string()),
            cli: two_members,
            prefixes: LEAVES.iter().map(|leaf| (*leaf).to_string()).collect(),
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

/// The fleet store as the scale-in left it: a desired count of two, and `streams-2`'s last beat,
/// draining, of the boot that stopped, two minutes old.
async fn after_the_scale_in() -> Arc<dyn ObjectStore> {
    let fleet = mem();
    let desired = r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    let beat = format!(
        r#"{{"instance":"streams-2","boot_id":"stopped","ts_ms":{},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[],"draining":true,"url":""}}"#,
        crate::shard::now_ms() - 120_000
    );
    for (path, body) in [
        ("fleet/desired.json", desired.to_string()),
        ("fleet/streams-2.json", beat),
    ] {
        fleet
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
    fleet
}

/// Wait, bounded, until `done` holds; `what` names the step.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let t0 = tokio::time::Instant::now();
    while !done() {
        assert!(t0.elapsed() < BOUND, "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A stream whose route lands in the leaf `0`, created at `rig`: its path.
async fn stream_in_0(rig: &HttpRig) -> String {
    use super::billing_closure_debts::{JSON, raw_put, stored};
    for taken in 0..200 {
        let name = format!("rejoin-{taken}");
        raw_put(rig.addr, &name, &[JSON], b"").await;
        let desc = stored(&rig.state, &rig.state.deployment.raw_adapter_sref(&name)).await;
        let route = desc.resolve_segment("").shard_route;
        if rig.state.shards.prefix_for(&route) == "0" {
            return format!("/v1/stream/{name}");
        }
    }
    panic!("no stream lands in 0");
}

/// Append to `path` at each of `rigs` in turn until `fenced` holds; every status is ignored (a
/// fenced write is answered as the fence's).
async fn append_until(what: &str, path: &str, rigs: &[&HttpRig], fenced: impl Fn() -> bool) {
    let json = [("content-type", "application/json")];
    let t0 = tokio::time::Instant::now();
    while !fenced() {
        assert!(t0.elapsed() < BOUND, "{what}");
        for rig in rigs {
            let _ = hreq(rig.addr, "POST", path, &json, br#"[{"n":1}]"#).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **A stopped ordinal that boots again moves nothing** (Addendum 20 (a), as the join turns it; the
/// join ruling §1 item 7). `streams-1` holds `streams-2`'s draining beat of the stopped boot, so its
/// view is the ring of itself, and it serves `0`. Its read of `streams-2`'s beat is held from then
/// on (a held member is read on one read in `PEER_READ_TICKS`; the hold keeps that read from
/// landing). `streams-2` boots again: its listing, if any, is not its own boot's, and its previous
/// beat is draining, so it is a joiner and owns nothing until its join CAS, which waits for
/// `streams-1`'s pins (C5, C7, C13). `0` then reaches it only by a handoff, giver first. Both halves
/// of the old interleaving are gone: no engine is fenced ahead on either member, and `streams-1`
/// never yields `0` at its tick (it closes `0` only as a handoff's giver).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_stopped_ordinal_that_boots_again_moves_nothing() {
    let (data, fleet) = (mem(), after_the_scale_in().await);
    let held = FaultStore::new(fleet.clone(), 0x2020, FaultProfile::clean());
    let a = member(&data, held.clone(), "streams-1", RigRuntime::first()).await;
    store_topology(&a).await;
    let owner = |rig: &HttpRig| rig.state.ownership.effective_owner("0");
    until("streams-1's view is the ring of itself", || {
        a.state.ownership.ring_active() == ["streams-1"]
            && owner(&a).as_deref() == Some("streams-1")
    })
    .await;
    let path = stream_in_0(&a).await;
    append_until("streams-1 opens 0", &path, &[&a], || {
        a.state.shards.is_open("0")
    })
    .await;
    let engaged = held.hold_class_under(
        StoreOp::Get,
        ObjClass::Fleet,
        "fleet/streams-2.json",
        u64::MAX,
    );
    let b = member(&data, fleet, "streams-2", RigRuntime::incarnation(1)).await;
    b.state
        .peer
        .set_peer("streams-1", &format!("http://{}", a.addr));
    let json = [("content-type", "application/json")];
    let handed = |rig: &HttpRig| rig.state.fleet.standing().last_handoff().is_some();
    let mut yielded = false;
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(10) {
        for rig in [&a, &b] {
            let _ = hreq(rig.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
        }
        // A tick yield: streams-1 lost `0` with no handoff of its own behind it.
        yielded |= !a.state.shards.is_open("0") && !handed(&a);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let fenced = |rig: &HttpRig| rig.state.shards.fenced_ahead();
    let read_held = engaged.load(std::sync::atomic::Ordering::SeqCst) > 0;
    held.release_hold();
    assert_eq!(
        (fenced(&a), fenced(&b), yielded, read_held),
        (0, 0, false, true)
    );
    for rig in [&a, &b] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}
