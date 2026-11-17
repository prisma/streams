#![cfg(test)]
//! **A planned move hands off giver first** (meas149-rerun F6; S5-ADDENDUM-F6 rev 2,
//! GATE-DECISION-f6-addendum-2026-11-17). The rerun's case: the split's actor `streams-1` holds
//! both children by their born overrides; once those age past `REBALANCE_RETURN_SECS`,
//! return-home gives one child back to its ring home `streams-2`, while appends to both children
//! arrive at both members (the router spreads them), on a store 20 ms away, under the rig's
//! in-flight cap of 512. Before F6 the giver serves the child until its next tick, the target's
//! first routed request opens it, and that open fences the giver's live writer: its in-flight
//! appends answer `503 shard_moving` (the raw surface's `outcome_unknown`).

use super::billing_closure_debts::{JSON, stored};
use super::fixture_requests::hreq;
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_birth::{Pair, age_the_children, overrides_of, split_with_a_held_append};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const BOUND: Duration = Duration::from_secs(30);

/// How long a planned move may hold an append (`shard::transaction::barrier::HOLD`).
const HOLD: Duration = Duration::from_secs(2);

/// Appenders per stream and member: enough that a commit group is in flight at any moment.
const LANES: usize = 2;

/// A stream created through `addr` (served by `state`) whose route the cell gives to the leaf
/// `leaf`: the first `h-<n>` after `taken`.
async fn stream_in(
    state: &Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
    leaf: &str,
    taken: &mut usize,
) -> String {
    loop {
        *taken += 1;
        assert!(*taken < 400, "no stream lands in {leaf}");
        let name = format!("h-{taken}");
        created(state, addr, &name).await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        if state.shards.prefix_for(&desc.resolve_segment("").shard_route) == leaf {
            return name;
        }
    }
}

/// Create `name` through `addr`, retrying a `503` (a shard still settling after the cell's boot)
/// for up to 10 s, and printing what each refusal met.
async fn created(state: &crate::http::AppState, addr: std::net::SocketAddr, name: &str) {
    let path = format!("/v1/stream/{name}");
    let t0 = Instant::now();
    loop {
        let (status, _, body) = hreq(addr, "PUT", &path, &[JSON], b"").await;
        if status == 201 {
            return;
        }
        let leaves: Vec<_> = LEAVES
            .iter()
            .map(|p| {
                let shards = &state.shards;
                let hold = shards.handing_off(p);
                (p, state.ownership.effective_owner(p), shards.is_open(p), hold, shards.describe_for_test(p))
            })
            .collect();
        eprintln!("HANDOFF create {name}: {status} {} {leaves:?}", String::from_utf8_lossy(&body));
        assert!(status == 503 && t0.elapsed() < BOUND / 3, "create {name}: {status}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// One acked append: the member that acked it and when.
struct Ack {
    n: u32,
    by: String,
    at: Instant,
}

/// What the appenders saw, per stream.
#[derive(Default)]
struct Tally {
    acked: BTreeMap<String, Vec<Ack>>,
    /// Every final answer but `204`, by (stream, status, code).
    refused: BTreeMap<(String, u16, String), u32>,
    /// `409 not_ring_owner` answers replayed to the owner they named, as the SDK does (#264).
    replayed: u32,
}

/// The cell's two members by name and address, for the appenders.
type Members = [(&'static str, std::net::SocketAddr); 2];

/// Append `[{"n": <n>}]` to `stream` through `members[lane % 2]` until `stop`, numbering from
/// `next`.
async fn appender(
    members: Members,
    stream: String,
    lane: usize,
    next: Arc<AtomicU32>,
    (stop, tally): (Arc<AtomicBool>, Arc<Mutex<Tally>>),
) {
    let mut turn = lane;
    while !stop.load(Ordering::SeqCst) {
        let first = members[turn % 2];
        turn += 1;
        let n = next.fetch_add(1, Ordering::SeqCst);
        let (status, by, code, replays) = append_following(members, first, &stream, n).await;
        let mut tally = tally.lock().unwrap();
        tally.replayed += replays;
        if status == 204 {
            let ack = Ack { n, by, at: Instant::now() };
            tally.acked.entry(stream.clone()).or_default().push(ack);
        } else {
            *tally.refused.entry((stream.clone(), status, code)).or_default() += 1;
        }
    }
}

/// Append `[{"n": <n>}]` to `stream` through `first`, replaying a `409 not_ring_owner` to the
/// member its `Streams-Replay-To` names, at most three times, as the SDK does (#264). The final
/// status, the member that answered it (`Streams-Forwarded-To` if it forwarded), the error code,
/// and how many replays it took.
async fn append_following(
    members: Members,
    first: (&'static str, std::net::SocketAddr),
    stream: &str,
    n: u32,
) -> (u16, String, String, u32) {
    let path = format!("/v1/stream/{stream}");
    let body = format!(r#"[{{"n":{n}}}]"#);
    let (mut name, mut addr) = first;
    let mut replays = 0;
    loop {
        let (status, headers, answer) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
        let code = serde_json::from_slice::<serde_json::Value>(&answer)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
            .unwrap_or_default();
        let owner = headers.get("streams-replay-to").cloned();
        let next = owner.and_then(|o| members.iter().copied().find(|(m, _)| *m == o));
        match next {
            Some(member) if status == 409 && replays < 3 => {
                replays += 1;
                (name, addr) = member;
            }
            _ => {
                let by = headers.get("streams-forwarded-to").cloned();
                return (status, by.unwrap_or(name.to_string()), code, replays);
            }
        }
    }
}

/// Load each of `streams` with about 4 MiB of unflushed appends (records of 4 KiB): the memtable a
/// served shard carries into a move.
async fn preload(addr: std::net::SocketAddr, streams: &[String]) {
    let record = format!(r#"[{{"pad":"{}"}}]"#, "x".repeat(4_000));
    for stream in streams {
        let path = format!("/v1/stream/{stream}");
        let sends = (0..1_024).map(|_| hreq(addr, "POST", &path, &[JSON], record.as_bytes()));
        use futures_util::StreamExt;
        let mut sends = futures_util::stream::iter(sends).buffer_unordered(32);
        while let Some((status, _, body)) = sends.next().await {
            assert_eq!(status, 204, "preload: {}", String::from_utf8_lossy(&body));
        }
    }
}

/// The `n`s stored in `stream`, read through `addr`.
async fn stored_ns(addr: std::net::SocketAddr, stream: &str) -> Vec<u32> {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{stream}"), &[], b"").await;
    assert_eq!(status, 200, "{stream}: {}", String::from_utf8_lossy(&body));
    let records: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap_or_default();
    let n = |r: &serde_json::Value| r["n"].as_u64().and_then(|n| u32::try_from(n).ok());
    let mut ns: Vec<u32> = records.iter().filter_map(n).collect();
    ns.sort_unstable();
    ns
}

/// **A returned child moves without fencing its writer** (F6, red first). T5's cell, loaded and
/// aged past the return window, with appends to both children arriving at both members. Asserts
/// that the giver's engine of the returned child closes without `Fenced`, that no append is
/// refused (no `503`, so no `outcome_unknown`, and no `429` shed), that every acked append is
/// stored once and nothing unacked is, and that from the giver's last ack of the returned
/// child's stream to the target's first is under `HOLD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_returned_child_moves_without_fencing_its_writer() {
    let pair = Pair::loaded().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let mut taken = 0;
    let streams = [
        stream_in(&pair.a.state, pair.a.addr, "0", &mut taken).await,
        stream_in(&pair.a.state, pair.a.addr, "1", &mut taken).await,
    ];
    preload(pair.a.addr, &streams).await;
    let engines = ["0", "1"].map(|c| pair.a.state.shards.open(c).expect("the actor holds both"));
    let members: Members = [("streams-1", pair.a.addr), ("streams-2", pair.b.addr)];
    let (stop, tally) = (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(Tally::default())));
    let lanes = streams.iter().flat_map(|stream| {
        let next = Arc::new(AtomicU32::new(0));
        (0..2 * LANES).map(move |lane| (stream.clone(), lane, next.clone()))
    });
    let appenders = lanes.map(|(stream, lane, next)| {
        appender(members, stream, lane, next, (stop.clone(), tally.clone()))
    });
    let scenario = async {
        age_the_children(&pair.a.state).await;
        let returned = |doc: &crate::fleet::Overrides| {
            doc.pending_events.iter().any(|e| e.event_type == "rebalance_return")
        };
        let t0 = Instant::now();
        while !returned(&overrides_of(&pair.a).await) {
            assert!(t0.elapsed() < BOUND, "a child returns home");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let t0 = Instant::now();
        while !["0", "1"].iter().any(|c| pair.b.state.shards.is_open(c)) {
            assert!(t0.elapsed() < BOUND, "the target opens the returned child");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        stop.store(true, Ordering::SeqCst);
    };
    futures_util::future::join(futures_util::future::join_all(appenders), scenario).await;
    for (rig, name) in [(&pair.a, "a"), (&pair.b, "b")] {
        let st = &rig.state;
        let leaves = st.shards.prefixes().shards.shards.clone();
        let owners: Vec<_> = ["", "0", "1"].iter().map(|p| (p, st.ownership.effective_owner(p), st.shards.is_open(p))).collect();
        eprintln!("HANDOFF {name} leaves={leaves:?} owners={owners:?} overrides={:?}", st.ownership.overrides());
    }
    let gone = usize::from(!pair.b.state.shards.is_open("0"));
    let reason = engines[gone].db.subscribe().borrow().close_reason;
    assert!(
        !matches!(reason, Some(slatedb::CloseReason::Fenced)),
        "the giver's engine of {gone} closes without a fence: {reason:?}"
    );
    let fenced = pair.a.state.fleet.standing().handoffs_fenced();
    assert_eq!(fenced, 0, "no handoff fell back to a takeover by fence");
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    let acked: usize = tally.acked.values().map(Vec::len).sum();
    eprintln!("HANDOFF acked={acked} replayed={} refused={:?}", tally.replayed, tally.refused);
    assert_eq!(tally.refused, BTreeMap::new(), "no append is refused across the move");
    assert_handed(&tally.acked[&streams[gone]], &pair.a.state);
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|a| a.n).collect();
        acked.sort_unstable();
        assert_eq!(stored_ns(pair.a.addr, stream).await, acked, "{stream}: stored = acked");
    }
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// The count-balance cell's leaves: the ring gives 00 and 01 to `streams-1`, 10 and 11 to
/// `streams-2` (`fleet_count_balance`).
const LEAVES: [&str; 4] = ["00", "01", "10", "11"];

/// The rig's in-flight cap of 512 and a ring that keeps both members for the scenario.
fn balance_members(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
    cli.admit_max_inflight = 512;
}

/// `name` of the count-balance cell over `fleet` and `data`, its fleet started.
async fn balance_member(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    name: &str,
    runtime: RigRuntime,
) -> HttpRig {
    let options = HttpRigOptions {
        fleet_store: Some(fleet.clone()),
        instance: Some(name.to_string()),
        prefixes: LEAVES.iter().map(|leaf| (*leaf).to_string()).collect(),
        cli: balance_members,
        ..Default::default()
    };
    let rig = http_rig_build(data.clone(), runtime, options).await;
    assert!(crate::fleet::start_configured(rig.state.clone(), &rig.tasks));
    rig
}

/// The count-balance cell: `streams-1` and `streams-2` over one data store 20 ms away, the four
/// [`LEAVES`], and 10 and 11 overridden to `streams-1`, older than the move cooldown and younger
/// than return-home's window. `streams-1` holds four of a share of two, so its count balance moves
/// 00, which no override names, to `streams-2` once its boot cooldown has run.
async fn balance_cell() -> (HttpRig, HttpRig) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    let fleet = mem();
    let put = |path: &'static str, body: String| {
        let fleet = fleet.clone();
        async move { fleet.put(&Path::from(path), PutPayload::from(body)).await.unwrap() }
    };
    put("fleet/desired.json", r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.into()).await;
    let aged = crate::shard::now_ms() - 120_000;
    let entry = |to: &str| crate::fleet::OverrideEntry { to: to.to_string(), ms: aged };
    let mut overrides = crate::fleet::Overrides::default();
    for leaf in ["10", "11"] {
        overrides.entries.insert(leaf.to_string(), entry("streams-1"));
    }
    put("fleet/overrides.json", serde_json::to_string(&overrides).unwrap()).await;
    let plan = FaultPlan { latency_pct: 100, latency_ms: (20, 20), ..FaultPlan::CLEAN };
    let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0xF6C, plan);
    // streams-1 first: a member alone in its first view would own 10 and 11 by the void overrides
    // and open them, and streams-1's later open would fence it.
    let a = balance_member(&fleet, &data, "streams-1", RigRuntime::first()).await;
    let b = balance_member(&fleet, &data, "streams-2", RigRuntime::incarnation(1)).await;
    let urls = serde_json::json!({
        "streams-1": format!("http://{}", a.addr),
        "streams-2": format!("http://{}", b.addr),
    });
    put("fleet/urls.json", urls.to_string()).await;
    let t0 = Instant::now();
    while [&a, &b].iter().any(|rig| rig.state.ownership.ring_active().len() < 2) {
        assert!(t0.elapsed() < BOUND, "both views name both members");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (a, b)
}

/// **A count-balance move does not fence its writer** (F6, red first). [`balance_cell`]'s 00,
/// loaded, with appends to it arriving at both members, moved by `streams-1`'s count balance to
/// `streams-2`. The same assertions as the returned child's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_count_balance_move_does_not_fence_its_writer() {
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let stream = stream_in(&a.state, a.addr, "00", &mut taken).await;
    preload(a.addr, std::slice::from_ref(&stream)).await;
    let giver = a.state.shards.open("00").expect("streams-1 holds 00");
    let members: Members = [("streams-1", a.addr), ("streams-2", b.addr)];
    let (stop, tally) = (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(Tally::default())));
    let next = Arc::new(AtomicU32::new(0));
    let appenders = (0..2 * LANES).map(|lane| {
        appender(members, stream.clone(), lane, next.clone(), (stop.clone(), tally.clone()))
    });
    let scenario = async {
        // Count balance waits its own move cooldown from its boot.
        a.clock.advance_monotonic(Duration::from_secs(61));
        let t0 = Instant::now();
        while !b.state.shards.is_open("00") {
            assert!(t0.elapsed() < BOUND, "count balance moves 00 to streams-2");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        stop.store(true, Ordering::SeqCst);
    };
    futures_util::future::join(futures_util::future::join_all(appenders), scenario).await;
    let reason = giver.db.subscribe().borrow().close_reason;
    assert!(!matches!(reason, Some(slatedb::CloseReason::Fenced)), "{reason:?}");
    assert_eq!(a.state.fleet.standing().handoffs_fenced(), 0, "no takeover by fence");
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    eprintln!("HANDOFF balance replayed={} refused={:?}", tally.replayed, tally.refused);
    assert_eq!(tally.refused, BTreeMap::new(), "no append is refused across the move");
    assert_handed(&tally.acked[&stream], &a.state);
    let mut acked: Vec<u32> = tally.acked[&stream].iter().map(|a| a.n).collect();
    acked.sort_unstable();
    assert_eq!(stored_ns(a.addr, &stream).await, acked, "stored = acked");
    eprintln!("HANDOFF shutdown begins");
    for rig in [a, b] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// Both members acked `acks`, and the target's first ack followed the giver's last within `HOLD`.
/// Prints the giver's phases (park, flush, close, park to release) and the release to the target's
/// first ack: the two spans the ruling bounds at 0.6 s and 0.9 s.
fn assert_handed(acks: &[Ack], giver: &crate::http::AppState) {
    let last_by_giver = acks.iter().filter(|a| a.by == "streams-1").map(|a| a.at).max();
    let first_by_target = acks.iter().filter(|a| a.by == "streams-2").map(|a| a.at).min();
    let (Some(last), Some(first)) = (last_by_giver, first_by_target) else {
        panic!("both members acked the moved shard's stream");
    };
    let gap = first.saturating_duration_since(last);
    eprintln!("HANDOFF gap={gap:?} (giver's last ack to target's first)");
    let (phases, released) = giver.fleet.standing().last_handoff().expect("the giver handed off");
    let after = first.saturating_duration_since(released);
    eprintln!(
        "HANDOFF phases park={:?} flush={:?} close={:?} park_to_release={:?} release_to_first_ack={after:?}",
        phases.park, phases.flush, phases.close, phases.release
    );
    assert!(gap < HOLD, "giver's last ack to target's first: {gap:?}");
}
