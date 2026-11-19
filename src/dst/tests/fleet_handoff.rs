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
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_birth::{Pair, age_the_children, overrides_of, split_with_a_held_append};
use super::fleet_handoff_cases::ErrorLines;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

pub(super) const BOUND: Duration = Duration::from_secs(30);

/// How long a planned move may hold an append (`shard::transaction::barrier::HOLD`).
pub(super) const HOLD: Duration = Duration::from_secs(2);

/// Appenders per stream and member: enough that a commit group is in flight at any moment.
pub(super) const LANES: usize = 2;

/// A stream created through `addr` (served by `state`) whose route the cell gives to the leaf
/// `leaf`: the first `h-<n>` after `taken`.
pub(super) async fn stream_in(
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
        if state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == leaf
        {
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
        let (status, headers, body) = hreq(addr, "PUT", &path, &[JSON], b"").await;
        // 200: an earlier attempt that answered 503 had created it.
        if status == 201 || status == 200 {
            return;
        }
        let leaves: Vec<_> = LEAVES
            .iter()
            .map(|p| {
                let shards = &state.shards;
                let hold = shards.handing_off(p);
                (
                    p,
                    state.ownership.effective_owner(p),
                    shards.is_open(p),
                    hold,
                    shards.describe_for_test(p),
                )
            })
            .collect();
        let body = String::from_utf8_lossy(&body);
        eprintln!("HANDOFF create {name}: {status} {body} headers={headers:?} {leaves:?}");
        assert!(
            status == 503 && t0.elapsed() < BOUND / 3,
            "create {name}: {status}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// One acked append: the member that acked it and when.
pub(super) struct Ack {
    pub(super) n: u32,
    pub(super) by: String,
    pub(super) at: Instant,
}

/// What the appenders saw, per stream.
#[derive(Default)]
pub(super) struct Tally {
    pub(super) acked: BTreeMap<String, Vec<Ack>>,
    /// Every final answer but `204`, by (stream, status, code).
    pub(super) refused: BTreeMap<(String, u16, String), u32>,
    /// `409 not_ring_owner` answers replayed to the owner they named, as the SDK does (#264).
    pub(super) replayed: u32,
}

/// The cell's two members by name and address, for the appenders.
pub(super) type Members = [(&'static str, std::net::SocketAddr); 2];

/// Append `[{"n": <n>}]` to `stream` through `members[lane % 2]` until `stop`, numbering from
/// `next`.
pub(super) async fn appender(
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
            let ack = Ack {
                n,
                by,
                at: Instant::now(),
            };
            tally.acked.entry(stream.clone()).or_default().push(ack);
        } else {
            *tally
                .refused
                .entry((stream.clone(), status, code))
                .or_default() += 1;
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

/// The `n`s stored in `stream`, read through `addr` page after page (`stream-next-offset`) until a
/// page says `stream-up-to-date`: a stream the handoff DSTs load holds more than one read page.
pub(super) async fn stored_ns(addr: std::net::SocketAddr, stream: &str) -> Vec<u32> {
    let n = |r: &serde_json::Value| r["n"].as_u64().and_then(|n| u32::try_from(n).ok());
    let (mut ns, mut offset) = (Vec::new(), None::<String>);
    for _ in 0..256 {
        let path = offset.as_ref().map_or_else(
            || format!("/v1/stream/{stream}"),
            |at| format!("/v1/stream/{stream}?offset={at}"),
        );
        let (status, headers, body) = hreq(addr, "GET", &path, &[], b"").await;
        assert_eq!(status, 200, "{stream}: {}", String::from_utf8_lossy(&body));
        let records: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap_or_default();
        ns.extend(records.iter().filter_map(n));
        let next = headers.get("stream-next-offset").cloned();
        if headers.contains_key("stream-up-to-date") || next.is_none() || next == offset {
            ns.sort_unstable();
            return ns;
        }
        offset = next;
    }
    panic!("{stream}: no page said stream-up-to-date within 256 pages");
}

/// **The handoff DSTs' read of what a stream stored reads all of it** (`stored_ns`). A stream
/// holding 8,000 records, appended 500 a request, reads back exactly `0..8000`: more than one
/// byte-bounded page of the raw read (one held 4,096 of these; one held 3,072 of
/// `a_returned_child_moves_without_fencing_its_writer`'s when it compared that page with 3,620 acked).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn stored_ns_reads_every_page_of_a_stream() {
    let (_state, addr) = super::fixture_http::http_rig(mem()).await;
    super::billing_closure_debts::raw_put(addr, "pages", &[JSON], b"").await;
    for batch in 0..16_u32 {
        let records: Vec<String> = (batch * 500..(batch + 1) * 500)
            .map(|n| format!(r#"{{"n":{n}}}"#))
            .collect();
        let body = format!("[{}]", records.join(","));
        let (status, _, _) = hreq(addr, "POST", "/v1/stream/pages", &[JSON], body.as_bytes()).await;
        assert_eq!(status, 204, "batch {batch}");
    }
    assert_eq!(
        stored_ns(addr, "pages").await,
        (0..8_000).collect::<Vec<u32>>()
    );
}

/// **A returned child moves without fencing its writer** (F6, red first). T5's cell, loaded and
/// aged past the return window, with appends to both children arriving at both members. Asserts
/// that the giver's engine of the returned child closes without `Fenced`, that no append is
/// refused (no `503`, so no `outcome_unknown`, and no `429` shed), that every acked append is
/// stored once and nothing unacked is, and that from the giver's last ack of the returned
/// child's stream to the target's first is under `HOLD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_returned_child_moves_without_fencing_its_writer() {
    let errors = ErrorLines::install();
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
    let (stop, tally) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(Mutex::new(Tally::default())),
    );
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
            doc.pending_events
                .iter()
                .any(|e| e.event_type == "rebalance_return")
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
        let owners: Vec<_> = ["", "0", "1"]
            .iter()
            .map(|p| (p, st.ownership.effective_owner(p), st.shards.is_open(p)))
            .collect();
        eprintln!(
            "HANDOFF {name} leaves={leaves:?} owners={owners:?} overrides={:?}",
            st.ownership.overrides()
        );
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
    eprintln!(
        "HANDOFF acked={acked} replayed={} refused={:?}",
        tally.replayed, tally.refused
    );
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the move"
    );
    assert_handed(&tally.acked[&streams[gone]], &pair.a.state);
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|a| a.n).collect();
        acked.sort_unstable();
        assert_eq!(
            stored_ns(pair.a.addr, stream).await,
            acked,
            "{stream}: stored = acked"
        );
    }
    errors.report();
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// The count-balance cell's leaves: the ring gives 00 and 01 to `streams-1`, 10 and 11 to
/// `streams-2` (`fleet_count_balance`).
pub(super) const LEAVES: [&str; 4] = ["00", "01", "10", "11"];

/// The rig's in-flight cap of 512 and a ring that keeps both members for the scenario.
fn balance_members(cli: &mut crate::config::CliArgs) {
    cli.scale_in_secs = 3_600;
    cli.admit_max_inflight = 512;
}

/// `name` of the count-balance cell over `fleet` and `data`, its fleet started.
pub(super) async fn balance_member(
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
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

/// The count-balance cell: `streams-1` and `streams-2` over one data store 20 ms away, the four
/// [`LEAVES`], and 10 and 11 overridden to `streams-1`, older than the move cooldown and younger
/// than return-home's window. `streams-1` holds four of a share of two, so its count balance moves
/// 00, which no override names, to `streams-2` once its boot cooldown has run.
pub(super) async fn balance_cell() -> (HttpRig, HttpRig) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    let fleet = mem();
    let put = |path: &'static str, body: String| {
        let fleet = fleet.clone();
        async move {
            fleet
                .put(&Path::from(path), PutPayload::from(body))
                .await
                .unwrap()
        }
    };
    put(
        "fleet/desired.json",
        r#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.into(),
    )
    .await;
    let aged = crate::shard::now_ms() - 120_000;
    let entry = |to: &str| crate::fleet::OverrideEntry {
        to: to.to_string(),
        ms: aged,
    };
    let mut overrides = crate::fleet::Overrides::default();
    for leaf in ["10", "11"] {
        overrides
            .entries
            .insert(leaf.to_string(), entry("streams-1"));
    }
    put(
        "fleet/overrides.json",
        serde_json::to_string(&overrides).unwrap(),
    )
    .await;
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0xF6C, plan);
    // streams-1 first: a member alone in its first view would own 10 and 11 by the void overrides
    // and open them, and streams-1's later open would fence it.
    let a = balance_member(&fleet, &data, "streams-1", RigRuntime::first()).await;
    // streams-2 starts once streams-1's beat is stored: a first view of itself alone would give it
    // 10 and 11 (their overrides then name no member), and its eager open would fence streams-1.
    let t0 = Instant::now();
    while fleet
        .head(&Path::from("fleet/streams-1.json"))
        .await
        .is_err()
    {
        assert!(t0.elapsed() < BOUND, "streams-1 beats");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let b = balance_member(&fleet, &data, "streams-2", RigRuntime::incarnation(1)).await;
    let urls = serde_json::json!({
        "streams-1": format!("http://{}", a.addr),
        "streams-2": format!("http://{}", b.addr),
    });
    put("fleet/urls.json", urls.to_string()).await;
    let t0 = Instant::now();
    while [&a, &b]
        .iter()
        .any(|rig| rig.state.ownership.ring_active().len() < 2)
    {
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
    let errors = ErrorLines::install();
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let stream = stream_in(&a.state, a.addr, "00", &mut taken).await;
    preload(a.addr, std::slice::from_ref(&stream)).await;
    let giver = a.state.shards.open("00").expect("streams-1 holds 00");
    let members: Members = [("streams-1", a.addr), ("streams-2", b.addr)];
    let (stop, tally) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(Mutex::new(Tally::default())),
    );
    let next = Arc::new(AtomicU32::new(0));
    let appenders = (0..2 * LANES).map(|lane| {
        appender(
            members,
            stream.clone(),
            lane,
            next.clone(),
            (stop.clone(), tally.clone()),
        )
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
    assert!(
        !matches!(reason, Some(slatedb::CloseReason::Fenced)),
        "{reason:?}"
    );
    assert_eq!(
        a.state.fleet.standing().handoffs_fenced(),
        0,
        "no takeover by fence"
    );
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    eprintln!(
        "HANDOFF balance replayed={} refused={:?}",
        tally.replayed, tally.refused
    );
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the move"
    );
    assert_handed(&tally.acked[&stream], &a.state);
    let mut acked: Vec<u32> = tally.acked[&stream].iter().map(|a| a.n).collect();
    acked.sort_unstable();
    assert_eq!(stored_ns(a.addr, &stream).await, acked, "stored = acked");
    errors.report();
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
    let last_by_giver = acks
        .iter()
        .filter(|a| a.by == "streams-1")
        .map(|a| a.at)
        .max();
    let first_by_target = acks
        .iter()
        .filter(|a| a.by == "streams-2")
        .map(|a| a.at)
        .min();
    let (Some(last), Some(first)) = (last_by_giver, first_by_target) else {
        panic!("both members acked the moved shard's stream");
    };
    let gap = first.saturating_duration_since(last);
    eprintln!("HANDOFF gap={gap:?} (giver's last ack to target's first)");
    let (phases, released) = giver
        .fleet
        .standing()
        .last_handoff()
        .expect("the giver handed off");
    let after = first.saturating_duration_since(released);
    eprintln!(
        "HANDOFF phases park={:?} flush={:?} close={:?} park_to_release={:?} release_to_first_ack={after:?}",
        phases.park, phases.flush, phases.close, phases.release
    );
    assert!(gap < HOLD, "giver's last ack to target's first: {gap:?}");
    assert!(!phases.fenced, "the giver's close ended clean");
}

/// The returned child's cell after the move, with the target's mirror stale: `streams-2`'s fleet
/// tick read the overrides before the move and publishes that read after it (its view pause), so
/// for the next tick period it still names `streams-1` for the child `streams-1` handed it. The
/// child's index and a stream in it.
pub(super) async fn moved_with_a_stale_target(pair: &Pair) -> (usize, String) {
    let (status, _) = split_with_a_held_append(pair).await;
    assert_eq!(status, 204, "the held append");
    let mut taken = 0;
    let streams = [
        stream_in(&pair.a.state, pair.a.addr, "0", &mut taken).await,
        stream_in(&pair.a.state, pair.a.addr, "1", &mut taken).await,
    ];
    let (reached, at_read) = tokio::sync::oneshot::channel();
    let (release, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    pair.b.state.fleet.standing().pause_view_publication(point);
    tokio::time::timeout(BOUND, at_read)
        .await
        .expect("the target's tick reads")
        .unwrap();
    age_the_children(&pair.a.state).await;
    let a = &pair.a.state;
    let moved = || {
        ["0", "1"]
            .iter()
            .position(|c| a.ownership.effective_owner(c).as_deref() == Some("streams-2"))
    };
    let t0 = Instant::now();
    while moved().is_none() {
        assert!(t0.elapsed() < BOUND, "streams-1 hands a child off");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let gone = moved().unwrap_or_default();
    release.send(()).unwrap();
    let b = &pair.b.state;
    let child = ["0", "1"][gone];
    let t0 = Instant::now();
    while b.ownership.effective_owner(child).as_deref() != Some("streams-1") {
        assert!(
            t0.elapsed() < BOUND,
            "the target publishes its read from before the move"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (gone, streams[gone].clone())
}

/// **A relayed `409` refreshes the target's overrides once** (F6 step 8, the relayed-409 addendum
/// R6). An append to the stale target is forwarded to the giver, whose `409` names the target;
/// the target relays it, reads the overrides once on the way, and strips the version from the
/// client's answer (R4). The client's one replay to the target is acked, and the stream holds that
/// record once. Red without the relayed refresh: the replay is answered `409 not_ring_owner` again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_relayed_409_refreshes_the_targets_overrides_once() {
    let pair = Pair::loaded().await;
    let (_, stream) = moved_with_a_stale_target(&pair).await;
    let (b, b_addr) = (&pair.b.state, pair.b.addr);
    let reads = b.fleet.standing().tags().reads();
    let path = format!("/v1/stream/{stream}");
    let body = br#"[{"n":7}]"#;
    let (status, headers, _) = hreq(b_addr, "POST", &path, &[JSON], body).await;
    let first = (status, headers.get("streams-replay-to").cloned());
    assert_eq!(
        first,
        (409, Some("streams-2".to_string())),
        "the giver's 409, relayed"
    );
    assert!(
        !headers.contains_key("streams-overrides-version"),
        "R4: {headers:?}"
    );
    let read = b.fleet.standing().tags().reads() - reads;
    let (status, _, answer) = hreq(b_addr, "POST", &path, &[JSON], body).await;
    assert_eq!(
        status,
        204,
        "the replay: {}",
        String::from_utf8_lossy(&answer)
    );
    assert_eq!(read, 1, "one overrides read");
    assert_eq!(
        stored_ns(pair.a.addr, &stream).await,
        [7],
        "the record is stored once"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **A burst that sees one new version makes one read** (R1). 32 appends sent at once to the stale
/// target each meet the giver's relayed `409`; the target reads the overrides once, and every
/// append's replay is acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_burst_of_relayed_409s_makes_one_read() {
    let pair = Pair::loaded().await;
    let (_, stream) = moved_with_a_stale_target(&pair).await;
    let (b, b_addr) = (&pair.b.state, pair.b.addr);
    let reads = b.fleet.standing().tags().reads();
    let path = format!("/v1/stream/{stream}");
    let send = |n: u32| {
        let path = path.clone();
        async move {
            let body = format!(r#"[{{"n":{n}}}]"#);
            hreq(b_addr, "POST", &path, &[JSON], body.as_bytes())
                .await
                .0
        }
    };
    let first = futures_util::future::join_all((0..32).map(send)).await;
    assert!(first.iter().all(|s| *s == 409 || *s == 204), "{first:?}");
    assert_eq!(
        b.fleet.standing().tags().reads() - reads,
        1,
        "one read for the burst"
    );
    let replays = first
        .iter()
        .enumerate()
        .filter(|(_, s)| **s == 409)
        .map(|(n, _)| send(u32::try_from(n).expect("32 appends")));
    let replayed = futures_util::future::join_all(replays).await;
    assert!(replayed.iter().all(|s| *s == 204), "{replayed:?}");
    assert_eq!(
        stored_ns(pair.a.addr, &stream).await.len(),
        32,
        "each record stored once"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **Two alternating stale versions read at most once per floor** (R2). Forwards naming two
/// versions the member never holds, alternating, for one second: each read that learns nothing
/// holds the next off for 100 ms, so the member reads at most once per 100 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn alternating_stale_versions_read_at_most_once_per_floor() {
    let pair = Pair::loaded().await;
    let (b, b_addr) = (&pair.b.state, pair.b.addr);
    let forwarded = format!("v={}", b.shards.prefixes().shards.version);
    let reads = b.fleet.standing().tags().reads();
    let t0 = Instant::now();
    let mut sent = 0u32;
    while t0.elapsed() < Duration::from_secs(1) {
        let stale = if sent.is_multiple_of(2) {
            "stale-a"
        } else {
            "stale-b"
        };
        let headers = [
            ("streams-forwarded", forwarded.as_str()),
            ("streams-overrides-version", stale),
        ];
        hreq(b_addr, "GET", "/v1/stream/born", &headers, b"").await;
        sent += 1;
    }
    let read = b.fleet.standing().tags().reads() - reads;
    let elapsed = t0.elapsed();
    let most = u64::try_from(elapsed.as_millis() / 100).expect("ms") + 1;
    eprintln!("HANDOFF floor sent={sent} reads={read} in {elapsed:?}");
    assert!(
        sent > 20 && read >= 1 && read <= most,
        "{read} reads for {sent} forwards in {elapsed:?}"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// Age the cell's children past the return window and wait until `streams-1` has handed one off
/// and `streams-2` serves it, then 2 s more; then raise `stop`. The child moved.
async fn return_a_child(pair: &Pair, stop: &AtomicBool) -> usize {
    age_the_children(&pair.a.state).await;
    let t0 = Instant::now();
    while !["0", "1"].iter().any(|c| pair.b.state.shards.is_open(c)) {
        assert!(t0.elapsed() < BOUND, "the target opens the returned child");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    usize::from(!pair.b.state.shards.is_open("0"))
}

/// Every final read status, with its `Retry-After`, and how often it was answered.
type Seen = Arc<Mutex<BTreeMap<(u16, String), u32>>>;

/// Read `stream` through `members[lane % 2]` until `stop`, following a `409` to the member it names;
/// every final status, with its `Retry-After`.
async fn reader(
    members: Members,
    stream: String,
    lane: usize,
    (stop, seen): (Arc<AtomicBool>, Seen),
) {
    let path = format!("/v1/stream/{stream}");
    let mut turn = lane;
    while !stop.load(Ordering::SeqCst) {
        let mut addr = members[turn % 2].1;
        turn += 1;
        let mut answer = hreq(addr, "GET", &path, &[], b"").await;
        for _ in 0..3 {
            let owner = answer.1.get("streams-replay-to").cloned();
            let next = owner.and_then(|o| members.iter().copied().find(|(m, _)| *m == o));
            let Some(member) = next.filter(|_| answer.0 == 409) else {
                break;
            };
            addr = member.1;
            answer = hreq(addr, "GET", &path, &[], b"").await;
        }
        let tagged = answer.1.contains_key("streams-overrides-version");
        assert!(
            !tagged,
            "R4, A5: no client answer carries it: {:?}",
            answer.1
        );
        let retry = answer.1.get("retry-after").cloned().unwrap_or_default();
        *seen.lock().unwrap().entry((answer.0, retry)).or_default() += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// **Reads across a handoff are served or retried** (D2, E1). T5's cell, loaded; readers on both
/// children's streams at both members, and a light appender, while a child returns home. Every
/// read is served (`200`) or answered `503` with `Retry-After: 1` (a read in flight at the giver's
/// close, #259), never `500`; the latter are counted and stay a handful.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn reads_across_a_handoff_are_served_or_retried() {
    reads_across_a_handoff(true).await;
}

/// **Reads of a moved shard with no writer are served by its new owner** (D2, E1; the peer read
/// hop). As above with no appender, so nothing forwards to the target before the reads do: a
/// read either member relays to the other (`/v1/internal/segment-read`) teaches the target the
/// move, and the reads stay served or retried a handful of times, never answered `503` until the
/// target's next tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn reads_of_a_moved_shard_with_no_writer_are_served_by_its_new_owner() {
    reads_across_a_handoff(false).await;
}

/// Readers on both children's streams at both members, and an appender to the first when
/// `with_writer`, while a child returns home.
async fn reads_across_a_handoff(with_writer: bool) {
    let pair = Pair::loaded().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let mut taken = 0;
    let streams = [
        stream_in(&pair.a.state, pair.a.addr, "0", &mut taken).await,
        stream_in(&pair.a.state, pair.a.addr, "1", &mut taken).await,
    ];
    preload(pair.a.addr, &streams).await;
    let members: Members = [("streams-1", pair.a.addr), ("streams-2", pair.b.addr)];
    let stop = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(Mutex::new(BTreeMap::new()));
    let readers = streams.iter().flat_map(|stream| {
        (0..2).map(|lane| reader(members, stream.clone(), lane, (stop.clone(), seen.clone())))
    });
    let tally = Arc::new(Mutex::new(Tally::default()));
    let next = Arc::new(AtomicU32::new(0));
    let appended = appender(members, streams[0].clone(), 0, next, (stop.clone(), tally));
    let writer = async move {
        if with_writer {
            appended.await;
        }
    };
    let reads = pair.b.state.fleet.standing().tags().reads();
    let moved = async { return_a_child(&pair, &stop).await };
    let target = &pair.b.state;
    let opened = async {
        while !["0", "1"].iter().any(|c| target.shards.is_open(c)) && !stop.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        target.fleet.standing().tags().reads() - reads
    };
    let both = futures_util::future::join(futures_util::future::join_all(readers), writer);
    let ((_, gone), at_open) =
        futures_util::future::join(futures_util::future::join(both, moved), opened).await;
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    let read = target.fleet.standing().tags().reads() - reads;
    eprintln!("HANDOFF reads moved={gone} {seen:?} overrides_reads={at_open}/{read}");
    let refused: u32 = seen
        .iter()
        .filter(|((s, _), _)| *s != 200)
        .map(|(_, n)| n)
        .sum();
    let retried = seen.get(&(503, "1".to_string())).copied().unwrap_or(0);
    assert_eq!(
        refused, retried,
        "every refused read is a 503 with Retry-After 1: {seen:?}"
    );
    assert!(
        retried <= 8,
        "in-flight reads at the close stay a handful: {seen:?}"
    );
    assert!(
        with_writer || at_open == 1,
        "the target reads the overrides once before it serves the moved child: {at_open}"
    );
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// **A producer keeps its sequence across a handoff** (D8). One producer appends sequences 0..
/// to a stream in each child, in order, through both members, following `409`s, while a child
/// returns home. Every sequence is acked once, and each stream stores each sequence exactly once,
/// in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_producer_keeps_its_sequence_across_a_handoff() {
    let pair = Pair::loaded().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let mut taken = 0;
    let streams = [
        stream_in(&pair.a.state, pair.a.addr, "0", &mut taken).await,
        stream_in(&pair.a.state, pair.a.addr, "1", &mut taken).await,
    ];
    let members: Members = [("streams-1", pair.a.addr), ("streams-2", pair.b.addr)];
    let stop = Arc::new(AtomicBool::new(false));
    let producers = streams
        .iter()
        .map(|stream| produce(members, stream.clone(), stop.clone()));
    let producers = futures_util::future::join_all(producers);
    let (sent, gone) = futures_util::future::join(producers, return_a_child(&pair, &stop)).await;
    eprintln!("HANDOFF producer moved={gone} sent={sent:?}");
    for (stream, sent) in streams.iter().zip(sent) {
        let stored = stored_ns(pair.a.addr, stream).await;
        assert_eq!(
            stored,
            (0..sent).collect::<Vec<_>>(),
            "{stream}: each sequence once, in order"
        );
    }
    eprintln!("HANDOFF shutdown begins");
    pair.shutdown().await;
}

/// Produce sequences 0.. to `stream` until `stop`, each acked `200` (a fresh append naming a
/// producer; a duplicate answers `204`); how many were sent.
async fn produce(members: Members, stream: String, stop: Arc<AtomicBool>) -> u32 {
    let mut seq = 0u32;
    while !stop.load(Ordering::SeqCst) {
        let status = produced(members, &stream, seq).await;
        assert_eq!(status, 200, "{stream} seq {seq}");
        seq += 1;
    }
    seq
}

/// Append `[{"n": seq}]` to `stream` as producer `p`, epoch 1, sequence `seq`, through
/// `streams-1`, following `409 not_ring_owner` to the member it names; the final status.
async fn produced(members: Members, stream: &str, seq: u32) -> u16 {
    let path = format!("/v1/stream/{stream}");
    let body = format!(r#"[{{"n":{seq}}}]"#);
    let seq_text = seq.to_string();
    let headers = [
        JSON,
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", seq_text.as_str()),
    ];
    let (mut addr, mut status) = (members[0].1, 0);
    for _ in 0..4 {
        let answer = hreq(addr, "POST", &path, &headers, body.as_bytes()).await;
        status = answer.0;
        let owner = answer.1.get("streams-replay-to").cloned();
        match owner.and_then(|o| members.iter().copied().find(|(m, _)| *m == o)) {
            Some((_, next)) if status == 409 => addr = next,
            _ => break,
        }
    }
    status
}
