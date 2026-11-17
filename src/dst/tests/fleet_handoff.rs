#![cfg(test)]
//! **A planned move hands off giver first** (meas149-rerun F6; S5-ADDENDUM-F6 rev 2,
//! GATE-DECISION-f6-addendum-2026-11-17). The rerun's case: the split's actor `streams-1` holds
//! both children by their born overrides; once those age past `REBALANCE_RETURN_SECS`,
//! return-home gives one child back to its ring home `streams-2`, while appends to both children
//! arrive at both members (the router spreads them), on a store 20 ms away, under the rig's
//! in-flight cap of 512. Before F6 the giver serves the child until its next tick, the target's
//! first routed request opens it, and that open fences the giver's live writer: its in-flight
//! appends answer `503 shard_moving` (the raw surface's `outcome_unknown`).

use super::billing_closure_debts::{JSON, raw_put, stored};
use super::fixture_requests::hreq;
use super::fleet_birth::{Pair, age_the_children, overrides_of, split_with_a_held_append};
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

/// A stream created through `pair.a` whose route the cell gives to the leaf `leaf`: the first
/// `h-<n>` after `taken`.
async fn stream_in(pair: &Pair, leaf: &str, taken: &mut usize) -> String {
    let state = &pair.a.state;
    loop {
        *taken += 1;
        assert!(*taken < 400, "no stream lands in {leaf}");
        let name = format!("h-{taken}");
        raw_put(pair.a.addr, &name, &[JSON], b"").await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        if state.shards.prefix_for(&desc.resolve_segment("").shard_route) == leaf {
            return name;
        }
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
    /// Every answer but `204`, by (stream, status, code).
    refused: BTreeMap<(String, u16, String), u32>,
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
    let path = format!("/v1/stream/{stream}");
    let mut turn = lane;
    while !stop.load(Ordering::SeqCst) {
        let (name, addr) = members[turn % 2];
        turn += 1;
        let n = next.fetch_add(1, Ordering::SeqCst);
        let body = format!(r#"[{{"n":{n}}}]"#);
        let (status, headers, answer) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
        let mut tally = tally.lock().unwrap();
        if status == 204 {
            let by = headers.get("streams-forwarded-to").cloned().unwrap_or(name.to_string());
            let ack = Ack { n, by, at: Instant::now() };
            tally.acked.entry(stream.clone()).or_default().push(ack);
        } else {
            let code = serde_json::from_slice::<serde_json::Value>(&answer)
                .ok()
                .and_then(|v| v["code"].as_str().map(str::to_string))
                .unwrap_or_default();
            *tally.refused.entry((stream.clone(), status, code)).or_default() += 1;
        }
    }
}

/// Load each of `streams` with about 4 MiB of unflushed appends (records of 4 KiB): the memtable a
/// served shard carries into a move.
async fn preload(pair: &Pair, streams: &[String]) {
    let record = format!(r#"[{{"pad":"{}"}}]"#, "x".repeat(4_000));
    for stream in streams {
        let path = format!("/v1/stream/{stream}");
        let sends = (0..1_024).map(|_| hreq(pair.a.addr, "POST", &path, &[JSON], record.as_bytes()));
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
        stream_in(&pair, "0", &mut taken).await,
        stream_in(&pair, "1", &mut taken).await,
    ];
    preload(&pair, &streams).await;
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
    let gone = usize::from(!pair.b.state.shards.is_open("0"));
    let reason = engines[gone].db.subscribe().borrow().close_reason;
    assert!(
        !matches!(reason, Some(slatedb::CloseReason::Fenced)),
        "the giver's engine of {gone} closes without a fence: {reason:?}"
    );
    let fenced = pair.a.state.fleet.standing().handoffs_fenced();
    assert_eq!(fenced, 0, "no handoff fell back to a takeover by fence");
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    assert_eq!(tally.refused, BTreeMap::new(), "no append is refused across the move");
    let acks = &tally.acked[&streams[gone]];
    let last_by_giver = acks.iter().filter(|a| a.by == "streams-1").map(|a| a.at).max();
    let first_by_target = acks.iter().filter(|a| a.by == "streams-2").map(|a| a.at).min();
    let (Some(last), Some(first)) = (last_by_giver, first_by_target) else {
        panic!("both members acked the returned child's stream");
    };
    assert!(
        first.saturating_duration_since(last) < HOLD,
        "giver's last ack to target's first: {:?}",
        first.saturating_duration_since(last)
    );
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|a| a.n).collect();
        acked.sort_unstable();
        assert_eq!(stored_ns(pair.a.addr, stream).await, acked, "{stream}: stored = acked");
    }
    pair.shutdown().await;
}
