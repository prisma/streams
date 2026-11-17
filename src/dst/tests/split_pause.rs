#![cfg(test)]
//! The split's pause on a loaded parent (F5, GATE-DECISION-f5-pause-log-reads-2026-11-17): the pause
//! reads the parent's sentinel rows (two index rows per resident stream) and its queues' trimmed
//! intervals from one snapshot. Its store round trips must not grow with the streams or the queues:
//! the reads fetch blocks in large ranges, several at once, and Preparing's identical read leaves
//! them cached. Each parent is made cold first: a flush caches the table's blocks as it writes them
//! (cache-on-write) and the open's index scan caches the index rows' blocks, so the leaf is closed,
//! reopened as a replacement instance opens it, and its tables evicted from its block cache.

use super::fixture_http::{HttpRig, HttpRigOptions, cold_sealer, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::topology_policy::store_topology;
use crate::dst::StoreOp;
use crate::dst::trace_store::{TraceEventKind, TraceStore};
use futures_util::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

type State = Arc<crate::http::AppState>;
type Engine = Arc<crate::shard::ShardEngine>;

/// The most store GETs the split may make from its park to its topology CAS, whatever the parent
/// holds (F5's bound).
const BOUND: usize = 64;

/// The trimmed queues of the queue case (400 rather than the ruling's 200: 200 queues' interval
/// scans stayed just under the bound before F5).
const QUEUES: usize = 400;

/// Requests the fixtures run at once.
const AT_ONCE: usize = 32;

/// Flushes `engine`'s memtable to a table.
async fn flush(engine: &crate::shard::ShardEngine) {
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    engine
        .db
        .flush_with_options(memtable)
        .await
        .expect("the memtable flushes to tables");
}

/// SlateDB's `min_compaction_sources`: a log holding this many L0 tables compacts them.
const COMPACTION_SOURCES: usize = 4;

/// Waits, at most 30 s, until `leaf`'s log holds fewer L0 tables than start a compaction: the
/// split then runs on a settled parent, so these tests count the pause's log reads only. A
/// compaction running through the pause cost 7.7 s in the checkpoint phase on Linux, its cause
/// not attributed; `a_split_of_a_compacting_parent_parks_only_after_the_compaction` pins that case.
async fn compacted(leaf: &crate::shard::ShardEngine) {
    let started = std::time::Instant::now();
    while leaf.db.manifest().l0().len() >= COMPACTION_SOURCES {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the leaf's L0 compacts"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Closes every engine of `state` and reopens the leaf `prefix` by appending `body` to `stream`,
/// as a replacement instance opens it, waits for its L0 to compact (`compacted`), then evicts every
/// cached block, index, filter and stats entry of the leaf's tables (its open's own index scan
/// cached some): the leaf, settled and cold.
async fn reopened_cold(
    state: &State,
    addr: std::net::SocketAddr,
    (prefix, stream): (&str, &str),
    body: &str,
) -> Engine {
    use slatedb::DbCacheManagerOps;
    engine_shutdown(state).await;
    let path = format!("/v1/stream/{stream}");
    let json = [("content-type", "application/json")];
    let (status, _, _) = hreq(addr, "POST", &path, &json, body.as_bytes()).await;
    assert_eq!(status, 204, "the leaf reopens");
    let leaf = state.shards.open(prefix).expect("the leaf is open");
    compacted(&leaf).await;
    let manifest = leaf.db.manifest();
    for view in manifest.all_sst_views() {
        let all = slatedb::CacheTarget::all();
        leaf.db
            .evict_cached_sst(&view.sst, &all)
            .await
            .expect("evict");
    }
    leaf
}

/// A one-leaf cell over `store` whose sealer never seals, with `streams` streams each holding one
/// unsealed record, so each has its index rows in the sentinel; the memtable flushed to tables and
/// the leaf reopened cold, so no block of them is cached.
async fn loaded(store: Arc<dyn object_store::ObjectStore>, streams: usize) -> HttpRig {
    let rig = http_rig_build(
        store,
        RigRuntime::first(),
        HttpRigOptions {
            prefixes: vec![String::new()],
            seal: Some(cold_sealer),
            ..Default::default()
        },
    )
    .await;
    store_topology(&rig).await;
    let addr = rig.addr;
    futures_util::stream::iter(0..streams)
        .for_each_concurrent(AT_ONCE, |n| async move {
            let path = format!("/v1/stream/load-{n}");
            let json = [("content-type", "application/json")];
            let (status, _, _) = hreq(addr, "PUT", &path, &json, b"").await;
            assert_eq!(status, 201, "create {path}");
            let (status, _, _) = hreq(addr, "POST", &path, &json, br#"[{"n":0}]"#).await;
            assert_eq!(status, 204, "append to {path}");
        })
        .await;
    flush(&rig.state.shards.open("").expect("the leaf is open")).await;
    reopened_cold(&rig.state, addr, ("", "load-0"), r#"[{"n":1}]"#).await;
    rig
}

/// Each object's live `'R'` count (chunks, bytes).
type Counts = std::collections::BTreeMap<crate::spill::object::ObjectId, (u64, u64)>;

/// The split of `parent`, paused at its park: the store GETs traced from the park to the topology
/// CAS, the parent's `'R'` counts at the park (read before the window opens), and the published
/// split.
async fn pause_gets(
    state: &State,
    parent: &Engine,
    trace: &TraceStore,
) -> (usize, Counts, crate::topology::split::Published) {
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    parent.gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, parent, "op-load", "inst-a", &*clock);
    let parked_at = AtomicU64::new(0);
    let control = async {
        reached_rx.await.expect("the split parks");
        let at_park = super::split_purged::counted(parent).await;
        let last = trace.events().last().map_or(0, |event| event.seq);
        parked_at.store(last, Ordering::Relaxed);
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
        at_park
    };
    let (published, at_park) = futures_util::future::join(split, control).await;
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let parked_at = parked_at.load(Ordering::Relaxed);
    let events = trace.events();
    let window = events.iter().filter(|event| event.seq > parked_at);
    let cas = window
        .clone()
        .find(|event| event.op == StoreOp::Put && event.path.ends_with("topology.json"))
        .map(|event| event.seq)
        .expect("the publish's topology CAS");
    let gets = window
        .filter(|event| event.seq < cas && event.kind == TraceEventKind::Operation)
        .filter(|event| event.op == StoreOp::Get)
        .count();
    (gets, at_park, published)
}

/// The GETs of the pause of a loaded parent with `streams` streams, and its phases.
async fn loaded_pause(streams: usize) -> (usize, String) {
    let trace = TraceStore::verbatim(super::fixture_storage::mem());
    let rig = loaded(trace.clone(), streams).await;
    let parent = rig.state.shards.open("").expect("the leaf is open");
    let (gets, _, published) = pause_gets(&rig.state, &parent, &trace).await;
    let phases = crate::topology::split::shown_phases(&published);
    engine_shutdown(&rig.state).await;
    (gets, phases)
}

/// **The pause of a loaded parent stays within the hold** (F5, GATE-DECISION-f5-pause-log-reads).
/// A parent whose 1,000 and then 8,000 streams each hold an unsealed record, its rows flushed to
/// tables and cold: from the park to the topology CAS the split makes at most 64 store GETs, the
/// same bound at both sizes. Red before F5: the sentinel scan fetches one block per GET, so the
/// count grows with the streams (8,000 rather than the ruling's 4,000: a stream's two index rows
/// take fewer blocks than the ruling estimated, and 4,000 streams stayed just under the bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_pause_of_a_loaded_parent_stays_within_the_hold() {
    let (one, one_phases) = loaded_pause(1_000).await;
    let (eight, eight_phases) = loaded_pause(8_000).await;
    println!("PAUSE_GETS 1000={one} [{one_phases}] 8000={eight} [{eight_phases}]");
    assert!(
        one <= BOUND && eight <= BOUND,
        "GETs from park to CAS: {one} at 1,000 streams ({one_phases}), {eight} at 8,000 ({eight_phases})"
    );
}

/// A queue route with the stream key and the JSON `body` (`""`: none): its status and its JSON
/// answer.
async fn q(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, Value) {
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (status, _, raw) = preq(addr, method, path, &key, body.as_bytes()).await;
    (status, serde_json::from_slice(&raw).unwrap_or(Value::Null))
}

/// `wanted` queues created in the leaf `00` of `cell`: their names.
async fn queues_in_00(cell: &super::fixture_split::Cell, wanted: usize) -> Vec<String> {
    let (state, addr) = (&cell.state, cell.addr);
    let landed = futures_util::stream::iter(0..wanted * 8)
        .map(|n| async move {
            let name = format!("pq-{n}");
            let (status, v) = q(addr, "PUT", &format!("/v1/queues/{name}"), "").await;
            assert_eq!(status, 201, "{v}");
            let sref = crate::tenant::ProjectId::new("proj-test")
                .unwrap()
                .stream_ref(&name);
            let desc = state.registry.get(&sref).await.unwrap().unwrap();
            let route = desc.resolve_segment("").shard_route;
            (state.shards.prefix_for(&route) == "00").then_some(name)
        })
        .buffer_unordered(AT_ONCE)
        .filter_map(|landed| async move { landed })
        .take(wanted);
    let names: Vec<String> = landed.collect().await;
    assert_eq!(names.len(), wanted, "queues in 00");
    names
}

/// `name`'s five messages, leased in one claim or more: their tokens.
async fn leased_five(addr: std::net::SocketAddr, name: &str) -> Vec<Value> {
    let mut tokens = Vec::new();
    for _ in 0..4 {
        let body = r#"{"max":5,"leaseMs":60000,"waitMs":3000}"#;
        let path = format!("/v1/queues/{name}/groups/default:claim");
        let (status, v) = q(addr, "POST", &path, body).await;
        assert_eq!(status, 200, "{v}");
        let messages = v["messages"].as_array().cloned().unwrap_or_default();
        tokens.extend(messages.iter().map(|m| m["token"].clone()));
        if tokens.len() >= 5 {
            break;
        }
    }
    assert_eq!(tokens.len(), 5, "{name}'s five messages leased");
    tokens
}

/// Each of `names` sent five messages, each sealed apart, leased, and its second and third acked;
/// the folds and trim passes run until none is left, so each queue's trim released two chunks into
/// one interval.
async fn trimmed(cell: &super::fixture_split::Cell, names: &[String]) {
    let addr = cell.addr;
    for n in 0..5 {
        futures_util::stream::iter(names)
            .for_each_concurrent(AT_ONCE, |name| async move {
                let path = format!("/v1/queues/{name}/messages");
                let body = format!(r#"{{"body":{{"n":{n}}}}}"#);
                let (status, v) = q(addr, "POST", &path, &body).await;
                assert_eq!(status, 201, "{v}");
            })
            .await;
        cell.parent.seal_requested().await.expect("seal");
    }
    futures_util::stream::iter(names)
        .for_each_concurrent(AT_ONCE, |name| async move {
            let tokens = leased_five(addr, name).await;
            let (one, two) = (&tokens[1], &tokens[2]);
            let ops =
                format!(r#"{{"ops":[{{"token":{one},"op":"ack"}},{{"token":{two},"op":"ack"}}]}}"#);
            let path = format!("/v1/queues/{name}/groups/default:settle");
            let (status, v) = q(addr, "POST", &path, &ops).await;
            assert_eq!(status, 200, "{v}");
        })
        .await;
    while cell.parent.fold_once().await.expect("fold").more {}
    let mut released = 0;
    loop {
        let pass = cell.parent.trim_pass().await.expect("trim");
        released += pass.released;
        if pass.released == 0 && pass.hints == 0 {
            break;
        }
    }
    let queues = u64::try_from(names.len()).unwrap();
    assert_eq!(released, 2 * queues, "two chunks per queue");
}

/// **The pause of a parent with trimmed queues reads their intervals at once** (F5,
/// GATE-DECISION-f5-pause-log-reads). The leaf `00` holds 400 queues, each with a completed trim
/// pass (its `'H'` row and one `'k'` interval), in tables and cold. From the park to the topology
/// CAS the split makes at most 64 store GETs, its pause read the 400 queues' intervals, and the
/// children's `'R'` rows sum to the parent's, per object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_pause_of_a_parent_with_trimmed_queues_reads_its_intervals_at_once() {
    use super::split_purged::counted;
    let trace = TraceStore::verbatim(super::fixture_storage::mem());
    let cell = super::fixture_split::cell_over(trace.clone()).await;
    cell.parent.gc_state().configure(|s| s.auto = false);
    let names = queues_in_00(&cell, QUEUES).await;
    trimmed(&cell, &names).await;
    flush(&cell.parent).await;
    let body = super::fixture_split::body(9);
    let parent = reopened_cold(&cell.state, cell.addr, ("00", &cell.low), &body).await;
    let (gets, at_park, published) = pause_gets(&cell.state, &parent, &trace).await;
    let phases = crate::topology::split::shown_phases(&published);
    println!("PAUSE_GETS queues={QUEUES} {gets} [{phases}]");
    let heads = published.notes.iter().find(|(note, _)| note == "heads");
    assert_eq!(
        heads.map(|(_, n)| *n),
        u64::try_from(QUEUES).ok(),
        "{phases}"
    );
    assert!(gets <= BOUND, "GETs from park to CAS: {gets} ({phases})");
    let mut summed = Counts::new();
    for (_, _, child) in &published.children {
        for (id, (chunks, bytes)) in counted(child).await {
            let at = summed.entry(id).or_default();
            (at.0, at.1) = (at.0 + chunks, at.1 + bytes);
        }
    }
    assert_eq!(
        summed, at_park,
        "the children's 'R' rows sum to the parent's"
    );
    engine_shutdown(&cell.state).await;
}

/// Appends `[{"n": k}]` to `stream` for k = 1, 2, ... until `done`: each answer's status and the
/// next offset it reports, in order.
async fn appending(
    addr: std::net::SocketAddr,
    stream: &str,
    done: &std::sync::atomic::AtomicBool,
) -> Vec<(u16, Option<String>)> {
    let (path, json) = (
        format!("/v1/stream/{stream}"),
        [("content-type", "application/json")],
    );
    let mut answers = Vec::new();
    for k in 1u32.. {
        if done.load(Ordering::Relaxed) {
            break;
        }
        let body = format!(r#"[{{"n":{k}}}]"#);
        let (status, headers, _) = hreq(addr, "POST", &path, &json, body.as_bytes()).await;
        answers.push((status, headers.get("stream-next-offset").cloned()));
    }
    answers
}

/// **The pause of a parent of 10,000 streams on a store 20 ms away stays within the hold** (F5's
/// real-time case, a measurement: ignored in the suite, run once under the machine's lock). Appends
/// arrive through the split: none is answered 503, and the stream then reads every acked record
/// once, in order, the last ack's offset being the one the stream serves next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a timing measurement, run under the machine's lock"]
async fn the_pause_of_ten_thousand_streams_twenty_ms_away_stays_within_the_hold() {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    const HOLD: std::time::Duration = std::time::Duration::from_secs(2);
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let store: Arc<dyn object_store::ObjectStore> =
        FaultStore::uniform(super::fixture_storage::mem(), 0x5412, plan);
    let rig = loaded(store, 10_000).await;
    let parent = rig.state.shards.open("").expect("the leaf is open");
    let done = std::sync::atomic::AtomicBool::new(false);
    let clock = rig.state.runtime.clock.clone();
    let split = async {
        let published =
            crate::topology::split::split(&rig.state.shards, &parent, "op-10k", "inst-a", &*clock)
                .await;
        done.store(true, Ordering::Relaxed);
        published
    };
    let appended = appending(rig.addr, "load-1", &done);
    let (published, answers) = futures_util::future::join(split, appended).await;
    let published = published
        .map_err(|refused| refused.to_string())
        .expect("the split publishes");
    let phases = crate::topology::split::shown_phases(&published);
    eprintln!(
        "PAUSE {} ms at 10,000 streams, 20 ms away [{phases}]",
        published.pause.as_millis()
    );
    let refused: Vec<_> = answers
        .iter()
        .filter(|(status, _)| *status != 204)
        .collect();
    assert!(refused.is_empty(), "every append acked: {refused:?}");
    let (records, next) = super::fixture_split::records_and_next(rig.addr, "load-1").await;
    let read: Vec<u64> = records
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|record| record["n"].as_u64())
        .collect();
    let acked = u64::try_from(answers.len()).unwrap();
    assert_eq!(
        read,
        (0..=acked).collect::<Vec<_>>(),
        "each ack once, in order"
    );
    assert_eq!(
        answers.last().and_then(|a| a.1.clone()),
        next,
        "the last ack's offset"
    );
    assert!(published.pause < HOLD, "{:?} ({phases})", published.pause);
    engine_shutdown(&rig.state).await;
}
