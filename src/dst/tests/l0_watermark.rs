//! The L0 watermark (`shard::layout::l0_watermark`, edge #234): with
//! compaction held back, a shard log's L0s climb toward SlateDB's stall at
//! `l0_max_ssts`. The shed engages at the high mark, below the stall:
//! appends and queue sends answer a retryable 503 `shard_l0_backpressure`,
//! while reads, settles and the control plane are served, and nothing is
//! refused below the mark. Once compaction catches up the shed releases
//! and writes resume. The negative control runs the same climb with the
//! shed out of reach and meets the stall.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::fixture_http::{HttpRigOptions, cold_sealer, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use crate::shard::ShardEngine;

/// The line the rig's shard logs run with (SlateDB's default
/// `l0_max_ssts`): the engines' watermark is set to it, as boot sets the
/// process's line from the shard settings.
fn line() -> usize {
    slatedb::config::Settings::default().l0_max_ssts
}

/// How long the watermark's tick (1 s) and a memtable flush may take.
const WITHIN: Duration = Duration::from_secs(10);

struct Rig {
    state: Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
    fault: Arc<FaultStore>,
    engine: Arc<ShardEngine>,
}

/// A rig whose sealer never flushes by itself, with a stream `s` and a
/// queue `q` holding one claimed message (its lease token returned) on its
/// one shard, and then the compactor's writes parked, so nothing compacts
/// the shard log until `release_hold`.
async fn rig(label: &str) -> (Rig, String) {
    let fault = FaultStore::uniform(mem(), 0x10, FaultPlan::CLEAN);
    let options = HttpRigOptions {
        seal: Some(cold_sealer),
        ..Default::default()
    };
    let store: Arc<dyn object_store::ObjectStore> = fault.clone();
    let (state, addr) = http_rig_build(store, RigRuntime::first(), options)
        .await
        .parts();
    let stream = json!({ "format": { "kind": "json" } });
    assert_eq!(
        call(addr, "PUT", "/v1/streams/s", stream).await.0,
        201,
        "{label}"
    );
    let (st, v) = call(addr, "PUT", "/v1/queues/q", Value::Null).await;
    assert_eq!(st, 201, "{v}");
    assert_eq!(send(addr, 0).await.0, 201);
    let (st, v) = call(
        addr,
        "POST",
        "/v1/queues/q/groups/default:claim",
        json!({ "max": 1, "leaseMs": 600_000, "waitMs": 3_000 }),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let token = v["messages"][0]["token"].as_str().unwrap().to_owned();
    let engine = state
        .shards
        .engines()
        .pop()
        .expect("the rig's shard is open");
    // From here the compactor's next job waits on its parked write (the
    // open itself awaits the compactor's first one).
    fault.hold_class_under(
        StoreOp::Put,
        ObjClass::Other,
        "00/shard/compactions",
        1 << 20,
    );
    let rig = Rig {
        state,
        addr,
        fault,
        engine,
    };
    (rig, token)
}

async fn call(addr: std::net::SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let (st, _, v) = call_headers(addr, method, path, body).await;
    (st, v)
}

async fn call_headers(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Value,
) -> (u16, HashMap<String, String>, Value) {
    let raw = if body.is_null() {
        Vec::new()
    } else {
        body.to_string().into_bytes()
    };
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, headers, b) = preq(addr, method, path, &key, &raw).await;
    let v = serde_json::from_slice(&b).unwrap_or_else(|_| json!(String::from_utf8_lossy(&b)));
    (st, headers, v)
}

async fn append(addr: std::net::SocketAddr, n: usize) -> (u16, HashMap<String, String>, Value) {
    call_headers(addr, "POST", "/v1/streams/s/records", json!({ "n": n })).await
}

async fn send(addr: std::net::SocketAddr, n: usize) -> (u16, Value) {
    let (st, _, v) = send_headers(addr, n).await;
    (st, v)
}

async fn send_headers(
    addr: std::net::SocketAddr,
    n: usize,
) -> (u16, HashMap<String, String>, Value) {
    let body = json!({ "body": { "n": n } });
    call_headers(addr, "POST", "/v1/queues/q/messages", body).await
}

/// Flush the memtable (one L0 when it holds rows) within `WITHIN`; false
/// when the flush is still waiting then (SlateDB's stall).
async fn flush(engine: &ShardEngine) -> bool {
    match tokio::time::timeout(WITHIN, engine.flush_memtable()).await {
        Ok(flushed) => {
            flushed.expect("the memtable flush");
            true
        }
        Err(_) => false,
    }
}

/// Wait until the watermark's tick has read `at_least` L0s.
async fn ticked(engine: &ShardEngine, at_least: usize) {
    let read = async {
        while engine.l0_gauge().count < at_least {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WITHIN, read)
        .await
        .unwrap_or_else(|_| panic!("the tick read {at_least} L0s"));
}

/// Append and flush one L0 at a time until the tick reads `until` L0s
/// (or the shed answers): every append below the high mark is admitted.
/// Returns the appends admitted.
async fn climb(r: &Rig, until: usize) -> usize {
    let high = r.engine.l0_marks().high;
    let mut admitted = 0;
    loop {
        let count = r.engine.l0_gauge().count;
        if count >= until || r.engine.l0_gauge().shedding {
            return admitted;
        }
        let (st, _, v) = append(r.addr, admitted).await;
        assert_eq!(
            st, 200,
            "an append at {count} L0s, below the high mark {high}: {v}"
        );
        admitted += 1;
        assert!(
            flush(&r.engine).await,
            "a flush at {count} L0s (below the line)"
        );
        ticked(&r.engine, count + 1).await;
    }
}

/// A 503 `shard_l0_backpressure` with `Retry-After: 2`, as `(status,
/// code, retry-after)`.
fn shed_answer<'a>(
    st: u16,
    h: &'a HashMap<String, String>,
    v: &'a Value,
) -> (u16, &'a Value, Option<&'a str>) {
    (
        st,
        &v["error"]["code"],
        h.get("retry-after").map(String::as_str),
    )
}

/// While shed: the product append, the raw append and the queue send each
/// answer the shed's 503.
async fn writes_are_shed(r: &Rig, n: usize) {
    let shed = (503, &json!("shard_l0_backpressure"), Some("2"));
    let (st, h, v) = append(r.addr, n).await;
    assert_eq!(shed_answer(st, &h, &v), shed, "the product append: {v}");
    let raw_headers = [
        ("stream-encryption-key", PRISMA_KEY),
        ("content-type", "application/json"),
    ];
    let raw = br#"[{"raw":1}]"#;
    let (st, h, b) = preq(r.addr, "POST", "/v1/stream/s", &raw_headers, raw).await;
    let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
    assert_eq!(shed_answer(st, &h, &v), shed, "the raw append: {v}");
    let (st, h, v) = send_headers(r.addr, 1).await;
    assert_eq!(shed_answer(st, &h, &v), shed, "the queue send: {v}");
}

/// While shed, after `sheds` shed writes: `/v1/debug/load` and
/// `/operator/data.json` name the shed shard and its marks.
async fn operator_views_name_the_shed(r: &Rig, sheds: u64) {
    let (st, _, b) = hreq(r.addr, "GET", "/v1/debug/load", &[], b"").await;
    let load: Value = serde_json::from_slice(&b).unwrap();
    let ms = &load["maintenance_shards"];
    let shard = &ms["shards"][0];
    let list = (
        &ms["owned_shards"],
        &ms["shards_engaged"],
        &ms["l0_shards_shedding"],
    );
    assert_eq!(
        (st, list),
        (200, (&json!(1), &json!(0), &json!(1))),
        "{load}"
    );
    let l0 = &shard["l0"];
    assert_eq!(
        (
            &shard["prefix"],
            &shard["shard_shed"],
            &l0["shedding"],
            &l0["sheds"]
        ),
        (&json!("00"), &json!(false), &json!(true), &json!(sheds)),
        "{load}"
    );
    let marks = (&l0["high"], &l0["low"], &l0["line"]);
    assert_eq!(marks, (&json!(6), &json!(4), &json!(8)), "{load}");
    assert_eq!(ms["l0_max"], l0["l0"]);
    let (st, _, b) = hreq(r.addr, "GET", "/operator/data.json", &[], b"").await;
    let local = &serde_json::from_slice::<Value>(&b).unwrap()["local"];
    let fields = (
        &local["l0_shards_shedding"],
        &local["l0_write_sheds"],
        &local["l0_max"],
    );
    assert_eq!(
        (st, fields),
        (200, (&json!(1), &json!(sheds), &ms["l0_max"])),
        "{local}"
    );
}

/// While shed: a read, a settle (the claimed message's `token`), a create
/// and a delete are served, and the log still flushes.
async fn the_rest_is_served(r: &Rig, admitted: usize, token: &str) {
    let (st, v) = call(r.addr, "GET", "/v1/streams/s/records", Value::Null).await;
    assert_eq!(
        (st, v.as_array().map(Vec::len)),
        (200, Some(admitted)),
        "{v}"
    );
    let settle = json!({ "ops": [{ "token": token, "op": "ack" }] });
    let (st, v) = call(r.addr, "POST", "/v1/queues/q/groups/default:settle", settle).await;
    assert_eq!(
        (st, &v["results"][0]["outcome"]),
        (200, &json!("ok")),
        "{v}"
    );
    let stream = json!({ "format": { "kind": "json" } });
    assert_eq!(call(r.addr, "PUT", "/v1/streams/t", stream).await.0, 201);
    let deleted = call(r.addr, "DELETE", "/v1/streams/t", Value::Null).await;
    assert_eq!(deleted.0, 204, "{}", deleted.1);
    assert!(flush(&r.engine).await, "the shed log flushes: no stall");
}

/// The shed engages at the high mark, below the stall; appends and sends
/// answer 503 `shard_l0_backpressure` with `Retry-After: 2`; reads,
/// claims, settles and the control plane are served and the log flushes;
/// once compaction catches up the shed releases and appends resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_l0_watermark_sheds_writes_before_the_stall_and_resumes_after_catch_up() {
    let (r, token) = rig("shed").await;
    r.engine.set_l0_line(line());
    let m = r.engine.l0_marks();
    assert_eq!((m.line, m.high, m.low), (8, 6, 4), "SlateDB's default line");
    let admitted = climb(&r, m.high).await;
    ticked(&r.engine, m.high).await;
    let gauge = r.engine.l0_gauge();
    assert!(gauge.shedding, "the shed engaged at {} L0s", gauge.count);
    assert!(gauge.count < m.line, "below the stall: {}", gauge.count);
    writes_are_shed(&r, admitted).await;
    operator_views_name_the_shed(&r, 3).await;
    the_rest_is_served(&r, admitted, &token).await;
    let gauge = r.engine.l0_gauge();
    assert!(
        gauge.count < m.line,
        "still below the stall: {}",
        gauge.count
    );
    assert_eq!(gauge.sheds, 3, "the two appends and the send");

    // Compaction catches up: the shed releases below the low mark.
    r.fault.release_hold();
    let released = async {
        while r.engine.l0_gauge().shedding {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), released)
        .await
        .expect("compaction drained the L0s and the shed released");
    assert!(r.engine.l0_gauge().count < m.low);
    let (st, _, v) = append(r.addr, admitted).await;
    assert_eq!(st, 200, "appends resume: {v}");
    assert_eq!(send(r.addr, 2).await.0, 201, "sends resume");
    assert_eq!(r.engine.l0_gauge().sheds, 3);
    engine_shutdown(&r.state).await;
}

/// The negative control: the same climb with the shed out of reach (its
/// line far above the log's) admits every append and reaches the stall:
/// a flush at `l0_max_ssts` L0s waits until compaction frees one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn without_the_shed_the_climb_meets_slatedbs_l0_stall() {
    let (r, _token) = rig("control").await;
    r.engine.set_l0_line(4_096);
    climb(&r, line()).await;
    let gauge = r.engine.l0_gauge();
    assert_eq!(
        (gauge.count, gauge.shedding, gauge.sheds),
        (line(), false, 0)
    );
    let (st, _, v) = append(r.addr, 0).await;
    assert_eq!(st, 200, "nothing sheds: {v}");
    assert!(
        !flush(&r.engine).await,
        "a flush at the line waits: SlateDB's L0 stall"
    );
    r.fault.release_hold();
    assert!(flush(&r.engine).await, "compaction frees the stall");
    engine_shutdown(&r.state).await;
}
