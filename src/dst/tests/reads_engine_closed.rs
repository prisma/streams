#![cfg(test)]
//! Reads whose shard engine stopped serving under them (edge #259; the D
//! ramp's read 500s of 2026-11-08). A read resolves a serving engine, and
//! a move then retires it (or its next owner fences it, or its sealer is
//! gone) before the read executes: the read answers the directory's own
//! refusal for a closing shard, retryable with `Retry-After: 1` (raw `503
//! shard_closing`; product `503 temporarily_unavailable`), and its retry
//! reaches the shard's next engine and reads exactly the records. Before:
//! raw `500 internal` with the engine's message; product `500 internal
//! "read failed"`.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_livefeed::wait_parked;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;
use crate::failpoints::Fp;
use crate::shard_directory::{RetireOutcome, RetirementReason};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
/// A reply as the wire gave it: status, lowercased headers, body.
type Reply = (u16, std::collections::HashMap<String, String>, Vec<u8>);

const KEY: (&str, &str) = ("prisma-encryption-key", PRISMA_KEY);
const JSON: (&str, &str) = ("content-type", "application/json");

/// How the engine stops serving while the read is parked after its resolve.
#[derive(Clone, Copy)]
enum Stop {
    /// A move retires it (the sweep's eviction: remove, holdoff, close).
    Retire,
    /// Its sealer, which the read's sealed part waits for, is gone (the
    /// engine's sealer never opened: its opens fail until released).
    SealerGone,
}

/// The answer's status, `Retry-After` and JSON body.
fn answer(reply: &Reply) -> (u16, Option<&str>, Value) {
    let (status, headers, body) = reply;
    let body = serde_json::from_slice(body).unwrap_or(Value::Null);
    (
        *status,
        headers.get("retry-after").map(String::as_str),
        body,
    )
}

async fn prefix_of(state: &State, name: &str) -> String {
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    state
        .shards
        .prefix_for(&desc.resolve_segment("").shard_route)
}

/// The read failpoint's key for `name`: its segment identity in hex.
async fn identity_hex(state: &State, name: &str) -> String {
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    crate::crypto::hex(&desc.resolve_segment("").identity)
}

/// Retires `prefix`'s engine, waits until it terminated and lifts the
/// holdoff, so the next request opens the shard's next engine at once.
async fn retire(state: &State, prefix: &str) {
    let retired = state
        .shards
        .retire(prefix, RetirementReason::SweepEviction, |_, _| true);
    let RetireOutcome::Retired(engine) = retired else {
        panic!("the engine serving {prefix:?} was not resident");
    };
    let shutdown = engine.shutdown_handle();
    for _ in 0..1000 {
        if shutdown.terminated() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(shutdown.terminated(), "the retired engine terminates");
    state.shards.clear_holdoff(prefix);
}

/// A move retires `prefix`'s engine (the sweep's eviction) while a read
/// holds it.
fn retire_under(state: &State, prefix: &str, engine: &crate::shard::ShardEngine) {
    let retired = state
        .shards
        .retire(prefix, RetirementReason::SweepEviction, |_, _| true);
    assert!(matches!(retired, RetireOutcome::Retired(_)));
    assert!(engine.is_closed(), "the retirement closed {prefix:?}");
}

/// Aborts `engine`'s sealer and waits until it stopped.
async fn abort_sealer(engine: &crate::shard::ShardEngine) {
    let sealer = engine.test_abort_task("sealer");
    for _ in 0..500 {
        if sealer.is_finished() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the sealer never stopped");
}

/// Runs `request`, a read of `name`, parks it after its engine resolved
/// and its handle loaded, stops that engine as `stop` says, and releases
/// it. Then retires whatever engine is left and lifts the holdoff, so the
/// caller's retry meets the shard's next engine.
async fn stopped_under(
    state: &State,
    name: &str,
    stop: Stop,
    request: impl std::future::Future<Output = Reply>,
) -> Reply {
    let prefix = prefix_of(state, name).await;
    if matches!(stop, Stop::SealerGone) {
        retire(state, &prefix).await;
        crate::shard::inject_seal_open_faults(&prefix, u32::MAX);
    }
    let point = identity_hex(state, name).await;
    crate::failpoints::park_read_after_resolve(&point);
    let stopping = async {
        wait_parked(Fp::ReadAfterResolve, &point, 1).await;
        let engine = state.shards.open(&prefix).expect("the read's engine");
        match stop {
            Stop::Retire => retire_under(state, &prefix, &engine),
            Stop::SealerGone => abort_sealer(&engine).await,
        }
        crate::failpoints::release_read_after_resolve(&point);
    };
    let (reply, ()) = futures_util::future::join(request, stopping).await;
    crate::shard::inject_seal_open_faults(&prefix, 0);
    if state.shards.open(&prefix).is_some() {
        retire(state, &prefix).await;
    }
    state.shards.clear_holdoff(&prefix);
    reply
}

/// A raw stream with one sealed record (`{"n":1}`) and one unsealed
/// (`{"n":2}`).
async fn raw_stream(state: &State, addr: SocketAddr, name: &str) {
    let path = format!("/v1/stream/{name}");
    let (status, _, _) = hreq(addr, "PUT", &path, &[JSON], b"").await;
    assert!(status == 200 || status == 201, "create {name}: {status}");
    two_records(
        state,
        name,
        || hreq(addr, "POST", &path, &[JSON], br#"[{"n":1}]"#),
        || hreq(addr, "POST", &path, &[JSON], br#"[{"n":2}]"#),
    )
    .await;
}

/// A product stream with one sealed record (`{"n":1}`) and one unsealed
/// (`{"n":2}`).
async fn product_stream(state: &State, addr: SocketAddr, name: &str) {
    let path = format!("/v1/streams/{name}");
    let format = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(preq(addr, "PUT", &path, &[KEY], format).await.0, 201);
    let records = format!("{path}/records");
    two_records(
        state,
        name,
        || preq(addr, "POST", &records, &[KEY], br#"{"n":1}"#),
        || preq(addr, "POST", &records, &[KEY], br#"{"n":2}"#),
    )
    .await;
}

/// Appends `first`, seals it, turns the engine's own sealer off and
/// appends `second`, which stays unsealed.
async fn two_records<F, G, A, B>(state: &State, name: &str, first: F, second: G)
where
    F: FnOnce() -> A,
    G: FnOnce() -> B,
    A: std::future::Future<Output = Reply>,
    B: std::future::Future<Output = Reply>,
{
    assert!(matches!(first().await.0, 200 | 204), "the first append");
    let engine = state
        .shards
        .open(&prefix_of(state, name).await)
        .expect("open");
    for _ in 0..2 {
        engine
            .seal_once(crate::shard::seal::Trigger::Requested)
            .await
            .expect("seal");
    }
    engine.configure_seal(|s| s.auto = false);
    assert!(matches!(second().await.0, 200 | 204), "the second append");
}

/// The raw read's refusal: the directory's code for a closing shard.
fn raw_refused() -> (u16, Option<&'static str>, Value) {
    let message = "the shard's engine closed under the read; retry";
    (
        503,
        Some("1"),
        json!({"error": {"code": "shard_closing", "message": message}}),
    )
}

/// The product read's refusal: the row of a shard opening here.
fn product_refused() -> (u16, Option<&'static str>, Value) {
    let error = json!({
        "code": "temporarily_unavailable",
        "message": "read service unavailable; retry",
        "retryable": true,
    });
    (503, Some("1"), json!({ "error": error }))
}

fn both_records() -> Value {
    json!([{"n": 1}, {"n": 2}])
}

async fn raw_read_meets(stop: Stop, name: &str) {
    let (state, addr) = http_rig(mem()).await;
    raw_stream(&state, addr, name).await;
    let path = format!("/v1/stream/{name}?offset=-1");
    let reply = stopped_under(&state, name, stop, hreq(addr, "GET", &path, &[], b"")).await;
    assert_eq!(answer(&reply), raw_refused(), "the read the stop met");
    let (status, _, body) = hreq(addr, "GET", &path, &[], b"").await;
    let records: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    assert_eq!((status, records), (200, both_records()), "the retry");
    engine_shutdown(&state).await;
}

async fn product_read_meets(stop: Stop, name: &str) {
    let (state, addr) = http_rig(mem()).await;
    product_stream(&state, addr, name).await;
    let path = format!("/v1/streams/{name}/records");
    let request = preq(addr, "GET", &path, &[KEY], b"");
    let reply = stopped_under(&state, name, stop, request).await;
    assert_eq!(answer(&reply), product_refused(), "the read the stop met");
    let (status, _, body) = preq(addr, "GET", &path, &[KEY], b"").await;
    let records: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    assert_eq!((status, records), (200, both_records()), "the retry");
    engine_shutdown(&state).await;
}

/// **A raw read whose engine a move retires under it answers 503
/// `shard_closing`, `Retry-After: 1`, and its retry reads both records.**
/// Before: `500 internal` "the shard engine closed; retry".
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_raw_read_whose_engine_is_retired_under_it_answers_shard_closing() {
    raw_read_meets(Stop::Retire, "ec-raw").await;
}

/// **A product read whose engine a move retires under it answers 503
/// `temporarily_unavailable`, retryable, `Retry-After: 1`, and its retry
/// reads both records.** Before: `500 internal` "read failed".
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_product_read_whose_engine_is_retired_under_it_is_temporarily_unavailable() {
    product_read_meets(Stop::Retire, "ec-product").await;
}

/// **A raw read whose engine's sealer is gone answers 503 `shard_closing`.**
/// The field's main case: the engine's sealer never opened, and is gone
/// when the read's sealed part waits for it. Before: `500 internal` "no
/// sealer task is running to open the sealer".
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_raw_read_whose_engines_sealer_is_gone_answers_shard_closing() {
    raw_read_meets(Stop::SealerGone, "ec-raw-sealer").await;
}

/// **A product read whose engine's sealer is gone is temporarily
/// unavailable.** Before: `500 internal` "read failed".
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_product_read_whose_engines_sealer_is_gone_is_temporarily_unavailable() {
    product_read_meets(Stop::SealerGone, "ec-product-sealer").await;
}

/// POSTs a live wait for `subs` on the rig's own project.
async fn live_wait(state: &State, addr: SocketAddr, body: Value) -> Reply {
    let project = state.deployment.deployment_tenant().as_str().to_string();
    let path = format!("/v1/projects/{project}/live:wait");
    preq(addr, "POST", &path, &[], body.to_string().as_bytes()).await
}

/// The live wait's entry `id`.
fn entry(body: &Value, id: &str) -> Value {
    body["results"]
        .as_array()
        .and_then(|r| r.iter().find(|e| e["id"] == id))
        .cloned()
        .unwrap_or(Value::Null)
}

/// **A live wait's tail whose read meets its engine's retirement answers
/// its entry 503 `temporarily_unavailable`, retryable.** A tail pinned
/// before two records were appended reads them; its engine is retired
/// under that read: the entry is the product read's refusal. The next wait
/// reads both records. Before: the entry was `500 internal` "read failed".
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_live_wait_tail_whose_engine_is_retired_under_its_read_answers_503() {
    let (state, addr) = http_rig(mem()).await;
    let name = "ec-wait";
    let path = format!("/v1/streams/{name}");
    let format = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(preq(addr, "PUT", &path, &[KEY], format).await.0, 201);
    let tail = |cursor: &str| json!({"id": "t", "kind": "tail", "stream": name, "key": PRISMA_KEY, "cursor": cursor});
    let (status, _, body) = live_wait(&state, addr, json!({"subs": [tail("now")]})).await;
    let pinned: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, 200, "{pinned}");
    let cursor = entry(&pinned, "t")["cursor"].as_str().unwrap().to_string();
    let records = format!("{path}/records");
    two_records(
        &state,
        name,
        || preq(addr, "POST", &records, &[KEY], br#"{"n":1}"#),
        || preq(addr, "POST", &records, &[KEY], br#"{"n":2}"#),
    )
    .await;
    let waiting = live_wait(
        &state,
        addr,
        json!({"waitMs": 2000, "subs": [tail(&cursor)]}),
    );
    let reply = stopped_under(&state, name, Stop::Retire, waiting).await;
    let (status, _, body) = answer(&reply);
    let error = product_refused().2["error"].clone();
    assert_eq!(
        (status, entry(&body, "t")),
        (200, json!({"id": "t", "status": 503, "error": error})),
        "{body}"
    );
    let retry = live_wait(
        &state,
        addr,
        json!({"waitMs": 2000, "subs": [tail(&cursor)]}),
    )
    .await;
    let (status, _, body) = answer(&retry);
    assert_eq!(
        (status, entry(&body, "t")["records"].clone()),
        (200, both_records()),
        "the next wait: {body}"
    );
    engine_shutdown(&state).await;
}

/// **A handle load's closed store is the engine's close; any other store
/// failure is the store's.** The handle load (`ShardEngine::read_handle`,
/// the `stream_handle` sites) classifies the store's own kind: a store
/// that closed (a fence, a retirement) answers 503 (edge #259); an
/// unavailable one stays `Other`, a 500.
#[test]
fn a_handle_loads_closed_store_is_the_engines_close() {
    use crate::application::read::StorageFailure;
    let kind = |e: slatedb::Error| match StorageFailure::from(e) {
        StorageFailure::EngineClosed(m) => ("closed", m),
        StorageFailure::Other(m) => ("other", m),
    };
    let fenced = slatedb::Error::closed(
        "detected newer DB client".into(),
        slatedb::CloseReason::Fenced,
    );
    let clean = slatedb::Error::closed("db is closed".into(), slatedb::CloseReason::Clean);
    let down = slatedb::Error::unavailable("the store failed".into());
    assert_eq!(
        [kind(fenced), kind(clean), kind(down)],
        [
            (
                "closed",
                "Closed error: detected newer DB client".to_string()
            ),
            ("closed", "Closed error: db is closed".to_string()),
            ("other", "Unavailable error: the store failed".to_string()),
        ]
    );
}
