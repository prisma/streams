//! F2: a group the retiring engine WROTE but had not made durable when it
//! retired, composed over the public wire with successor ownership.
//!
//! The owner's ratified model: a retiring engine stops publishing live
//! state, but retirement does not prove its accepted storage write
//! disappeared. When durable success cannot be established the client gets
//! the retryable shard-moving/unknown outcome, never a plain success and
//! never a definitive rejection; owed-final obligations are retained; the
//! successor recovers canonical state; retries preserve producer and
//! idempotency identity. R17-B (`shard::retirement_tests`) pins the engine
//! half; these scenarios pin what a client and a successor runtime see.
//!
//! Mechanism, shared with R17-B: the shard's next WAL PUT is parked in the
//! fault store, so the committer's `db.write` returns and the group waits
//! in the terminal handoff for a durability it cannot reach. The fleet
//! retirement then strands it. The parked PUT is released only after the
//! client was answered, so the retiring close makes the group durable and
//! the successor, a second runtime over the same store, replays it.
//!
//! Three writes are stranded that way: a producer-keyed raw append, a raw
//! append-and-close, and a product `:seal` with a final record.

use super::fixture_http::{
    HttpRig, HttpRigOptions, cold_absorber, engine_shutdown, http_rig_build,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

type Reply = (u16, HashMap<String, String>, Vec<u8>);

const JSON: (&str, &str) = ("content-type", "application/json");
const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];

/// One runtime over `store`, its absorber never due and paused, so no
/// absorption write reaches the shard's WAL while the hold is up.
async fn runtime(store: &Arc<FaultStore>, incarnation: u64) -> HttpRig {
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::incarnation(incarnation),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            ..Default::default()
        },
    )
    .await;
    rig.state
        .runtime
        .history
        .paused
        .store(true, Ordering::Relaxed);
    rig
}

/// Polls `ready` every few milliseconds until it holds; fails after 10 s
/// naming `what`.
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what} did not happen within 10 s"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The current descriptor, read through the store.
async fn descriptor(state: &crate::http::AppState, name: &str) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}

/// Every record of the stream as the raw read serves it, with the page's
/// headers.
async fn records(
    addr: std::net::SocketAddr,
    name: &str,
) -> (HashMap<String, String>, Vec<serde_json::Value>) {
    let (status, headers, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(
        status,
        200,
        "read {name}: {}",
        String::from_utf8_lossy(&body)
    );
    (headers, serde_json::from_slice(&body).unwrap())
}

/// Every record of the collection as the product read serves it, with the
/// page's headers.
async fn product_records(
    addr: std::net::SocketAddr,
    name: &str,
) -> (HashMap<String, String>, Vec<serde_json::Value>) {
    let path = format!("/v1/streams/{name}/records");
    let (status, headers, body) = preq(addr, "GET", &path, &KEY, b"").await;
    assert_eq!(
        status,
        200,
        "read {name}: {}",
        String::from_utf8_lossy(&body)
    );
    (headers, serde_json::from_slice(&body).unwrap())
}

fn copies(recs: &[serde_json::Value], field: &str) -> usize {
    recs.iter().filter(|r| r.get(field).is_some()).count()
}

/// A response header by its lowercased name; absent headers stay absent so
/// an assertion shows the whole answer instead of an index panic.
fn header<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    headers.get(name).map(String::as_str)
}

/// A response body as text, so a mismatch prints what the wire said.
fn text(body: &[u8]) -> &str {
    std::str::from_utf8(body).unwrap_or("<not UTF-8>")
}

/// `Stream-Next-Offset` for a single-segment stream whose next offset is
/// `next`, as the raw surface encodes it.
fn position(next: u64) -> String {
    crate::http::tail_token(next)
}

/// The first runtime serving a raw JSON stream `name` whose warm-up record
/// is durable, so the shard is resident before the hold.
async fn raw_stream(store: &Arc<FaultStore>, name: &str) -> HttpRig {
    let rig = runtime(store, 0).await;
    let path = format!("/v1/stream/{name}");
    let (status, _, _) = hreq(rig.addr, "PUT", &path, &[JSON], b"").await;
    assert!(status == 200 || status == 201, "create {name}: {status}");
    let (status, headers, _) = hreq(rig.addr, "POST", &path, &[JSON], br#"[{"warm":0}]"#).await;
    assert_eq!(
        (status, header(&headers, "stream-next-offset")),
        (204, Some(position(1).as_str())),
        "the warm-up record is durable before the hold: {headers:?}"
    );
    rig
}

/// The first runtime serving a JSON collection `name` whose warm-up record
/// is durable.
async fn product_collection(store: &Arc<FaultStore>, name: &str) -> HttpRig {
    let rig = runtime(store, 0).await;
    let path = format!("/v1/streams/{name}");
    let format = br#"{"format":{"kind":"json"}}"#;
    let (status, _, _) = preq(rig.addr, "PUT", &path, &KEY, format).await;
    assert_eq!(status, 201, "create {name}");
    let (status, _, body) = preq(
        rig.addr,
        "POST",
        &format!("{path}/records"),
        &KEY,
        br#"{"warm":0}"#,
    )
    .await;
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (status, &body["count"], &body["duplicate"]),
        (200, &serde_json::json!(1), &serde_json::json!(false)),
        "the warm-up record is durable before the hold: {body}"
    );
    rig
}

/// The stranded write: `request`'s group is written and waiting on the
/// parked WAL PUT when the fleet retires the engine. Returns the retired
/// engine and the client's answer.
struct Retired {
    rig: HttpRig,
    prefix: String,
    engine: Arc<crate::shard::ShardEngine>,
    reply: Reply,
}

async fn strand(
    store: &Arc<FaultStore>,
    rig: HttpRig,
    request: impl std::future::Future<Output = Reply>,
) -> Retired {
    let held = rig.state.shards.held_prefixes();
    assert_eq!(held.len(), 1, "one shard serves the stream: {held:?}");
    let prefix = held[0].clone();
    let engine = rig
        .state
        .shards
        .open(&prefix)
        .expect("the shard is resident");
    // The shard's next WAL PUT parks; the history partition is outside the
    // hold, and the successor opens only after it is released. The Db lives
    // where the rig's opener puts it, `{prefix}/shard`, object-store
    // normalised (the root prefix "" is `shard`).
    let shard_db = object_store::path::Path::from(format!("{prefix}/shard"));
    let engaged = store.hold_class_under(StoreOp::Put, ObjClass::Wal, &format!("{shard_db}/"), 1);
    let retire = async {
        until("the group is written and its WAL PUT is parked", || {
            engaged.load(Ordering::SeqCst) > 0 && engine.oldest_inflight_ms() > 0
        })
        .await;
        match rig.state.shards.retire(
            &prefix,
            crate::shard_directory::RetirementReason::FleetEviction,
            |_, _| true,
        ) {
            crate::shard_directory::RetireOutcome::Retired(retired) => {
                assert!(Arc::ptr_eq(&retired, &engine));
            }
            crate::shard_directory::RetireOutcome::Kept => {
                panic!("an unconditional fleet retirement was declined")
            }
            crate::shard_directory::RetireOutcome::Absent => {
                panic!("the serving engine left before the retirement")
            }
        }
    };
    let (reply, ()) = futures_util::future::join(request, retire).await;
    Retired {
        rig,
        prefix,
        engine,
        reply,
    }
}

/// The retryable shard-moving/unknown answer, exactly as the raw wire gives
/// it today: no offset, no closure claim, one second to retry.
fn assert_shard_moving(reply: &Reply) {
    let (status, headers, body) = reply;
    assert_eq!(
        (
            *status,
            header(headers, "retry-after"),
            header(headers, "content-type")
        ),
        (503, Some("1"), Some("application/json")),
        "a stranded write is answered shard-moving/unknown, never a success: {headers:?} {}",
        String::from_utf8_lossy(body)
    );
    let body: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"error": {"code": "shard_moving", "message": "shard fenced by a new owner; retry"}})
    );
    assert!(
        !headers.contains_key("stream-next-offset") && !headers.contains_key("stream-closed"),
        "a stranded write is neither acknowledged nor declared absent: {headers:?}"
    );
}

/// The same outcome as the product surface renders it today: the raw
/// `shard_moving` becomes 503 `temporarily_unavailable`, retryable, one
/// second to retry, and nothing claims the collection sealed.
fn assert_temporarily_unavailable(reply: &Reply) {
    let (status, headers, body) = reply;
    assert_eq!(
        (
            *status,
            header(headers, "retry-after"),
            header(headers, "content-type"),
            header(headers, "cache-control"),
            header(headers, "prisma-sealed"),
        ),
        (
            503,
            Some("1"),
            Some("application/json"),
            Some("no-store"),
            None
        ),
        "a stranded seal is answered unknown and retryable, never a success: {headers:?} {}",
        String::from_utf8_lossy(body)
    );
    let body: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"error": {
            "code": "temporarily_unavailable",
            "message": "retry shortly",
            "retryable": true,
        }})
    );
}

/// Lets the retiring close reach the store, waits for the retired
/// incarnation to terminate, and starts the successor runtime over the same
/// store: its open replays what the close made durable.
async fn successor(store: &Arc<FaultStore>, retired: &Retired) -> HttpRig {
    store.release_hold();
    let shutdown = retired.engine.shutdown_handle();
    until("the retired engine terminates", || shutdown.terminated()).await;
    assert!(
        !retired.rig.state.shards.is_open(&retired.prefix),
        "the retiring runtime serves nothing for the prefix"
    );
    runtime(store, 1).await
}

/// A producer-keyed append stranded by its engine's retirement is answered
/// 503 `shard_moving` (outcome unknown); the successor runtime holds the
/// record exactly once; the exact retry of the same producer tuple is a
/// duplicate at the original position, not a second copy; and the producer
/// continues from the successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stranded_producer_append_is_moved_then_held_once_by_the_successor_and_its_retry_is_a_duplicate()
 {
    let store = FaultStore::new(mem(), 2026, FaultProfile::clean());
    let name = "f2-append";
    let producer = [
        JSON,
        ("producer-id", "writer"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let body = br#"[{"stranded":1}]"#;
    let path = format!("/v1/stream/{name}");
    let rig = raw_stream(&store, name).await;
    let addr = rig.addr;
    let retired = strand(&store, rig, hreq(addr, "POST", &path, &producer, body)).await;
    assert_shard_moving(&retired.reply);

    let next = successor(&store, &retired).await;
    let (_, recs) = records(next.addr, name).await;
    assert_eq!(
        (copies(&recs, "stranded"), recs.len()),
        (1, 2),
        "the successor recovers the stranded record exactly once: {recs:?}"
    );

    let (status, headers, body_out) = hreq(next.addr, "POST", &path, &producer, body).await;
    assert_eq!(
        (
            status,
            header(&headers, "producer-epoch"),
            header(&headers, "producer-seq"),
            header(&headers, "stream-next-offset"),
            header(&headers, "x-ack-closed"),
            header(&headers, "stream-closed"),
            text(&body_out),
        ),
        (
            204,
            Some("1"),
            Some("0"),
            Some(position(2).as_str()),
            Some("false"),
            None,
            "",
        ),
        "the retry is the original commit's duplicate, not a second copy: {headers:?}"
    );
    let (_, recs) = records(next.addr, name).await;
    assert_eq!(
        (copies(&recs, "stranded"), recs.len()),
        (1, 2),
        "the duplicate stored nothing: {recs:?}"
    );

    let continued = [
        JSON,
        ("producer-id", "writer"),
        ("producer-epoch", "1"),
        ("producer-seq", "1"),
    ];
    let (status, headers, _) =
        hreq(next.addr, "POST", &path, &continued, br#"[{"after":1}]"#).await;
    assert_eq!(
        (
            status,
            header(&headers, "producer-seq"),
            header(&headers, "stream-next-offset")
        ),
        (200, Some("1"), Some(position(3).as_str())),
        "the producer continues on the successor from the sequence it committed: {headers:?}"
    );
    engine_shutdown(&next.state).await;
}

/// The seal claim exactly as the retirement left it: unmarked, owing this
/// operation's final record.
fn assert_owed(desc: &crate::registry::StreamDesc, operation: &str, generation: u64) {
    assert!(
        !desc.sealed,
        "nothing publishes Sealed before the final is marked durable"
    );
    let claim = desc
        .sealing
        .as_ref()
        .expect("the retirement retained the owed-final claim");
    assert_eq!(
        (
            claim.operation_id.as_str(),
            claim.claim_generation,
            claim.owes_final()
        ),
        (operation, generation, true),
        "{:?}",
        claim.intent
    );
    assert_eq!(
        claim.intent,
        crate::registry::SealIntent::Final {
            routing_key: String::new(),
            request_hash: operation.to_string(),
            final_committed: false,
        }
    );
}

/// A raw append-and-close whose final record the engine wrote before
/// retiring is answered 503 `shard_moving`; the seal claim stays owed, with
/// the same operation and generation, on the retiring runtime and on the
/// successor; the successor holds the final exactly once with the segment
/// closed; the exact retry of the close is the committed final's duplicate,
/// marks it and publishes Sealed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stranded_final_record_keeps_its_owed_seal_and_the_exact_retry_completes_it_on_the_successor()
 {
    let store = FaultStore::new(mem(), 2027, FaultProfile::clean());
    let name = "f2-final";
    let close = [JSON, ("stream-closed", "true")];
    let body = br#"[{"fin":"a"}]"#;
    let path = format!("/v1/stream/{name}");
    let rig = raw_stream(&store, name).await;
    let addr = rig.addr;
    let retired = strand(&store, rig, hreq(addr, "POST", &path, &close, body)).await;
    assert_shard_moving(&retired.reply);
    let owed = descriptor(&retired.rig.state, name).await;
    let operation = owed
        .sealing
        .as_ref()
        .expect("the retirement retained the owed-final claim")
        .operation_id
        .clone();
    assert!(!operation.is_empty(), "a close has an operation identity");
    assert_owed(&owed, &operation, 1);

    let next = successor(&store, &retired).await;
    let (headers, recs) = records(next.addr, name).await;
    assert_eq!(
        (
            copies(&recs, "fin"),
            recs.len(),
            header(&headers, "stream-closed")
        ),
        (1, 2, Some("true")),
        "the successor recovers the final once, in a closed segment: {recs:?} {headers:?}"
    );
    assert_owed(&descriptor(&next.state, name).await, &operation, 1);

    let (status, headers, body_out) = hreq(next.addr, "POST", &path, &close, body).await;
    assert_eq!(
        (
            status,
            header(&headers, "stream-closed"),
            header(&headers, "x-ack-closed"),
            header(&headers, "stream-next-offset"),
            text(&body_out),
        ),
        (
            204,
            Some("true"),
            Some("true"),
            Some(position(2).as_str()),
            "",
        ),
        "the exact retry completes the owed final: {headers:?}"
    );
    assert_sealed(&descriptor(&next.state, name).await);
    let (headers, recs) = records(next.addr, name).await;
    assert_eq!(
        (
            copies(&recs, "fin"),
            recs.len(),
            header(&headers, "stream-closed")
        ),
        (1, 2, Some("true")),
        "the completed seal stored no second final: {recs:?}"
    );
    engine_shutdown(&next.state).await;
}

/// The seal the exact retry completed: Sealed published, no claim left.
fn assert_sealed(desc: &crate::registry::StreamDesc) {
    assert!(
        desc.sealed && desc.sealing.is_none(),
        "the retry marked the final and published Sealed: sealed={} sealing={:?}",
        desc.sealed,
        desc.sealing
    );
}

/// A product `:seal` whose final record the engine wrote before retiring is
/// answered 503 `temporarily_unavailable`, retryable; the claim still owes
/// that request's final on the retiring runtime and on the successor; the
/// successor holds the final exactly once; the exact retry of the seal
/// completes it and seals the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stranded_product_seal_final_stays_owed_and_its_exact_retry_seals_on_the_successor() {
    let store = FaultStore::new(mem(), 2028, FaultProfile::clean());
    let name = "f2-seal";
    let seal = format!("/v1/streams/{name}:seal");
    let body = br#"{"final":{"fin":"p"}}"#;
    let rig = product_collection(&store, name).await;
    let addr = rig.addr;
    let retired = strand(&store, rig, preq(addr, "POST", &seal, &KEY, body)).await;
    assert_temporarily_unavailable(&retired.reply);
    let operation = crate::application::lifecycle::seal_op_id_full(br#"{"fin":"p"}"#, "", None);
    assert_owed(&descriptor(&retired.rig.state, name).await, &operation, 1);

    let next = successor(&store, &retired).await;
    let (headers, recs) = product_records(next.addr, name).await;
    assert_eq!(
        (
            copies(&recs, "fin"),
            recs.len(),
            header(&headers, "prisma-sealed")
        ),
        (1, 2, Some("true")),
        "the successor recovers the final once, in a closed segment: {recs:?} {headers:?}"
    );
    assert_owed(&descriptor(&next.state, name).await, &operation, 1);

    let (status, headers, answer) = preq(next.addr, "POST", &seal, &KEY, body).await;
    assert_eq!(
        (
            status,
            header(&headers, "content-type"),
            serde_json::from_slice::<serde_json::Value>(&answer).unwrap()
        ),
        (
            200,
            Some("application/json"),
            serde_json::json!({"sealed": true})
        ),
        "the exact retry completes the owed final: {headers:?}"
    );
    assert_sealed(&descriptor(&next.state, name).await);
    let (headers, recs) = product_records(next.addr, name).await;
    assert_eq!(
        (
            copies(&recs, "fin"),
            recs.len(),
            header(&headers, "prisma-sealed")
        ),
        (1, 2, Some("true")),
        "the completed seal stored no second final: {recs:?}"
    );
    engine_shutdown(&next.state).await;
}
