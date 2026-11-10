#![cfg(test)]
//! Native queues on segments through the real HTTP router (S3b-DESIGN §4.4,
//! §12; part 2 step 6): a queue's messages are pages of its stream, sealed
//! into segment objects, leased from pages read outside the committer, and
//! given back by the trim. Every assertion reads the wire answers and the
//! shard's stored state: the gauges, the refcounts, the objects.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;
type Engine = Arc<crate::shard::ShardEngine>;

async fn q(addr: Addr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let raw = if body.is_null() {
        Vec::new()
    } else {
        body.to_string().into_bytes()
    };
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(addr, method, path, &key, &raw).await;
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// The queue's engine and hash, with its collector driven by the test (no
/// grace, no loop of its own).
async fn queue_engine(state: &State, name: &str) -> (Engine, [u8; 16]) {
    let sref = crate::tenant::ProjectId::new("proj-test")
        .unwrap()
        .stream_ref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let engine = state
        .shards
        .resolve(&seg.shard_route, crate::shard_directory::Adoption::Internal)
        .await
        .unwrap();
    engine.gc_state().configure(|s| {
        s.auto = false;
        s.grace = Duration::ZERO;
    });
    (engine, seg.identity)
}

/// Create `name` and send `n` messages `{"n": i}` of `pad` bytes each, in
/// batches of 100.
async fn filled(addr: Addr, name: &str, n: u32, pad: usize) {
    assert_eq!(
        q(addr, "PUT", &format!("/v1/queues/{name}"), Value::Null)
            .await
            .0,
        201
    );
    for start in (0..n).step_by(100) {
        let messages: Vec<Value> = (start..(start + 100).min(n))
            .map(|i| json!({ "body": { "n": i, "pad": "x".repeat(pad) } }))
            .collect();
        let path = format!("/v1/queues/{name}/messages:batch");
        let (st, v) = q(addr, "POST", &path, json!({ "messages": messages })).await;
        assert_eq!(st, 201, "{v}");
    }
}

/// One claim of up to `max`: each message's `n` and token.
async fn claim(addr: Addr, name: &str, max: u32) -> Vec<(u64, Value)> {
    let body = json!({ "max": max, "maxBytes": 8 << 20, "leaseMs": 60_000 });
    let path = format!("/v1/queues/{name}/groups/default:claim");
    let (st, v) = q(addr, "POST", &path, body).await;
    assert_eq!(st, 200, "{v}");
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["body"]["n"].as_u64().unwrap_or(u64::MAX),
                m["token"].clone(),
            )
        })
        .collect()
}

async fn settle(addr: Addr, name: &str, op: &str, tokens: &[Value]) {
    let ops: Vec<Value> = tokens
        .iter()
        .map(|t| json!({ "token": t, "op": op }))
        .collect();
    let path = format!("/v1/queues/{name}/groups/default:settle");
    let (st, v) = q(addr, "POST", &path, json!({ "ops": ops })).await;
    assert_eq!(st, 200, "{v}");
}

/// The queue's (queuedMessages, queuedBytes).
async fn gauges(addr: Addr, name: &str) -> (u64, u64) {
    let (_, v) = q(addr, "GET", &format!("/v1/queues/{name}"), Value::Null).await;
    let s = &v["stats"];
    (
        s["queuedMessages"].as_u64().unwrap(),
        s["queuedBytes"].as_u64().unwrap(),
    )
}

/// **DST 1: a large backlog acked** (§4.4). 1,000 messages sealed into
/// segments are claimed and acked in ten rounds: each round's ack takes
/// exactly its messages out of the gauge (bytes falling with them), and once
/// the backlog is gone the trim gives back every chunk, every object's count
/// reaches zero and the collector deletes exactly those objects, leaving no
/// `'R'` row behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_large_acked_backlog_is_given_back_to_its_last_object() {
    let (state, addr) = http_rig(mem()).await;
    filled(addr, "big", 1_000, 200).await;
    let (engine, hash) = queue_engine(&state, "big").await;
    engine.seal_requested().await.expect("seal");
    let chunks = engine.live_chunks(hash).await.unwrap();
    let objects: std::collections::BTreeSet<_> = chunks.iter().map(|c| c.object).collect();
    assert!(!chunks.is_empty() && engine.unsealed_page_for_tests(&hash, 0).is_none());
    let (mut left, mut bytes) = gauges(addr, "big").await;
    assert_eq!(left, 1_000);
    for round in 0..10 {
        let leased = claim(addr, "big", 100).await;
        let want: Vec<u64> = (round * 100..(round + 1) * 100).collect();
        assert_eq!(leased.iter().map(|l| l.0).collect::<Vec<_>>(), want);
        let tokens: Vec<Value> = leased.into_iter().map(|l| l.1).collect();
        settle(addr, "big", "ack", &tokens).await;
        let (now, now_bytes) = gauges(addr, "big").await;
        assert_eq!(
            now,
            left - 100,
            "round {round}: its hundred out of the gauge"
        );
        assert!(now_bytes < bytes, "round {round}: bytes fall with them");
        (left, bytes) = (now, now_bytes);
    }
    assert_eq!((left, bytes), (0, 0));
    let report = engine.trim_pass().await.unwrap();
    assert_eq!(report.released, chunks.len() as u64);
    let counted = engine.refcount_rows().await.unwrap();
    assert!(
        counted.iter().all(|(id, _)| !objects.contains(id)),
        "no 'R' row of the queue's objects: {counted:?}"
    );
    assert_eq!(
        engine.collect().await.unwrap(),
        objects.len() as u64,
        "exactly its objects deleted"
    );
    engine_shutdown(&state).await;
}

/// **DST 2: the first message failed, the rest acked** (§4.4): exactly one
/// chunk stays pinned, the failed message's, and every other is given back.
/// (The cleaner copying it out when the queue rule pays comes with part 2
/// step 4, package 6's cleaner change.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_failed_first_message_pins_exactly_its_own_chunk() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/pin", Value::Null).await.0, 201);
    let (engine, hash) = queue_engine(&state, "pin").await;
    for n in 0..4 {
        let path = "/v1/queues/pin/messages";
        assert_eq!(
            q(addr, "POST", path, json!({ "body": { "n": n } })).await.0,
            201
        );
        engine.seal_requested().await.expect("seal");
    }
    let chunks = engine.live_chunks(hash).await.unwrap();
    assert_eq!(chunks.len(), 4, "one chunk each");
    let leased = claim(addr, "pin", 4).await;
    settle(addr, "pin", "fail", &[leased[0].1.clone()]).await;
    let rest: Vec<Value> = leased[1..].iter().map(|l| l.1.clone()).collect();
    settle(addr, "pin", "ack", &rest).await;
    assert_eq!(engine.trim_pass().await.unwrap().released, 3);
    let head = engine
        .head_row(&hash)
        .await
        .unwrap()
        .expect("a Trimming row");
    let iv = crate::native_queue::intervals::Interval { first: 1, last: 3 };
    assert_eq!(
        (head.reclaimed, engine.intervals(&hash).await.unwrap()),
        (0, vec![iv]),
        "the failed message's chunk holds the prefix; the rest are released"
    );
    let counted: std::collections::BTreeMap<_, _> = engine
        .refcount_rows()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(id, r)| Some((id, r?.live_chunks)))
        .collect();
    let live: Vec<u64> = chunks
        .iter()
        .map(|c| counted.get(&c.object).copied().unwrap_or(0))
        .collect();
    assert_eq!(live, [1, 0, 0, 0], "exactly one chunk pinned");
    engine_shutdown(&state).await;
}

/// **F1 over segments: one unreadable message among 1,000.** Its page is
/// corrupted while unsealed (its clear header admits, its envelope will not
/// open), and all 1,000 are sealed. Claimed in rounds from their sealed
/// pages, the other 999 are delivered once each, in order; the one fails
/// alone, cause `unreadable`, and never holds the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn one_unreadable_message_among_a_thousand_sealed_fails_alone() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(
        q(addr, "PUT", "/v1/queues/poison", Value::Null).await.0,
        201
    );
    let (engine, hash) = queue_engine(&state, "poison").await;
    engine.pause_seal(true);
    let path = "/v1/queues/poison/messages:batch";
    for start in (0..1_000u32).step_by(100) {
        let messages: Vec<Value> = (start..start + 100)
            .map(|n| json!({ "body": { "n": n } }))
            .collect();
        assert_eq!(
            q(addr, "POST", path, json!({ "messages": messages }))
                .await
                .0,
            201
        );
    }
    engine.corrupt_unsealed_page_for_tests(&hash, 500);
    engine.pause_seal(false);
    engine.seal_requested().await.expect("seal");
    assert!(
        engine.unsealed_page_for_tests(&hash, 500).is_none(),
        "sealed"
    );
    let mut delivered = Vec::new();
    for _ in 0..11 {
        let leased = claim(addr, "poison", 100).await;
        delivered.extend(leased.iter().map(|l| l.0));
        let tokens: Vec<Value> = leased.into_iter().map(|l| l.1).collect();
        if !tokens.is_empty() {
            settle(addr, "poison", "ack", &tokens).await;
        }
    }
    let want: Vec<u64> = (0..1_000).filter(|n| *n != 500).collect();
    assert_eq!(delivered, want, "the 999 others, once each, in order");
    let (_, v) = q(
        addr,
        "GET",
        "/v1/queues/poison/groups/default/failed?limit=10",
        Value::Null,
    )
    .await;
    let failed = v["messages"].as_array().unwrap();
    assert_eq!(
        (failed.len(), &failed[0]["failure"]["cause"]),
        (1, &json!("unreadable")),
        "{v}"
    );
    let (_, v) = q(addr, "GET", "/v1/queues/poison", Value::Null).await;
    let s = &v["stats"];
    assert_eq!(
        (&s["failed"], &s["queuedMessages"]),
        (&json!(1), &json!(0)),
        "{v}"
    );
    engine_shutdown(&state).await;
}

/// **F3: 1,000 parked claims reserve zero bytes.** A queue whose sealed
/// backlog was claimed and acked has nothing for a claim; 1,000 claims
/// waiting on it park, and while they wait the instance's read memory holds
/// nothing for any of them (a claim's hold of the pages it prepared ends
/// before it parks). Each then answers empty at its wait's end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_thousand_parked_claims_reserve_zero_bytes() {
    let (state, addr) = http_rig(mem()).await;
    filled(addr, "idle", 100, 64).await;
    let (engine, _) = queue_engine(&state, "idle").await;
    engine.seal_requested().await.expect("seal");
    let leased = claim(addr, "idle", 100).await;
    let tokens: Vec<Value> = leased.into_iter().map(|l| l.1).collect();
    settle(addr, "idle", "ack", &tokens).await;
    let admission = &state.admission;
    let path = "/v1/queues/idle/groups/default:claim";
    let waits = (0..1_000).map(|_| async {
        let body = json!({ "max": 1, "waitMs": 2_000 });
        let (st, v) = q(addr, "POST", path, body).await;
        (st, v["messages"].as_array().map(Vec::len))
    });
    let probe = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while admission.parked() < 1_000 {
            assert!(
                std::time::Instant::now() < deadline,
                "parked {}",
                admission.parked()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        admission.read_memory().0
    };
    let (answers, held) = tokio::join!(futures_util::future::join_all(waits), probe);
    assert_eq!(held, 0, "1,000 parked claims hold no read memory");
    assert!(
        answers.iter().all(|a| *a == (200, Some(0))),
        "each answers empty"
    );
    engine_shutdown(&state).await;
}
