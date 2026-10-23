//! Native queues through the real HTTP router (docs/QUEUES-DESIGN.md §11,
//! edge change #186): every assertion reads the wire answers and, where a
//! route deletes or keeps stored state, the shard rows themselves.
use std::sync::Arc;

use serde_json::{Value, json};

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

/// Another valid key: a request carrying it is not the queue's key holder.
const OTHER_KEY: &str = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=";

/// A queue route with the stream key under its `Streams-` alias; `Null`
/// sends no body.
async fn q(addr: std::net::SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    q_key(addr, method, path, body, PRISMA_KEY).await
}

async fn q_key(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Value,
    key: &str,
) -> (u16, Value) {
    let raw = if body.is_null() {
        Vec::new()
    } else {
        body.to_string().into_bytes()
    };
    let (st, _, b) = preq(addr, method, path, &[("streams-encryption-key", key)], &raw).await;
    let v = if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&b).unwrap_or_else(|_| json!(String::from_utf8_lossy(&b)))
    };
    (st, v)
}

/// The queue's engine and storage hash, as the routes resolve them.
async fn queue_engine(
    state: &Arc<crate::http::AppState>,
    name: &str,
) -> (Arc<crate::shard::ShardEngine>, [u8; 16]) {
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
    (engine, seg.identity)
}

/// Every queue row stored under `hash` (`hash ‖ 'Q' ‖ family`).
async fn queue_rows(engine: &crate::shard::ShardEngine, hash: &[u8; 16]) -> usize {
    let mut prefix = hash.to_vec();
    prefix.push(crate::native_queue::keys::TAG);
    let mut rows = engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut n = 0;
    while rows.next().await.unwrap().is_some() {
        n += 1;
    }
    n
}

async fn claim_one(addr: std::net::SocketAddr, queue: &str, lease_ms: u64) -> Value {
    let (st, v) = q(
        addr,
        "POST",
        &format!("/v1/queues/{queue}/groups/default:claim"),
        json!({ "max": 1, "leaseMs": lease_ms, "waitMs": 3_000 }),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["messages"].as_array().unwrap().len(), 1, "{v}");
    v["messages"][0].clone()
}

async fn settle(addr: std::net::SocketAddr, queue: &str, ops: Value) -> Vec<String> {
    let (st, v) = q(
        addr,
        "POST",
        &format!("/v1/queues/{queue}/groups/default:settle"),
        json!({ "ops": ops }),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    v["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["outcome"].as_str().unwrap().to_string())
        .collect()
}

/// Create → send → claim (the body decrypted) → ack: the page and rows of
/// the acked message are gone and a replayed or foreign token is stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sent_message_is_claimed_decrypted_and_acked() {
    let (state, addr) = http_rig(mem()).await;
    let (st, v) = q(addr, "PUT", "/v1/queues/jobs", Value::Null).await;
    assert_eq!(
        (st, &v["name"], &v["config"]["group"]["maxAttempts"]),
        (201, &json!("jobs"), &json!(5))
    );
    assert_eq!(
        q(addr, "PUT", "/v1/queues/jobs", json!({})).await.0,
        200,
        "same config"
    );
    let (st, v) = q(
        addr,
        "PUT",
        "/v1/queues/jobs",
        json!({ "retentionMs": 172_800_000 }),
    )
    .await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (409, &json!("queue_config_conflict"))
    );
    let body = json!({ "body": { "n": 1 }, "headers": { "schema": "v1" }, "key": "k1" });
    let (st, sent) = q(addr, "POST", "/v1/queues/jobs/messages", body).await;
    assert_eq!((st, &sent["duplicate"]), (201, &json!(false)), "{sent}");
    let id = sent["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("msg_"));
    let (st, v) = q_key(
        addr,
        "POST",
        "/v1/queues/jobs/groups/default:claim",
        json!({}),
        OTHER_KEY,
    )
    .await;
    assert_eq!((st, &v["error"]["code"]), (403, &json!("wrong_key")));
    let m = claim_one(addr, "jobs", 30_000).await;
    assert_eq!(
        (
            &m["id"],
            &m["body"],
            &m["headers"]["schema"],
            &m["key"],
            &m["contentType"]
        ),
        (
            &json!(id),
            &json!({ "n": 1 }),
            &json!("v1"),
            &json!("k1"),
            &json!("application/json")
        )
    );
    assert_eq!(
        (&m["attempt"], &m["cycle"], &m["lineage"]["originId"]),
        (&json!(1), &json!(1), &json!(id))
    );
    let token = m["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("lt_"));
    let ops = json!([{ "token": token, "op": "ack" }, { "token": "lt_garbage", "op": "ack" }]);
    assert_eq!(settle(addr, "jobs", ops).await, ["ok", "stale"]);
    assert_eq!(
        settle(addr, "jobs", json!([{ "token": token, "op": "ack" }])).await,
        ["stale"]
    );
    let (st, v) = q(addr, "GET", "/v1/queues/jobs", Value::Null).await;
    let s = &v["stats"];
    assert_eq!(st, 200);
    assert_eq!(
        (
            &s["counters"]["sent"],
            &s["counters"]["acked"],
            &s["queuedMessages"],
            &s["leased"]
        ),
        (&json!(1), &json!(1), &json!(0), &json!(0))
    );
    let (engine, hash) = queue_engine(&state, "jobs").await;
    let page = crate::crypto_page::shard_page_key(&hash, 0);
    assert!(
        engine.db.get(page).await.unwrap().is_none(),
        "the acked page is deleted"
    );
    let meta = crate::native_queue::keys::by_offset(&hash, crate::native_queue::keys::META, 0);
    assert!(
        engine.db.get(meta).await.unwrap().is_none(),
        "and its metadata row"
    );
    engine_shutdown(&state).await;
}

/// Bytes sent in URL-safe base64 without padding come back standard and
/// padded, and the queue list names the project's queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bytes_come_back_as_padded_base64_and_the_list_names_the_queue() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/jobs", Value::Null).await.0, 201);
    let (st, _) = q(
        addr,
        "POST",
        "/v1/queues/jobs/messages",
        json!({ "bodyBase64": "aGk" }),
    )
    .await;
    assert_eq!(st, 201);
    let m = claim_one(addr, "jobs", 30_000).await;
    assert_eq!(
        (&m["bodyBase64"], &m["body"], &m["contentType"]),
        (
            &json!("aGk="),
            &Value::Null,
            &json!("application/octet-stream")
        )
    );
    let (st, v) = q(addr, "GET", "/v1/queues", Value::Null).await;
    assert_eq!(
        (st, &v["queues"], &v["next"]),
        (200, &json!([{ "name": "jobs" }]), &Value::Null)
    );
    engine_shutdown(&state).await;
}

/// A retried delivery comes back with its next attempt, and an expired
/// lease does once the sweep moved it and its backoff passed; the expired
/// token is then stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_and_an_expired_lease_redeliver_with_the_next_attempt() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/rq", Value::Null).await.0, 201);
    assert_eq!(
        q(
            addr,
            "POST",
            "/v1/queues/rq/messages",
            json!({ "body": "x" })
        )
        .await
        .0,
        201
    );
    let first = claim_one(addr, "rq", 30_000).await;
    assert_eq!(first["attempt"], 1);
    let retry = json!([{ "token": first["token"], "op": "retry", "delayMs": 0, "code": "busy" }]);
    assert_eq!(settle(addr, "rq", retry).await, ["ok"]);
    let second = claim_one(addr, "rq", 1_000).await;
    assert_eq!(
        (&second["id"], &second["attempt"]),
        (&first["id"], &json!(2))
    );
    let (st, v) = q(
        addr,
        "POST",
        "/v1/queues/rq/groups/default:claim",
        json!({}),
    )
    .await;
    assert_eq!(
        (st, v["messages"].as_array().unwrap().len()),
        (200, 0),
        "leased: nothing due"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let (engine, hash) = queue_engine(&state, "rq").await;
    engine
        .submit_native_queue(hash, crate::native_queue::NqOp::Sweep)
        .await
        .unwrap();
    let third = claim_one(addr, "rq", 30_000).await;
    assert_eq!((&third["id"], &third["attempt"]), (&first["id"], &json!(3)));
    let late = json!([{ "token": second["token"], "op": "ack" }]);
    assert_eq!(settle(addr, "rq", late).await, ["stale"]);
    engine_shutdown(&state).await;
}

/// Dedup answers the original id; the envelope limit, the body rule,
/// scheduling past retention and the admission budget refuse exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedup_answers_the_original_and_refusals_are_exact() {
    let (state, addr) = http_rig(mem()).await;
    let cfg = json!({ "maxQueuedMessages": 3 });
    assert_eq!(q(addr, "PUT", "/v1/queues/dq", cfg).await.0, 201);
    let msg = json!({ "body": 1, "dedupKey": "d1" });
    let (st, a) = q(addr, "POST", "/v1/queues/dq/messages", msg.clone()).await;
    assert_eq!((st, &a["duplicate"]), (201, &json!(false)));
    let (st, b) = q(addr, "POST", "/v1/queues/dq/messages", msg).await;
    assert_eq!(
        (st, &b["duplicate"], &b["id"]),
        (200, &json!(true), &a["id"])
    );
    let both = json!({ "body": 1, "bodyBase64": "AA==" });
    let (st, v) = q(addr, "POST", "/v1/queues/dq/messages", both).await;
    assert_eq!((st, &v["error"]["code"]), (400, &json!("invalid_message")));
    let big = json!({ "body": "x".repeat(140_000) });
    let (st, v) = q(addr, "POST", "/v1/queues/dq/messages", big).await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (413, &json!("message_too_large"))
    );
    let late = json!({ "body": 1, "delayMs": 5 * 86_400_000_i64 });
    let (st, v) = q(addr, "POST", "/v1/queues/dq/messages", late).await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (400, &json!("deliver_at_beyond_retention"))
    );
    let batch = json!({ "messages": [{ "body": 2 }, { "body": 3 }] });
    let (st, v) = q(addr, "POST", "/v1/queues/dq/messages:batch", batch).await;
    assert_eq!((st, v["results"].as_array().unwrap().len()), (201, 2));
    let (st, v) = q(addr, "POST", "/v1/queues/dq/messages", json!({ "body": 4 })).await;
    assert_eq!(
        (st, &v["error"]["code"], &v["error"]["retryable"]),
        (429, &json!("queue_full"), &json!(true))
    );
    let (_, v) = q(addr, "GET", "/v1/queues/dq", Value::Null).await;
    assert_eq!(v["stats"]["queuedMessages"], 3);
    engine_shutdown(&state).await;
}

/// A failed delivery is listed with its envelope and cause, redriven into
/// a new cycle, and reclaimed once acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_message_is_listed_and_redriven() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/fq", Value::Null).await.0, 201);
    let (_, sent) = q(
        addr,
        "POST",
        "/v1/queues/fq/messages",
        json!({ "body": { "a": 1 } }),
    )
    .await;
    let id = sent["id"].clone();
    let m = claim_one(addr, "fq", 30_000).await;
    let fail = json!([{ "token": m["token"], "op": "fail", "code": "boom", "error": "text" }]);
    assert_eq!(settle(addr, "fq", fail).await, ["ok"]);
    let (st, v) = q(
        addr,
        "GET",
        "/v1/queues/fq/groups/default/failed?limit=10",
        Value::Null,
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let failed = &v["messages"][0];
    assert_eq!(
        (
            &failed["id"],
            &failed["body"],
            &failed["failure"]["cause"],
            &failed["failure"]["code"]
        ),
        (&id, &json!({ "a": 1 }), &json!("explicit"), &json!("boom"))
    );
    let path = format!("/v1/queues/fq/messages/{}", id.as_str().unwrap());
    let (st, v) = q(addr, "GET", &path, Value::Null).await;
    assert_eq!((st, &v["delivery"]["state"]), (200, &json!("failed")));
    let (st, v) = q(
        addr,
        "POST",
        "/v1/queues/fq/groups/default:redrive",
        json!({ "ids": [id] }),
    )
    .await;
    assert_eq!(
        (st, &v["redriven"], &v["stoppedAt"]),
        (200, &json!(1), &Value::Null)
    );
    let again = claim_one(addr, "fq", 30_000).await;
    assert_eq!(
        (&again["id"], &again["cycle"], &again["attempt"]),
        (&id, &json!(2), &json!(1))
    );
    assert_eq!(
        settle(
            addr,
            "fq",
            json!([{ "token": again["token"], "op": "ack" }])
        )
        .await,
        ["ok"]
    );
    let (st, v) = q(addr, "GET", &path, Value::Null).await;
    assert_eq!((st, &v["error"]["code"]), (404, &json!("reclaimed")));
    let (st, v) = q(addr, "GET", "/v1/queues/fq/messages/msg_zzzz", Value::Null).await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (404, &json!("message_not_found"))
    );
    engine_shutdown(&state).await;
}

/// Peek, cancel, republish (with lineage) and discard act on exactly the
/// messages they name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peek_cancel_republish_and_discard_act_on_the_named_messages() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/oq", Value::Null).await.0, 201);
    let (_, a) = q(
        addr,
        "POST",
        "/v1/queues/oq/messages",
        json!({ "body": "a" }),
    )
    .await;
    let (_, b) = q(
        addr,
        "POST",
        "/v1/queues/oq/messages",
        json!({ "body": "b" }),
    )
    .await;
    let (_, v) = q(
        addr,
        "GET",
        "/v1/queues/oq/groups/default/pending",
        Value::Null,
    )
    .await;
    let states: Vec<_> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["state"].clone())
        .collect();
    assert_eq!(states, [json!("pending"), json!("pending")]);
    let (_, v) = q(
        addr,
        "POST",
        "/v1/queues/oq:cancel",
        json!({ "ids": [a["id"]] }),
    )
    .await;
    assert_eq!(v["cancelled"], 1);
    let m = claim_one(addr, "oq", 30_000).await;
    assert_eq!(m["id"], b["id"], "the cancelled message is skipped");
    settle(addr, "oq", json!([{ "token": m["token"], "op": "fail" }])).await;
    let (st, v) = q(
        addr,
        "POST",
        "/v1/queues/oq:republish",
        json!({ "ids": [b["id"]] }),
    )
    .await;
    assert_eq!((st, &v["results"][0]["from"]), (200, &b["id"]));
    let fresh = claim_one(addr, "oq", 30_000).await;
    assert_eq!(
        (
            &fresh["id"],
            &fresh["body"],
            &fresh["lineage"]["republishedFrom"],
            &fresh["lineage"]["originId"]
        ),
        (&v["results"][0]["id"], &json!("b"), &b["id"], &b["id"])
    );
    let (_, v) = q(
        addr,
        "POST",
        "/v1/queues/oq/groups/default:discard",
        json!({ "ids": [b["id"]] }),
    )
    .await;
    assert_eq!(v["discarded"], 1);
    let (_, v) = q(
        addr,
        "GET",
        "/v1/queues/oq/groups/default/failed",
        Value::Null,
    )
    .await;
    assert_eq!(v["messages"], json!([]));
    engine_shutdown(&state).await;
}

/// Pause holds claims at once; after resume a parked claim takes a message
/// sent while it waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_holds_claims_and_a_parked_claim_takes_a_late_message() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/oq", Value::Null).await.0, 201);
    let held = json!({ "body": "held" });
    assert_eq!(q(addr, "POST", "/v1/queues/oq/messages", held).await.0, 201);
    let (st, v) = q(
        addr,
        "POST",
        "/v1/queues/oq/groups/default:pause",
        Value::Null,
    )
    .await;
    assert_eq!((st, &v["paused"]), (200, &json!(true)));
    let (_, v) = q(
        addr,
        "POST",
        "/v1/queues/oq/groups/default:claim",
        json!({ "waitMs": 2_000 }),
    )
    .await;
    assert_eq!(
        (&v["paused"], v["messages"].as_array().unwrap().len()),
        (&json!(true), 0)
    );
    q(
        addr,
        "POST",
        "/v1/queues/oq/groups/default:resume",
        Value::Null,
    )
    .await;
    assert_eq!(claim_one(addr, "oq", 30_000).await["body"], "held");
    let parked = q(
        addr,
        "POST",
        "/v1/queues/oq/groups/default:claim",
        json!({ "waitMs": 5_000 }),
    );
    let later = async {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        q(
            addr,
            "POST",
            "/v1/queues/oq/messages",
            json!({ "body": "late" }),
        )
        .await
    };
    let ((st, v), _) = tokio::join!(parked, later);
    assert_eq!(
        (st, &v["messages"][0]["body"]),
        (200, &json!("late")),
        "{v}"
    );
    engine_shutdown(&state).await;
}

/// DELETE fences sends at once; the cleanup leaves no queue row and no
/// page, the queue is then gone, and its name can be created again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_queue_is_fenced_then_gone_without_rows() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/zq", Value::Null).await.0, 201);
    let batch = json!({ "messages": [{ "body": 1 }, { "body": 2 }, { "body": 3 }] });
    assert_eq!(
        q(addr, "POST", "/v1/queues/zq/messages:batch", batch)
            .await
            .0,
        201
    );
    claim_one(addr, "zq", 30_000).await;
    let (engine, hash) = queue_engine(&state, "zq").await;
    assert!(queue_rows(&engine, &hash).await > 0);
    let (st, v) = q(addr, "DELETE", "/v1/queues/zq", Value::Null).await;
    assert_eq!((st, &v["deleting"]), (202, &json!(true)));
    let (st, v) = q(addr, "POST", "/v1/queues/zq/messages", json!({ "body": 4 })).await;
    let code = v["error"]["code"].as_str().unwrap().to_string();
    assert!(
        (st, code.as_str()) == (409, "queue_deleting")
            || (st, code.as_str()) == (404, "queue_not_found"),
        "{st} {v}"
    );
    let mut gone = false;
    for _ in 0..100 {
        if q(addr, "GET", "/v1/queues/zq", Value::Null).await.0 == 404 {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(gone, "the cleanup finishes");
    assert_eq!(queue_rows(&engine, &hash).await, 0, "no queue row remains");
    for offset in 0..3 {
        let page = crate::crypto_page::shard_page_key(&hash, offset);
        assert!(
            engine.db.get(page).await.unwrap().is_none(),
            "page {offset}"
        );
    }
    assert_eq!(
        q(addr, "PUT", "/v1/queues/zq", Value::Null).await.0,
        201,
        "a new incarnation"
    );
    engine_shutdown(&state).await;
}

/// A queue stream is refused on every stream surface, a stream is not a
/// queue, and the first release has one group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_streams_are_refused_on_the_stream_surfaces() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(q(addr, "PUT", "/v1/queues/sq", Value::Null).await.0, 201);
    let refused = [
        ("GET", "/v1/streams/sq/records", Value::Null),
        ("POST", "/v1/streams/sq/records", json!({ "x": 1 })),
        (
            "PUT",
            "/v1/streams/sq",
            json!({ "format": { "kind": "json" } }),
        ),
        ("GET", "/v1/streams/sq", Value::Null),
        ("DELETE", "/v1/streams/sq", Value::Null),
        ("GET", "/v1/stream/sq", Value::Null),
        ("POST", "/v1/stream/sq", json!({ "x": 1 })),
        ("DELETE", "/v1/stream/sq", Value::Null),
    ];
    for (method, path, body) in refused {
        let (st, v) = q(addr, method, path, body).await;
        assert_eq!(
            (st, &v["error"]["code"]),
            (409, &json!("queue_via_queue_routes")),
            "{method} {path}"
        );
    }
    let fork = [
        ("stream-forked-from", "sq"),
        ("stream-encryption-key", PRISMA_KEY),
        ("content-type", "application/vnd.prisma.queue+json"),
    ];
    let (st, _, b) = preq(addr, "PUT", "/v1/stream/sqfork", &fork, b"").await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    assert!(String::from_utf8_lossy(&b).contains("queue_via_queue_routes"));
    let (st, v) = q(addr, "GET", "/v1/queues/sq", Value::Null).await;
    assert_eq!(st, 200, "the queue itself is untouched: {v}");
    let stream = json!({ "format": { "kind": "json" } });
    assert_eq!(q(addr, "PUT", "/v1/streams/plain", stream).await.0, 201);
    let (st, v) = q(addr, "PUT", "/v1/queues/plain", Value::Null).await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (409, &json!("queue_config_conflict"))
    );
    let (st, v) = q(addr, "GET", "/v1/queues/plain", Value::Null).await;
    assert_eq!((st, &v["error"]["code"]), (404, &json!("queue_not_found")));
    let (st, v) = q(addr, "POST", "/v1/queues/sq/groups/other:claim", json!({})).await;
    assert_eq!((st, &v["error"]["code"]), (404, &json!("group_not_found")));
    let hook = json!({ "url": "https://example.com/hook" });
    let (st, v) = q(addr, "PUT", "/v1/queues/sq/groups/default/wakeup", hook).await;
    assert_eq!(
        (st, &v["error"]["code"]),
        (503, &json!("wakeup_unavailable")),
        "no wake key here"
    );
    let (st, _) = q(
        addr,
        "DELETE",
        "/v1/queues/sq/groups/default/wakeup",
        Value::Null,
    )
    .await;
    assert_eq!(st, 204);
    engine_shutdown(&state).await;
}
