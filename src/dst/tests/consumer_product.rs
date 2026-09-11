//! Consumer product.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, consumer_version, preq};
use super::fixture_storage::mem;

/// Stage 2a: consumer config lifecycle — idempotent create, conflict,
/// get, delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_config_lifecycle() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cc",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let cfg = br#"{"visibilityTimeoutMs":5000,"maxAttempts":3}"#;
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        cfg,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        cfg,
    )
    .await;
    assert_eq!(st, 200, "identical config is idempotent");
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityTimeoutMs":9000}"#,
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "consumer_config_conflict");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["visibilityTimeoutMs"], 5000);
    assert_eq!(v["maxAttempts"], 3);
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/nope",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404);
    // Round 16: deletion is the generation-fenced saga — 204 means
    // collection-wide, GET 404s afterwards, and a retried DELETE is
    // an idempotent 204 on the tombstone. Round 17: the request names
    // the INCARNATION via its version token; the retry reuses the same
    // token against the tombstone.
    let ver = consumer_version(addr, "cc", "work").await;
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/cc/consumers/work",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "GET",
        "/v1/streams/cc/consumers/work",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 404, "a deleted consumer must be gone");
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/cc/consumers/work",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "delete retry is idempotent on the tombstone");
    engine_shutdown(&state).await;
}

/// Stage 2a §2.3: per-key FIFO — a key with an active lease blocks its
/// later records; other keys flow; the ack unblocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_per_key_fifo() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cf",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for (k, n) in [("a", 0), ("a", 1), ("b", 0)] {
        let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/cf/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cf/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"max":10}"#,
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    let got: Vec<(String, i64)> = msgs
        .iter()
        .map(|m| {
            (
                m["routingKey"].as_str().unwrap().to_string(),
                m["value"]["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![("a".into(), 0), ("b".into(), 0)],
        "a/1 must be blocked behind a/0's active lease"
    );
    let a0_token = msgs[0]["leaseToken"].as_str().unwrap().to_string();

    // Ack a/0: a/1 becomes deliverable.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{a0_token}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 1);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cf/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"max":10}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["routingKey"], "a");
    assert_eq!(msgs[0]["value"]["n"], 1);
    assert_eq!(msgs[0]["attempts"], 1);
    engine_shutdown(&state).await;
}

/// Stage 2a §2.7: visibility expiry redelivers with attempts+1; a stale
/// (superseded) lease token is counted and cannot settle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_expiry_and_stale_fencing() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cv",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cv/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "k"),
        ],
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cv/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":1000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let first = v["messages"][0]["leaseToken"].as_str().unwrap().to_string();
    assert_eq!(v["messages"][0]["attempts"], 1);

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":30000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["messages"][0]["attempts"], 2, "expired lease redelivers");
    let fresh = v["messages"][0]["leaseToken"].as_str().unwrap().to_string();

    // The superseded first token is stale: counted, cannot ack.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{first}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 0);
    assert_eq!(v["stale"], 1);
    // The fresh token acks.
    let body = format!("{{\"acks\":[{{\"leaseToken\":\"{fresh}\"}}]}}");
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cv/consumers/w:settle",
        &[("prisma-encryption-key", PRISMA_KEY)],
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["acked"], 1);
    engine_shutdown(&state).await;
}

/// Stage 2a §2.8: exceeding maxAttempts appends ONE record to the
/// dead-letter stream (durable before the source settles) and the
/// source message leaves the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_dlq_flow() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for s in ["cd", "cd-dlq"] {
        let path = format!("/v1/streams/{s}");
        let (st, _, _) = preq(
            addr,
            "PUT",
            &path,
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201);
    }
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/cd/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "p"),
        ],
        b"{\"poison\":true}",
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cd/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"maxAttempts":1,"deadLetterStream":"cd-dlq"}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Attempt 1, then let it expire.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"visibilityMs":1000}"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    // The next pull classifies it poison: DLQ append + source settle.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["messages"].as_array().unwrap().is_empty(),
        "poison is not redelivered"
    );
    // DLQ stream holds exactly one record with the source metadata.
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/cd-dlq/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "exactly one DLQ record");
    assert_eq!(recs[0]["sourceStream"], "cd");
    assert_eq!(recs[0]["consumer"], "w");
    assert_eq!(recs[0]["routingKey"], "p");
    assert_eq!(recs[0]["attempts"], 1);
    assert_eq!(recs[0]["value"]["poison"], true);
    // Queue is drained: another pull with the key unblocked and empty
    // backlog.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/cd/consumers/w:pull",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(v["messages"].as_array().unwrap().is_empty());
    engine_shutdown(&state).await;
}

/// Stage 2a §2.9: consumption across a split — the sealed predecessor's
/// backlog delivers (and settles) fully before any successor record,
/// per-key order holds end to end, exactly once.
#[expect(
    clippy::too_many_lines,
    reason = "lineage consumption scenario; consuming the sealed predecessor's backlog, splitting and continuing on the children form one causal sequence; helper phases would hide which lineage step delivered or settled out of order"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_consumer_drains_lineage_across_split() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cl",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/cl/consumers/w",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(st, 201);
    for n in 0..3 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cl/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("cl"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    for n in 3..6 {
        for k in ["ga", "gb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{n}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/cl/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    // Pull + ack until drained; record delivery order per key.
    let mut per_key: std::collections::HashMap<String, Vec<i64>> = Default::default();
    let mut total = 0usize;
    for _round in 0..40 {
        let (st, _, b) = preq(
            addr,
            "POST",
            "/v1/streams/cl/consumers/w:pull",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"max":10}"#,
        )
        .await;
        assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let msgs = v["messages"].as_array().unwrap().clone();
        if msgs.is_empty() {
            if v["backlog"].as_u64() == Some(0) && total == 12 {
                break;
            }
            continue;
        }
        let mut acks = Vec::new();
        for m in &msgs {
            per_key
                .entry(m["routingKey"].as_str().unwrap().to_string())
                .or_default()
                .push(m["value"]["n"].as_i64().unwrap());
            total += 1;
            acks.push(serde_json::json!({"leaseToken": m["leaseToken"]}));
        }
        let body = serde_json::json!({ "acks": acks }).to_string();
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/cl/consumers/w:settle",
            &[("prisma-encryption-key", PRISMA_KEY)],
            body.as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    assert_eq!(total, 12, "every record exactly once: {per_key:?}");
    assert_eq!(
        per_key["ga"],
        vec![0, 1, 2, 3, 4, 5],
        "ga in order across the split"
    );
    assert_eq!(
        per_key["gb"],
        vec![0, 1, 2, 3, 4, 5],
        "gb in order across the split"
    );
    engine_shutdown(&state).await;
}

/// Dead-letter delivery writes with the SOURCE collection's key, so the
/// link is only meaningful between collections that share one. The gate
/// is at configuration time, where the caller can still act on it —
/// otherwise the mismatch surfaces much later as a poisoned key that
/// can never drain, with every DLQ append refused in silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_letter_link_requires_a_shared_key() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    for (name, key) in [
        ("dlq-src", PRISMA_KEY),
        ("dlq-same", PRISMA_KEY),
        ("dlq-other", OTHER_KEY),
    ] {
        let (st, _, b) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &[("prisma-encryption-key", key)],
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(st, 201, "{name}: {}", String::from_utf8_lossy(&b));
    }
    let put_dlq = |target: &str| {
        let body = format!(r#"{{"deadLetterStream":"{target}"}}"#);
        async move {
            preq(
                addr,
                "PUT",
                "/v1/streams/dlq-src/consumers/w",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("content-type", "application/json"),
                ],
                body.as_bytes(),
            )
            .await
        }
    };
    let code = |b: &[u8]| -> String {
        serde_json::from_slice::<serde_json::Value>(b)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
            .unwrap_or_default()
    };

    let (st, _, b) = put_dlq("dlq-missing").await;
    assert_eq!(st, 400);
    assert_eq!(code(&b), "unknown_dead_letter_stream");

    let (st, _, b) = put_dlq("dlq-src").await;
    assert_eq!(st, 400, "self-DLQ is a delivery loop");
    assert_eq!(code(&b), "invalid_config");

    let (st, _, b) = put_dlq("dlq-other").await;
    assert_eq!(st, 400, "{}", String::from_utf8_lossy(&b));
    assert_eq!(code(&b), "dead_letter_key_mismatch");

    let (st, _, b) = put_dlq("dlq-same").await;
    assert!(st == 200 || st == 201, "{}", String::from_utf8_lossy(&b));

    // Sealing the target closes the link for anyone configuring it next.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dlq-same:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/dlq-src/consumers/w2",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/json"),
        ],
        br#"{"deadLetterStream":"dlq-same"}"#,
    )
    .await;
    assert_eq!(st, 400);
    assert_eq!(code(&b), "dead_letter_sealed");
    engine_shutdown(&state).await;
}
