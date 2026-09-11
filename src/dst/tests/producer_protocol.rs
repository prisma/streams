//! Producer protocol.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::{mem, open_engine_with_settings, skey};
use crate::dst::{FaultPlan, FaultStore};

/// These are distinct wire operations: seals carry neither data nor a producer.
enum LaneWrite {
    Sequence(Option<String>),
    Producer { id: &'static str, seq: u64 },
    Seal,
}

/// Own the common packet shape while each scenario controls its lane and operation.
struct LaneSender<'a> {
    engine: &'a crate::shard::ShardEngine,
    key: &'a crate::crypto::StreamKey,
    body: &'static [u8],
}

impl LaneSender<'_> {
    async fn send(
        &self,
        identity: [u8; 16],
        lineage: Vec<[u8; 16]>,
        routing_key: String,
        operation: LaneWrite,
    ) -> Result<crate::shard::AppendAck, crate::shard::AppendErr> {
        let subkey = crate::crypto::derive_subkey(self.key, &identity, &routing_key, 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash: identity,
            route: identity,
            entries: vec![bytes::Bytes::from_static(self.body)],
            usage: crate::usage::counters(&identity),
            key_hash: crate::crypto::stream_hash(&routing_key),
            routing_key,
            producer_lineage: lineage,
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes: 0,
            finish: crate::shard::AppendFinish::Open,
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            seal_gen: None,
            billing: None,
            resp: tx,
        };
        match operation {
            LaneWrite::Sequence(seq) => req.seq = seq,
            LaneWrite::Producer { id, seq } => {
                req.producer = Some(crate::shard::ProducerReq {
                    id: id.into(),
                    epoch: 1,
                    seq,
                    request_hash: None,
                });
            }
            LaneWrite::Seal => {
                req.entries.clear();
                req.finish = crate::shard::AppendFinish::Close;
            }
        }
        assert!(self.engine.try_enqueue(req).is_ok());
        rx.await.expect("resp")
    }
}

/// ROUTING-V3 §3.6: Stream-Seq is scoped to the ROUTING KEY. Two keys
/// advance independent lanes on one segment; a regression within one
/// key still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_seq_is_scoped_to_the_routing_key() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 108, FaultPlan::new(0, 0, 0));
    let key = skey();
    let hash = [0xB7u8; 16];
    let engine = open_engine_with_settings(
        store.clone(),
        "dst-keyseq",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    let sender = LaneSender {
        engine: &engine,
        key: &key,
        body: b"{}",
    };
    let send = |rk: &'static str, seq: &'static str| {
        sender.send(
            hash,
            Vec::new(),
            rk.to_owned(),
            LaneWrite::Sequence(Some(seq.to_owned())),
        )
    };
    assert!(send("a", "s1").await.is_ok());
    assert!(send("b", "s1").await.is_ok(), "key b has its own lane");
    assert!(send("a", "s2").await.is_ok());
    assert!(send("b", "s2").await.is_ok());
    // Regression WITHIN a key conflicts; the other key is untouched.
    match send("a", "s2").await {
        Err(crate::shard::AppendErr::SeqConflict { current }) => {
            assert_eq!(current.as_deref(), Some("s2"));
        }
        other => panic!("expected per-key seq conflict, got {other:?}"),
    }
    assert!(send("b", "s3").await.is_ok());
    engine.begin_close();
}

/// ROUTING-V3 §3.6 release gate: a producer retry whose first attempt
/// committed on the SEALED PARENT segment must be recognized by the
/// child through the predecessor chain — duplicate ack carrying the
/// parent's committed offset, and NO offset consumed on the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_retries_across_a_split_commit_once() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 109, FaultPlan::new(0, 0, 0));
    let key = skey();
    let parent = [0xC1u8; 16];
    let child = [0xC2u8; 16];
    let engine = open_engine_with_settings(
        store.clone(),
        "dst-splitprod",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    let sender = LaneSender {
        engine: &engine,
        key: &key,
        body: br#"{"p":1}"#,
    };
    let send = |identity, lineage, seq, close| {
        let operation = if close {
            LaneWrite::Seal
        } else {
            LaneWrite::Producer { id: "prod-1", seq }
        };
        sender.send(identity, lineage, "pk".into(), operation)
    };

    // Commit (epoch 1, seq 0) on the parent, then seal it — the split.
    let ack1 = send(parent, vec![], 0, false).await.expect("parent commit");
    assert!(!ack1.duplicate);
    let parent_off = ack1.last_offset;
    send(parent, vec![], 0, true).await.expect("seal parent");

    // The ambiguous retry lands on the CHILD with the predecessor chain:
    // recognized as a duplicate, answered with the PARENT's offset, and
    // the child consumes no offset.
    let ack2 = send(child, vec![parent], 0, false)
        .await
        .expect("child retry");
    assert!(ack2.duplicate, "retry across the seal must be a duplicate");
    assert_eq!(
        ack2.last_offset, parent_off,
        "duplicate must answer with the ORIGINAL committed offset"
    );
    let child_next = {
        let h = engine.stream_handle(child).await.unwrap();
        let st = h.state.lock().unwrap();
        st.durable.next.max(st.applied.next)
    };
    assert_eq!(
        child_next, 0,
        "the duplicate must not consume a child offset"
    );

    // The NEXT sequence commits on the child normally, seeded state
    // continuing the chain.
    let ack3 = send(child, vec![parent], 1, false).await.expect("child s1");
    assert!(!ack3.duplicate);
    assert_eq!(ack3.last_offset, 0, "first real child record at offset 0");
    engine.begin_close();
}

/// Review blocker 4: a sequence the PARENT accepted must conflict on
/// the child — Stream-Seq resolves through the sealed predecessor
/// chain (nearest identity wins), and the next sequence continues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_seq_resolves_through_predecessors() {
    let store = mem();
    let key = skey();
    let parent = [0xD1u8; 16];
    let child = [0xD2u8; 16];
    let engine = open_engine_with_settings(
        store.clone(),
        "dst-seqchain",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    let sender = LaneSender {
        engine: &engine,
        key: &key,
        body: br#"{"s":1}"#,
    };
    let send = |identity, lineage, seq: Option<&str>, close| {
        let operation = if close {
            LaneWrite::Seal
        } else {
            LaneWrite::Sequence(seq.map(str::to_owned))
        };
        sender.send(identity, lineage, "sk".into(), operation)
    };

    send(parent, vec![], Some("s10"), false)
        .await
        .expect("parent accepts s10");
    send(parent, vec![], None, true).await.expect("seal parent");

    // The parent's lane must gate the child through the chain.
    match send(child, vec![parent], Some("s10"), false).await {
        Err(crate::shard::AppendErr::SeqConflict { current }) => {
            assert_eq!(current.as_deref(), Some("s10"));
        }
        other => panic!("s10 on the child must conflict, got {other:?}"),
    }
    match send(child, vec![parent], Some("s09"), false).await {
        Err(crate::shard::AppendErr::SeqConflict { .. }) => {}
        other => panic!("s09 on the child must conflict, got {other:?}"),
    }
    let ack = send(child, vec![parent], Some("s11"), false)
        .await
        .expect("s11 advances the chained lane");
    assert_eq!(ack.last_offset, 0, "first real child record");
    engine.begin_close();
}

/// Review finding 5: producer sessions are scoped per ROUTING KEY. One
/// producer id runs independent sequence lanes on two keys of one
/// segment, and each lane follows ITS key through a split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_lanes_scoped_per_routing_key() {
    let store = mem();
    let key = skey();
    let parent = [0xD3u8; 16];
    let child = [0xD4u8; 16];
    let engine = open_engine_with_settings(
        store.clone(),
        "dst-prodkeys",
        crate::shard::ShardConfig::default(),
        slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await;
    let sender = LaneSender {
        engine: &engine,
        key: &key,
        body: br#"{"p":1}"#,
    };
    let send = |identity, lineage, rk: &str, seq, close| {
        let operation = if close {
            LaneWrite::Seal
        } else {
            LaneWrite::Producer { id: "prod-x", seq }
        };
        sender.send(identity, lineage, rk.to_owned(), operation)
    };

    // Alternating sequences on two keys, ONE producer id: independent
    // lanes must both start at 0 and advance without cross-talk.
    let k1s0 = send(parent, vec![], "k1", 0, false).await.expect("k1 s0");
    assert!(!k1s0.duplicate);
    let k2s0 = send(parent, vec![], "k2", 0, false).await.expect("k2 s0");
    assert!(!k2s0.duplicate, "k2's lane is independent of k1's");
    let k1s1 = send(parent, vec![], "k1", 1, false).await.expect("k1 s1");
    assert!(!k1s1.duplicate);
    let k2s1 = send(parent, vec![], "k2", 1, false).await.expect("k2 s1");
    assert!(!k2s1.duplicate);

    send(parent, vec![], "k1", 9, true).await.expect("seal");

    // Across the split, each key's duplicate answers with ITS OWN
    // original offset — not the other key's, not the tail.
    let d1 = send(child, vec![parent], "k1", 1, false)
        .await
        .expect("k1 retry");
    assert!(d1.duplicate);
    assert_eq!(d1.last_offset, k1s1.last_offset, "k1's own offset");
    let d2 = send(child, vec![parent], "k2", 1, false)
        .await
        .expect("k2 retry");
    assert!(d2.duplicate);
    assert_eq!(d2.last_offset, k2s1.last_offset, "k2's own offset");

    // And fresh sequences continue independently on the child.
    assert!(
        !send(child, vec![parent], "k1", 2, false)
            .await
            .expect("k1 s2")
            .duplicate
    );
    assert!(
        !send(child, vec![parent], "k2", 2, false)
            .await
            .expect("k2 s2")
            .duplicate
    );
    engine.begin_close();
}

/// Stage 4: payload shape never changes operation meaning — append
/// stores ONE message (arrays stay array-valued records), appendMany
/// stores element-wise, both through the one committer path, with the
/// product response contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_append_and_append_many() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // append([1,2,3]) = ONE array-valued message.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "customer-42"),
        ],
        b"[1,2,3]",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["count"], 1);
    assert_eq!(v["duplicate"], false);
    assert_eq!(v["sealed"], false);
    let cursor1 = v["cursor"].as_str().unwrap().to_string();
    assert!(!cursor1.is_empty());

    // appendMany([{a},{b}]) = TWO messages, atomic, contiguous.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records:batch",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "customer-42"),
        ],
        br#"[{"id":1},{"id":2}]"#,
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["count"], 2);

    // The key sequence now holds 3 records: [1,2,3], {id:1}, {id:2} —
    // verified through the RAW keyed read (shared storage).
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/orders/records?routingKey=customer-42",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "1 array message + 2 batch messages");
    assert_eq!(recs[0], serde_json::json!([1, 2, 3]));
    assert_eq!(recs[1]["id"], 1);
    assert_eq!(recs[2]["id"], 2);

    // The returned cursor decodes and points past the appended records.
    let key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("orders"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("customer-42");
    let c = crate::product_cursor::KeyCursor::decode(
        &cursor1,
        state.deployment.deployment_tenant(),
        &key,
        &epoch,
        &kh,
    )
    .expect("cursor decodes");
    assert_eq!(c.offset, 1, "cursor after the first single append");

    assert_invalid_json_appends_are_rejected(addr).await;
    assert_bytes_stream_operation_contract(addr).await;
    engine_shutdown(&state).await;
}

/// Stage 4 §7: a duplicate producer request through the product route
/// returns duplicate: true and stores nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_append_producer_duplicate() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/pdup",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "ga"),
        ("producer-id", "checkout"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pdup/records",
        &hdrs,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], false);
    // Exact retry: recognized as duplicate, nothing stored twice.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/pdup/records",
        &hdrs,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/pdup/records?routingKey=ga",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "duplicate stored nothing");
    engine_shutdown(&state).await;
}

/// Stage 5 §7: the product checkpoint records the request hash — an
/// exact retry is a duplicate answering with the ORIGINAL cursor; the
/// same tuple with a different request is 409 producer_sequence_reused;
/// gaps and stale epochs carry the product taxonomy. The raw standards
/// route never compares bodies (pinned protocol).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_producer_hash_discipline() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/ph",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("0"),
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let c0 = v["cursor"].as_str().unwrap().to_string();
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("1"),
        b"{\"n\":2}",
    )
    .await;
    assert_eq!(st, 200);

    // Move the tail past the checkpoint with a plain (non-producer)
    // append, then retry the LATEST seq: the duplicate's cursor must
    // name the ORIGINAL commit (offset 2 = after n:2 at offset 1), not
    // the tail (offset 3). Older seqs degrade to the tail — the
    // checkpoint retains only the latest result (spec §7 last_result).
    let plain = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "g"),
    ];
    let (st, _, _) = preq(addr, "POST", "/v1/streams/ph/records", &plain, b"{\"n\":3}").await;
    assert_eq!(st, 200);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("1"),
        b"{\"n\":2}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("ph"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.epoch_bytes().unwrap();
    let kh = crate::crypto::stream_hash("g");
    let kc = crate::product_cursor::KeyCursor::decode(
        v["cursor"].as_str().unwrap(),
        state.deployment.deployment_tenant(),
        &skey(),
        &epoch,
        &kh,
    )
    .unwrap();
    assert_eq!(
        kc.offset, 2,
        "duplicate cursor = original commit, not the tail"
    );
    let kc0 = crate::product_cursor::KeyCursor::decode(
        &c0,
        state.deployment.deployment_tenant(),
        &skey(),
        &epoch,
        &kh,
    )
    .unwrap();
    assert_eq!(kc0.offset, 1, "first append's cursor");

    assert_producer_conflict_taxonomy(addr).await;
    assert_raw_duplicates_ignore_body(addr).await;
    engine_shutdown(&state).await;
}

/// Stage 5 §8: the request hash follows the routing key's predecessor
/// chain — after a split, an exact retry on the successor deduplicates
/// and a reused sequence with a different body still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_producer_hash_survives_split() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/psp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "ga"),
        ("producer-id", "svc"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":1}").await;
    assert_eq!(st, 200);
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("psp"),
            0,
            0x8000_0000_0000_0000
        )
        .await
    );
    // Exact retry lands on the successor: chain lookup finds the row
    // (with its hash) on the sealed parent.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":1}").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true, "retry across the split deduplicates");
    // Same tuple, different body: the hash traveled too.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/psp/records", &hdrs, b"{\"a\":2}").await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_sequence_reused");
    engine_shutdown(&state).await;
}

// Assertion phases retain the calling scenario's server and previously committed records.
fn producer_headers(seq: &str) -> [(&str, &str); 5] {
    [
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "g"),
        ("producer-id", "checkout"),
        ("producer-epoch", "1"),
        ("producer-seq", seq),
    ]
}

async fn assert_invalid_json_appends_are_rejected(addr: std::net::SocketAddr) {
    // Validation: empty batch, invalid JSON, oversized routing key.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records:batch",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"[]",
    )
    .await;
    assert_eq!(st, 400);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "empty_batch");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{not json",
    )
    .await;
    assert_eq!(st, 400);
    let long_key = "k".repeat(1025);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/orders/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", &long_key),
        ],
        b"1",
    )
    .await;
    assert_eq!(st, 400);
}

async fn assert_bytes_stream_operation_contract(addr: std::net::SocketAddr) {
    // Bytes stream: batch is 405; single stores the body as one record.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/blobs",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"bytes"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/blobs/records:batch",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"xx",
    )
    .await;
    assert_eq!(st, 405, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/blobs/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"raw-bytes-here",
    )
    .await;
    assert_eq!(st, 200);
}

async fn assert_producer_conflict_taxonomy(addr: std::net::SocketAddr) {
    // Older-seq retry: still a duplicate (no reuse conflict).
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("0"),
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["duplicate"], true);

    // Same tuple, different body: 409 producer_sequence_reused, nothing
    // stored.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("1"),
        b"{\"n\":99}",
    )
    .await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_sequence_reused");
    let (st, _, b) = preq(
        addr,
        "GET",
        "/v1/streams/ph/records?routingKey=g",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the reused sequence stored nothing");

    // Gap: 409 producer_gap with expected/received details.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/ph/records",
        &producer_headers("5"),
        b"{\"n\":5}",
    )
    .await;
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "producer_gap");
    assert_eq!(v["error"]["details"]["expected"], 2);
    assert_eq!(v["error"]["details"]["received"], 5);

    // Stale epoch: 403 stale_producer_epoch with the current epoch.
    let stale = vec![
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "g"),
        ("producer-id", "checkout"),
        ("producer-epoch", "0"),
        ("producer-seq", "0"),
    ];
    let (st, _, b) = preq(addr, "POST", "/v1/streams/ph/records", &stale, b"{\"n\":0}").await;
    assert_eq!(st, 403);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "stale_producer_epoch");
    assert_eq!(v["error"]["details"]["currentEpoch"], 1);
}

async fn assert_raw_duplicates_ignore_body(addr: std::net::SocketAddr) {
    // Raw standards route: the pinned protocol's duplicate contract
    // does NOT compare bodies — same tuple, different body, still 204.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/rawdup",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let rawh = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawdup", &rawh, b"[1]").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawdup", &rawh, b"[2]").await;
    assert_eq!(st, 204, "raw duplicate never compares bodies");
}
