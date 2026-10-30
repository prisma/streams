//! Reads history: keyed and forked reads across sealed segment objects and
//! unsealed pages (layout 6).

use super::fixture_storage::{
    append_n, append_sized, drain_filtered, mem, open_engine, skey, wait_all_sealed,
};
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

/// An engine on `store` at `prefix` whose sealer seals only when the test
/// asks, in chunks of at most `pages_per_chunk` pages.
async fn manual_seal_engine(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    pages_per_chunk: usize,
) -> Arc<crate::shard::ShardEngine> {
    let engine = open_engine(store, prefix).await;
    engine.seal_ready().await.expect("seal open");
    engine.configure_seal(|s| {
        s.auto = false;
        s.max_pages_per_chunk = pages_per_chunk;
    });
    engine
}

/// Seal every durable record of `hash`.
async fn seal(engine: &Arc<crate::shard::ShardEngine>, hash: [u8; 16]) {
    engine
        .seal_once(crate::shard::seal::Trigger::Requested)
        .await
        .expect("seal");
    wait_all_sealed(engine, &[hash]).await;
}

/// The engine's sealer counter `name` (`GET /v1/debug/shards`).
fn seal_counter(engine: &crate::shard::ShardEngine, name: &str) -> u64 {
    engine.seal_json()[name].as_u64().expect("a seal counter")
}

/// Append one record `{"op": op, "att": 0, "k": rk}` under routing key
/// `rk` (the shape `drain_filtered` decodes).
async fn append_op(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    rk: &str,
    op: u64,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: vec![bytes::Bytes::from(
            serde_json::json!({"op": op, "att": 0, "k": rk})
                .to_string()
                .into_bytes(),
        )],
        usage: crate::usage::counters(&hash),
        routing_key: rk.to_string(),
        key_hash: crate::crypto::RoutingKeyHash::of(rk).0,
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: crate::crypto::derive_subkey(key, &hash, rk, 0),
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
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    rx.await.expect("resp").expect("ack");
}

/// ROUTING-V3 §5/§8.5 in layout 6: a sparse key spread across many sealed
/// chunks pages through and the cursor advances via consumed_to — every
/// match returned exactly once, in order, across multiple partial
/// responses, with no per-offset GET pattern: each chunk of only the
/// default key is skipped by its key summary without a GET, and each chunk
/// of the key costs exactly one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn sparse_key_reads_page_with_bounded_spans() {
    let key = skey();
    let hash = [0xB1u8; 16];
    let engine = manual_seal_engine(mem(), "dst-sparsekey", 1).await;
    // 24 requests of one "sp" record, each followed by a request of 511
    // default-key records (12,288 offsets): every request is one page, so
    // a one-page chunk holds either the key's record or none of the key.
    let mut expected = Vec::new();
    for i in 0..24u64 {
        append_op(&engine, hash, &key, "sp", i).await;
        expected.push((i, 0u32));
        append_n(&engine, hash, &key, 511, 40).await;
    }
    seal(&engine, hash).await;
    let (skips, gets) = (
        seal_counter(&engine, "summary_skips"),
        seal_counter(&engine, "sealed_gets"),
    );
    let got = drain_filtered(&engine, hash, &key, "sp").await;
    assert_eq!(
        got, expected,
        "sparse keyed paging lost or reordered records"
    );
    assert_eq!(
        seal_counter(&engine, "summary_skips") - skips,
        24,
        "each default-key chunk skipped by its summary, without a GET"
    );
    assert_eq!(
        seal_counter(&engine, "sealed_gets") - gets,
        24,
        "one GET per chunk of the key"
    );
    engine.begin_close();
}

/// ROUTING-V3 §8.6 in layout 6 (no postings, no envelope): a sealed chunk
/// whose bytes were altered in the store must never surface as a page
/// with records missing, completed or not. A keyed read of the key the
/// chunk holds fails loudly as corruption, at both visibilities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn corrupt_postings_fall_back_to_the_envelope() {
    let key = skey();
    let hash = [0xB3u8; 16];
    let store = mem();
    // No ring: the sealed range is read from the segment object.
    let engine = manual_seal_engine(store.clone(), "dst-corruptp", 8192).await;
    for i in 0..30u64 {
        append_op(&engine, hash, &key, "ck", i).await;
    }
    seal(&engine, hash).await;
    corrupt_the_one_chunk(&store).await;
    let handle = engine.stream_handle(hash).await.expect("handle");
    for deliver in [
        crate::shard::Deliver::Durable,
        crate::shard::Deliver::Applied,
    ] {
        let read = crate::http::read_merged(
            &key,
            &hash,
            &handle,
            &engine,
            0,
            Some("ck"),
            8 * 1024 * 1024,
            deliver,
        )
        .await;
        let error = match read {
            Ok(page) => panic!(
                "{deliver:?}: a corrupt chunk served {} records (completed {}, last {:?})",
                page.recs.len(),
                page.completed,
                page.last
            ),
            Err(error) => error,
        };
        assert!(error.contains("corrupt"), "{deliver:?}: {error}");
    }
    engine.begin_close();
}

/// Flip one byte inside the last page of the store's one segment object's
/// one chunk (before the chunk's CRC): the chunk spans the object up to
/// the table the trailer locates.
async fn corrupt_the_one_chunk(store: &Arc<dyn ObjectStore>) {
    use futures_util::TryStreamExt;
    let objects: Vec<_> = store
        .list(None)
        .try_filter(|meta| std::future::ready(meta.location.as_ref().contains("/hseg/")))
        .try_collect()
        .await
        .expect("list");
    assert_eq!(objects.len(), 1, "one seal, one segment object");
    let path = &objects[0].location;
    let mut body = store
        .get(path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    let trailer = body.len() - crate::spill::object::TRAILER_LEN;
    let table_off = u64::from_le_bytes(body[trailer..trailer + 8].try_into().unwrap());
    let at = usize::try_from(table_off).unwrap() - crate::spill::object::CHUNK_CRC_LEN - 4;
    body[at] ^= 0x40;
    store.put(path, body.into()).await.unwrap();
}

/// Keyed drain from `from` for routing key "cold": (offsets, next cursor).
async fn keyed_offsets(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    mut from: u64,
) -> (Vec<u64>, u64) {
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut offs = Vec::new();
    for _ in 0..64 {
        let res = crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            Some("cold"),
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        .expect("keyed read");
        for rec in &res.recs {
            offs.push(rec.off);
        }
        from = res.scanned_through(from);
        if res.completed {
            break;
        }
    }
    (offs, from)
}

/// A cold directory load (restart, ownership move) knows the chunks its
/// rebuild found. Records of the same key sealed afterwards into a second
/// object must reach a catch-up reader resuming from its old cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records() {
    let key = skey();
    let hash = [0xC7u8; 16];
    let store = mem();
    let warm = manual_seal_engine(store.clone(), "dst-coldclaim", 8192).await;
    let mut first = Vec::new();
    for _ in 0..4 {
        first.push(append_sized(&warm, hash, &key, "cold", 64).await);
    }
    seal(&warm, hash).await;
    warm.begin_close();

    // The instance that did not seal this data: its directory is rebuilt
    // from the shard log at open.
    let engine = manual_seal_engine(store, "dst-coldclaim", 8192).await;
    let (got, cursor) = keyed_offsets(&engine, hash, &key, 0).await;
    assert_eq!(got, first);
    assert_eq!(cursor, 4, "the cursor consumed the sealed range");
    assert_eq!(
        seal_counter(&engine, "directory_entries"),
        1,
        "the rebuilt directory"
    );

    // Same key, sealed AFTER the cold load, into a second object.
    let mut later = Vec::new();
    for _ in 0..4 {
        later.push(append_sized(&engine, hash, &key, "cold", 64).await);
    }
    seal(&engine, hash).await;
    let (caught_up, _) = keyed_offsets(&engine, hash, &key, cursor).await;
    assert_eq!(
        caught_up, later,
        "catch-up from the old cursor skipped durable records"
    );
    engine.begin_close();
}

/// A fork whose boundary falls inside a page of its parent's sealed
/// chunk reads the parent exactly up to the boundary (the page is sliced,
/// its later records never leak into the fork), then continues with the
/// fork's own records. Without a ring the parent's records come from the
/// segment object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fork_reads_its_parent_history_up_to_a_boundary_inside_a_page() {
    use super::fixture_http::{HttpRigOptions, cold_sealer, engine_shutdown, http_rig_build};
    use super::fixture_requests::hreq;
    let rig = http_rig_build(
        mem(),
        super::fixture_runtime::RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                tail_ring_bytes: 0,
                ..Default::default()
            },
            seal: Some(cold_sealer),
            ..Default::default()
        },
    )
    .await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(rig.addr, "PUT", "/v1/stream/pgparent", &ct, b"").await;
    assert!(st == 200 || st == 201, "create: {st}");
    let value = |n: &str| serde_json::from_str::<serde_json::Value>(&format!(r#"{{"n":{n}}}"#));
    let records: Vec<_> = (0..10).map(|n| value(&n.to_string()).unwrap()).collect();
    let body = serde_json::to_vec(&records).unwrap();
    let (st, _, _) = hreq(rig.addr, "POST", "/v1/stream/pgparent", &ct, &body).await;
    assert!(st == 200 || st == 204, "append: {st}");
    let sref = rig.state.deployment.raw_adapter_sref("pgparent");
    let desc = rig.state.registry.get(&sref).await.unwrap().unwrap();
    let route = desc.segment_route_by_id(0).unwrap();
    let engine = rig.state.engine_for(&route).await.unwrap();
    let hash = desc.storage_hash();
    engine.seal_requested().await.expect("seal");
    // The one request is one sealed page holding records 0 to 9, in one
    // chunk, and nothing of the parent is left unsealed.
    let handle = engine.stream_handle(hash).await.unwrap();
    {
        let st = handle.state.lock().unwrap();
        assert_eq!((st.durable.absorbed, st.durable.next), (10, 10), "sealed");
        assert!(st.pending.is_empty(), "no unsealed page left");
    }
    assert_eq!(seal_counter(&engine, "directory_entries"), 1, "one chunk");
    let gets = seal_counter(&engine, "sealed_gets");
    let fork = [
        ("content-type", "application/json"),
        ("stream-forked-from", "pgparent"),
        ("stream-fork-offset", "0000000000000000_0000000000000006"),
    ];
    let (st, _, b) = hreq(rig.addr, "PUT", "/v1/stream/pgchild", &fork, b"").await;
    assert_eq!(st, 201, "fork: {}", String::from_utf8_lossy(&b));
    let read = |name: &'static str| async move {
        let (st, _, b) = hreq(rig.addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
        assert_eq!(st, 200, "read {name}: {}", String::from_utf8_lossy(&b));
        serde_json::from_slice::<Vec<serde_json::Value>>(&b).unwrap()
    };
    assert_eq!(read("pgchild").await, records[..6]);
    assert!(
        seal_counter(&engine, "sealed_gets") > gets,
        "the fork read its parent's records from the segment object"
    );
    let own = value(r#""own""#).unwrap();
    let body = serde_json::to_vec(&[&own]).unwrap();
    let (st, _, _) = hreq(rig.addr, "POST", "/v1/stream/pgchild", &ct, &body).await;
    assert!(st == 200 || st == 204, "child append: {st}");
    let mut expected = records[..6].to_vec();
    expected.push(own);
    assert_eq!(read("pgchild").await, expected);
    assert_eq!(read("pgparent").await, records);
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(std::time::Duration::from_secs(5)).await;
}

/// A fork whose boundary falls inside a page its parent still holds
/// unsealed (nothing sealed) reads the parent exactly up to the boundary
/// from the unsealed pages, with and without a ring, then the fork's own
/// records; the parent still reads whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fork_inside_an_unabsorbed_parent_page_reads_to_its_boundary() {
    use super::fixture_http::{HttpRigOptions, cold_sealer, engine_shutdown, http_rig_build};
    use super::fixture_requests::hreq;
    for ring in [0usize, 1 << 20] {
        let rig = http_rig_build(
            mem(),
            super::fixture_runtime::RigRuntime::first(),
            HttpRigOptions {
                shard: crate::shard::ShardConfig {
                    tail_ring_bytes: ring,
                    ..Default::default()
                },
                seal: Some(cold_sealer),
                ..Default::default()
            },
        )
        .await;
        let ct = [("content-type", "application/json")];
        let (st, _, _) = hreq(rig.addr, "PUT", "/v1/stream/tparent", &ct, b"").await;
        assert!(st == 200 || st == 201, "create: {st}");
        let value = |n: &str| serde_json::from_str::<serde_json::Value>(&format!(r#"{{"n":{n}}}"#));
        let records: Vec<_> = (0..10).map(|n| value(&n.to_string()).unwrap()).collect();
        let body = serde_json::to_vec(&records).unwrap();
        let (st, _, _) = hreq(rig.addr, "POST", "/v1/stream/tparent", &ct, &body).await;
        assert!(st == 200 || st == 204, "append: {st}");
        let sref = rig.state.deployment.raw_adapter_sref("tparent");
        let desc = rig.state.registry.get(&sref).await.unwrap().unwrap();
        let route = desc.segment_route_by_id(0).unwrap();
        let engine = rig.state.engine_for(&route).await.unwrap();
        let handle = engine.stream_handle(desc.storage_hash()).await.unwrap();
        let tail = handle.state.lock().unwrap().durable.clone();
        assert_eq!(
            (tail.absorbed, tail.next),
            (0, 10),
            "ring {ring}: the page is unsealed"
        );
        let fork = [
            ("content-type", "application/json"),
            ("stream-forked-from", "tparent"),
            ("stream-fork-offset", "0000000000000000_0000000000000006"),
        ];
        let (st, _, b) = hreq(rig.addr, "PUT", "/v1/stream/tchild", &fork, b"").await;
        assert_eq!(st, 201, "fork: {}", String::from_utf8_lossy(&b));
        let read = |name: &'static str| async move {
            let (st, _, b) = hreq(rig.addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
            assert_eq!(st, 200, "read {name}: {}", String::from_utf8_lossy(&b));
            serde_json::from_slice::<Vec<serde_json::Value>>(&b).unwrap()
        };
        assert_eq!(read("tchild").await, records[..6], "ring {ring}");
        let own = value(r#""own""#).unwrap();
        let body = serde_json::to_vec(&[&own]).unwrap();
        let (st, _, _) = hreq(rig.addr, "POST", "/v1/stream/tchild", &ct, &body).await;
        assert!(st == 200 || st == 204, "child append: {st}");
        let mut expected = records[..6].to_vec();
        expected.push(own);
        assert_eq!(read("tchild").await, expected, "ring {ring}");
        assert_eq!(read("tparent").await, records, "ring {ring}");
        engine_shutdown(&rig.state).await;
        rig.tasks.shutdown(std::time::Duration::from_secs(5)).await;
    }
}
