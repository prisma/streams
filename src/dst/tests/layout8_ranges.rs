#![cfg(test)]
//! Layout 8's key families on one shard (S3a-DESIGN §1, §2, §4.1, §6): streams and queues created
//! through the product path, interleaved by hash, with appends, sends, claims, a seal, a fold and
//! further appends. Scanning the whole log afterwards:
//! - every key is an entity's row (`hash ‖ 'S' | 'Q' | 'V' ‖ …`) or a sentinel row of a tag with a
//!   split rule, and the entity hashes are exactly the created streams' and queues';
//! - the unsealed index names exactly the streams whose durable tail holds unsealed pages, and the
//!   usage-dirty index's versions are the tails' own;
//! - the usage debt index covers every stream holding a month final.

use std::collections::{BTreeMap, BTreeSet};

use super::billing_closure_debts::{JSON, ROW, raw_put, stored};
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::keyspace::{FINALS, QUEUE_TAG, SENTINEL, STREAM};
use serde_json::{Value, json};

const STREAMS: usize = 50;
const QUEUES: usize = 20;

type Addr = std::net::SocketAddr;

/// A queue route with the stream key.
async fn q(addr: Addr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(addr, method, path, &key, body.to_string().as_bytes()).await;
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// The log's keys and values, in key order.
async fn rows(engine: &crate::shard::ShardEngine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = engine.db.scan(..).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        out.push((row.key.to_vec(), row.value.to_vec()));
    }
    out
}

/// Creates the streams and queues and drives them: appends, sends, claims on every other queue,
/// a seal and a fold, then one more append to every fifth stream. The streams' names.
async fn populate(
    addr: Addr,
    state: &crate::http::AppState,
) -> (Vec<String>, std::sync::Arc<crate::shard::ShardEngine>) {
    let names: Vec<String> = (0..STREAMS).map(|i| format!("l8-{i}")).collect();
    for (i, name) in names.iter().enumerate() {
        raw_put(addr, name, &[JSON], b"").await;
        for _ in 0..1 + i % 3 {
            let (st, _, _) = hreq(addr, "POST", &format!("/v1/stream/{name}"), &[JSON], ROW).await;
            assert_eq!(st, 204, "append to {name}");
        }
    }
    for i in 0..QUEUES {
        let queue = format!("/v1/queues/l8q-{i}");
        assert_eq!(
            q(addr, "PUT", &queue, json!({})).await.0,
            201,
            "create {queue}"
        );
        let (st, v) = q(
            addr,
            "POST",
            &format!("{queue}/messages"),
            json!({ "body": { "i": i } }),
        )
        .await;
        assert_eq!(st, 201, "send to {queue}: {v}");
        if i % 2 == 0 {
            let claim = json!({ "max": 1, "leaseMs": 30_000, "waitMs": 1_000 });
            let (st, v) = q(
                addr,
                "POST",
                &format!("{queue}/groups/default:claim"),
                claim,
            )
            .await;
            assert_eq!(st, 200, "claim from {queue}: {v}");
        }
    }
    let engines = state.shards.engines();
    assert_eq!(engines.len(), 1, "one shard");
    let engine = engines[0].clone();
    engine.seal_requested().await.expect("seal");
    engine.fold_once().await.expect("fold");
    for name in names.iter().step_by(5) {
        let (st, _, _) = hreq(addr, "POST", &format!("/v1/stream/{name}"), &[JSON], ROW).await;
        assert_eq!(st, 204, "append to {name} after the fold");
    }
    (names, engine)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_key_family_lies_in_its_range_and_the_indexes_are_exact() {
    let options = HttpRigOptions {
        prefixes: vec![String::new()],
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let (names, engine) = populate(addr, &state).await;
    let engine = &engine;
    let mut streams = BTreeMap::new();
    for name in &names {
        let desc = stored(&state, &state.deployment.raw_adapter_sref(name)).await;
        streams.insert(desc.storage_hash(), name.clone());
    }

    let log = rows(engine).await;
    let mut entities = BTreeSet::new();
    let mut finals = BTreeSet::new();
    for (key, _) in &log {
        let (lead, rest) = key.split_first_chunk::<16>().expect("a 16-byte lead");
        let family = rest.first().copied();
        if *lead == SENTINEL {
            let t = family.expect("a sentinel row has a tag");
            let ruled = crate::keyspace::split_rules::split_rule(t).is_some();
            assert!(
                ruled,
                "a sentinel row of tag {} has no split rule",
                t as char
            );
            continue;
        }
        assert!(
            matches!(family, Some(STREAM | QUEUE_TAG | FINALS)),
            "an entity key of family {family:?}: {}",
            crate::crypto::hex(key)
        );
        entities.insert(*lead);
        if family == Some(FINALS) {
            finals.insert(*lead);
        }
    }
    let db = &engine.db;
    let unsealed: BTreeSet<[u8; 16]> = crate::shard::open_index::unsealed_members(db)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let dirty: BTreeMap<[u8; 16], u64> = crate::shard::open_index::dirty_members(db)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let debt_prefix = crate::billing::usage_debt_key(&[0; 16])[..17].to_vec();
    let debts: BTreeSet<[u8; 16]> = log
        .iter()
        .filter_map(|(key, _)| key.strip_prefix(&debt_prefix[..])?.try_into().ok())
        .collect();
    let stream_hashes: BTreeSet<[u8; 16]> = streams.keys().copied().collect();
    assert!(stream_hashes.is_subset(&entities), "every stream has rows");
    assert_eq!(
        entities.len(),
        STREAMS + QUEUES,
        "the streams' and the queues' hashes, no other"
    );
    assert!(
        finals.is_subset(&debts),
        "the usage debt index covers every final"
    );

    let mut expected = BTreeSet::new();
    for (hash, name) in &streams {
        let handle = engine.stream_handle(*hash).await.unwrap();
        let tail = handle.state.lock().unwrap().durable.clone();
        if tail.absorbed < tail.next {
            expected.insert(*hash);
        }
        if let Some(version) = dirty.get(hash) {
            let usage = tail.usage.as_ref().map(|u| u.usage_version);
            assert_eq!(
                usage,
                Some(*version),
                "{name}'s usage-dirty row is its tail's version"
            );
        }
    }
    assert!(
        !expected.is_empty(),
        "the appends after the fold left unsealed pages"
    );
    assert_eq!(
        unsealed, expected,
        "the unsealed index names exactly the unsealed streams"
    );
    engine_shutdown(&state).await;
}
