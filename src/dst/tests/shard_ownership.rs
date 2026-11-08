#![cfg(test)]
//! A shard owns exactly the rows of the streams and queues the registry routes to it (layout 8's
//! G3, S3a-DESIGN rev 3 §1.1; ROUTE-HASH-DECISION-2026-11-08 §5, test 1). On a cell of four
//! bit-prefix shards, with every stream and queue created through the product path:
//! - every per-entity key a shard's log holds, and every chunk entry of its seal directory, names
//!   an entity hash inside the shard's prefix;
//! - every stream and queue reads back whole after a seal, a fold and a cleaner pass on every shard.
//!
//! An entity hash minted apart from the route breaks the first on about three quarters of the
//! rows. A walk that filters by range then drops those rows' chunks, which breaks the second.

use super::billing_closure_debts::{JSON, ROW, engine_of, raw_put, stored};
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::keyspace::EntityHash;
use crate::keyspace::range::Range;
use serde_json::{Value, json};

/// Four shards whose prefixes are bits: a label (`"02"`) is no prefix, and its range is every hash.
const PREFIXES: [&str; 4] = ["00", "01", "10", "11"];
const STREAMS: usize = 64;
const QUEUES: usize = 8;
const APPENDS: usize = 3;
const MESSAGES: usize = 2;

type Addr = std::net::SocketAddr;

/// A queue route with the stream key; `Null` sends no body.
async fn q(addr: Addr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let raw = if body.is_null() {
        Vec::new()
    } else {
        body.to_string().into_bytes()
    };
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(addr, method, path, &key, &raw).await;
    let v = serde_json::from_slice(&b).unwrap_or(Value::Null);
    (st, v)
}

/// The chunk entries of a seal-directory row whose stream's hash is outside `range`; nothing for
/// another sentinel row.
fn foreign_chunks(range: Range, key: &[u8], value: &[u8]) -> Vec<String> {
    let Some(object) = crate::spill::directory::parse_directory_key(key) else {
        return Vec::new();
    };
    let entries = crate::spill::directory::decode_row(object, value).unwrap();
    entries
        .iter()
        .filter(|(hash, _)| !range.contains(EntityHash::from_stored(*hash)))
        .map(|(hash, _)| format!("chunk {}", crate::crypto::hex(&hash[..4])))
        .collect()
}

/// Every key of a shard's log outside the sentinel whose hash is outside the shard's range, and
/// every seal-directory entry whose hash is.
async fn foreign_rows(engine: &crate::shard::ShardEngine) -> (usize, Vec<String>) {
    let range = Range::of(&engine.prefix);
    let mut rows = engine.db.scan(..).await.unwrap();
    let (mut entity_rows, mut foreign) = (0, Vec::new());
    while let Some(row) = rows.next().await.unwrap() {
        let Some(hash) = row.key.first_chunk::<16>() else {
            continue;
        };
        if *hash == crate::keyspace::SENTINEL {
            foreign.extend(foreign_chunks(range, &row.key, &row.value));
            continue;
        }
        entity_rows += 1;
        if !range.contains(EntityHash::from_stored(*hash)) {
            foreign.push(format!("row {}", crate::crypto::hex(&row.key[..17])));
        }
    }
    (entity_rows, foreign)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn every_row_of_a_shard_lies_in_its_range() {
    let options = HttpRigOptions {
        prefixes: PREFIXES.iter().map(|p| (*p).to_string()).collect(),
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    for i in 0..STREAMS {
        let name = format!("own-{i}");
        raw_put(addr, &name, &[JSON], b"").await;
        for _ in 0..APPENDS {
            let (st, _, _) = hreq(addr, "POST", &format!("/v1/stream/{name}"), &[JSON], ROW).await;
            assert_eq!(st, 204, "append to {name}");
        }
    }
    for i in 0..QUEUES {
        let queue = format!("/v1/queues/ownq-{i}");
        assert_eq!(
            q(addr, "PUT", &queue, json!({})).await.0,
            201,
            "create {queue}"
        );
        for m in 0..MESSAGES {
            let body = json!({ "body": { "m": m } });
            let (st, v) = q(addr, "POST", &format!("{queue}/messages"), body).await;
            assert_eq!(st, 201, "send to {queue}: {v}");
        }
    }
    let engines = state.shards.engines();
    assert_eq!(
        engines.len(),
        PREFIXES.len(),
        "every shard holds some of the streams"
    );
    for engine in &engines {
        engine.seal_requested().await.expect("seal");
        engine.fold_once().await.expect("fold");
        engine.clean_pass().await.expect("clean");
    }
    for i in 0..STREAMS {
        let name = format!("own-{i}");
        let path = format!("/v1/stream/{name}?offset=-1");
        let (st, _, body) = hreq(addr, "GET", &path, &[], b"").await;
        assert_eq!(st, 200, "read {name}");
        let records: Vec<Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(records.len(), APPENDS, "{name} reads back whole");
        let desc = stored(&state, &state.deployment.raw_adapter_sref(&name)).await;
        let engine = engine_of(&state, &desc, 0).await;
        let hash = desc.dynamic_segment_identity(0);
        assert!(
            Range::of(&engine.prefix).contains(EntityHash::from_stored(hash)),
            "{name} is served by shard {} but its hash {} is outside it",
            engine.prefix,
            crate::crypto::hex(&hash[..4])
        );
    }
    for i in 0..QUEUES {
        let claim = json!({ "max": 8, "leaseMs": 30_000, "waitMs": 1_000 });
        let path = format!("/v1/queues/ownq-{i}/groups/default:claim");
        let (st, v) = q(addr, "POST", &path, claim).await;
        assert_eq!(st, 200, "{v}");
        let claimed = v["messages"].as_array().map_or(0, Vec::len);
        assert_eq!(claimed, MESSAGES, "queue {i} reads back whole: {v}");
    }
    let mut total = 0;
    for engine in &engines {
        let (rows, foreign) = foreign_rows(engine).await;
        assert!(
            foreign.is_empty(),
            "shard {} holds {} of {rows} entity rows outside its range: {:?}",
            engine.prefix,
            foreign.len(),
            &foreign[..foreign.len().min(6)]
        );
        total += rows;
    }
    assert!(
        total > STREAMS + QUEUES,
        "the shards hold the entities' rows"
    );
    engine_shutdown(&state).await;
}
