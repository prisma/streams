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

/// The two members of the boot scenarios and the ring that keeps the cell's one shard `""` on the
/// first (rendezvous: `" streams-1"` scores over `" streams-2"`).
const RING: [&str; 2] = ["streams-1", "streams-2"];
const BOOT_STREAM: &str = "boot";

/// A member of a fleet over `fleet`, with the rig's one shard `""`.
fn member(name: &str, fleet: &std::sync::Arc<dyn object_store::ObjectStore>) -> HttpRigOptions {
    HttpRigOptions {
        fleet_store: Some(fleet.clone()),
        instance: Some(name.to_string()),
        ..Default::default()
    }
}

/// The fleet documents that alone give the ring of both members: `streams-1`'s fresh beat and a
/// desired count of two.
async fn seed_ring(fleet: &std::sync::Arc<dyn object_store::ObjectStore>) {
    use object_store::ObjectStoreExt;
    let beat = format!(
        r#"{{"instance":"streams-1","ts_ms":{},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[""],"draining":false,"url":"https://streams-1.invalid"}}"#,
        crate::shard::now_ms() + 60_000
    );
    let path = object_store::path::Path::from("fleet/streams-1.json");
    fleet.put(&path, beat.into_bytes().into()).await.unwrap();
    let desired = br#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_vec();
    let path = object_store::path::Path::from("fleet/desired.json");
    fleet.put(&path, desired.into()).await.unwrap();
}

/// One product append to the boot stream at `addr`: its status, headers and error code.
async fn boot_append(addr: Addr) -> (u16, std::collections::HashMap<String, String>, String) {
    let path = format!("/v1/streams/{BOOT_STREAM}/records");
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("prisma-routing-key", "k"),
    ];
    let (status, headers, body) = preq(addr, "POST", &path, &headers, br#"{"n":1}"#).await;
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let code = body["error"]["code"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (status, headers, code)
}

/// Appends to the boot stream at `addr` every 10 ms until `stop`; each answer other than `200`,
/// counted by status and code.
async fn steady_appends(
    addr: Addr,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::collections::BTreeMap<(u16, String), usize> {
    let mut refused = std::collections::BTreeMap::new();
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        let (status, _, code) = boot_append(addr).await;
        if status != 200 {
            *refused.entry((status, code)).or_insert(0) += 1;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    refused
}

/// `streams-2`'s boot beside `streams-1` at `a_addr` (`a_booting_member_opens_nothing_before_its_
/// first_view`): before its first tick its telemetry round and a customer append open nothing and
/// are refused; after the tick the append is forwarded to `streams-1` and acked. Stops the load.
async fn boot_second_member(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    fleet: &std::sync::Arc<dyn object_store::ObjectStore>,
    a_addr: Addr,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> super::fixture_http::HttpRig {
    let b = http_rig_build(store, RigRuntime::incarnation(1), member(RING[1], fleet)).await;
    let telemetry = crate::ops::emit_metrics_once(&b.state).await;
    let (status, headers, code) = boot_append(b.addr).await;
    assert_eq!(
        b.state.shards.open_count(),
        0,
        "B opens nothing before its view"
    );
    let retry = headers.get("retry-after").map(String::as_str);
    assert_eq!(
        (status, retry, code.as_str()),
        (503, Some("1"), "temporarily_unavailable")
    );
    assert!(
        telemetry.is_err(),
        "B's telemetry is refused: {telemetry:?}"
    );
    assert!(crate::fleet::start_configured(b.state.clone(), &b.tasks));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    while b.state.ownership.effective_owner("").as_deref() != Some(RING[0]) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "B's first tick names A"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // The rig's peer URL is plain http, which the tick's URL check refuses: the test supplies it.
    let mut forwarded = (0, None);
    for _ in 0..3 {
        b.state
            .peer
            .set_peer(RING[0], &format!("http://{}", a_addr));
        let (status, headers, _) = boot_append(b.addr).await;
        forwarded = (status, headers.get("streams-forwarded-to").cloned());
        if status == 200 {
            break;
        }
    }
    assert_eq!(
        forwarded,
        (200, Some(RING[0].to_string())),
        "forwarded to A"
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    b
}

/// **A booting fleet member opens nothing before its first view** (GATE-DECISION-meas149-
/// reshaping F3, which decides DESIGN-DECISION-c3-incomplete-rig Addendum 9's open item: in fleet
/// mode an empty view means "not known", never "mine"). Two members over one store and one fleet
/// store. `streams-1` (A) serves the cell's one shard `""` under a steady stream of appends; the
/// ring of both members keeps `""` on A. `streams-2` (B) starts with the fleet documents alone
/// giving that ring, and before its first fleet tick runs its telemetry round and takes one
/// customer append. B opens no shard, its append is answered `503 temporarily_unavailable` with
/// `Retry-After: 1` and its telemetry round is refused. After B's first tick names A the owner, the
/// same append is forwarded to A and acked. Throughout, A's engine is the same incarnation and none
/// of its appends is refused. Before F3, B's telemetry opened `""` and fenced A (the meas-149
/// 10,000 step: `shard db closed: Fenced`, 621 sheds, a 7.2 s p99).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_booting_member_opens_nothing_before_its_first_view() {
    let (store, fleet) = (mem(), mem());
    let a = http_rig_build(store.clone(), RigRuntime::first(), member(RING[0], &fleet)).await;
    let ring: Vec<String> = RING.map(str::to_string).to_vec();
    a.state
        .ownership
        .set_view(ring, std::collections::HashMap::new());
    super::billing_closure_debts::product_put(a.addr, BOOT_STREAM).await;
    assert_eq!(boot_append(a.addr).await.0, 200, "A serves the boot stream");
    let served = a.state.shards.open("").expect("A serves the cell's shard");
    seed_ring(&fleet).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (refused, b) = futures_util::future::join(
        steady_appends(a.addr, stop.clone()),
        boot_second_member(store, &fleet, a.addr, stop),
    )
    .await;
    assert_eq!(
        refused,
        std::collections::BTreeMap::new(),
        "A refuses nothing"
    );
    let now = a.state.shards.open("").expect("A still serves the shard");
    assert!(
        std::sync::Arc::ptr_eq(&served, &now) && !served.is_closed(),
        "never fenced"
    );
    assert_eq!(b.state.shards.open_count(), 0, "B holds no engine");
    let report = b.tasks.shutdown(std::time::Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&b.state).await;
    engine_shutdown(&a.state).await;
}

/// **Before its first view a fleet instance opens nothing; a view of itself alone opens**
/// (the directory's pair of Addendum 9's beat pair). A fleet member with no view: the request
/// path's resolve and the fleet's own open of `""` both answer the retryable refusal (`Retry-After:
/// 1`) and open nothing, arming no holdoff. With a view that names this instance alone, both open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fleet_instance_opens_only_once_a_view_is_known() {
    use crate::shard_directory::{Adoption, ResolveError};
    use crate::sharddir::OpenOutcome;
    let rig = http_rig_build(mem(), RigRuntime::first(), member(RING[0], &mem())).await;
    let shards = &rig.state.shards;
    let refused = shards.resolve(&[0; 16], Adoption::External).await.err();
    let waited = shards
        .open_or_wait("", std::time::Duration::from_secs(1))
        .await;
    let waited = match waited {
        OpenOutcome::Wait {
            code,
            retry_after_secs,
        } => Some((code, retry_after_secs)),
        _ => None,
    };
    let refusal = ResolveError::Opening {
        prefix: String::new(),
        code: crate::ownership::NO_VIEW,
        retry_after_secs: 1,
    };
    assert_eq!(
        (refused, waited),
        (Some(refusal), Some((crate::ownership::NO_VIEW, 1)))
    );
    assert_eq!(shards.open_count(), 0, "nothing opens before a view");
    assert_eq!(shards.open_stats()["started"], 0, "{}", shards.open_stats());
    let alone = vec![RING[0].to_string()];
    rig.state
        .ownership
        .set_view(alone, std::collections::HashMap::new());
    let engine = shards.resolve(&[0; 16], Adoption::External).await;
    assert!(
        engine.is_ok(),
        "the request path opens once the view names this instance"
    );
    let opened = shards
        .open_or_wait("", std::time::Duration::from_secs(1))
        .await;
    assert!(
        matches!(opened, OpenOutcome::Ready(_)),
        "and so does the fleet's open"
    );
    engine_shutdown(&rig.state).await;
}

/// **A fleet instance is unready until its first view** (edge change #263): `/readyz` and
/// `/health` answer `503 fleet view not yet published` while the view names no member, so a
/// router that probes readiness sends nothing to an instance that would refuse it; with a view they
/// answer `ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_fleet_instance_is_unready_until_its_first_view() {
    let rig = http_rig_build(mem(), RigRuntime::first(), member(RING[0], &mem())).await;
    for path in ["/readyz", "/health"] {
        let (status, _, body) = hreq(rig.addr, "GET", path, &[], b"").await;
        let answer = (status, String::from_utf8_lossy(&body).into_owned());
        let unready = (503, "fleet view not yet published".to_string());
        assert_eq!(answer, unready, "{path} before the view");
    }
    let ring: Vec<String> = RING.map(str::to_string).to_vec();
    rig.state
        .ownership
        .set_view(ring, std::collections::HashMap::new());
    let (status, _, body) = hreq(rig.addr, "GET", "/readyz", &[], b"").await;
    assert_eq!(
        (status, &body[..]),
        (200, &b"ok"[..]),
        "ready once a view names a member"
    );
    engine_shutdown(&rig.state).await;
}
