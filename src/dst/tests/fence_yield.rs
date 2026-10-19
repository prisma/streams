//! Fence yield (#179): on a fleet-off cell, a process that loses its shard
//! engine to a newer boot of its cell never opens a shard again, so the two
//! processes of a deploy or a restart stop fencing each other in turn.
//!
//! Each runtime is a full HTTP rig over one shared in-memory store, started as
//! bootstrap starts a fleet-off runtime (`fleet::start_configured`), which
//! registers its boot in `_cell/boot`.

use super::fixture_http::{HttpRig, HttpRigOptions, http_rig_build};
use super::fixture_requests::{drain_no_closure, hreq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use std::time::Duration;

const STREAM: &str = "/v1/stream/fence-yield";
const JSON: [(&str, &str); 1] = [("content-type", "application/json")];
/// The rig's one shard is the root prefix.
const PREFIX: &str = "";
const SUPERSEDED: &str = "shard storage unavailable: superseded by a newer boot";

/// A fleet-off runtime of the cell, started as bootstrap starts it, once
/// `_cell/boot` names it.
async fn boot(store: &Arc<dyn ObjectStore>, incarnation: u64) -> HttpRig {
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::incarnation(incarnation),
        HttpRigOptions::default(),
    )
    .await;
    assert!(
        !crate::fleet::start_configured(rig.state.clone(), &rig.tasks),
        "a fleet-off runtime starts no fleet loop"
    );
    let own = rig.state.runtime.identity.boot_id.clone();
    let mut named = None;
    for _ in 0..500 {
        named = registered(store).await;
        if named.as_deref() == Some(own.as_str()) {
            return rig;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("boot {incarnation} never registered: the record names {named:?}");
}

/// The boot `_cell/boot` names.
async fn registered(store: &Arc<dyn ObjectStore>) -> Option<String> {
    let found = store
        .get(&object_store::path::Path::from("_cell/boot"))
        .await
        .ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&found.bytes().await.ok()?).ok()?;
    doc["bootId"].as_str().map(str::to_string)
}

fn started(rig: &HttpRig) -> u64 {
    rig.state.shards.open_stats()["started"].as_u64().unwrap()
}

/// An answer as (status, `Retry-After`, error code).
type Answer = (u16, Option<String>, String);

fn answer(reply: (u16, std::collections::HashMap<String, String>, Vec<u8>)) -> Answer {
    let (status, headers, body) = reply;
    let code = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|doc| doc["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default();
    (status, headers.get("retry-after").cloned(), code)
}

async fn append(rig: &HttpRig, body: &str) -> Answer {
    answer(hreq(rig.addr, "POST", STREAM, &JSON, body.as_bytes()).await)
}

async fn read(rig: &HttpRig) -> Answer {
    answer(hreq(rig.addr, "GET", &format!("{STREAM}?x=1"), &[], b"").await)
}

async fn health(rig: &HttpRig) -> (u16, String) {
    let (status, _, body) = hreq(rig.addr, "GET", "/health", &[], b"").await;
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Waits up to 10 s for `rig` to report itself superseded; its last answer.
async fn superseded(rig: &HttpRig) -> (u16, String) {
    let mut last = health(rig).await;
    for _ in 0..200 {
        if last.1 == SUPERSEDED {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = health(rig).await;
    }
    last
}

const REFUSED: (u16, Option<&str>, &str) = (503, Some("1"), "shard_closing");
/// The acknowledged records, in order; nothing a refused or fenced append sent.
const ACKED: [&str; 3] = [r#"{"n":0}"#, r#"{"n":1}"#, r#"{"n":2}"#];

/// Every record `rig` reads back, as stored JSON text.
async fn stored(rig: &HttpRig) -> Vec<String> {
    let (records, _) = drain_no_closure(rig.addr, "fence-yield", None).await;
    records.iter().map(serde_json::Value::to_string).collect()
}

/// A's answers to two appends and two reads after its holdoff was cleared.
async fn driven(rig: &HttpRig) -> Vec<Answer> {
    rig.state.shards.clear_holdoff(PREFIX);
    let mut answers = Vec::new();
    for n in 0..2 {
        answers.push(append(rig, &format!(r#"[{{"refused":{n}}}]"#)).await);
        answers.push(read(rig).await);
    }
    answers
}

fn refused(answers: &[Answer]) -> bool {
    answers
        .iter()
        .all(|(status, after, code)| (*status, after.as_deref(), code.as_str()) == REFUSED)
}

/// A serves; B, a newer boot, appends and fences it. A meets the fence on its
/// next append (that first answer is fix C's, out of scope), reads the record,
/// and yields: its holdoff cleared, it still opens nothing, answers every
/// append and read 503 `shard_closing` with `Retry-After: 1`, and reports
/// itself superseded, while B keeps its one engine and reads back exactly the
/// acknowledged records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_older_boot_never_retakes() {
    let store = mem();
    let a = boot(&store, 1).await;
    assert_eq!(hreq(a.addr, "PUT", STREAM, &JSON, b"").await.0, 201);
    assert_eq!(append(&a, r#"[{"n":0}]"#).await.0, 204);
    let a_opens = started(&a);
    let b = boot(&store, 2).await;
    assert_eq!(append(&b, r#"[{"n":1}]"#).await.0, 204);
    let b_engine = b.state.shards.resident_incarnation(PREFIX);
    assert!(b_engine.is_some(), "B serves the shard");
    let first = append(&a, r#"[{"fenced":0}]"#).await;
    assert_ne!(first.0, 204, "the fenced owner acknowledged a write");
    let a_health = superseded(&a).await;
    let answers = driven(&a).await;
    assert_eq!(
        started(&a),
        a_opens,
        "A re-took the shard after a newer boot fenced it; its answers {answers:?}"
    );
    assert!(refused(&answers), "A's answers: {answers:?}");
    assert_eq!(a_health, (503, SUPERSEDED.to_string()));
    assert_eq!(hreq(a.addr, "GET", "/readyz", &[], b"").await.0, 503);
    assert_eq!(append(&b, r#"[{"n":2}]"#).await.0, 204);
    assert_eq!(b.state.shards.resident_incarnation(PREFIX), b_engine);
    assert_eq!(started(&b), 1, "B opened once");
    assert_eq!(health(&b).await, (200, "ok".to_string()));
    assert_eq!(stored(&b).await, ACKED);
}

/// A, the older boot, opens the shard after B, the newest, did (as an open A
/// started before B registered lands after B's): A fences B. B is the boot
/// the record names, so it stays ready and re-takes at its next request, and
/// A yields at its next lost engine: in the end exactly one serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stray_older_open_does_not_unseat_the_newest() {
    let store = mem();
    let a = boot(&store, 1).await;
    let b = boot(&store, 2).await;
    assert_eq!(hreq(b.addr, "PUT", STREAM, &JSON, b"").await.0, 201);
    assert_eq!(append(&b, r#"[{"n":0}]"#).await.0, 204);
    let stray = hreq(a.addr, "PUT", STREAM, &JSON, b"").await;
    assert_eq!(stray.0, 200, "A opened the shard and found the stream");
    assert_eq!(started(&a), 1, "A's stray open");
    let b_fenced = append(&b, r#"[{"fenced":0}]"#).await;
    assert_ne!(b_fenced.0, 204, "B wrote through a fence");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        health(&b).await,
        (200, "ok".to_string()),
        "the newest never yields"
    );
    b.state.shards.clear_holdoff(PREFIX);
    assert_eq!(append(&b, r#"[{"n":1}]"#).await.0, 204, "B re-takes");
    let a_fenced = append(&a, r#"[{"fenced":1}]"#).await;
    assert_ne!(a_fenced.0, 204, "A wrote through a fence");
    assert_eq!(superseded(&a).await, (503, SUPERSEDED.to_string()));
    let a_opens = started(&a);
    let answers = driven(&a).await;
    assert!(refused(&answers), "A's answers: {answers:?}");
    assert_eq!(started(&a), a_opens, "A opened after it yielded");
    assert_eq!(append(&b, r#"[{"n":2}]"#).await.0, 204);
    assert_eq!(stored(&b).await, ACKED);
}

/// B fences A, then stops without closing anything (a crash). C, a fresh boot,
/// registers, serves every acknowledged record and keeps appending; A, still
/// superseded, opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_newest_serves_and_the_superseded_stays_out() {
    let store = mem();
    let a = boot(&store, 1).await;
    assert_eq!(hreq(a.addr, "PUT", STREAM, &JSON, b"").await.0, 201);
    assert_eq!(append(&a, r#"[{"n":0}]"#).await.0, 204);
    let b = boot(&store, 2).await;
    assert_eq!(append(&b, r#"[{"n":1}]"#).await.0, 204);
    assert_ne!(append(&a, r#"[{"fenced":0}]"#).await.0, 204);
    assert_eq!(superseded(&a).await, (503, SUPERSEDED.to_string()));
    let a_opens = started(&a);
    let report = b.tasks.shutdown(Duration::from_secs(5)).await;
    assert!(report.aborted.is_empty(), "B's stop: {report:?}");
    let c = boot(&store, 3).await;
    assert_eq!(append(&c, r#"[{"n":2}]"#).await.0, 204);
    let answers = driven(&a).await;
    assert!(refused(&answers), "A's answers: {answers:?}");
    assert_eq!(started(&a), a_opens, "A opened after it yielded");
    assert_eq!(started(&c), 1, "C opened once");
    assert_eq!(stored(&c).await, ACKED);
}

/// A deploy's two processes share the cell's read spool (one instance name),
/// so B, the newer boot, opening it fences A's handle. A keeps serving reads
/// until it stops, and its stop owes the ledger that window; the spool
/// refuses it ("detected newer DB client"), so it once died with A. Every
/// read A metered, before and after B started, and the appends of both,
/// reach the rollup once, after B's sweeps and drains, also when an import
/// whose delete was lost imports the handed-off window a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_boots_last_read_window_is_billed_once() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let store = mem();
    let a = boot(&store, 1).await;
    crate::billing::spawn_telemetry(a.state.clone(), &a.tasks);
    wait_spool(&a).await;
    assert_eq!(hreq(a.addr, "PUT", STREAM, &JSON, b"").await.0, 201);
    assert_eq!(append(&a, r#"[{"n":0}]"#).await.0, 204);
    for _ in 0..3 {
        assert_eq!(read(&a).await.0, 200);
    }
    let b = boot(&store, 2).await;
    crate::billing::open_read_spool(&b.state).await.unwrap();
    for _ in 0..3 {
        assert_eq!(read(&a).await.0, 200, "A still serves its shard");
    }
    assert_eq!(
        a.state.billing.unflushed_reads().2,
        0,
        "no window sealed yet"
    );
    let metered = a.state.billing.reads().snapshot_active();
    assert_eq!(metered.len(), 1, "{metered:?}");
    let (identity, owed) = metered.into_iter().next().unwrap();
    assert_eq!(owed.read_operations, 6, "{owed:?}");
    let report = a.tasks.shutdown(Duration::from_secs(10)).await;
    assert!(report.aborted.is_empty(), "A's stop: {report:?}");
    assert_eq!(a.state.billing.unflushed_reads(), (0, 0, 0));
    assert_eq!(handed_off(&store).await.len(), 1, "A handed its window off");
    assert_eq!(append(&b, r#"[{"n":1}]"#).await.0, 204);
    let rollup = crate::rollup::UsageRollup::open(mem(), "", &b.state.config);
    super::fixture_http::install_rollup(&b.state, rollup.await.unwrap()).await;
    let handed = handed_off(&store).await;
    for pass in 0..2 {
        crate::billing::sweep_owned_outboxes(&b.state).await;
        b.state.billing.reads().seal_if_aged(0);
        let drain = || crate::billing::drain_once(&b.state);
        while drain().await.expect("B's drain") > 0 {}
        while crate::billing::rollup_step(&b.state).await.expect("rollup") > 0 {}
        assert!(handed_off(&store).await.is_empty(), "the import deleted it");
        if pass == 0 {
            // An import whose delete was lost imports the window again.
            restore(&store, &handed).await;
        }
    }
    let (year, month) = crate::billing::utc_year_month(crate::shard::now_ms());
    let month = crate::billing::month_str(year, month);
    let rollup = b.state.rollup.get().unwrap();
    let (account, project) = (&identity.account_id, &identity.project_id);
    let row = rollup.month_row(&month, account, project, &identity.stream_id);
    let row = row.await.unwrap().expect("A's reads reached the rollup");
    let billed = (
        row.read_operations,
        row.read_payload_bytes,
        row.read_records,
    );
    let owed = (
        owed.read_operations,
        owed.read_payload_bytes,
        owed.read_records,
    );
    assert_eq!(billed, owed, "A's metered reads, billed once");
    let named = rollup.name_row(&month, account, project, &identity.stream_name);
    let named = named.await.unwrap().expect("the stream's ingest");
    assert_eq!(named.ingest_records, 2, "A's and B's appends, once each");
}

/// The handed-off read windows in the store, with their bodies.
async fn handed_off(store: &Arc<dyn ObjectStore>) -> Vec<(object_store::path::Path, bytes::Bytes)> {
    let prefix = object_store::path::Path::from("telemetry/read-handoff");
    let listed = store.list(Some(&prefix));
    let listed = futures_util::TryStreamExt::try_collect::<Vec<_>>(listed);
    let mut handed = Vec::new();
    for meta in listed.await.unwrap() {
        let body = store
            .get(&meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        handed.push((meta.location, body));
    }
    handed
}

async fn restore(
    store: &Arc<dyn ObjectStore>,
    handed: &[(object_store::path::Path, bytes::Bytes)],
) {
    for (path, body) in handed {
        store.put(path, body.clone().into()).await.unwrap();
    }
}

async fn wait_spool(rig: &HttpRig) {
    for _ in 0..500 {
        if rig.state.billing.read_spool_open() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the read spool never opened");
}
