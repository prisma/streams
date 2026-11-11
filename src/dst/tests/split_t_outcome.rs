#![cfg(test)]
//! A split whose `'T'` write fails (S4-DESIGN §6.4; DESIGN-DECISION-c3-incomplete-rig-2026-11-11
//! addendum 6). `write_retired` puts `'T'` and flushes it, and either step may fail with `'T'`
//! durable or about to be (a put that landed, a flush whose response was lost, the close's own
//! flush). So once the put was attempted the split only rolls forward: it refuses `Unfinished`,
//! never resumes the parent and deletes nothing, the dropped barrier closes the parent, and the
//! next opener decides from what the log holds: forward from `'T'`, or back without it.

use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::merge_finish_k::{AtPark, Batch, FINAL, append_answer, assert_quiet, probe_at_park};
use super::split_k::{Cell, cell, records};
use crate::shard::ShardEngine;
use crate::topology::gate::{PausePoint, TFault};
use crate::topology::intent::{self, Intent, State};
use crate::topology::split::Refused;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long a reopen waits for its opener to decide: the lapse (11 s) and the open's patience.
const CLAIM_BOUND: Duration = Duration::from_secs(40);

/// A WAL object's id and its records' sequence numbers and keys.
type WalRows = Vec<(u64, Vec<(u64, Vec<u8>)>)>;

/// The WAL of `engine`'s log: each object's records.
async fn wal_rows(store: &Arc<dyn object_store::ObjectStore>, engine: &ShardEngine) -> WalRows {
    let path = engine.log_path();
    let reader = slatedb::WalReader::new(path.as_str(), store.clone());
    let mut objects = Vec::new();
    for file in reader.list(..).await.expect("the WAL lists") {
        let mut records = file.iterator().await.expect("the WAL object reads");
        let mut rows = Vec::new();
        while let Some(record) = records.next().await.expect("a WAL record") {
            rows.push((record.seq, record.key.to_vec()));
        }
        objects.push((file.id, rows));
    }
    objects
}

/// A split refused after its `'T'` write failed: the refusal, the park's account, the queued
/// index batch, and the parent's WAL at the end of the park (before the attempt).
struct Failed {
    refused: Refused,
    at: AtPark,
    batch: Batch,
    parked: WalRows,
}

/// The split of `cell`'s parent with its `'T'` write failing at `fault`, paused at its park while
/// `merge_finish_k::probe_at_park` runs there.
async fn split_failing_t(cell: &Cell, fault: TFault) -> Failed {
    let gate = &cell.parent.gc_state().gate;
    gate.fail_t(fault);
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    gate.pause_at_park(PausePoint { reached, go });
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-t", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let store = cell.state.data_store.clone();
        let sources = [cell.parent.clone()];
        let names = [cell.low.as_str(), cell.high.as_str()];
        let probed = probe_at_park(&store, &sources, cell.addr, names).await;
        let parked = wal_rows(&store, &cell.parent).await;
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
        (probed, parked)
    };
    let (split, ((at, batch), parked)) = futures_util::future::join(split, control).await;
    let refused = split.err().expect("the split does not publish");
    Failed {
        refused,
        at,
        batch,
        parked,
    }
}

/// What follows a failed `'T'` write on `cell`'s parent, until it closed: the queued batch is
/// refused as final, an append is answered the holdoff's 503 `shard_moving` (never acknowledged),
/// the dropped barrier closes the parent, and its WAL holds exactly what it held at the park plus,
/// when the put landed, one object holding the `'T'` row alone (the close's flush).
async fn closed_after(cell: &Cell, failed: Failed, landed: bool) {
    assert_quiet(&failed.at);
    let queued = tokio::time::timeout(Duration::from_secs(20), failed.batch).await;
    assert_eq!(queued, Ok(Err(FINAL.to_string())), "the batch after 'T'");
    // The closed parent's anti-flap holdoff answers first: it lived under 30 s, so its departure
    // is one strike, 6 s (`sharddir::holdoff_for(1)`), counted down in whole seconds.
    let (status, retry, code) = append_answer(cell.addr, &cell.low, 40).await;
    let retry = retry.and_then(|r| r.parse::<u64>().ok());
    assert_eq!(
        (status, code.as_deref()),
        (503, Some("shard_moving")),
        "no append is acknowledged after the failed 'T'"
    );
    assert!(matches!(retry, Some(1..=6)), "{retry:?}");
    let closed = tokio::time::timeout(Duration::from_secs(20), cell.parent.closed()).await;
    assert!(closed.is_ok(), "the dropped barrier closes the parent");
    let mut want = failed.parked;
    let ended = wal_rows(&cell.state.data_store, &cell.parent).await;
    if landed {
        let next = want.last().map_or(1, |(id, _)| id + 1);
        let seq = want
            .iter()
            .flat_map(|(_, rows)| rows)
            .map(|(seq, _)| *seq)
            .max();
        let retired = crate::topology::fence::retired_key();
        want.push((next, vec![(seq.unwrap_or_default() + 1, retired)]));
    }
    assert_eq!(ended, want, "the parent's WAL after the park");
}

/// A cold-seal split cell (no seal runs unrequested) whose parent holds an unsealed record of each
/// stream, so the probe's seal at the park has a group to refuse (`merge_finish_k::assert_quiet`).
async fn cell_with_records() -> Cell {
    let cell = super::split_k::cold_cell().await;
    for (name, n) in [(&cell.low, 20), (&cell.high, 21)] {
        assert_eq!(append_answer(cell.addr, name, n).await.0, 204, "{name}");
    }
    cell
}

/// The cell's intent.
async fn intent_of(cell: &Cell) -> Intent {
    let store = &cell.state.data_store;
    intent::read(&**store).await.unwrap().unwrap().intent
}

/// Whether `path` holds no object.
async fn empty(store: &Arc<dyn object_store::ObjectStore>, path: &str) -> bool {
    use futures_util::TryStreamExt;
    let at = object_store::path::Path::from(path);
    let listed: Vec<_> = store.list(Some(&at)).try_collect().await.unwrap();
    listed.is_empty()
}

/// Whether the log at `path` has an initialized manifest (a complete clone).
async fn initialized(store: &Arc<dyn object_store::ObjectStore>, path: &str) -> bool {
    let admin = slatedb::admin::AdminBuilder::new(path, store.clone()).build();
    matches!(admin.read_manifest(None).await, Ok(Some(m)) if m.initialized())
}

/// The intent's end as `(state, lease_epoch, owner kind)`, waited for up to `CLAIM_BOUND`.
async fn ended(cell: &Cell) -> (State, u64, String) {
    let t0 = Instant::now();
    loop {
        let left = intent_of(cell).await;
        if left.is_terminal() || t0.elapsed() > CLAIM_BOUND {
            let kind = left.lease_owner.split(':').next().unwrap_or_default();
            return (left.state, left.lease_epoch, kind.to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A split whose `'T'` landed before its flush failed only rolls forward.** At the park the probe
/// queues an index batch on the parent; then the put of `'T'` lands and the flush is reported
/// failed. The split refuses `Unfinished` (not `RolledBack`), the queued batch is refused as final,
/// an append issued after the failure is answered 503 `shard_moving`, never acknowledged, the
/// parent closes with nothing past the park in its WAL but one object holding the `'T'` row alone
/// (the close's flush of the put that landed), the intent stays `Preparing` and both children's
/// clones stay complete. The parent's reopen holds through the lapse (no sooner than 11 s), then
/// its opener claims the intent and publishes the split `'T'` names; installed, each child serves
/// its stream exactly as it was acknowledged before the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_t_landed_before_its_flush_failed_only_rolls_forward() {
    let cell = cell_with_records().await;
    let (want_low, want_high) = (
        records(cell.addr, &cell.low).await,
        records(cell.addr, &cell.high).await,
    );
    let store = cell.state.data_store.clone();
    let failed = split_failing_t(&cell, TFault::AfterPut).await;
    let refused = &failed.refused;
    assert!(
        matches!(refused, Refused::Unfinished(why) if why == "'T': injected: the flush failed"),
        "{refused}"
    );
    closed_after(&cell, failed, true).await;
    let left = intent_of(&cell).await;
    assert_eq!(
        (left.state, left.lease_epoch),
        (State::Preparing, 1),
        "live"
    );
    let children = crate::topology::recover::children_of(&left);
    for (prefix, home) in &children {
        let path = crate::topology::model::born_home_path(prefix, *home);
        assert!(initialized(&store, &path).await, "{path} is complete");
    }
    let (reserved, _) = crate::topology::store::read(&*store).await.unwrap();
    let homes = [children[0].1, children[1].1];
    engine_shutdown(&cell.state).await;
    let t0 = Instant::now();
    let path = format!("/v1/stream/{}", cell.low);
    let stored = loop {
        let (status, _, _) = hreq(cell.addr, "GET", &path, &[], b"").await;
        assert_ne!(status, 200, "the parent never serves past its 'T'");
        let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
        if stored.version > reserved.version {
            break stored;
        }
        assert!(t0.elapsed() < CLAIM_BOUND, "the opener rolls forward");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        t0.elapsed() >= intent::LEASE + intent::SKEW,
        "the claim waited the lapse: {:?}",
        t0.elapsed()
    );
    assert_eq!(stored, reserved.split("00", homes, "op-t").unwrap());
    assert_eq!(
        ended(&cell).await,
        (State::Reclaimed, 2, "opener".to_string())
    );
    let now = cell.state.runtime.clock.monotonic();
    let installed =
        crate::topology::revalidate::once(&cell.state.registry, &cell.state.shards, now);
    assert!(installed.await.unwrap(), "the published split installs");
    assert_eq!(records(cell.addr, &cell.low).await, want_low);
    assert_eq!(records(cell.addr, &cell.high).await, want_high);
    engine_shutdown(&cell.state).await;
}

/// **A split whose `'T'` put failed is rolled back only by the parent's next opener.** At the park
/// the probe queues an index batch; then the put of `'T'` fails with nothing landed. The split
/// still refuses `Unfinished` (it cannot tell this from a put that landed), the queued batch is
/// refused as final, an append is answered 503, the parent closes with nothing past the park in
/// its WAL, and the intent stays `Preparing`. The parent's reopen holds through the lapse
/// (no sooner than 11 s); its opener finds no `'T'`, claims the intent (lease epoch 2) and rolls
/// the split back: both children's paths and the pause's checkpoint are deleted, the intent ends
/// `RolledBack`, and the parent serves both streams exactly as acknowledged before the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_t_put_failed_is_rolled_back_only_by_the_next_opener() {
    let cell = cell_with_records().await;
    let (want_low, want_high) = (
        records(cell.addr, &cell.low).await,
        records(cell.addr, &cell.high).await,
    );
    let store = cell.state.data_store.clone();
    let log = cell.parent.log_path();
    let failed = split_failing_t(&cell, TFault::BeforePut).await;
    let refused = &failed.refused;
    assert!(
        matches!(refused, Refused::Unfinished(why) if why == "'T': injected: the put failed"),
        "{refused}"
    );
    closed_after(&cell, failed, false).await;
    let left = intent_of(&cell).await;
    assert_eq!(
        (left.state, left.lease_epoch),
        (State::Preparing, 1),
        "live"
    );
    let children = crate::topology::recover::children_of(&left);
    engine_shutdown(&cell.state).await;
    let t0 = Instant::now();
    let path = format!("/v1/stream/{}", cell.low);
    loop {
        let (status, _, _) = hreq(cell.addr, "GET", &path, &[], b"").await;
        if status == 200 {
            break;
        }
        assert!(
            t0.elapsed() < CLAIM_BOUND,
            "the opener rolls back and serves"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        t0.elapsed() >= intent::LEASE + intent::SKEW,
        "the claim waited the lapse: {:?}",
        t0.elapsed()
    );
    assert_eq!(
        ended(&cell).await,
        (State::RolledBack, 2, "opener".to_string())
    );
    for (prefix, home) in &children {
        let path = crate::topology::model::born_home_path(prefix, *home);
        assert!(empty(&store, &path).await, "{path} is deleted");
    }
    let admin = slatedb::admin::AdminBuilder::new(log.as_str(), store.clone()).build();
    let named = admin.list_checkpoints(Some("topo-op-t")).await.unwrap();
    assert!(named.is_empty(), "the pause's checkpoint is deleted");
    assert_eq!(records(cell.addr, &cell.low).await, want_low);
    assert_eq!(records(cell.addr, &cell.high).await, want_high);
    engine_shutdown(&cell.state).await;
}

/// A request's status, its error code and message.
async fn answer(cell: &Cell, method: &str, body: &[u8]) -> (u16, String, String) {
    let path = format!("/v1/stream/{}", cell.low);
    let headers: &[(&str, &str)] = if body.is_empty() {
        &[]
    } else {
        &[super::billing_closure_debts::JSON]
    };
    let (status, _, body) = hreq(cell.addr, method, &path, headers, body).await;
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let field = |name: &str| {
        error["error"][name]
            .as_str()
            .unwrap_or_default()
            .to_string()
    };
    (status, field("code"), field("message"))
}

/// The first answer to a request past the shard's anti-flap holdoff, every answer inside it a 503
/// `shard_moving`.
async fn past_holdoff(cell: &Cell, method: &str, body: &[u8]) -> (u16, String, String) {
    let t0 = Instant::now();
    loop {
        let answered = answer(cell, method, body).await;
        if answered.0 != 503 {
            return answered;
        }
        assert_eq!(
            answered.1, "shard_moving",
            "inside the holdoff: {}",
            answered.2
        );
        assert!(t0.elapsed() < Duration::from_secs(20), "the holdoff ends");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **A published child whose log is missing is refused at its open, never served as a new, empty
/// log** (addendum 6's second ruling, outcome A's guard; edge #251 amendment R3-e). Leaf `00`
/// splits into `000` and `001`; then `000`'s log is deleted (as a rollback's clear would have).
/// SlateDB's builder creates a new database at a path with no manifest, and the identity check
/// refuses it: an operation-born home's log carries the `'Y'` its clone seeded, so one with none is
/// missing. Past the holdoff its shutdown armed, a read of the low stream answers 500
/// `shard_open_failed`, `log_missing`; an append inside the failed open's holdoff answers 503
/// `shard_moving`; an append past it answers 500 `shard_open`, `log_missing` again; never a 404,
/// 200 or 204. The log at `000`'s path holds no
/// `'Y'`, and the sibling `001` serves its stream exactly as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_born_home_whose_log_is_missing_is_refused_not_served_empty() {
    let cell = cell().await;
    let want_high = records(cell.addr, &cell.high).await;
    let store = cell.state.data_store.clone();
    let clock = cell.state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-m", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    let (_, home, low) = published
        .children
        .iter()
        .find(|(prefix, _, _)| prefix == "000")
        .expect("the low child");
    let log = low.log_path();
    assert_eq!(log, crate::topology::model::born_home_path("000", *home));
    engine_shutdown(&cell.state).await;
    let closed = low.shutdown_handle().wait(Duration::from_secs(20)).await;
    assert_eq!(
        closed,
        Ok(()),
        "the low child's log closes before it is deleted"
    );
    crate::topology::recover::delete_child(&store, &log).await;
    let left = {
        use futures_util::TryStreamExt;
        let at = object_store::path::Path::from(log.as_str());
        let listed: Vec<_> = store.list(Some(&at)).try_collect().await.unwrap();
        listed
            .into_iter()
            .map(|m| m.location.to_string())
            .collect::<Vec<_>>()
    };
    assert!(left.is_empty(), "{log} is deleted: {left:?}");
    let missing = format!(
        "log_missing: shard log {log} (prefix 000, home {home}) has no identity row; an \
         operation-born home's log is created only by its clone (RUNBOOK)"
    );
    let (status, code, message) = past_holdoff(&cell, "GET", b"").await;
    assert_eq!(
        (status, code.as_str()),
        (500, "shard_open_failed"),
        "{message}"
    );
    assert!(message.contains(&missing), "{message}");
    let (status, code, message) = answer(&cell, "POST", br#"[{"n":50}]"#).await;
    assert_eq!((status, code.as_str()), (503, "shard_moving"), "{message}");
    let (status, code, message) = past_holdoff(&cell, "POST", br#"[{"n":51}]"#).await;
    assert_eq!((status, code.as_str()), (500, "shard_open"), "{message}");
    assert!(message.contains(&missing), "{message}");
    let raw = crate::keyspace::db_builder(log.as_str(), store.clone())
        .build()
        .await
        .unwrap();
    let identity = [crate::keyspace::SENTINEL.as_slice(), b"Y"].concat();
    assert_eq!(
        raw.get(&identity).await.unwrap(),
        None,
        "no 'Y' was written"
    );
    raw.close().await.unwrap();
    assert_eq!(records(cell.addr, &cell.high).await, want_high);
    engine_shutdown(&cell.state).await;
}
