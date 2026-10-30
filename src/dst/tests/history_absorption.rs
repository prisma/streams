//! History absorption, layout 6: records acked on the shard log stay
//! readable, exactly, once the sealer has moved them into segment objects —
//! keylessly, across chunks of one page, across an engine handoff, and for
//! a residual too small for any byte trigger.

use super::fixture_storage::{drain_filtered, mem, open_engine, open_engine_with_sealer, skey};
use crate::dst::{FaultPlan, FaultStore, OpLog, Workload, drain_observed, mech};
use std::time::Duration;

/// Looks at the stream's durable tail up to `tries` times, 25 ms apart,
/// until `ready` holds; returns the last tail seen, if the handle existed.
async fn durable_tail(
    engine: &crate::shard::ShardEngine,
    hash: [u8; 16],
    tries: usize,
    ready: impl Fn(&crate::shard::TailFields) -> bool,
) -> Option<crate::shard::TailFields> {
    let mut last = None;
    for _ in 0..tries {
        let Ok(handle) = engine.stream_handle(hash).await else {
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        let tail = handle.state.lock().unwrap().durable.clone();
        let done = ready(&tail);
        last = Some(tail);
        if done {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    last
}

/// The stream's durable records from offset 0 through the production
/// merged reader: (offset, payload) in order.
async fn read_all(
    engine: &std::sync::Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> Vec<(u64, Vec<u8>)> {
    let handle = engine.stream_handle(hash).await.expect("handle");
    let mut out = Vec::new();
    for _ in 0..64 {
        let from = out.last().map_or(0, |(off, _)| off + 1);
        let page = crate::http::read_merged(
            key,
            &hash,
            &handle,
            engine,
            from,
            None,
            8 * 1024 * 1024,
            crate::shard::Deliver::Durable,
        )
        .await
        .expect("read");
        out.extend(page.recs.iter().map(|rec| (rec.off, rec.payload.to_vec())));
        if page.completed || page.recs.is_empty() {
            break;
        }
    }
    out
}

/// I1 across the tier boundary. Records acknowledged on the shard log must
/// still be readable — through the production merged reader — after the
/// sealer has moved them into segment objects, while later records are
/// still unsealed pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn acked_records_survive_absorption_into_history() {
    let store = FaultStore::uniform(mem(), 17, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [8u8; 16];
    let (engine, _cadence) = open_engine_with_sealer(store.clone(), "dst-hist").await;

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["h1", "h2"], 40, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    // Wait for the sealed boundary to advance past zero.
    let sealed = durable_tail(&engine, hash, 400, |t| t.absorbed > 0)
        .await
        .map_or(0, |t| t.absorbed);
    assert!(
        sealed > 0,
        "the sealer never advanced the boundary — the scenario would only \
         have tested the shard log again"
    );

    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("sealed={sealed}: {e}\ncoverage={:?}", cov.snapshot());
    }
    if let Err(e) = cov.require(&[mech::READ_FROM_HISTORY]) {
        panic!("{e}");
    }
    engine.begin_close();
}

/// CHAOS 2026-08-09, layout 6: records larger than a chunk's bound must
/// still seal whole. Here a chunk holds one page, and ONE append carries
/// six 100 KiB records (each past the 64 KiB page target, so a page each):
/// one requested seal must move the boundary to next across six chunks,
/// and every record must read back byte for byte from the object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn absorber_drains_records_larger_than_the_per_stream_gather_cap() {
    let store = FaultStore::uniform(mem(), 77, FaultPlan::CLEAN);
    let key = skey();
    let hash = [77u8; 16];
    let engine = open_engine(store.clone(), "dst-bigrec").await;
    engine.configure_seal(|s| {
        s.auto = false;
        s.max_pages_per_chunk = 1;
    });

    const RECORDS: usize = 6;
    const SIZE: usize = 100 * 1024;
    let fills: Vec<u8> = (b'a'..).take(RECORDS).collect();
    {
        let subkey = crate::crypto::derive_subkey(&key, &hash, "k", 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: hash,
            entries: fills
                .iter()
                .map(|fill| bytes::Bytes::from(vec![*fill; SIZE]))
                .collect(),
            usage: crate::usage::counters(&hash),
            routing_key: "k".to_string(),
            key_hash: crate::crypto::stream_hash("k"),
            producer_lineage: Vec::new(),
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
        assert!(engine.try_enqueue(req).is_ok(), "enqueue rejected");
        rx.await.expect("ack channel").expect("append acked");
    }

    let report = engine.seal_requested().await.expect("seal");
    assert_eq!(
        (report.streams, report.pages, report.chunks),
        (1, RECORDS as u64, RECORDS),
        "one stream's six pages seal as six one-page chunks"
    );
    let tail = durable_tail(&engine, hash, 1, |_| true)
        .await
        .expect("tail");
    assert_eq!(
        (tail.absorbed, tail.next, tail.unabsorbed_bytes),
        (RECORDS as u64, RECORDS as u64, 0),
        "the seal must reach next and retire every unsealed byte"
    );
    let expected: Vec<(u64, Vec<u8>)> = (0u64..)
        .zip(&fills)
        .map(|(off, fill)| (off, vec![*fill; SIZE]))
        .collect();
    assert!(
        read_all(&engine, hash, &key).await == expected,
        "the sealed records must read back exactly"
    );
    engine.begin_close();
}

/// Keyless sealing: the sealer holds no customer key — it moves encrypted
/// pages into the object and writes key summaries of hashed routing keys —
/// yet every record must decode on read, where the client supplies the
/// key, and both filters must work over the sealed chunks. (v1 required
/// the key server-side and stranded key-expired backlogs;
/// docs/COST-WIDE1.md §2.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn v2_absorbs_without_customer_keys() {
    let store = FaultStore::uniform(mem(), 61, FaultPlan::new(0, 0, 10));
    let cov = store.coverage();
    let key = skey();
    let hash = [50u8; 16];
    let engine = open_engine(store.clone(), "dst-v2nokey").await;
    engine.configure_seal(|s| s.auto = false);

    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&engine, hash, &key, &["", "vk"], 25, false, &mut log)
        .await;
    assert!(log.total_acked() > 0, "nothing acked");

    let report = engine.seal_requested().await.expect("keyless seal");
    assert_eq!(report.streams, 1, "the seal took the stream");
    let tail = durable_tail(&engine, hash, 1, |_| true)
        .await
        .expect("tail");
    assert!(
        tail.absorbed > 0 && tail.absorbed == tail.next,
        "keyless sealing must reach next: absorbed={} next={}",
        tail.absorbed,
        tail.next
    );

    // Reads (client-supplied key) must see every acked record, all from
    // sealed chunks, and filters must work against their key summaries.
    let observed = drain_observed(&engine, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!(
            "keyless sealing lost records (sealed={}): {e}",
            tail.absorbed
        );
    }
    let unkeyed = drain_filtered(&engine, hash, &key, "").await;
    assert_eq!(&unkeyed, &log.acked[""], "empty-key filter broken");
    let keyed = drain_filtered(&engine, hash, &key, "vk").await;
    assert_eq!(&keyed, &log.acked["vk"], "keyed filter broken");
    engine.begin_close();
}

/// Sealed history must survive the owner handing the shard to a NEW
/// engine: the successor fences the old writer, rebuilds its directory
/// from the `'K'` rows the old owner's seals committed, and serves every
/// acked record — sealed by either owner or still unsealed — without any
/// customer key reaching a sealer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn v2_history_survives_engine_handoff() {
    let store = FaultStore::uniform(mem(), 67, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let hash = [51u8; 16];
    let prefix = "dst-v2reopen";

    let (a, _cadence_a) = open_engine_with_sealer(store.clone(), prefix).await;
    let mut log = OpLog::default();
    let mut w = Workload::new(cov.clone());
    w.run(&a, hash, &key, &["r"], 20, false, &mut log).await;
    let sealed = durable_tail(&a, hash, 400, |t| t.absorbed > 0)
        .await
        .map_or(0, |t| t.absorbed);
    assert!(sealed > 0, "no seal before the handoff");

    // Successor opens the same shard; its open fences the old owner and
    // rebuilds the directory from the committed seals.
    let (b, _cadence_b) = open_engine_with_sealer(store.clone(), prefix).await;
    b.seal_ready().await.expect("the successor's sealer opens");
    assert_eq!(
        b.seal_json()["directory_streams"],
        1,
        "the successor's directory covers the old owner's sealed stream"
    );
    // Same Workload: op numbering must continue, or the post-handoff ops
    // collide with the pre-handoff ones in the shared OpLog.
    w.run(&b, hash, &key, &["r"], 5, false, &mut log).await;

    let observed = drain_observed(&b, hash, &key, &cov).await;
    if let Err(e) = log.audit(&observed) {
        panic!("sealed history lost records across the handoff: {e}");
    }
    a.begin_close();
    b.begin_close();
}

/// R26-1 regression, layout 6: age sealing takes EVERYTHING — there is no
/// sparse floor. A sub-threshold residual that never retires ages the
/// no-progress clock past MAX_ABSORB_LAG_SECS, the LagSecs latch sheds
/// every append on the instance — and shed appends are the only way the
/// residual could ever grow eligible. The 2026-08-11 soak measured a
/// 154 KiB residual at 938 s of stall, one evaluator tick from that
/// deadlock.
///
/// Gate: the sealer is off while a tiny stream (two small frames) and a
/// fat one are written; then every byte trigger is put out of reach and
/// the age made immediate. BOTH streams must age-seal and the shard's
/// maintenance ledger must return to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch() {
    let store = FaultStore::uniform(mem(), 57, FaultPlan::CLEAN);
    let cov = store.coverage();
    let key = skey();
    let tiny = [40u8; 16];
    let fat = [41u8; 16];
    let engine = open_engine(store.clone(), "dst-defer").await;
    engine.configure_seal(|s| s.auto = false);

    let mut tiny_log = OpLog::default();
    let mut w1 = Workload::new(cov.clone());
    w1.run(&engine, tiny, &key, &["d"], 2, false, &mut tiny_log)
        .await;
    let mut fat_log = OpLog::default();
    let mut w2 = Workload::new(cov.clone());
    w2.run(&engine, fat, &key, &["d"], 60, false, &mut fat_log)
        .await;
    assert!(
        engine.maintenance_snapshot().unabsorbed_frame_bytes > 0,
        "workloads committed but the ledger shows no backlog"
    );
    engine.configure_seal(|s| {
        s.memtable_bytes = u64::MAX;
        s.unsealed_bytes = u64::MAX;
        s.wal_objects_seal = u64::MAX;
        s.age_seal = Duration::from_millis(1);
        s.tick = Duration::from_millis(20);
        s.auto = true;
    });

    // BOTH streams must age-seal — the tiny one especially.
    let aged = |t: &crate::shard::TailFields| t.absorbed > 0 && t.absorbed == t.next;
    for (name, hash) in [("tiny", tiny), ("fat", fat)] {
        let sealed = durable_tail(&engine, hash, 400, aged)
            .await
            .is_some_and(|t| aged(&t));
        assert!(sealed, "the {name} stream never age-sealed");
    }

    // With everything retired the shard's maintenance ledger is zero —
    // the no-progress latch has nothing to age on. THIS is the deadlock
    // gate: a permanent residual here means an eventual instance-wide
    // LagSecs shed that no append could ever clear.
    let mut drained = false;
    for _ in 0..400 {
        if engine.maintenance_snapshot().unabsorbed_frame_bytes == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        drained,
        "maintenance ledger kept a residual after full sealing"
    );

    // Both streams stay fully readable through the merged reader.
    let obs_tiny = drain_observed(&engine, tiny, &key, &cov).await;
    tiny_log
        .audit(&obs_tiny)
        .expect("tiny stream readable after sealing");
    let obs_fat = drain_observed(&engine, fat, &key, &cov).await;
    fat_log
        .audit(&obs_fat)
        .expect("fat stream readable after sealing");
    engine.begin_close();
}
