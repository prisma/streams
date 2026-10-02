use super::observations::{http_op, parse_server_timing_us, tally_storm};
use super::resources::BulkGate;
use super::{StoreResources, classify};
use std::sync::atomic::Ordering;

/// Build a window: `n` copies of `(op, class)`.
fn ops(spec: &[((u8, u8), usize)]) -> Vec<(u8, u8)> {
    spec.iter()
        .flat_map(|(oc, n)| std::iter::repeat_n(*oc, *n))
        .collect()
}

const GET_WAL: (u8, u8) = (2, 0);
const PUT_SST: (u8, u8) = (0, 2);
const DEL_WAL: (u8, u8) = (4, 0);
const PUT_WAL: (u8, u8) = (0, 0);

#[test]
fn wal_read_storm_fires_on_the_eu_central_shape() {
    // The real 60 s window that took eu-central-1 out of the soak:
    // 12,666 get:wal, no put:sst, no delete:wal (docs/SOAK-REGIONS.md).
    let w = tally_storm(ops(&[(GET_WAL, 12_666), (PUT_WAL, 5)]).into_iter());
    assert!(w.stalled, "{w:?}");
    assert_eq!(w.wal_gets, 12_666);
    assert_eq!((w.sst_puts, w.wal_deletes), (0, 0));
}

#[test]
fn wal_read_storm_stays_quiet_on_a_healthy_window() {
    // ap-northeast-1's window in the same run: reads come from SSTs and
    // the WAL is being trimmed.
    let w = tally_storm(
        ops(&[
            (PUT_WAL, 1_242),
            (PUT_SST, 132),
            (DEL_WAL, 1_255),
            (GET_WAL, 40),
        ])
        .into_iter(),
    );
    assert!(!w.stalled, "{w:?}");
}

#[test]
fn wal_read_storm_ignores_an_idle_instance() {
    // No compaction and no trimming, because there is nothing to do.
    // Without the floor this reads identically to a stall.
    let w = tally_storm(ops(&[(GET_WAL, 12)]).into_iter());
    assert!(!w.stalled, "{w:?}");
}

#[test]
fn wal_read_storm_clears_as_soon_as_trimming_resumes() {
    // A single delete:wal in the window is enough: the loop is turning.
    let w = tally_storm(ops(&[(GET_WAL, 12_666), (DEL_WAL, 1)]).into_iter());
    assert!(!w.stalled, "{w:?}");
}

#[test]
fn server_timing_parse() {
    assert_eq!(parse_server_timing_us("total;dur=12.4"), Some(12_400));
    assert_eq!(
        parse_server_timing_us("cache;desc=hit, total;dur=3"),
        Some(3_000)
    );
    assert_eq!(parse_server_timing_us("total; dur=0.5"), Some(500));
    assert_eq!(parse_server_timing_us("edge;dur=9"), None);
    assert_eq!(parse_server_timing_us("garbage"), None);
}

#[test]
fn http_op_mapping() {
    assert_eq!(http_op("PUT", None), 0);
    assert_eq!(http_op("PUT", Some("partNumber=2&uploadId=x")), 1);
    assert_eq!(http_op("GET", Some("list-type=2&prefix=a")), 5);
    assert_eq!(http_op("GET", None), 2);
    assert_eq!(http_op("DELETE", None), 4);
}

// ---- R27-4 bulk gate ---------------------------------------------------
// Gate mechanisms can be tested independently of runtime assembly.

/// N tasks each transfer `op_bytes` through the gate; the observed
/// peak of concurrently-held bytes must never exceed the cap.
#[expect(
    clippy::disallowed_methods,
    reason = "bulk-gate concurrency fixture; every spawned task is joined within the test; independent waiters are required to exercise occupied capacity"
)]
#[tokio::test]
async fn bulk_gate_bounds_concurrent_bytes() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    let cap: u32 = 16 << 20;
    let op: u64 = 8 << 20;
    let gate = Arc::new(BulkGate::new(std::num::NonZeroU32::new(cap).unwrap()));
    let cur = Arc::new(AtomicI64::new(0));
    let peak = Arc::new(AtomicI64::new(0));
    let mut js = Vec::new();
    for _ in 0..12 {
        let (g, c, p) = (gate.clone(), cur.clone(), peak.clone());
        js.push(tokio::spawn(async move {
            let _permit = g.acquire(op).await;
            let now = c.fetch_add(op as i64, Ordering::SeqCst) + op as i64;
            p.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            c.fetch_sub(op as i64, Ordering::SeqCst);
        }));
    }
    for j in js {
        j.await.unwrap();
    }
    assert!(
        peak.load(Ordering::SeqCst) <= cap as i64,
        "peak {} exceeded cap {}",
        peak.load(Ordering::SeqCst),
        cap
    );
    assert!(
        gate.waits.load(Ordering::Relaxed) > 0,
        "12 ops of 8MiB through a 16MiB gate must have queued"
    );
    assert_eq!(gate.inflight_bytes.load(Ordering::Relaxed), 0);
}

/// An op larger than the whole cap clamps to the cap: it serializes
/// against everything else but always completes (no starvation, no
/// arithmetic overflow of the semaphore).
#[tokio::test]
async fn bulk_gate_oversized_op_clamps_and_completes() {
    let gate = BulkGate::new(std::num::NonZeroU32::new(4 << 20).unwrap());
    {
        let _p = gate.acquire(64 << 20).await; // 16x the cap
        assert_eq!(gate.inflight_bytes.load(Ordering::Relaxed), 4 << 20);
    }
    assert_eq!(gate.inflight_bytes.load(Ordering::Relaxed), 0);
    // and again, proving the full capacity was returned
    let _p2 = gate.acquire(64 << 20).await;
}

/// Only sst-class ops are gated: WAL (ack path), manifest (CAS
/// liveness), fleet and other must return no permit even when a
/// gate is configured — they can never queue behind compaction.
#[tokio::test]
async fn bulk_gate_exempts_non_sst_classes() {
    let resources = StoreResources::new(&crate::config::StorageConfig {
        bulk_inflight_max_bytes: 8 << 20,
        ..Default::default()
    });
    let _held = resources.bulk_permit(2, 8 << 20).await.unwrap();
    for (path, gated) in [
        ("pilot/shards/root-3/wal/00000042.sst", false), // class wal
        ("pilot/manifest/00000007.manifest", false),
        ("pilot/shards/root-3/compacted/ulid.sst", true),
        ("pilot/fleet/owners/streams-1", false),
        ("pilot/registry/doc.json", false),
    ] {
        let class = classify(path);
        assert_eq!(class == 2, gated, "path {path} class {class}");
        if !gated {
            assert!(
                resources.bulk_permit(class, 8 << 20).await.is_none(),
                "non-sst class {class} must never take a permit"
            );
        }
    }
}

/// Liveness: a waiter blocked on a full gate proceeds as soon as the
/// holder finishes its leaf op — permits are never held across
/// stream consumption, so this is the whole deadlock argument.
#[expect(
    clippy::disallowed_methods,
    reason = "bulk-gate concurrency fixture; every spawned task is joined within the test; independent waiters are required to exercise occupied capacity"
)]
#[tokio::test]
async fn bulk_gate_waiter_proceeds_when_holder_releases() {
    use std::sync::Arc;
    let gate = Arc::new(BulkGate::new(std::num::NonZeroU32::new(8 << 20).unwrap()));
    let held = gate.acquire(8 << 20).await;
    let g2 = gate.clone();
    let waiter = tokio::spawn(async move {
        let _p = g2.acquire(8 << 20).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!waiter.is_finished(), "gate full: waiter must be parked");
    drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("waiter must run once capacity frees")
        .unwrap();
}

// ---- Cumulative totals -------------------------------------------------

/// How much one cumulative `totals` count (`/`-separated beneath it) grew
/// between two /v1/debug/store snapshots.
fn total_growth(before: &serde_json::Value, after: &serde_json::Value, key: &str) -> u64 {
    let read = |snapshot: &serde_json::Value| {
        key.split('/')
            .fold(&snapshot["totals"], |node, part| &node[part])
            .as_u64()
            .unwrap_or(0)
    };
    read(after)
        .checked_sub(read(before))
        .expect("cumulative totals never decrease")
}

/// One MiB: the fixture's payloads are large enough that what other tests
/// of this process put or get through a wrapped store cannot make up for
/// a payload the totals failed to count.
const MIB: usize = 1 << 20;

/// Drive one wrapped store through every outcome of every op: a 1 MiB
/// object put, refused twice (412), read, revalidated (304), missed (404),
/// read beyond its end (an error), headed, copied, listed (once abandoned)
/// and deleted, beside a completed 2 MiB multipart upload.
async fn drive_every_outcome(store: &impl object_store::ObjectStore) {
    use futures_util::StreamExt;
    use object_store::{Error, GetOptions, ObjectStoreExt, PutMode, PutPayload, path::Path};
    let path = Path::from("totals-fixture/object");
    let put = store.put(&path, vec![7u8; MIB].into()).await.unwrap();
    let small = || PutPayload::from_static(b"x");
    let exists = store.put_opts(&path, small(), PutMode::Create.into()).await;
    assert!(matches!(exists, Err(Error::AlreadyExists { .. })));
    let stale = object_store::UpdateVersion {
        e_tag: Some("\"stale\"".into()),
        version: None,
    };
    let refused = store
        .put_opts(&path, small(), PutMode::Update(stale).into())
        .await;
    assert!(matches!(refused, Err(Error::Precondition { .. })));
    let mut upload = store
        .put_multipart(&Path::from("totals-fixture/parts"))
        .await
        .unwrap();
    upload.put_part(vec![9u8; 2 * MIB].into()).await.unwrap();
    upload.complete().await.unwrap();
    let body = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(body.len(), MIB);
    let revalidate = GetOptions {
        if_none_match: put.e_tag,
        ..GetOptions::default()
    };
    let unchanged = store.get_opts(&path, revalidate).await;
    assert!(matches!(unchanged, Err(Error::NotModified { .. })));
    let missing = store.get(&Path::from("totals-fixture/missing")).await;
    assert!(matches!(missing, Err(Error::NotFound { .. })));
    let end = u64::try_from(4 * MIB).unwrap();
    let beyond = store.get_range(&path, end..end + 1).await;
    assert!(matches!(beyond, Err(Error::Generic { .. })));
    store.head(&path).await.unwrap();
    store
        .copy(&path, &Path::from("totals-fixture/copy"))
        .await
        .unwrap();
    let prefix = Path::from("totals-fixture");
    let mut abandoned = store.list(Some(&prefix));
    assert!(abandoned.next().await.is_some());
    drop(abandoned);
    assert_eq!(store.list(Some(&prefix)).collect::<Vec<_>>().await.len(), 3);
    store.delete(&path).await.unwrap();
}

/// GET /v1/debug/store's `totals` count every operation the store wrappers
/// finish, by the outcome the provider bills: 2xx; the 404 answers, which
/// Tigris bills at the operation's class; the unbilled 304 and 412 answers;
/// and every other failure, cancellations included. They
/// also carry the bytes put and got. Other tests in this process may run
/// wrapped operations of their own, so these growths are lower bounds; the
/// observation tests pin the exact counts on a private `StoreStats`.
#[tokio::test]
async fn debug_store_totals_count_every_wrapped_operation_by_billed_outcome() {
    let config = crate::config::StorageConfig::default();
    let resources = std::sync::Arc::new(StoreResources::new(&config));
    let store = super::TimingStore::new(object_store::memory::InMemory::new(), resources.clone());
    let sample = || super::snapshot(60, false, &resources, &serde_json::Value::Null);
    let before = sample();
    drive_every_outcome(&store).await;
    let after = sample();
    let since = after["totals"]["since_ms"].as_u64();
    assert!(
        since.is_some_and(|ms| ms > 0 && Some(ms) <= after["ts_ms"].as_u64()),
        "GET /v1/debug/store carries no cumulative totals: {after}"
    );
    assert_eq!(after["totals"]["since_ms"], before["totals"]["since_ms"]);
    let mib = u64::try_from(MIB).unwrap();
    for (key, least) in [
        ("ops/put:other/ok", 1),
        ("ops/put:other/unbilled", 2),
        ("ops/mpu:other/ok", 1),
        ("ops/get:other/ok", 1),
        ("ops/get:other/not_found", 1),
        ("ops/get:other/unbilled", 1),
        ("ops/get:other/err", 1),
        ("ops/head:other/ok", 1),
        ("ops/copy:other/ok", 1),
        ("ops/list:other/ok", 1),
        ("ops/list:other/err", 1),
        ("ops/delete:other/ok", 1),
        ("bytes_put", 3 * mib),
        ("bytes_got", mib),
    ] {
        let grew = total_growth(&before, &after, key);
        assert!(grew >= least, "{key} grew {grew}, under {least}: {after}");
    }
}
