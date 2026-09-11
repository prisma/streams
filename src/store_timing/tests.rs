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
    let gate = Arc::new(BulkGate::new(cap));
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
    let gate = BulkGate::new(4 << 20);
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
    let gate = Arc::new(BulkGate::new(8 << 20));
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
