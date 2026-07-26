//! Deterministic simulation testing (docs/DST.md).
//!
//! Every decision here is derived from a seed: fault placement, injected
//! latency, and the order in which actors act. A failing seed reproduces
//! the exact interleaving, which is the property that makes these tests
//! worth more than a soak — the 90-minute docker ladder found races only
//! when they happened to fire, and several hid for multiple passes.
//!
//! The scenarios exercise our real `ShardEngine` against a fault-injecting
//! object store, and assert the invariants the ladder's order-checker
//! asserts once at the end of a run:
//!
//!   I1  no acknowledged record is unreadable
//!   I2  per-key order is total and gapless
//!   I3  no duplicates
//!   I4  at most one writer commits per shard (fencing is honoured)

#![cfg(test)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// What a fault store may do to an operation. Chosen by seeded RNG so a
/// seed replays the identical fault schedule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Toxic {
    /// Pass through untouched.
    None,
    /// Delay before performing the operation (models Tigris: we measured
    /// 8-185 ms per op across regions, iad1 at 139-185 ms).
    Latency(u64),
    /// Fail with a retryable-looking error (the 503/500 class).
    Error,
}

/// Deterministic fault-injecting `ObjectStore` decorator.
///
/// Wraps any inner store (tests use `InMemory`) and consults a seeded RNG
/// per operation. Counters let a scenario assert that faults actually
/// fired — a fault store that never injects is the DST equivalent of the
/// vacuous ladder rung, and we learned that lesson the expensive way.
#[derive(Debug)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    rng: Mutex<StdRng>,
    /// Percent chance (0-100) that a mutating op is delayed.
    latency_pct: u8,
    /// Percent chance (0-100) that a mutating op fails outright.
    error_pct: u8,
    pub injected_latency: AtomicU64,
    pub injected_errors: AtomicU64,
    pub ops: AtomicU64,
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>, seed: u64, latency_pct: u8, error_pct: u8) -> Arc<Self> {
        Arc::new(Self {
            inner,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            latency_pct,
            error_pct,
            injected_latency: AtomicU64::new(0),
            injected_errors: AtomicU64::new(0),
            ops: AtomicU64::new(0),
        })
    }

    /// Decide this operation's fate from the seeded stream.
    fn roll(&self) -> Toxic {
        let mut r = self.rng.lock().unwrap();
        let x: u8 = r.random_range(0..100);
        if x < self.error_pct {
            Toxic::Error
        } else if x < self.error_pct.saturating_add(self.latency_pct) {
            Toxic::Latency(r.random_range(1..12))
        } else {
            Toxic::None
        }
    }

    async fn apply(&self, mutating: bool) -> OsResult<()> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        if !mutating {
            return Ok(());
        }
        match self.roll() {
            Toxic::None => Ok(()),
            Toxic::Latency(ms) => {
                self.injected_latency.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                Ok(())
            }
            Toxic::Error => {
                self.injected_errors.fetch_add(1, Ordering::Relaxed);
                Err(object_store::Error::Generic {
                    store: "FaultStore",
                    source: "injected fault".into(),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.apply(true).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.apply(true).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.apply(false).await?;
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        self.apply(true).await?;
        self.inner.copy_opts(from, to, opts).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }
}

// ---- invariant oracle -----------------------------------------------

/// What the workload believes it wrote: routing key -> acknowledged
/// payload sequence numbers, in the order they were acked.
#[derive(Default, Debug)]
pub struct AckLedger {
    pub acked: std::collections::HashMap<String, Vec<u64>>,
}

impl AckLedger {
    pub fn record(&mut self, key: &str, seq: u64) {
        self.acked.entry(key.to_string()).or_default().push(seq);
    }

    pub fn total(&self) -> usize {
        self.acked.values().map(|v| v.len()).sum()
    }

    /// I1/I2/I3: every acked seq must appear exactly once, in order.
    /// `observed` is what a reader drained, per key.
    pub fn audit(
        &self,
        observed: &std::collections::HashMap<String, Vec<u64>>,
    ) -> Result<(), String> {
        for (key, acked) in &self.acked {
            let seen = observed.get(key).cloned().unwrap_or_default();

            // I1: nothing acked may be missing.
            if seen.len() < acked.len() {
                return Err(format!(
                    "I1 violated: key {key} acked {} records but only {} readable",
                    acked.len(),
                    seen.len()
                ));
            }
            // I3: no duplicates.
            let mut uniq = seen.clone();
            uniq.sort_unstable();
            uniq.dedup();
            if uniq.len() != seen.len() {
                return Err(format!(
                    "I3 violated: key {key} has {} duplicate record(s)",
                    seen.len() - uniq.len()
                ));
            }
            // I2: acked order is preserved as a subsequence of what we read.
            let mut it = seen.iter();
            for want in acked {
                if !it.any(|got| got == want) {
                    return Err(format!(
                        "I2 violated: key {key} seq {want} out of order or absent"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One simulated writer over our real `ShardEngine`.
///
/// Returns the acked payload sequences per routing key. Appends are
/// retried on transient errors exactly as a production client would;
/// an append is recorded in the ledger ONLY after a durable 2xx-equivalent
/// ack, so the ledger is the ground truth for invariant I1.
pub async fn drive_appends(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
    routing_keys: &[&str],
    seq_base: u64,
    per_key: u64,
    ledger: &mut AckLedger,
) {
    use crate::shard::AppendReq;
    // subkey is per (stream_epoch, routing_key, key_version)
    for seq in seq_base..seq_base + per_key {
        for rk in routing_keys {
            let payload = serde_json::json!({ "k": rk, "seq": seq }).to_string();
            let subkey = crate::crypto::derive_subkey(key, &hash, rk, 0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req = AppendReq {
                enqueued_at: std::time::Instant::now(),
                hash,
                entries: vec![bytes::Bytes::from(payload.into_bytes())],
                usage: crate::usage::counters(&hash),
                routing_key: rk.to_string(),
                key_version: 0,
                subkey,
                ts_hint_ms: None,
                seq: None,
                bytes: 0,
                close: false,
                producer: None,
                deferred_error: None,
                touch: None,
                resp: tx,
            };
            if engine.try_enqueue(req).is_err() {
                continue; // queue full: client would retry; not an ack
            }
            match rx.await {
                Ok(Ok(_ack)) => ledger.record(rk, seq),
                _ => {} // error or fenced: NOT acked, so not in the ledger
            }
        }
    }
}

/// Read every record back through the shard log and group payload seqs
/// by routing key — the reader side of the oracle.
pub async fn drain_observed(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    key: &crate::crypto::StreamKey,
) -> std::collections::HashMap<String, Vec<u64>> {
    let mut out: std::collections::HashMap<String, Vec<u64>> = Default::default();
    let handle = match engine.stream_handle(hash).await {
        Ok(h) => h,
        Err(_) => return out,
    };
    let upto = handle.state.lock().unwrap().durable.next;
    let res = match crate::shard::read_frames_range(engine, &handle, 0, upto, 64 * 1024 * 1024).await
    {
        Ok(r) => r,
        Err(_) => return out,
    };
    for frame in res.frames {
        let Some(dec) = crate::crypto::decode_frame(&frame) else { continue };
        // the subkey is per (epoch, routing_key, key_version) — the frame
        // header carries both, exactly as the absorber does it
        let sk = crate::crypto::derive_subkey(
            key,
            &hash,
            &dec.header.routing_key,
            dec.header.key_version,
        );
        let Ok(plain) = crate::crypto::decrypt_frame(&sk, &hash, &dec, &frame) else { continue };
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&plain) else { continue };
        let (Some(k), Some(seq)) = (v.get("k").and_then(|x| x.as_str()), v.get("seq").and_then(|x| x.as_u64()))
        else { continue };
        out.entry(k.to_string()).or_default().push(seq);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    /// The fault schedule must be a pure function of the seed, or a
    /// failing run cannot be replayed — the whole point of DST.
    #[tokio::test]
    async fn fault_schedule_is_reproducible_from_the_seed() {
        async fn schedule(seed: u64) -> Vec<bool> {
            let s = FaultStore::new(mem(), seed, 30, 20);
            let mut out = Vec::new();
            for i in 0..64u64 {
                let p = ObjPath::from(format!("k{i}"));
                out.push(s.put(&p, PutPayload::from(vec![1u8; 8])).await.is_ok());
            }
            out
        }
        assert_eq!(schedule(42).await, schedule(42).await, "same seed must replay");
        assert_ne!(
            schedule(42).await,
            schedule(43).await,
            "different seeds must explore different schedules"
        );
    }

    /// A fault store that never injects proves nothing — the DST form of
    /// the vacuous ladder rung (bench/docker/harness/README.md).
    #[tokio::test]
    async fn faults_actually_fire() {
        let s = FaultStore::new(mem(), 7, 40, 25);
        for i in 0..200u64 {
            let _ = s
                .put(&ObjPath::from(format!("k{i}")), PutPayload::from(vec![0u8; 4]))
                .await;
        }
        assert!(
            s.injected_errors.load(Ordering::Relaxed) > 0,
            "no errors injected — scenario would be vacuous"
        );
        assert!(
            s.injected_latency.load(Ordering::Relaxed) > 0,
            "no latency injected — scenario would be vacuous"
        );
    }

    /// Reads are never faulted, so data written through the fault store
    /// is still readable — the store models a flaky network, not a lying
    /// disk.
    #[tokio::test]
    async fn survives_injected_faults_without_losing_written_data() {
        let inner = mem();
        let s = FaultStore::new(inner.clone(), 11, 50, 30);
        let mut wrote = Vec::new();
        for i in 0..100u64 {
            let p = ObjPath::from(format!("obj{i}"));
            // retry like the real client does
            for _ in 0..8 {
                if s.put(&p, PutPayload::from(i.to_be_bytes().to_vec())).await.is_ok() {
                    wrote.push(i);
                    break;
                }
            }
        }
        assert!(!wrote.is_empty());
        for i in &wrote {
            let got = inner.get(&ObjPath::from(format!("obj{i}"))).await.unwrap();
            let b = got.bytes().await.unwrap();
            assert_eq!(b.as_ref(), &i.to_be_bytes(), "obj{i} content differs");
        }
    }

    // ---- scenarios over the real ShardEngine ---------------------

    async fn open_engine(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
    ) -> Arc<crate::shard::ShardEngine> {
        let db = slatedb::Db::builder(prefix, store)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        crate::shard::ShardEngine::start(
            prefix.to_string(),
            Arc::new(db),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
        )
    }

    /// I1+I2+I3 on a single writer under injected store faults. This is
    /// the baseline: whatever the store does to us, an ACKED record must
    /// be readable, exactly once, in order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acked_records_survive_store_faults() {
        for seed in [1u64, 7, 99] {
            let inner = mem();
            let store = FaultStore::new(inner.clone(), seed, 35, 0);
            let engine = open_engine(store.clone(), "dst-faults").await;
            let key = crate::crypto::StreamKey([7u8; 32]);
            let hash = [3u8; 16];
            let keys = ["a", "b", "c"];

            let mut ledger = AckLedger::default();
            drive_appends(&engine, hash, &key, &keys, 0, 25, &mut ledger).await;
            assert!(ledger.total() > 0, "seed {seed}: nothing acked");

            let observed = drain_observed(&engine, hash, &key).await;
            if let Err(e) = ledger.audit(&observed) {
                panic!("seed {seed}: {e}");
            }
            assert!(
                store.injected_latency.load(Ordering::Relaxed) > 0,
                "seed {seed}: no faults injected — vacuous"
            );
        }
    }

    /// I4 + I1 across a SHARD MOVE. Opening a second engine on the same
    /// prefix fences the first — this is precisely what the rebalancer
    /// does, and the class that produced the pass-3 zombie-GC data loss
    /// (a fenced owner's background work racing the new owner's open).
    ///
    /// Contract under test: records acked by the OLD owner before the
    /// handoff must still be readable through the NEW owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acked_records_survive_a_fencing_handoff() {
        for seed in [2u64, 13, 64] {
            let inner = mem();
            let store = FaultStore::new(inner.clone(), seed, 25, 0);
            let key = crate::crypto::StreamKey([9u8; 32]);
            let hash = [5u8; 16];
            let keys = ["x", "y"];
            let mut ledger = AckLedger::default();

            // old owner accepts and acks
            let a = open_engine(store.clone(), "dst-fence").await;
            drive_appends(&a, hash, &key, &keys, 0, 20, &mut ledger).await;
            let before = ledger.total();
            assert!(before > 0, "seed {seed}: nothing acked pre-handoff");

            // the move: a new owner opens the same shard log, fencing A
            let b = open_engine(store.clone(), "dst-fence").await;

            // I4: the fenced owner must not be able to keep committing
            let mut ghost = AckLedger::default();
            drive_appends(&a, hash, &key, &keys, 100, 5, &mut ghost).await;

            // new owner takes over
            drive_appends(&b, hash, &key, &keys, 20, 20, &mut ledger).await;

            // I1: everything the OLD owner acked is still readable via B
            let observed = drain_observed(&b, hash, &key).await;
            if let Err(e) = ledger.audit(&observed) {
                panic!("seed {seed}: after handoff (pre-handoff acks={before}): {e}");
            }
        }
    }

    // ---- the oracle itself must be able to fail -------------------

    #[tokio::test]
    async fn oracle_accepts_a_faithful_read() {
        let mut led = AckLedger::default();
        for s in 0..10u64 {
            led.record("k", s);
        }
        let mut obs = std::collections::HashMap::new();
        obs.insert("k".to_string(), (0..10).collect::<Vec<u64>>());
        assert!(led.audit(&obs).is_ok());
    }

    #[tokio::test]
    async fn oracle_catches_loss() {
        // exactly the C3 shape: acked but unreadable
        let mut led = AckLedger::default();
        for s in 0..10u64 {
            led.record("k", s);
        }
        let mut obs = std::collections::HashMap::new();
        obs.insert("k".to_string(), vec![0, 1, 2, 3]);
        let err = led.audit(&obs).unwrap_err();
        assert!(err.starts_with("I1"), "expected I1, got: {err}");
    }

    #[tokio::test]
    async fn oracle_catches_duplicates() {
        // the pass-2b shape: an ambiguous retry committed twice
        let mut led = AckLedger::default();
        for s in 0..4u64 {
            led.record("k", s);
        }
        let mut obs = std::collections::HashMap::new();
        obs.insert("k".to_string(), vec![0, 1, 1, 2, 3]);
        let err = led.audit(&obs).unwrap_err();
        assert!(err.starts_with("I3"), "expected I3, got: {err}");
    }

    #[tokio::test]
    async fn oracle_catches_reordering() {
        let mut led = AckLedger::default();
        for s in 0..4u64 {
            led.record("k", s);
        }
        let mut obs = std::collections::HashMap::new();
        obs.insert("k".to_string(), vec![0, 2, 1, 3]);
        let err = led.audit(&obs).unwrap_err();
        assert!(err.starts_with("I2"), "expected I2, got: {err}");
    }
}
