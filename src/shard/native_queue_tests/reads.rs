//! What a claim waits for on a cold backlog (docs/QUEUES-PRICING-BENCHMARK.md
//! section 11): the committer runs a shard's queue operations one at a time,
//! so a claim that waits for a block of the availability index holds every
//! operation of the shard for an object-store round trip. A claim's walk
//! keeps the blocks after its own in flight, so the claims that follow find
//! theirs in the block cache instead of each paying a round trip.
#![cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, memory::InMemory};
use slatedb::config::{FlushOptions, FlushType, Settings};
use slatedb::db_cache::foyer::FoyerCacheOptions;
use tokio::sync::watch;

use super::{HASH, ShardConfig, ShardEngine, create, msg, send_op, set_clock};
use crate::native_queue::{DAY_MS, DeliveryView, NqOp, NqOut, QueueConfig};
use crate::runtime::block_cache::BlockCache;

/// Messages of the backlog: their availability entries span hundreds of
/// 4 KiB blocks, far more than one claim reads.
const BACKLOG: usize = 2_000;
/// Body bytes of each message: incompressible, so each sealed page (and
/// the availability entry that carries it inline) is about 1 KiB.
const BODY: usize = 900;
/// Messages a claim takes, as the benchmark's consumers do.
const CLAIM: usize = 10;
/// Claims that must each find their blocks already read.
const CLAIMS: usize = 12;
/// How long a claim may take while the store holds every table read: one
/// that waits for a block never answers until the store opens.
const UNHELD_WITHIN: Duration = Duration::from_secs(5);

/// Holds every read of a compacted table (L0 and sorted runs) while closed
/// and counts them, started and still in flight; WAL replay, manifests
/// and writes pass. A claim that answers while the store is closed waited
/// for no block.
#[derive(Debug)]
struct HeldTables {
    inner: InMemory,
    /// Whether table reads are held.
    closed: watch::Sender<bool>,
    started: AtomicU64,
    in_flight: AtomicU64,
}

impl HeldTables {
    fn close(&self) {
        self.closed.send_replace(true);
    }

    fn open(&self) {
        self.closed.send_replace(false);
    }

    /// Open, then wait until every read the walk started has landed and
    /// no new one starts for a while (spawned fetches may start late).
    async fn open_and_land(&self) {
        self.open();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut quiet = 0;
        let mut seen = self.started.load(Ordering::SeqCst);
        while quiet < 10 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the table reads never landed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            let now = self.started.load(Ordering::SeqCst);
            if now == seen && self.in_flight.load(Ordering::SeqCst) == 0 {
                quiet += 1;
            } else {
                quiet = 0;
                seen = now;
            }
        }
    }
}

impl std::fmt::Display for HeldTables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldTables")
    }
}

#[async_trait]
impl ObjectStore for HeldTables {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if !location.as_ref().contains("compacted") {
            return self.inner.get_opts(location, options).await;
        }
        self.started.fetch_add(1, Ordering::SeqCst);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let opened = self
            .closed
            .subscribe()
            .wait_for(|closed| !closed)
            .await
            .is_ok();
        assert!(opened, "the store outlives its own gate");
        let got = self.inner.get_opts(location, options).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        got
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Incompressible text (pages are compressed before sealing).
fn noise(n: usize, seed: u32) -> String {
    let mut x = 0x9E37_79B9_u32 ^ seed.wrapping_mul(0x85EB_CA6B);
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            char::from(b'!' + u8::try_from(x % 90).unwrap())
        })
        .collect()
}

/// A claim of `CLAIM` messages, or `None` if it did not answer within
/// `UNHELD_WITHIN`.
async fn claim_within(e: &ShardEngine) -> Option<Vec<DeliveryView>> {
    let claim = NqOp::Claim {
        max: CLAIM,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
    };
    let answer = tokio::time::timeout(UNHELD_WITHIN, e.submit_native_queue(HASH, claim))
        .await
        .ok()?;
    match answer.unwrap() {
        NqOut::Claimed { deliveries, .. } => Some(deliveries),
        other => panic!("claim answered {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_claim_on_a_cold_backlog_waits_for_no_block_its_predecessors_read() {
    let store = Arc::new(HeldTables {
        inner: InMemory::new(),
        closed: watch::Sender::new(false),
        started: AtomicU64::new(0),
        in_flight: AtomicU64::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let cache = Arc::new(BlockCache::new(FoyerCacheOptions {
        max_capacity: 64 << 20,
        shards: 1,
    }));
    // No compactor: the table the test flushes is the table the claims read.
    let db = crate::keyspace::db_builder("nq-cold-walk", object_store.clone())
        .with_settings(Settings {
            compactor_options: None,
            ..Default::default()
        })
        .with_db_cache(cache.clone())
        .build()
        .await
        .unwrap();
    let db = Arc::new(db);
    // A dropped absorber receiver closes the engine.
    let e = ShardEngine::start(
        "nq-cold-walk".to_owned(),
        db.clone(),
        object_store,
        ShardConfig::default(),
        None,
        Default::default(),
    );
    create(&e, QueueConfig::default()).await;
    set_clock(&db, super::now_ms() + DAY_MS).await;
    for batch in 0..BACKLOG / 1_000 {
        let bodies = (0..1_000).map(|i| noise(BODY, u32::try_from(batch * 1_000 + i).unwrap()));
        let msgs = bodies.map(|b| msg(&b)).collect();
        super::op(&e, send_op(msgs)).await.unwrap();
    }
    let flush = FlushOptions {
        flush_type: FlushType::MemTable,
    };
    db.flush_with_options(flush).await.unwrap();
    // The flush cached what it wrote: start cold, as after a compaction.
    cache.release();

    // The first claim reads its blocks (and the table's index) from the store.
    let first = claim_within(&e).await.expect("an open store answers");
    let mut taken: Vec<u64> = first.iter().map(|d| d.offset).collect();
    for n in 1..=CLAIMS {
        store.open_and_land().await;
        store.close();
        let Some(claimed) = claim_within(&e).await else {
            store.open();
            panic!(
                "claim {n} of a cold backlog waited for the store: the walk before it \
                 left the blocks it reads unread"
            );
        };
        taken.extend(claimed.iter().map(|d| d.offset));
    }
    store.open();
    let all = u64::try_from(CLAIM * (CLAIMS + 1)).unwrap();
    assert_eq!(
        taken,
        (0..all).collect::<Vec<_>>(),
        "every claim took the next messages in availability order"
    );
}
