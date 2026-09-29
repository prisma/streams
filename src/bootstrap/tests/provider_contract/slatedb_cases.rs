//! SlateDB writer fencing through the store under test (ASM-SLATEDB-FENCE),
//! opened with the server's own settings and written through the server's
//! commit pipeline: a second writer fences the first, and a WAL PUT whose
//! reply is lost is never a second success.
#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::{CloseReason, Db, ErrorKind};

use super::Backend;
use super::faults::{Conditional, FaultyStore, PutFault};

/// Generous bound for one SlateDB write against a remote store.
const WRITE_BOUND: Duration = Duration::from_secs(60);

pub(super) async fn open(b: &Backend, store: Arc<dyn ObjectStore>, path: &str) -> Db {
    Db::builder(path, store)
        .with_settings(b.settings.clone())
        .build()
        .await
        .unwrap_or_else(|e| panic!("{}: open {path}: {e}", b.name))
}

pub(super) fn is_fenced(error: &slatedb::Error) -> bool {
    error.kind() == ErrorKind::Closed(CloseReason::Fenced)
}

/// Bound of the proof that the pipeline flushed a write on a local store.
const FLUSH_BOUND: Duration = Duration::from_secs(10);

/// Write one key, flush the WAL as the configured commit pipeline does, and
/// wait until SlateDB reports the write durable.
pub(super) async fn bounded_put(
    b: &Backend,
    db: &Db,
    key: &[u8],
    value: &[u8],
) -> Result<(), slatedb::Error> {
    let durable = async {
        let written = db.put(key, value).await?;
        if b.pump {
            // The pump (`ShardEngine::start`) logs a failed flush and
            // leaves the verdict to the write's own durability result.
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .ok();
        }
        written.await_durable().await
    };
    tokio::time::timeout(WRITE_BOUND, durable)
        .await
        .expect("a SlateDB write did not finish")
}

/// The pipeline itself makes a write durable. Under the pump SlateDB's own
/// timer is set to 600 s for this one database, so only the writer's flush
/// can have done it; under the tick the timer is the server's. SlateDB's
/// timer fires once as the database opens and can carry a write that
/// arrives before it, so the proof is the second write.
pub(super) async fn the_pipeline_flushes_a_write(b: &Backend) {
    let mut settings = b.settings.clone();
    if b.pump {
        settings.flush_interval = Some(Duration::from_secs(600));
    }
    let path = b.slate_path("pipeline-flush");
    let db = Db::builder(path.as_str(), b.shard.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap_or_else(|e| panic!("{}: open {path}: {e}", b.name));
    for key in [&b"first"[..], b"second"] {
        let written = tokio::time::timeout(FLUSH_BOUND, bounded_put(b, &db, key, b"v")).await;
        assert!(
            matches!(written, Ok(Ok(()))),
            "{}: nothing flushed the WAL within {FLUSH_BOUND:?} of the {} write (pump: {}): {written:?}",
            b.name,
            String::from_utf8_lossy(key),
            b.pump
        );
    }
    db.close().await.unwrap();
}

/// A second writer on the path reads the first writer's durable data and
/// fences it: the first writer's next write fails `Fenced` and is not
/// visible to the second.
async fn second_writer_fences_the_first(b: &Backend) {
    let path = b.slate_path("fence");
    let first = open(b, b.shard.clone(), &path).await;
    bounded_put(b, &first, b"k1", b"first").await.unwrap();
    let second = open(b, b.shard.clone(), &path).await;
    assert_eq!(
        second.get(b"k1").await.unwrap(),
        Some(Bytes::from_static(b"first"))
    );
    let zombie = bounded_put(b, &first, b"k2", b"zombie").await;
    assert!(
        zombie.as_ref().is_err_and(is_fenced),
        "{}: the fenced writer's write: {zombie:?}",
        b.name
    );
    assert_eq!(second.get(b"k2").await.unwrap(), None, "{}", b.name);
    bounded_put(b, &second, b"k3", b"second").await.unwrap();
    second.close().await.unwrap();
    first.close().await.ok();
}

/// A WAL PUT that lands but whose reply is lost is retried by SlateDB's
/// retrying store and finds its path taken. It reports `Fenced` unless the
/// store returns SlateDB's put id on HEAD, in which case SlateDB recognises
/// its own landed PUT and reports one success. A WAL PUT that failed
/// before dispatch is retried and lands. Either way the batch is durable.
async fn ambiguous_wal_puts(b: &Backend, metadata_round_trip: bool) {
    let path = b.slate_path("ambiguous-wal");
    let faulty = FaultyStore::new(b.shard.clone());
    let writer = open(b, faulty.clone(), &path).await;
    faulty.arm(PutFault::FailBeforeDispatch, Conditional::Create, "/wal/");
    bounded_put(b, &writer, b"retried", b"landed")
        .await
        .unwrap();
    assert_eq!(faulty.fired(), 1, "{}: the WAL fault never fired", b.name);
    faulty.arm(PutFault::LoseReply, Conditional::Create, "/wal/");
    let lost = bounded_put(b, &writer, b"lost", b"landed").await;
    assert_eq!(faulty.fired(), 2, "{}: the WAL fault never fired", b.name);
    if metadata_round_trip {
        assert!(lost.is_ok(), "{}: own landed WAL PUT: {lost:?}", b.name);
    } else {
        assert!(
            lost.as_ref().is_err_and(is_fenced),
            "{}: a landed WAL PUT without put-id metadata: {lost:?}",
            b.name
        );
    }
    writer.close().await.ok();
    let reopened = open(b, b.shard.clone(), &path).await;
    for key in [&b"retried"[..], b"lost"] {
        assert_eq!(
            reopened.get(key).await.unwrap(),
            Some(Bytes::from_static(b"landed")),
            "{}: {} is not durable",
            b.name,
            String::from_utf8_lossy(key)
        );
    }
    reopened.close().await.unwrap();
}

pub(super) async fn run(b: &Backend, metadata_round_trip: bool) {
    second_writer_fences_the_first(b).await;
    ambiguous_wal_puts(b, metadata_round_trip).await;
}
