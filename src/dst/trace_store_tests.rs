//! TraceStore's focused tests (PR 3.2.1: moved beside the subsystem
//! they pin; `use super::*` sees trace_store's private internals).

use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use super::{TraceEventKind, TraceOutcome, TraceStore};
use crate::dst::{ObjClass, StoreOp};

fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// The trace is in id (= trace-lock acquisition) order, so a
/// before/after refactor diff compares equal iff the client issued the
/// same operations in the same order. For a single sequential client
/// with no streams, ids happen to be dense from 0 — but the CONTRACT
/// is unique + strictly increasing, not dense (see `StoreTraceEvent`).
#[tokio::test]
async fn events_are_ordered_by_dispatch_seq() {
    let s = TraceStore::verbatim(mem());
    let pa = ObjPath::from("shards/x/wal/1.sst");
    let pb = ObjPath::from("shards/x/wal/2.sst");
    s.put_opts(&pa, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.put_opts(&pb, PutPayload::from(vec![2u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.get_opts(&pa, GetOptions::default()).await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 3);
    for w in evs.windows(2) {
        assert!(w[0].seq < w[1].seq, "ids strictly increase in trace order");
    }
    assert_eq!(evs[0].op, StoreOp::Put);
    assert_eq!(evs[0].path, pa.as_ref());
    assert_eq!(evs[1].op, StoreOp::Put);
    assert_eq!(evs[1].path, pb.as_ref());
    assert_eq!(evs[2].op, StoreOp::Get);
    assert!(evs.iter().all(|e| e.outcome == TraceOutcome::Ok));

    // The operation ledger and reset.
    let c = s.operation_counts();
    assert!(c.contains(&(StoreOp::Put, ObjClass::Wal, 2)), "{c:?}");
    assert!(c.contains(&(StoreOp::Get, ObjClass::Wal, 1)), "{c:?}");
    s.reset();
    assert!(s.events().is_empty());
}

#[tokio::test]
async fn put_records_byte_count() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![7u8; 1234]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::Put);
    assert_eq!(evs[0].class, ObjClass::Wal);
    assert_eq!(evs[0].bytes, Some(1234));
}

#[tokio::test]
async fn ranged_get_records_span() {
    let inner = mem();
    let p = ObjPath::from("a/b/1.sst");
    inner
        .put_opts(&p, PutPayload::from(vec![0u8; 16]), PutOptions::default())
        .await
        .unwrap();
    let s = TraceStore::new(inner);

    let bounded = GetOptions {
        range: Some(object_store::GetRange::Bounded(2..7)),
        ..Default::default()
    };
    s.get_opts(&p, bounded).await.unwrap();
    let suffix = GetOptions {
        range: Some(object_store::GetRange::Suffix(4)),
        ..Default::default()
    };
    s.get_opts(&p, suffix).await.unwrap();
    s.get_opts(&p, GetOptions::default()).await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 3);
    assert_eq!(evs[0].bytes, Some(5), "bounded range records its span");
    assert_eq!(evs[1].bytes, Some(4), "suffix range records its span");
    assert_eq!(evs[2].bytes, None, "a full get has no span");
}

/// Redaction is the default: no tenant-derived segment survives, the
/// path still classifies identically, and payload bytes are never
/// retained.
#[tokio::test]
async fn redaction_hashes_paths_and_never_stores_payload_bytes_by_default() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("acme-corp/shards/secret-stream-name/wal/00000042.sst");
    s.put_opts(&p, PutPayload::from(vec![0xabu8; 8]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    let e = &evs[0];
    assert_ne!(e.path, p.as_ref());
    for leaked in ["acme-corp", "secret-stream-name", "00000042"] {
        assert!(
            !e.path.contains(leaked),
            "redacted path must not contain {leaked}: {}",
            e.path
        );
    }
    // Classification is preserved, both in the event and on re-classify.
    assert_eq!(e.class, ObjClass::Wal);
    assert_eq!(ObjClass::of(&e.path), ObjClass::Wal, "{}", e.path);
    assert!(e.path.ends_with(".sst"), "extension survives: {}", e.path);
    assert!(
        e.path.contains("/wal/"),
        "structural segments survive: {}",
        e.path
    );
    // Payload bytes are never retained by default.
    assert_eq!(e.content_hash, None);

    // The manifest marker survives redaction too.
    let s2 = TraceStore::new(mem());
    let mp = ObjPath::from("tenant9/shards/root-3/manifest-00001.json");
    s2.put_opts(&mp, PutPayload::from(vec![0u8; 1]), PutOptions::default())
        .await
        .unwrap();
    let e2 = &s2.events()[0];
    assert_eq!(e2.class, ObjClass::Manifest);
    assert_eq!(ObjClass::of(&e2.path), ObjClass::Manifest, "{}", e2.path);
    assert!(!e2.path.contains("tenant9"));
    assert!(!e2.path.contains("root-3"));
}

#[tokio::test]
async fn hash_on_mode_records_content_hashes() {
    let s = TraceStore::with_options(mem(), true, true);
    let payload = b"stream-payload-bytes".to_vec();
    let p = ObjPath::from("shards/x/wal/9.sst");
    s.put_opts(&p, PutPayload::from(payload.clone()), PutOptions::default())
        .await
        .unwrap();
    let want = {
        use sha2::Digest;
        let d = sha2::Sha256::digest(&payload);
        crate::crypto::hex(&d[..8])
    };
    assert_eq!(want.len(), 16, "16 hex chars, not payload bytes");
    let e = &s.events()[0];
    assert_eq!(e.content_hash.as_deref(), Some(want.as_str()));
}

#[tokio::test]
async fn outcome_is_recorded_on_error() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/nope.sst");
    let res = s.get_opts(&p, GetOptions::default()).await;
    assert!(res.is_err(), "get of a nonexistent object must fail");
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::Get);
    assert_eq!(evs[0].outcome, TraceOutcome::NotFound);
}

/// The session is Put events all the way down: open, one per part
/// with byte counts, and a complete that notes how many parts landed.
#[tokio::test]
async fn multipart_session_is_traced() {
    let s = TraceStore::verbatim(mem());
    let p = ObjPath::from("shards/x/wal/big.sst");
    let mut up = s
        .put_multipart_opts(&p, PutMultipartOptions::default())
        .await
        .unwrap();
    up.put_part(PutPayload::from(vec![1u8; 10])).await.unwrap();
    up.put_part(PutPayload::from(vec![2u8; 20])).await.unwrap();
    up.complete().await.unwrap();

    let evs = s.events();
    assert_eq!(evs.len(), 4);
    assert_eq!(evs[0].detail.as_deref(), Some("multipart-open"));
    assert_eq!(evs[0].bytes, None);
    assert_eq!(evs[1].detail.as_deref(), Some("part"));
    assert_eq!(evs[1].bytes, Some(10));
    assert_eq!(evs[2].detail.as_deref(), Some("part"));
    assert_eq!(evs[2].bytes, Some(20));
    assert_eq!(evs[3].detail.as_deref(), Some("complete parts=2"));
    assert!(
        evs.iter()
            .all(|e| e.op == StoreOp::Put && e.outcome == TraceOutcome::Ok)
    );
}

// ---- delete_stream: pass-through with EXACTLY ONE delegated call ----

/// A scripted delete_stream store: counts invocations and consumed
/// inputs, returns a canned output stream. Used to prove the trace
/// layer neither fans the call out nor manufactures results.
#[derive(Debug)]
struct DeleteSpy {
    inner: object_store::memory::InMemory,
    calls: std::sync::Arc<std::sync::atomic::AtomicU64>,
    consumed_inputs: std::sync::Arc<std::sync::atomic::AtomicU64>,
    // object_store::Error is not Clone, so scripted failures are
    // stored as their message and rebuilt at stream time.
    scripted_output: Vec<Result<ObjPath, String>>,
}

impl std::fmt::Display for DeleteSpy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteSpy({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeleteSpy {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
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
        self.inner.copy_opts(from, to, opts).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let consumed = self.consumed_inputs.clone();
        // Lazy pass-through, one pull of the input per output item:
        // each consumed Ok input counts and releases the next
        // scripted result (fewer scripted results than inputs = the
        // coalescing-store shape); an input error surfaces in place,
        // untouched; a dropped consumer stops driving the input.
        let scripted = self.scripted_output.clone().into_iter();
        futures_util::stream::unfold(
            (locations, scripted, consumed),
            |(mut locations, mut scripted, consumed)| async move {
                loop {
                    match locations.next().await {
                        Some(Ok(_)) => {
                            consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if let Some(r) = scripted.next() {
                                let r = r.map_err(|msg| object_store::Error::Generic {
                                    store: "spy",
                                    source: msg.into(),
                                });
                                return Some((r, (locations, scripted, consumed)));
                            }
                            // Scripted output exhausted: drain on.
                        }
                        Some(Err(e)) => return Some((Err(e), (locations, scripted, consumed))),
                        None => return None,
                    }
                }
            },
        )
        .boxed()
    }
}

fn spy(
    scripted: Vec<OsResult<ObjPath>>,
) -> (
    Arc<DeleteSpy>,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
    std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let consumed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    (
        Arc::new(DeleteSpy {
            inner: object_store::memory::InMemory::new(),
            calls: calls.clone(),
            consumed_inputs: consumed.clone(),
            scripted_output: scripted
                .into_iter()
                .map(|r| r.map_err(|e| e.to_string()))
                .collect(),
        }),
        calls,
        consumed,
    )
}

fn delete_err() -> object_store::Error {
    object_store::Error::Generic {
        store: "spy",
        source: "scripted delete failure".into(),
    }
}

/// Three inputs, three scripted results: one inner call, everything
/// passes through, input and result both traced.
#[tokio::test]
async fn delete_stream_delegates_exactly_once_and_traces_both_sides() {
    let (sp, calls, _consumed) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
        Ok(ObjPath::from("a/3.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one inner delete_stream"
    );
    assert_eq!(out.len(), 3, "every scripted result passes through");
    assert!(out.iter().all(|r| r.is_ok()));
    let evs = s.events();
    let inputs = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteInput)
        .count();
    let results = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteResult)
        .count();
    assert_eq!((inputs, results), (3, 3), "both sides traced: {evs:?}");
    // PR 3.2: the operation ledger counts ATTEMPTED deletes — the
    // Ok inputs the inner store consumed — exactly once each, never
    // the diagnostic result observations on top.
    let c = s.operation_counts();
    assert!(
        c.contains(&(StoreOp::Delete, ObjClass::Sst, 3)),
        "3 attempted deletes, not 6 observations: {c:?}"
    );
}

/// The inner store returns FEWER results than inputs (batching stores
/// coalesce): the client sees exactly what the store returned — the
/// trace layer must not invent completions.
#[tokio::test]
async fn delete_stream_never_fabricates_results() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 1, "no manufactured Ok for unreturned inputs");

    // And the extreme: empty output stays empty.
    let (sp2, _, _) = spy(vec![]);
    let s2 = TraceStore::verbatim(sp2);
    let out2: Vec<_> = s2
        .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
        .collect()
        .await;
    assert!(out2.is_empty(), "empty output must remain empty");
}

/// Input errors pass through untouched (and are traced as such).
#[tokio::test]
async fn delete_stream_preserves_input_errors() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/2.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Err(delete_err()),
                Ok(ObjPath::from("a/3.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 2);
    assert!(out[0].is_ok());
    assert!(out[1].is_err(), "the input error must surface unchanged");
    let evs = s.events();
    let err_inputs = evs
        .iter()
        .filter(|e| e.kind == TraceEventKind::DeleteInput && e.outcome != TraceOutcome::Ok)
        .count();
    assert_eq!(err_inputs, 1, "the input error is traced: {evs:?}");
    // PR 3.2: the error input is an observation, NOT an attempted
    // delete — only the two Ok inputs count in the ledger.
    let c = s.operation_counts();
    assert!(
        c.contains(&(StoreOp::Delete, ObjClass::Sst, 2)),
        "input errors never count as operations: {c:?}"
    );
}

/// Inner failures pass through unchanged (and are traced as such).
#[tokio::test]
async fn delete_stream_preserves_inner_failures() {
    let (sp, calls, _) = spy(vec![Ok(ObjPath::from("a/1.sst")), Err(delete_err())]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(
            futures_util::stream::iter(vec![
                Ok(ObjPath::from("a/1.sst")),
                Ok(ObjPath::from("a/2.sst")),
            ])
            .boxed(),
        )
        .collect()
        .await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(out.len(), 2);
    assert!(out[0].is_ok());
    assert!(out[1].is_err(), "the inner failure must surface unchanged");
}

/// Dropping the consumer mid-stream does not trigger extra delegated
/// calls — the one invocation happened at dispatch, and nothing
/// retries or replays behind the client's back.
#[tokio::test]
async fn delete_stream_dropped_consumer_triggers_no_extra_calls() {
    let (sp, calls, consumed) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
        Ok(ObjPath::from("a/3.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let mut stream = s.delete_stream(
        futures_util::stream::iter(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
            Ok(ObjPath::from("a/3.sst")),
        ])
        .boxed(),
    );
    let first = stream.next().await;
    assert!(matches!(first, Some(Ok(_))));
    drop(stream);
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "no replay on drop"
    );
    assert!(
        consumed.load(std::sync::atomic::Ordering::Relaxed) < 3,
        "the abandoned input stream stops being consumed"
    );
}

/// reset() refuses to run while an operation is in flight — the
/// alternative is silent misattribution of the late completion.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_in_flight() {
    use futures_util::StreamExt;
    let s = TraceStore::new(mem());
    let mut stream = s.list(Some(&ObjPath::from("anything")));
    drop(stream.next());
    // The list event is still Pending (stream abandoned but not dropped),
    // so in_flight > 0 and reset must refuse.
    let _ = stream;
    // Force the guard NOT to run: leak the stream.
    std::mem::forget(stream);
    s.reset();
}

/// After completions land, reset is clean and the id space keeps
/// moving: a later event cannot be mistaken for an earlier one.
#[tokio::test]
async fn reset_after_completion_is_clean_and_ids_stay_monotonic() {
    let s = TraceStore::new(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    s.reset();
    assert!(s.events().is_empty());
    s.put_opts(&p, PutPayload::from(vec![2u8; 4]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert!(
        evs[0].seq > 0,
        "ids are monotonic across reset, not re-based: {:?}",
        evs[0].seq
    );
}

/// A list stream the consumer abandons is recorded Cancelled (and
/// its lifetime retired, so reset still works) — not left Pending
/// forever and not silently treated as successful.
#[tokio::test]
async fn abandoned_list_stream_is_marked_cancelled() {
    let s = TraceStore::new(mem());
    {
        let _stream = s.list(Some(&ObjPath::from("nothing-here")));
    }
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].op, StoreOp::List);
    assert_eq!(evs[0].outcome, TraceOutcome::Cancelled, "{evs:?}");
    // The lifetime was retired: reset works.
    s.reset();
    assert!(s.events().is_empty());
}

// ---- PR 3.2: one lock owns id/event/lifetime association ----------

/// Acceptance 1-3: N tasks race `begin` through one barrier, then
/// completions land in REVERSE id order. Every event must resolve
/// exactly once — completion locates events by id through the log's
/// map, never by arithmetic on insertion position. Under the
/// pre-3.2 shape (id allocated with an atomic BEFORE the vector
/// lock) this interleaving could push events out of id order,
/// completions then found the wrong slot, both events stayed
/// Pending, and `reset()` panicked forever.
#[test]
fn concurrent_begins_with_reverse_finishes_resolve_every_event_exactly_once() {
    let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
    const N: usize = 8;
    for round in 0..50 {
        let barrier = std::sync::Barrier::new(N);
        let mut ops: Vec<super::TraceOperation> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let st = s.st.clone();
                    let b = &barrier;
                    scope.spawn(move || {
                        b.wait(); // all N contend for the trace lock at once
                        st.begin_operation(StoreOp::Put, "shards/x/wal/c.sst", None, None, None)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut ids: Vec<u64> = ops.iter().map(|o| o.seq.unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), N, "round {round}: ids must be unique");
        // Finish in REVERSE id order.
        ops.sort_by_key(|o| o.seq.unwrap());
        for op in ops.into_iter().rev() {
            op.finish(TraceOutcome::Ok);
        }
        let evs = s.events();
        assert_eq!(evs.len(), N, "round {round}");
        for w in evs.windows(2) {
            assert!(
                w[0].seq < w[1].seq,
                "round {round}: vector must be in id order: {evs:?}"
            );
        }
        assert!(
            evs.iter().all(|e| e.outcome == TraceOutcome::Ok),
            "round {round}: every event resolves exactly once, none stay Pending: {evs:?}"
        );
        s.reset(); // acceptance 9: nothing leaked a lifetime
        assert!(s.events().is_empty());
    }
}

/// Acceptance 4: reset racing `begin` can neither clear an active
/// operation out from under its completion nor orphan one. All
/// interleavings serialize on the one trace lock: reset either wins
/// (clears a quiet window; the op then lands in the fresh window) or
/// observes the active lifetime and refuses. After joining, nothing
/// is active and nothing is Pending — deterministically, in every
/// round.
#[test]
fn reset_racing_with_begin_cannot_orphan_an_operation() {
    let s = TraceStore::verbatim(Arc::new(object_store::memory::InMemory::new()));
    for round in 0..200 {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let st = s.st.clone();
            let b = &barrier;
            scope.spawn(move || {
                b.wait();
                let op = st.begin_operation(StoreOp::Put, "shards/x/wal/r.sst", None, None, None);
                std::thread::yield_now();
                op.finish(TraceOutcome::Ok);
            });
            barrier.wait();
            // A refused reset is a legal outcome of the race; a
            // poisoned lock or a lost event is not.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.reset()));
        });
        let log = s.st.log.lock().unwrap();
        assert!(log.active.is_empty(), "round {round}: orphaned lifetime");
        assert!(
            log.events
                .iter()
                .all(|e| e.outcome != TraceOutcome::Pending),
            "round {round}: a completed operation may never stay Pending"
        );
        drop(log);
        s.reset(); // at quiescence reset must always work
    }
}

// ---- PR 3.2: stream lifetime is distinct from observed outcome ----

/// A store whose list yields a scripted error FIRST, then delegates
/// to the real inner listing — the "stream keeps serving after an
/// error" case the ObjectStore trait explicitly allows.
#[derive(Debug)]
struct ListSpy {
    inner: Arc<object_store::memory::InMemory>,
}

impl std::fmt::Display for ListSpy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListSpy({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ListSpy {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        futures_util::stream::iter(vec![Err(object_store::Error::Generic {
            store: "list-spy",
            source: "scripted list failure".into(),
        })])
        .chain(self.inner.list(prefix))
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }
}

async fn list_spy_with_one_object() -> Arc<ListSpy> {
    let inner = Arc::new(object_store::memory::InMemory::new());
    inner
        .put_opts(
            &ObjPath::from("a/1.sst"),
            PutPayload::from(vec![1u8; 4]),
            PutOptions::default(),
        )
        .await
        .unwrap();
    Arc::new(ListSpy { inner })
}

/// Acceptance 5-6 / required test 1: a list stream that yielded an
/// error is still ALIVE, so reset must refuse. Pre-3.2 the item
/// error retired the operation and the documented "refuses while
/// anything is in flight" claim was false for exactly this case.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_an_errored_list_stream_is_still_open() {
    use futures_util::StreamExt;
    let spy = list_spy_with_one_object().await;
    let s = TraceStore::verbatim(spy);
    let mut stream = s.list(Some(&ObjPath::from("a")));
    let first = stream.next().await;
    assert!(matches!(first, Some(Err(_))), "scripted error first");
    // The stream can keep serving items; its lifetime is open.
    s.reset();
}

/// Required test 2: a list that errors, then serves another item,
/// then completes — the first fact (the error) is the recorded
/// outcome, exhaustion retires the lifetime exactly once, and reset
/// then works.
#[tokio::test]
async fn errored_list_stream_serves_on_and_retires_at_exhaustion() {
    use futures_util::StreamExt;
    let spy = list_spy_with_one_object().await;
    let s = TraceStore::verbatim(spy);
    let out: Vec<_> = s.list(Some(&ObjPath::from("a"))).collect().await;
    assert_eq!(out.len(), 2);
    assert!(out[0].is_err() && out[1].is_ok(), "error then live item");
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(
        evs[0].outcome,
        TraceOutcome::Error,
        "first fact wins; exhaustion does not overwrite it: {evs:?}"
    );
    s.reset(); // exhaustion retired the lifetime
    assert!(s.events().is_empty());
}

/// Required test 3: an unconsumed delete stream is an ACTIVE
/// operation — reset must refuse while it is alive. Pre-3.2 delete
/// streams had no lifetime at all and reset would clear the trace
/// under a stream still appending observations.
#[tokio::test]
#[should_panic(expected = "while operations are active")]
async fn reset_refuses_while_a_delete_stream_is_active() {
    let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let _stream =
        s.delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed());
    s.reset();
}

/// Required test 4: dropping a delete stream midway retires its
/// lifetime exactly once — reset works afterwards, repeatedly.
#[tokio::test]
async fn dropped_delete_stream_retires_exactly_once() {
    let (sp, _, _) = spy(vec![
        Ok(ObjPath::from("a/1.sst")),
        Ok(ObjPath::from("a/2.sst")),
    ]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let mut stream = s.delete_stream(
        futures_util::stream::iter(vec![
            Ok(ObjPath::from("a/1.sst")),
            Ok(ObjPath::from("a/2.sst")),
        ])
        .boxed(),
    );
    let first = stream.next().await;
    assert!(matches!(first, Some(Ok(_))));
    drop(stream);
    s.reset();
    assert!(s.events().is_empty());
    s.reset(); // idempotent at quiescence: nothing double-retired
}

/// Acceptance 8 (the double-count finding): one ordinary successful
/// delete = ONE attempted operation in the ledger, even though the
/// diagnostic trace holds both the input and the result observation.
#[tokio::test]
async fn ordinary_delete_counts_once_in_the_operation_ledger() {
    let (sp, _, _) = spy(vec![Ok(ObjPath::from("a/1.sst"))]);
    let s = TraceStore::verbatim(sp);
    use futures_util::StreamExt;
    let out: Vec<_> = s
        .delete_stream(futures_util::stream::iter(vec![Ok(ObjPath::from("a/1.sst"))]).boxed())
        .collect()
        .await;
    assert_eq!(out.len(), 1);
    let evs = s.events();
    assert_eq!(evs.len(), 2, "input + result observations: {evs:?}");
    assert_eq!(evs[0].kind, TraceEventKind::DeleteInput);
    assert_eq!(evs[1].kind, TraceEventKind::DeleteResult);
    let c = s.operation_counts();
    assert_eq!(
        c,
        vec![(StoreOp::Delete, ObjClass::Sst, 1)],
        "one delete attempted, not two: {c:?}"
    );
}

// ---- PR 3.2.1: async cancellation is RAII-safe ------------------------

/// A store whose async operations never resolve — the fixture for
/// polling a traced future to `Pending` and then dropping it.
#[derive(Debug)]
struct PendingStore;

impl std::fmt::Display for PendingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PendingStore")
    }
}

#[derive(Debug)]
struct PendingUpload;

#[async_trait::async_trait]
impl MultipartUpload for PendingUpload {
    fn put_part(&mut self, _data: PutPayload) -> object_store::UploadPart {
        Box::pin(std::future::pending())
    }
    async fn complete(&mut self) -> OsResult<PutResult> {
        std::future::pending().await
    }
    async fn abort(&mut self) -> OsResult<()> {
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl ObjectStore for PendingStore {
    async fn put_opts(
        &self,
        _location: &ObjPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        std::future::pending().await
    }
    async fn put_multipart_opts(
        &self,
        _location: &ObjPath,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(PendingUpload))
    }
    async fn get_opts(&self, _location: &ObjPath, _options: GetOptions) -> OsResult<GetResult> {
        std::future::pending().await
    }
    fn list(
        &self,
        _prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        futures_util::stream::pending().boxed()
    }
    async fn list_with_delimiter(&self, _prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        std::future::pending().await
    }
    async fn copy_opts(&self, _from: &ObjPath, _to: &ObjPath, _opts: CopyOptions) -> OsResult<()> {
        std::future::pending().await
    }
    fn delete_stream(
        &self,
        _locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        futures_util::stream::pending().boxed()
    }
}

/// Required cancellation test 1: a point-operation future polled to
/// `Pending` and then dropped records `Cancelled`, leaves no active
/// entry, and reset succeeds. Pre-3.2.1 the plain id never reached
/// `finish()` and the trace was poisoned forever.
#[tokio::test]
async fn dropping_a_pending_point_operation_records_cancelled() {
    use futures_util::FutureExt;
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/1.sst");
    // now_or_never polls exactly once, observes Pending, and DROPS the
    // future — the async-cancellation shape.
    assert!(
        s.get_opts(&p, GetOptions::default())
            .now_or_never()
            .is_none()
    );
    assert!(
        s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
            .now_or_never()
            .is_none()
    );
    assert!(s.list_with_delimiter(Some(&p)).now_or_never().is_none());
    assert!(
        s.copy_opts(&p, &ObjPath::from("a/2.sst"), CopyOptions::default())
            .now_or_never()
            .is_none()
    );
    let evs = s.events();
    assert_eq!(evs.len(), 4, "{evs:?}");
    assert!(
        evs.iter().all(|e| e.outcome == TraceOutcome::Cancelled),
        "every cancelled operation is recorded Cancelled, never left Pending: {evs:?}"
    );
    s.reset(); // acceptance 5: reset succeeds after every cancellation
    assert!(s.events().is_empty());
}

/// Required cancellation tests 2+3: a multipart part future dropped
/// UNPOLLED (the guard was captured at dispatch) and a multipart
/// complete polled to `Pending` then dropped both record `Cancelled`;
/// reset works afterwards.
#[tokio::test]
async fn dropping_multipart_futures_records_cancelled() {
    use futures_util::FutureExt;
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/big.sst");
    let mut up = s
        .put_multipart_opts(&p, PutMultipartOptions::default())
        .await
        .unwrap();
    // Unpolled: put_part begins at dispatch, so the guard exists inside
    // the returned future even before the first poll.
    let fut = up.put_part(PutPayload::from(vec![1u8; 8]));
    drop(fut);
    // Polled to Pending, then dropped.
    assert!(
        up.put_part(PutPayload::from(vec![2u8; 8]))
            .now_or_never()
            .is_none()
    );
    assert!(up.complete().now_or_never().is_none());
    let evs = s.events();
    assert_eq!(evs.len(), 4, "open + two parts + complete: {evs:?}");
    assert_eq!(evs[0].detail.as_deref(), Some("multipart-open"));
    assert_eq!(evs[0].outcome, TraceOutcome::Ok);
    assert!(
        evs[1..]
            .iter()
            .all(|e| e.outcome == TraceOutcome::Cancelled),
        "{evs:?}"
    );
    s.reset();
    assert!(s.events().is_empty());
}

/// Required cancellation test 7 (acceptance 6): normal completion
/// retires exactly once — the guard's later drop cannot overwrite a
/// recorded outcome with Cancelled.
#[tokio::test]
async fn completed_operation_is_never_overwritten_by_cancellation() {
    let s = TraceStore::verbatim(mem());
    let p = ObjPath::from("shards/x/wal/1.sst");
    s.put_opts(&p, PutPayload::from(vec![1u8; 4]), PutOptions::default())
        .await
        .unwrap();
    let evs = s.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].outcome, TraceOutcome::Ok, "{evs:?}");
    s.reset();
    assert!(s.events().is_empty());
}

/// An UNPOLLED point-operation future records nothing at all: with
/// async_trait the body (including `begin_operation`) runs on first
/// poll, so a never-started operation is absent from the trace — the
/// trace records operations that STARTED. Reset stays clean.
#[tokio::test]
async fn unpolled_point_operation_records_nothing() {
    let s = TraceStore::verbatim(Arc::new(PendingStore));
    let p = ObjPath::from("a/1.sst");
    let fut = s.get_opts(&p, GetOptions::default());
    drop(fut);
    assert!(s.events().is_empty(), "never polled => never dispatched");
    s.reset();
}
