//! The tracing `ObjectStore` decorator for refactor comparisons
//! (WP-00 deliverable 7; made a trustworthy oracle in PR 3.2/3.2.1).
//! Split out of the dst catch-all (PR 3.2.1).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use super::{ObjClass, StoreOp};

#[cfg(test)]
#[path = "trace_store_tests.rs"]
mod trace_store_tests;

// ---- the tracing store -----------------------------------------------

/// Outcome of one traced operation, coarse enough that traces stay
/// comparable across runs and stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOutcome {
    /// Dispatched but not yet resolved — only ever visible in a snapshot
    /// taken while the operation is in flight.
    Pending,
    Ok,
    NotFound,
    AlreadyExists,
    Precondition,
    NotModified,
    NotSupported,
    NotImplemented,
    InvalidPath,
    /// The consumer abandoned a streaming operation before it finished
    /// (dropped the list stream mid-page). Recorded, not hidden.
    Cancelled,
    /// Everything else (Generic, JoinError, …): the trace records THAT it
    /// failed, not the store's prose.
    Error,
}

impl TraceOutcome {
    fn of(e: &object_store::Error) -> Self {
        use object_store::Error as E;
        match e {
            E::NotFound { .. } => Self::NotFound,
            E::AlreadyExists { .. } => Self::AlreadyExists,
            E::Precondition { .. } => Self::Precondition,
            E::NotModified { .. } => Self::NotModified,
            E::NotSupported { .. } => Self::NotSupported,
            E::NotImplemented { .. } => Self::NotImplemented,
            E::InvalidPath { .. } => Self::InvalidPath,
            _ => Self::Error,
        }
    }
}

/// What one trace entry MEANS — the discriminator `operation_counts()`
/// keys on (PR 3.2: phases were previously hidden in `detail` strings,
/// which let a diagnostic observation masquerade as an attempted store
/// operation and double-count deletes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceEventKind {
    /// One attempted store operation (put/get/list/copy/multipart leg).
    Operation,
    /// Diagnostic observation: one input item a traced delete stream
    /// consumed. An `Ok` input IS an attempted delete (the inner store
    /// received it); an `Err` input is an observation only — the store
    /// never saw a path.
    DeleteInput,
    /// Diagnostic observation: one item the inner delete stream
    /// returned. Never counted as an operation — the trait does not
    /// promise results correspond to inputs (batching stores coalesce).
    DeleteResult,
}

/// One trace entry, in trace-lock acquisition order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreTraceEvent {
    /// Monotonic event id, unique for the life of the `TraceStore` and
    /// allocated under the same lock that inserts the event — so the
    /// vector order IS id order. NOT dense: `reset()` keeps the id
    /// space moving, and stream lifetime tokens consume ids without
    /// producing events.
    pub seq: u64,
    pub kind: TraceEventKind,
    pub op: StoreOp,
    pub class: ObjClass,
    /// The path as recorded: verbatim only when the store was built with
    /// `verbatim`, segment-redacted otherwise. Credentials and headers
    /// are never recorded — the `ObjectStore` interface never exposes
    /// them to a decorator.
    pub path: String,
    /// Payload length for puts, requested span for bounded/suffix ranged
    /// gets. `None` when not applicable or unknowable at dispatch
    /// (offset ranges, full gets, deletes, lists, copies).
    pub bytes: Option<u64>,
    pub outcome: TraceOutcome,
    /// First 16 hex chars of the payload sha256 — recorded ONLY when the
    /// store was built with `content_hashes: true`. Payload bytes
    /// themselves are never retained either way.
    pub content_hash: Option<String>,
    /// Free-form qualifier: copy destination, `head` marker, multipart
    /// markers (`multipart-open` / `part` / `complete parts=N` / `abort`).
    pub detail: Option<String>,
}

/// First 16 hex chars of sha256 — enough to distinguish payloads or path
/// segments in a trace without retaining them.
fn sha16(bytes: &[u8]) -> String {
    use sha2::Digest;
    let d = sha2::Sha256::digest(bytes);
    crate::crypto::hex(&d[..8])
}

/// Segment-preserving redaction. Tenant- and stream-derived segments are
/// replaced by a 16-hex-char sha256 prefix; the structural tokens the
/// `ObjClass` classifier keys on (`wal`, `compacted`, `fleet`, `routers`,
/// `shards`) survive verbatim, as does the `manifest` marker and the file
/// extension (`.sst` is itself a classification signal). The redacted
/// path therefore classifies identically to the original while leaking no
/// tenant material.
fn redact_path(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            if matches!(seg, "wal" | "compacted" | "fleet" | "routers" | "shards") {
                return seg.to_string();
            }
            let h = sha16(seg.as_bytes());
            let stem = if seg.contains("manifest") {
                format!("manifest-{h}")
            } else {
                h
            };
            match seg.rsplit_once('.') {
                Some((_, ext)) if !ext.is_empty() => format!("{stem}.{ext}"),
                _ => stem,
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Everything the trace correlates, behind ONE mutex (PR 3.2). The
/// previous shape allocated ids with relaxed atomics BEFORE taking the
/// vector lock, so two concurrent starts could insert out of id order
/// and completion — which derived a vector index from the id — would
/// silently resolve neither event: both stayed `Pending`, `in_flight`
/// never drained, and `reset()` panicked forever. Ids are now allocated
/// and inserted in the same critical section; completion looks events
/// up through an id→index map, never by arithmetic on insertion order.
#[derive(Debug, Default)]
struct TraceLog {
    /// Monotonic id source. Never rewinds — `reset()` clears the window
    /// but keeps the id space moving, so a stale completion can never
    /// land on a younger event.
    next_seq: u64,
    /// The current trace window, in id (= lock-acquisition) order.
    events: Vec<StoreTraceEvent>,
    /// id → index into `events`, for the CURRENT window only.
    // mt-lint: allow(name-keyed-map): keyed by trace event id
    index: HashMap<u64, usize>,
    /// Ids of operations whose LIFETIME is still open: point operations
    /// between begin and finish, streams between creation and
    /// exhaustion/drop. Lifetime is deliberately distinct from outcome —
    /// a list stream that has already yielded an error is still alive
    /// (the trait lets it keep serving items), and `reset()` refuses
    /// while ANY lifetime is open.
    active: std::collections::HashSet<u64>,
}

/// Shared trace state: the correlated log plus the two immutable
/// recording options.
#[derive(Debug)]
struct TraceState {
    log: Mutex<TraceLog>,
    redact: bool,
    content_hashes: bool,
}

impl TraceState {
    #[allow(clippy::too_many_arguments)]
    fn event(
        &self,
        seq: u64,
        kind: TraceEventKind,
        op: StoreOp,
        path: &str,
        bytes: Option<u64>,
        outcome: TraceOutcome,
        content_hash: Option<String>,
        detail: Option<String>,
    ) -> StoreTraceEvent {
        StoreTraceEvent {
            seq,
            kind,
            op,
            class: ObjClass::of(path),
            path: if self.redact {
                redact_path(path)
            } else {
                path.to_string()
            },
            bytes,
            outcome,
            content_hash,
            detail,
        }
    }

    /// Open one operation: allocate the id AND insert the Pending event
    /// in the same critical section, mark the lifetime active, and hand
    /// the caller the owning RAII guard (PR 3.2.1: a plain id could be
    /// abandoned by async cancellation between dispatch and completion,
    /// leaving a permanent Pending event and a stuck active entry that
    /// made `reset()` refuse forever).
    fn begin_operation(
        self: &Arc<Self>,
        op: StoreOp,
        path: &str,
        bytes: Option<u64>,
        content_hash: Option<String>,
        detail: Option<String>,
    ) -> TraceOperation {
        let ev = |seq| {
            self.event(
                seq,
                TraceEventKind::Operation,
                op,
                path,
                bytes,
                TraceOutcome::Pending,
                content_hash.clone(),
                detail.clone(),
            )
        };
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        let e = ev(seq);
        let at = log.events.len();
        log.events.push(e);
        log.index.insert(seq, at);
        log.active.insert(seq);
        drop(log);
        TraceOperation {
            st: self.clone(),
            seq: Some(seq),
        }
    }

    /// Open a lifetime WITHOUT an event of its own: a traced delete
    /// stream is observed through its per-item `DeleteInput` /
    /// `DeleteResult` entries, but its lifetime must still hold
    /// `reset()` off until the stream is exhausted or dropped.
    fn begin_lifetime(self: &Arc<Self>) -> TraceOperation {
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        log.active.insert(seq);
        drop(log);
        TraceOperation {
            st: self.clone(),
            seq: Some(seq),
        }
    }

    /// Push an already-resolved observation (delete-stream items are
    /// complete facts at the moment they pass through).
    fn observe(
        &self,
        kind: TraceEventKind,
        op: StoreOp,
        path: &str,
        detail: Option<String>,
        outcome: TraceOutcome,
    ) {
        let mut log = self.log.lock().unwrap();
        let seq = log.next_seq;
        log.next_seq += 1;
        let e = self.event(seq, kind, op, path, None, outcome, None, detail);
        let at = log.events.len();
        log.events.push(e);
        log.index.insert(seq, at);
    }

    /// Record an observed outcome WITHOUT retiring the lifetime — first
    /// fact wins. Streaming operations use this for item errors: the
    /// stream stays alive (and keeps blocking `reset()`) after it.
    fn note_outcome(&self, seq: u64, outcome: TraceOutcome) {
        let mut log = self.log.lock().unwrap();
        Self::note_locked(&mut log, seq, outcome);
    }

    fn note_locked(log: &mut TraceLog, seq: u64, outcome: TraceOutcome) {
        if let Some(&at) = log.index.get(&seq) {
            let e = &mut log.events[at];
            if e.outcome == TraceOutcome::Pending {
                e.outcome = outcome;
            }
        }
    }

    /// Resolve outcome (first fact wins) and retire the lifetime. Only
    /// [`TraceOperation`] calls this — single ownership plus the
    /// `Option` take make retirement exactly-once.
    fn finish(&self, seq: u64, outcome: TraceOutcome) {
        let mut log = self.log.lock().unwrap();
        log.active.remove(&seq);
        Self::note_locked(&mut log, seq, outcome);
    }

    fn hash_payload(&self, payload: &PutPayload) -> Option<String> {
        if !self.content_hashes {
            return None;
        }
        use sha2::Digest;
        let mut h = sha2::Sha256::new();
        for b in payload.iter() {
            h.update(b);
        }
        Some(crate::crypto::hex(&h.finalize()[..8]))
    }
}

/// Owns ONE active trace operation (PR 3.2.1). Every `begin_*` returns
/// this guard, and whoever holds it holds the operation's lifetime:
///
/// * `finish(outcome)` consumes the guard and records the real outcome
///   exactly once;
/// * `note(outcome)` records an observed fact WITHOUT retiring (stream
///   item errors — the stream stays alive and keeps blocking reset);
/// * dropping the guard retires the operation with `Cancelled` as the
///   fallback — an async operation cancelled between dispatch and
///   completion (its future dropped mid-await) can therefore never
///   leave a permanent `Pending` event or a stuck active entry.
///
/// First fact wins throughout: `finish` after `note`, or the drop path
/// after either, cannot overwrite a recorded outcome.
struct TraceOperation {
    st: Arc<TraceState>,
    seq: Option<u64>,
}

impl TraceOperation {
    /// Record an observed outcome without retiring the lifetime.
    fn note(&self, outcome: TraceOutcome) {
        if let Some(seq) = self.seq {
            self.st.note_outcome(seq, outcome);
        }
    }

    /// Retire with the real outcome, exactly once.
    fn finish(mut self, outcome: TraceOutcome) {
        if let Some(seq) = self.seq.take() {
            self.st.finish(seq, outcome);
        }
    }

    fn finish_with<T>(self, res: &OsResult<T>) {
        self.finish(match res {
            Ok(_) => TraceOutcome::Ok,
            Err(e) => TraceOutcome::of(e),
        });
    }
}

impl Drop for TraceOperation {
    fn drop(&mut self) {
        if let Some(seq) = self.seq.take() {
            self.st.finish(seq, TraceOutcome::Cancelled);
        }
    }
}

/// Tracing `ObjectStore` decorator for refactor comparisons.
///
/// Records every operation the client dispatches, so a refactor PR can
/// assert the exact object-store operation trace is unchanged by code
/// movement. Behavior is pass-through: nothing is delayed, altered, or
/// dropped.
///
/// *Ordering contract (PR 3.2).* The trace order is the order in which
/// operations acquired the trace lock at dispatch: id allocation and
/// event insertion happen in one critical section, so the event vector
/// is always in id order. Under a single client that is exactly the
/// call order; under concurrent clients it is a legal serialization of
/// their dispatches (which is the strongest order that exists for
/// concurrent starts). Completion order is not recorded — outcomes are
/// filled in afterwards BY ID through the log's id→index map, and a
/// snapshot taken mid-flight shows `TraceOutcome::Pending`.
#[derive(Debug)]
pub struct TraceStore {
    inner: Arc<dyn ObjectStore>,
    st: Arc<TraceState>,
}

impl std::fmt::Display for TraceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TraceStore({})", self.inner)
    }
}

impl TraceStore {
    /// Safe default: paths redacted, payload hashes off.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::with_options(inner, true, false)
    }

    /// Verbatim paths. Only for fixtures whose paths carry no tenant or
    /// stream material — redaction is the default precisely because real
    /// paths embed both.
    pub fn verbatim(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::with_options(inner, false, false)
    }

    /// Explicit knobs. `content_hashes` opts into retaining a 16-hex-char
    /// sha256 prefix of each put payload; payload bytes themselves are
    /// never retained either way.
    pub fn with_options(
        inner: Arc<dyn ObjectStore>,
        redact: bool,
        content_hashes: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            st: Arc::new(TraceState {
                log: Mutex::new(TraceLog::default()),
                redact,
                content_hashes,
            }),
        })
    }

    /// Snapshot of every trace entry so far — operations AND the
    /// diagnostic delete observations — in id order. This is the
    /// observation report; `operation_counts()` is the operation ledger.
    pub fn events(&self) -> Vec<StoreTraceEvent> {
        self.st.log.lock().unwrap().events.clone()
    }

    /// Drop everything recorded so far. Refuses (panics) while any
    /// operation's LIFETIME is still open — including a list stream
    /// that already yielded an error but has not been exhausted or
    /// dropped, and a delete stream that is still alive. Silently
    /// clearing under an open lifetime would let a late fact outlive
    /// the wipe and corrupt the comparison this type exists to make.
    pub fn reset(&self) {
        let mut log = self.st.log.lock().unwrap();
        if !log.active.is_empty() {
            let n = log.active.len();
            // Release the lock BEFORE panicking: a poisoned trace mutex
            // would turn one refused reset into a wedged harness.
            drop(log);
            panic!("cannot reset TraceStore while operations are active ({n} open)");
        }
        log.events.clear();
        log.index.clear();
    }

    /// (op, class) → attempted-operation count, sorted — the same shape
    /// FaultStore's ledger answers, for whole-trace budget assertions.
    ///
    /// The delete accounting is pinned (PR 3.2): an attempted delete is
    /// each `Ok` INPUT the inner store consumed. Returned results are
    /// never counted (the trait lets stores batch, reorder, coalesce or
    /// drop results), and an input ERROR is never counted (the store
    /// was not handed a path). A cost baseline therefore answers "how
    /// many store operations were attempted", not "how many diagnostic
    /// observations happened" — use `events()` for the latter.
    pub fn operation_counts(&self) -> Vec<(StoreOp, ObjClass, u64)> {
        let mut m: HashMap<(StoreOp, ObjClass), u64> = HashMap::new();
        for e in self.st.log.lock().unwrap().events.iter() {
            let attempted = match e.kind {
                TraceEventKind::Operation => true,
                TraceEventKind::DeleteInput => e.outcome == TraceOutcome::Ok,
                TraceEventKind::DeleteResult => false,
            };
            if attempted {
                *m.entry((e.op, e.class)).or_insert(0) += 1;
            }
        }
        let mut v: Vec<_> = m
            .into_iter()
            .map(|((op, class), n)| (op, class, n))
            .collect();
        v.sort_by_key(|(op, class, _)| (*op as u8, *class as u8));
        v
    }
}

#[async_trait::async_trait]
impl ObjectStore for TraceStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let op = self.st.begin_operation(
            StoreOp::Put,
            location.as_ref(),
            Some(payload.content_length() as u64),
            self.st.hash_payload(&payload),
            None,
        );
        let res = self.inner.put_opts(location, payload, opts).await;
        op.finish_with(&res);
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let op = self.st.begin_operation(
            StoreOp::Put,
            location.as_ref(),
            None,
            None,
            Some("multipart-open".to_string()),
        );
        let res = self.inner.put_multipart_opts(location, opts).await;
        op.finish_with(&res);
        res.map(|up| {
            Box::new(TracedMultipart {
                inner: up,
                st: self.st.clone(),
                path: location.as_ref().to_string(),
                parts: 0,
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        // Span of a ranged read, when the request itself fixes one. An
        // `Offset` span depends on the object size, unknowable at
        // dispatch. HEAD arrives as a Get (`ObjectStoreExt::head` is built
        // on get_opts), marked in `detail` so traces stay comparable.
        let bytes = options.range.as_ref().and_then(|r| match r {
            object_store::GetRange::Bounded(b) => Some(b.end.saturating_sub(b.start)),
            object_store::GetRange::Suffix(n) => Some(*n),
            object_store::GetRange::Offset(_) => None,
        });
        let detail = options.head.then(|| "head".to_string());
        let op = self
            .st
            .begin_operation(StoreOp::Get, location.as_ref(), bytes, None, detail);
        let res = self.inner.get_opts(location, options).await;
        op.finish_with(&res);
        res
    }

    /// Streaming list. The event is recorded at dispatch; the stream
    /// itself is pass-through. Outcome and LIFETIME are deliberately
    /// separate (PR 3.2): an item error records the outcome (first fact
    /// wins) but the stream stays ALIVE — the trait lets it serve more
    /// items — so `reset()` keeps refusing until clean exhaustion or
    /// drop retires the lifetime exactly once. Clean exhaustion upgrades
    /// a never-failed stream to Ok; a stream abandoned before any fact
    /// records Cancelled.
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        use futures_util::StreamExt;
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let guard = self.st.begin_operation(StoreOp::List, &p, None, None, None);
        let inner = self.inner.list(prefix);
        futures_util::stream::unfold((inner, guard), |(mut inner, guard)| async move {
            match inner.next().await {
                Some(Ok(m)) => Some((Ok(m), (inner, guard))),
                Some(Err(e)) => {
                    // Outcome only: the stream can legally continue.
                    guard.note(TraceOutcome::of(&e));
                    Some((Err(e), (inner, guard)))
                }
                None => {
                    guard.finish(TraceOutcome::Ok);
                    None
                }
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        let p = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let op = self.st.begin_operation(
            StoreOp::List,
            &p,
            None,
            None,
            Some("with-delimiter".to_string()),
        );
        let res = self.inner.list_with_delimiter(prefix).await;
        op.finish_with(&res);
        res
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        let dst = if self.st.redact {
            redact_path(to.as_ref())
        } else {
            to.as_ref().to_string()
        };
        let op = self.st.begin_operation(
            StoreOp::Copy,
            from.as_ref(),
            None,
            None,
            Some(format!("to={dst}")),
        );
        let res = self.inner.copy_opts(from, to, opts).await;
        op.finish_with(&res);
        res
    }

    /// Deletes are traced WITHOUT changing the call shape: exactly one
    /// delegated `delete_stream`, with input items and output items
    /// recorded as typed observations (`DeleteInput` / `DeleteResult`)
    /// as they pass through. The trait does not promise that results
    /// correspond to inputs (an implementation may batch, reorder,
    /// coalesce, or drop results), so NO outcome is ever fabricated: an
    /// empty output stays empty, input errors and inner failures pass
    /// through untouched. The stream also carries a LIFETIME token
    /// (PR 3.2): while it is alive — consuming inputs, producing
    /// outputs — `reset()` refuses; exhaustion or drop retires the
    /// lifetime exactly once.
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        use futures_util::StreamExt;
        let guard = self.st.begin_lifetime();
        let st_in = self.st.clone();
        let traced_in = locations.map(move |loc| {
            match &loc {
                Ok(p) => st_in.observe(
                    TraceEventKind::DeleteInput,
                    StoreOp::Delete,
                    p.as_ref(),
                    None,
                    TraceOutcome::Ok,
                ),
                Err(e) => st_in.observe(
                    TraceEventKind::DeleteInput,
                    StoreOp::Delete,
                    "",
                    None,
                    TraceOutcome::of(e),
                ),
            }
            loc
        });
        let out = self.inner.delete_stream(Box::pin(traced_in));
        let st_out = self.st.clone();
        futures_util::stream::unfold((out, guard), move |(mut out, guard)| {
            let st_out = st_out.clone();
            async move {
                match out.next().await {
                    Some(res) => {
                        match &res {
                            Ok(p) => st_out.observe(
                                TraceEventKind::DeleteResult,
                                StoreOp::Delete,
                                p.as_ref(),
                                None,
                                TraceOutcome::Ok,
                            ),
                            Err(e) => st_out.observe(
                                TraceEventKind::DeleteResult,
                                StoreOp::Delete,
                                "",
                                None,
                                TraceOutcome::of(e),
                            ),
                        }
                        Some((res, (out, guard)))
                    }
                    None => {
                        // Clean exhaustion retires the lifetime; the
                        // consumed guard cannot retire again.
                        guard.finish(TraceOutcome::Ok);
                        None
                    }
                }
            }
        })
        .boxed()
    }
}

/// Multipart session wrapper. `StoreOp` has no part variant, so the whole
/// session traces as `Put` events distinguished by `detail`:
/// `multipart-open` at creation (bytes=None), `part` per part (with the
/// part's byte count), `complete parts=N`, `abort`.
#[derive(Debug)]
struct TracedMultipart {
    inner: Box<dyn MultipartUpload>,
    st: Arc<TraceState>,
    /// Raw path, for classification and redaction on each event.
    path: String,
    parts: u64,
}

#[async_trait::async_trait]
impl MultipartUpload for TracedMultipart {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        // Begin at dispatch (put_part is synchronous), then the GUARD
        // rides inside the returned future: dropping that future —
        // polled or not — retires the part as Cancelled (PR 3.2.1).
        let op = self.st.begin_operation(
            StoreOp::Put,
            &self.path,
            Some(data.content_length() as u64),
            self.st.hash_payload(&data),
            Some("part".to_string()),
        );
        self.parts += 1;
        let fut = self.inner.put_part(data);
        Box::pin(async move {
            let res = fut.await;
            op.finish_with(&res);
            res
        })
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let op = self.st.begin_operation(
            StoreOp::Put,
            &self.path,
            None,
            None,
            Some(format!("complete parts={}", self.parts)),
        );
        let res = self.inner.complete().await;
        op.finish_with(&res);
        res
    }

    async fn abort(&mut self) -> OsResult<()> {
        let op = self.st.begin_operation(
            StoreOp::Put,
            &self.path,
            None,
            None,
            Some("abort".to_string()),
        );
        let res = self.inner.abort().await;
        op.finish_with(&res);
        res
    }
}
