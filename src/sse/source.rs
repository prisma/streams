//! Feed sources (LIVE-FEED Stage 3+6).
//!
//! `SingleSource` reads one live segment of a single-segment stream
//! (and forks, via the stitched chain). `LineageSource` (Stage 6)
//! reads one SELECTOR LANE across a materialized segment map: the
//! lane's segments chained over their sealed caps into one linearized
//! cursor space — the same space the feed's head/floor/ring already
//! use, translated back to `(seg_id, segment-local)` by `locate()`
//! for the wire.

use super::feed::{
    CursorCapability, FeedSourceRead, SourceBatch, SourceCutoff, SourceTransition, WirePosition,
    sig_compatible,
};
use crate::registry::StreamDesc;
use crate::shard::{ShardEngine, StreamHandle};
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct SingleSource {
    pub(crate) state: Arc<crate::http::AppState>,
    pub(crate) rk_filter: Option<String>,
    pub(crate) desc: StreamDesc,
    pub(crate) key: crate::crypto::StreamKey,
    pub(crate) epoch: [u8; 16],
    /// The live segment's shard route (round-11.2: every read
    /// confirms this instance still owns it — a moved tail is a typed
    /// WrongOwner cutoff, never stale local serving).
    pub(crate) route: [u8; 16],
    pub(crate) engine: Arc<ShardEngine>,
    pub(crate) handle: Arc<StreamHandle>,
}

impl SingleSource {
    fn lane_seg_id(&self) -> u32 {
        self.desc
            .resolve_segment(self.rk_filter.as_deref().unwrap_or(""))
            .seg_id
    }
}

#[async_trait::async_trait]
impl FeedSourceRead for SingleSource {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch> {
        // Round-11.2: a moved live tail is a typed cutoff, never a
        // stale local read.
        if !owned_here(&self.state, &self.route) {
            return Err(anyhow::Error::new(FatalSpanCutoff(
                super::feed::SourceCutoff::WrongOwner,
            )));
        }
        // FORKS: stitched reads traverse the ancestor chain and return
        // records in the CHILD's logical offset space — the same cursor
        // space every other lane uses.
        let out = if self.desc.forked_from.is_some() {
            crate::http::read_stitched(&self.state, &self.desc, &self.key, from, max_bytes)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        } else {
            crate::http::read_records(
                &self.state,
                &self.desc,
                &self.key,
                &self.epoch,
                &self.handle,
                &self.engine,
                from,
                self.rk_filter.as_deref(),
                max_bytes,
                crate::shard::Deliver::Durable,
            )
            .await
            .map_err(|e| anyhow::anyhow!(e))?
        };
        // HONEST scanned progress (finding 2): `last` advances over
        // NON-MATCHING ranges for filtered lanes — it is the consumed
        // boundary even when zero records matched. `completed` tells
        // the driver whether this page reached the durable frontier; a
        // partial page with no scanned progress is NOT a successful
        // drive (finding 6).
        let scan_to = out.last.map(|x| x + 1).unwrap_or(from);
        Ok(SourceBatch {
            scan_from: from,
            scan_to,
            records: out.recs,
            completed: out.completed,
        })
    }

    fn frontier(&self) -> u64 {
        self.handle.state.lock().unwrap().durable.next
    }

    fn closed(&self) -> bool {
        self.handle.state.lock().unwrap().durable.closed
    }

    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes {
        Bytes::from(crate::sse::wire::sse_data_event(&self.desc, &rec.payload))
    }

    fn advance_notify(&self) -> &tokio::sync::Notify {
        &self.handle.notify
    }

    fn locate(&self, logical_after: u64) -> WirePosition {
        WirePosition {
            seg_id: self.lane_seg_id(),
            local_after: logical_after,
        }
    }

    fn logicalize(&self, pos: WirePosition) -> Option<u64> {
        if pos.seg_id != self.lane_seg_id() || pos.local_after > self.frontier() {
            return None;
        }
        Some(pos.local_after)
    }

    fn cursor_capability(&self) -> CursorCapability {
        CursorCapability::Scalar
    }

    fn span_sig(&self) -> Vec<(u32, u64, Option<u64>)> {
        vec![(self.lane_seg_id(), 0, None)]
    }

    async fn next_source(&self) -> anyhow::Result<SourceTransition> {
        refresh_transition(
            &self.state,
            &self.desc,
            &self.key,
            &self.epoch,
            &self.rk_filter,
            &self.span_sig(),
        )
        .await
    }
}

/// One lineage span: a segment of this lane, chained into the
/// linearized cursor space at `logical_start`, bounded by its sealed
/// cap (`None` while live — only the LAST span may be live).
struct LineageSpan {
    seg_id: u32,
    logical_start: u64,
    cap: Option<u64>,
    #[allow(dead_code)] // identity is informational (engine/handle keying)
    identity: [u8; 16],
    reader: SpanReader,
}

/// HOW a span's records are read (round-10 two-instance model):
/// metadata (`seg_id`/`logical_start`/`cap`) answers `locate`/
/// `logicalize` without opening anything; only reads need a reader.
enum SpanReader {
    /// A SEALED span, OWNERSHIP-DYNAMIC (round-11.2): every page
    /// resolves the CURRENT effective owner first — local when this
    /// instance owns the shard (reader cached), remote otherwise via
    /// the typed one-redirect protocol. `owner_hint` remembers the
    /// last owner that served a page; a successful redirect updates
    /// it. This also covers a predecessor that was local at build
    /// time and later moved away — and one that moved TO this
    /// instance.
    Sealed {
        route: [u8; 16],
        target: crate::product::InternalTarget,
        owner_hint: std::sync::RwLock<Option<String>>,
        local: tokio::sync::Mutex<Option<(Arc<ShardEngine>, Arc<StreamHandle>)>>,
    },
    /// The LIVE tail: LOCAL ONLY (locked architecture). Every read
    /// confirms this instance is still the effective owner; a moved
    /// tail is a typed WrongOwner cutoff (resumable EOF, gateway
    /// reroutes), never remote waiting and never stale local serving.
    LiveLocal {
        route: [u8; 16],
        engine: Arc<ShardEngine>,
        handle: Arc<StreamHandle>,
    },
}

/// Round-11.2: a FATAL span error carried through anyhow — the feed
/// downcasts it and turns the source's lifecycle into the typed
/// cutoff instead of retrying forever.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FatalSpanCutoff(pub(crate) super::feed::SourceCutoff);

impl std::fmt::Display for FatalSpanCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fatal span cutoff: {:?}", self.0)
    }
}

impl std::error::Error for FatalSpanCutoff {}

/// Is this instance the effective owner of `route`'s shard? None-ring
/// (single instance) counts as ours.
fn owned_here(state: &crate::http::AppState, route: &[u8; 16]) -> bool {
    let prefix = crate::registry::shard_for_hash(&state.shard_prefixes, route);
    match state.effective_owner(&prefix) {
        None => true,
        Some(o) => o == state.instance_name,
    }
}

impl LineageSpan {
    /// Linearized position AFTER this span's last record (sealed
    /// spans only; a live span is open-ended).
    fn logical_end(&self) -> Option<u64> {
        self.cap.map(|c| self.logical_start + c)
    }
}

/// Stage 6: one selector lane across a materialized segment map. The
/// linearization is stable because a lane (routing key, `""` for the
/// default lane) always has exactly ONE live segment; sealed
/// predecessors contribute their frozen caps to the logical prefix.
pub(crate) struct LineageSource {
    state: Arc<crate::http::AppState>,
    desc: StreamDesc,
    key: crate::crypto::StreamKey,
    /// The stream key in wire form for internal relays (computed once).
    key_b64: String,
    epoch: [u8; 16],
    rk_filter: Option<String>,
    spans: Vec<LineageSpan>,
    /// Returned as the advance notify for a sealed (possibly remote)
    /// tail: a sealed tail never advances, so nothing ever fires it.
    idle_notify: tokio::sync::Notify,
}

/// Why a lineage cannot be built here, split by retry semantics
/// (review round 5: a TRANSIENT engine failure must never become a
/// feed-wide incarnation cutoff).
pub(crate) enum LineageBuildError {
    /// A lineage span is owned by another instance (409-class):
    /// disconnect-and-reroute.
    WrongOwner(String),
    /// The lineage itself is inconsistent (no span, live predecessor):
    /// NOT a continuation of this feed's cursor space.
    IncompatibleTopology(String),
    /// Transient: engine opening / anti-flap holdoff — retry.
    Transient(String),
}

impl std::fmt::Display for LineageBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner(m) => write!(f, "wrong-owner: {m}"),
            Self::IncompatibleTopology(m) => write!(f, "incompatible-topology: {m}"),
            Self::Transient(m) => write!(f, "transient: {m}"),
        }
    }
}

impl LineageSource {
    /// Build the lane's span chain from a descriptor's segment map
    /// (mirrors the legacy keyed-lineage construction: segments
    /// containing the lane's key point, ordered by
    /// `(created_ms, seg_id)`).
    pub(crate) async fn build(
        state: Arc<crate::http::AppState>,
        desc: StreamDesc,
        key: crate::crypto::StreamKey,
        epoch: [u8; 16],
        rk_filter: Option<String>,
    ) -> Result<Arc<Self>, LineageBuildError> {
        let map = desc.segments.as_ref().ok_or_else(|| {
            LineageBuildError::IncompatibleTopology(
                "lineage source needs a materialized segment map".into(),
            )
        })?;
        let lane = rk_filter.clone().unwrap_or_default();
        let point = StreamDesc::key_point(&lane);
        let mut segs: Vec<&crate::segmap::SegmentDesc> =
            map.segments.iter().filter(|s| s.contains(point)).collect();
        segs.sort_by_key(|s| (s.created_ms, s.seg_id));
        if segs.is_empty() {
            return Err(LineageBuildError::IncompatibleTopology(
                "lineage has no span for the lane's key point".into(),
            ));
        }
        let mut spans = Vec::with_capacity(segs.len());
        let mut logical = 0u64;
        for sg in segs {
            let identity = desc.dynamic_segment_identity(sg.seg_id);
            // EXTERNAL resolver (review round 5): these engines serve a
            // live customer subscription, so they carry the external
            // adoption stamp — the maintenance sweep can never hold or
            // close them as internal custody while the feed lives.
            // 409 not_ring_owner is the DURABLE wrong-owner signal;
            // 503-class errors (open wait, anti-flap) are transient.
            let cap = sg.sealed_next_offset;
            let route = desc.segment_route(sg);
            let reader = if cap.is_some() {
                // Round-11.2: SEALED spans are metadata + a lazy,
                // ownership-DYNAMIC reader — nothing is opened or
                // contacted at build time (locate/logicalize need no
                // engines, and a span's owner may change later).
                let Some(target) = crate::product::InternalTarget::of(&desc, sg.seg_id) else {
                    return Err(LineageBuildError::IncompatibleTopology(format!(
                        "segment {} has no internal target",
                        sg.seg_id
                    )));
                };
                SpanReader::Sealed {
                    route,
                    target,
                    owner_hint: std::sync::RwLock::new(None),
                    local: tokio::sync::Mutex::new(None),
                }
            } else {
                // The LIVE tail must be LOCAL (locked architecture).
                match state.engine_for(&route).await {
                    Ok(engine) => {
                        let handle = engine.stream_handle(identity).await.map_err(|e| {
                            LineageBuildError::Transient(format!("stream handle: {e}"))
                        })?;
                        state.keys.put(identity, key.clone(), epoch);
                        SpanReader::LiveLocal {
                            route,
                            engine,
                            handle,
                        }
                    }
                    Err(resp) if resp.status() == axum::http::StatusCode::CONFLICT => {
                        return Err(LineageBuildError::WrongOwner(format!(
                            "live segment {} is owned by another instance",
                            sg.seg_id
                        )));
                    }
                    Err(resp) => {
                        return Err(LineageBuildError::Transient(format!(
                            "segment {} engine unavailable ({})",
                            sg.seg_id,
                            resp.status()
                        )));
                    }
                }
            };
            spans.push(LineageSpan {
                seg_id: sg.seg_id,
                logical_start: logical,
                cap,
                identity,
                reader,
            });
            if let Some(c) = cap {
                logical += c;
            }
        }
        // Only the LAST span may be live; a live predecessor would make
        // every later span's logical start drift as it grew.
        if spans
            .iter()
            .take(spans.len().saturating_sub(1))
            .any(|s| s.cap.is_none())
        {
            return Err(LineageBuildError::IncompatibleTopology(
                "lineage has a live span before the tail".into(),
            ));
        }
        Ok(Arc::new(Self {
            state,
            desc,
            key_b64: key.to_b64(),
            key,
            epoch,
            rk_filter,
            spans,
            idle_notify: tokio::sync::Notify::new(),
        }))
    }

    /// One sealed-span page (round-11.2, ownership-dynamic): local
    /// when this instance owns the shard, otherwise the typed remote
    /// protocol with at most one verified redirect. Fatal outcomes
    /// ride `FatalSpanCutoff`; retryables stay anyhow errors (the
    /// session's bounded-backoff retry).
    #[allow(clippy::too_many_arguments)]
    async fn sealed_span_page(
        &self,
        span: &LineageSpan,
        route: &[u8; 16],
        target: &crate::product::InternalTarget,
        owner_hint: &std::sync::RwLock<Option<String>>,
        local: &tokio::sync::Mutex<Option<(Arc<ShardEngine>, Arc<StreamHandle>)>>,
        local_from: u64,
        budget: usize,
    ) -> anyhow::Result<crate::http::ReadOut> {
        use super::feed::SourceCutoff;
        if owned_here(&self.state, route) {
            let mut cached = local.lock().await;
            if cached.is_none() {
                match self.state.engine_for(route).await {
                    Ok(engine) => {
                        let handle = engine
                            .stream_handle(span.identity)
                            .await
                            .map_err(|e| anyhow::anyhow!("stream handle: {e}"))?;
                        self.state
                            .keys
                            .put(span.identity, self.key.clone(), self.epoch);
                        *cached = Some((engine, handle));
                    }
                    // Ownership raced away between the check and the
                    // open: fall through to the remote path below.
                    Err(resp) if resp.status() == axum::http::StatusCode::CONFLICT => {}
                    Err(resp) => {
                        anyhow::bail!("sealed span engine unavailable ({})", resp.status())
                    }
                }
            }
            if let Some((engine, handle)) = cached.as_ref() {
                return crate::http::read_records(
                    &self.state,
                    &self.desc,
                    &self.key,
                    &self.epoch,
                    handle,
                    engine,
                    local_from,
                    self.rk_filter.as_deref(),
                    budget,
                    crate::shard::Deliver::Durable,
                )
                .await
                .map_err(|e| anyhow::anyhow!(e));
            }
        } else {
            // Moved away: a stale local cache must not serve (a fenced
            // engine's reads fail anyway); the remote path owns it.
            *local.lock().await = None;
        }
        // REMOTE: the owner is the hint, or the ring's current answer.
        let owner = {
            let hinted = owner_hint.read().unwrap().clone();
            match hinted {
                Some(o) if o != self.state.instance_name => o,
                _ => {
                    let prefix = crate::registry::shard_for_hash(&self.state.shard_prefixes, route);
                    self.state
                        .effective_owner(&prefix)
                        .filter(|o| *o != self.state.instance_name)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "sealed span {} ownership indeterminate; retrying",
                                span.seg_id
                            )
                        })?
                }
            }
        };
        match crate::product::remote_span_page(
            &self.state,
            &owner,
            &self.desc.name,
            target,
            local_from,
            budget,
            &self.key_b64,
        )
        .await
        {
            Ok(page) => {
                *owner_hint.write().unwrap() = Some(page.owner);
                let mut out = page.out;
                if let Some(k) = self.rk_filter.as_deref() {
                    out.recs.retain(|r| r.rkey == k);
                }
                Ok(out)
            }
            Err(crate::product::RemoteSpanError::Retryable { status, code }) => {
                anyhow::bail!("remote span {}: retryable {status} {code:?}", span.seg_id)
            }
            Err(crate::product::RemoteSpanError::Transport(m)) => {
                anyhow::bail!("remote span {}: transport {m}", span.seg_id)
            }
            Err(crate::product::RemoteSpanError::InvalidResponse(m)) => {
                anyhow::bail!("remote span {}: invalid response {m}", span.seg_id)
            }
            Err(crate::product::RemoteSpanError::Unauthorized) => {
                Err(anyhow::Error::new(FatalSpanCutoff(SourceCutoff::FleetAuth)))
            }
            Err(crate::product::RemoteSpanError::TargetGone) => Err(anyhow::Error::new(
                FatalSpanCutoff(SourceCutoff::IncarnationChanged),
            )),
            Err(crate::product::RemoteSpanError::TargetMismatch) => Err(anyhow::Error::new(
                FatalSpanCutoff(SourceCutoff::TargetMismatch),
            )),
            Err(crate::product::RemoteSpanError::RedirectLoop { first, second }) => {
                tracing::warn!(
                    span = span.seg_id,
                    %first,
                    %second,
                    "sealed span redirect loop refused"
                );
                Err(anyhow::Error::new(FatalSpanCutoff(
                    SourceCutoff::RedirectLoop,
                )))
            }
            Err(crate::product::RemoteSpanError::WrongOwner { owner }) => {
                anyhow::bail!("remote span {}: unresolved owner {owner}", span.seg_id)
            }
        }
    }

    fn tail(&self) -> &LineageSpan {
        self.spans.last().expect("non-empty lineage")
    }
}

#[async_trait::async_trait]
impl FeedSourceRead for LineageSource {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch> {
        let mut cursor = from;
        let mut recs: Vec<crate::http::PlainRec> = Vec::new();
        let mut budget = max_bytes;
        let mut completed = false;
        for (i, span) in self.spans.iter().enumerate() {
            let span_end = span.logical_end();
            let covers =
                cursor >= span.logical_start && span_end.map(|e| cursor < e).unwrap_or(true);
            if !covers {
                continue;
            }
            let local_from = cursor - span.logical_start;
            let part = match &span.reader {
                SpanReader::LiveLocal {
                    route,
                    engine,
                    handle,
                } => {
                    // Round-11.2: a moved live tail is NEVER served
                    // from stale local state — typed WrongOwner cutoff
                    // (resumable EOF; the gateway reroutes).
                    if !owned_here(&self.state, route) {
                        return Err(anyhow::Error::new(FatalSpanCutoff(
                            super::feed::SourceCutoff::WrongOwner,
                        )));
                    }
                    crate::http::read_records(
                        &self.state,
                        &self.desc,
                        &self.key,
                        &self.epoch,
                        handle,
                        engine,
                        local_from,
                        self.rk_filter.as_deref(),
                        budget,
                        crate::shard::Deliver::Durable,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?
                }
                SpanReader::Sealed {
                    route,
                    target,
                    owner_hint,
                    local,
                } => {
                    self.sealed_span_page(
                        span, route, target, owner_hint, local, local_from, budget,
                    )
                    .await?
                }
            };
            // CONSUMED progress (finding 2/6): the scanned boundary,
            // capped at the span's sealed cap — match-free ranges
            // count, records beyond the cap belong to the next span.
            let scanned_after = part.last.map(|l| l + 1).unwrap_or(local_from);
            let consumed_local = match span.cap {
                Some(c) => scanned_after.min(c),
                None => scanned_after,
            };
            for r in part.recs {
                if span.cap.is_some_and(|c| r.off >= c) {
                    break;
                }
                budget = budget.saturating_sub(r.payload.len());
                recs.push(crate::http::PlainRec {
                    off: span.logical_start + r.off,
                    payload: r.payload,
                    rkey: r.rkey,
                });
            }
            let before = cursor;
            cursor = cursor.max(span.logical_start + consumed_local);
            let is_tail = i + 1 == self.spans.len();
            if is_tail {
                completed = part.completed;
                break;
            }
            let drained = span_end.is_some_and(|e| cursor >= e);
            if part.completed && !drained {
                anyhow::bail!("lineage span ended below its cap");
            }
            if !drained {
                // Partial page inside this span: honest stop.
                if cursor == before {
                    completed = false;
                }
                break;
            }
            // Span drained: hop to the next owner.
            if budget == 0 {
                break;
            }
        }
        Ok(SourceBatch {
            scan_from: from,
            scan_to: cursor,
            records: recs,
            completed,
        })
    }

    fn frontier(&self) -> u64 {
        let tail = self.tail();
        match (tail.logical_end(), &tail.reader) {
            (Some(e), _) => e,
            (None, SpanReader::LiveLocal { handle, .. }) => {
                tail.logical_start + handle.state.lock().unwrap().durable.next
            }
            (None, SpanReader::Sealed { .. }) => {
                unreachable!("a live tail is always LiveLocal")
            }
        }
    }

    fn closed(&self) -> bool {
        let tail = self.tail();
        if tail.cap.is_some() {
            // A sealed cap IS closure — no handle read needed (the
            // tail may be a remote sealed span after a full seal).
            return true;
        }
        match &tail.reader {
            SpanReader::LiveLocal { handle, .. } => handle.state.lock().unwrap().durable.closed,
            SpanReader::Sealed { .. } => unreachable!("a live tail is always LiveLocal"),
        }
    }

    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes {
        Bytes::from(crate::sse::wire::sse_data_event(&self.desc, &rec.payload))
    }

    fn advance_notify(&self) -> &tokio::sync::Notify {
        match &self.tail().reader {
            SpanReader::LiveLocal { handle, .. } => &handle.notify,
            // A sealed tail never advances: park on a notify nothing
            // fires (the session's other wakeups drive closure).
            SpanReader::Sealed { .. } => &self.idle_notify,
        }
    }

    fn locate(&self, logical_after: u64) -> WirePosition {
        locate_in_spans(&self.span_sig(), logical_after)
    }

    fn logicalize(&self, pos: WirePosition) -> Option<u64> {
        for span in &self.spans {
            if span.seg_id != pos.seg_id {
                continue;
            }
            let within = match (span.cap, &span.reader) {
                (Some(c), _) => pos.local_after <= c,
                (None, SpanReader::LiveLocal { handle, .. }) => {
                    pos.local_after <= handle.state.lock().unwrap().durable.next
                }
                (None, SpanReader::Sealed { .. }) => {
                    unreachable!("a live tail is always LiveLocal")
                }
            };
            if within {
                return Some(span.logical_start + pos.local_after);
            }
            return None;
        }
        None
    }

    fn cursor_capability(&self) -> CursorCapability {
        CursorCapability::Segmented
    }

    fn span_sig(&self) -> Vec<(u32, u64, Option<u64>)> {
        self.spans
            .iter()
            .map(|s| (s.seg_id, s.logical_start, s.cap))
            .collect()
    }

    async fn next_source(&self) -> anyhow::Result<SourceTransition> {
        refresh_transition(
            &self.state,
            &self.desc,
            &self.key,
            &self.epoch,
            &self.rk_filter,
            &self.span_sig(),
        )
        .await
    }
}

/// Stage 6.3: the ONE descriptor-refresh decision, shared by every
/// source implementation. Called ONLY under the feed's driver permit.
/// Genuine-close detection mirrors `http::genuine_closure` exactly
/// (no materialized map, or a <=1-segment map with nothing pending).
pub(crate) async fn refresh_transition(
    state: &Arc<crate::http::AppState>,
    desc: &StreamDesc,
    key: &crate::crypto::StreamKey,
    epoch: &[u8; 16],
    rk_filter: &Option<String>,
    current_sig: &[(u32, u64, Option<u64>)],
) -> anyhow::Result<SourceTransition> {
    let sref = desc.sref();
    // ONE total deadline across all resume attempts (review round 6:
    // a two-iteration loop with a fresh 10-s timeout per iteration
    // could hold the driving session — and its heartbeat — for ~20 s).
    // Absolute deadline form: a resume that COMPLETES at the edge still
    // earns its descriptor re-read (review round 7); only a fresh
    // resume attempt is gated on remaining budget.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    for _ in 0..2 {
        // Fresh read, bypassing the descriptor cache: the swap decision
        // must see the newest published topology.
        state.registry.invalidate(&sref);
        let Some(d) = state.registry.get(&sref).await? else {
            // Deleted: no continuation exists for this feed.
            return Ok(SourceTransition::IncarnationChanged(
                SourceCutoff::IncarnationChanged,
            ));
        };
        if d.stream_epoch != desc.stream_epoch {
            // Delete/recreate: a DIFFERENT incarnation, never a swap.
            return Ok(SourceTransition::IncarnationChanged(
                SourceCutoff::IncarnationChanged,
            ));
        }
        if d.sealed {
            // A SEALED descriptor may still extend a COMPATIBLE lineage
            // this feed has not drained (split + successor appends +
            // seal before this refresh). Genuine closure means "no
            // future data after the FULL lineage", so a compatible
            // extension is installed and drained FIRST; closure is
            // reported only when the feed head reaches the complete
            // frontier (review round 5: terminal-before-drain data
            // loss).
            if let Some(map) = &d.segments
                && map.pending.is_none()
                && map.segments.len() > 1
            {
                match LineageSource::build(
                    state.clone(),
                    d.clone(),
                    key.clone(),
                    *epoch,
                    rk_filter.clone(),
                )
                .await
                {
                    Ok(next) => {
                        let new_sig = next.span_sig();
                        if !sig_compatible(current_sig, &new_sig) {
                            return Ok(SourceTransition::IncarnationChanged(
                                SourceCutoff::IncompatibleTopology,
                            ));
                        }
                        if new_sig.len() > current_sig.len() {
                            return Ok(SourceTransition::NewSource(next));
                        }
                        // Same spans, everything drained: genuine close.
                    }
                    Err(LineageBuildError::Transient(e)) => {
                        // Transient engine failure mid-drain: RETRY —
                        // never a feed-wide incarnation cutoff.
                        tracing::warn!(stream = %sref, error = %e, "sealed lineage drain deferred");
                        return Ok(SourceTransition::RetryLater);
                    }
                    Err(LineageBuildError::WrongOwner(e)) => {
                        tracing::warn!(
                            stream = %sref,
                            error = %e,
                            "sealed lineage cannot be drained here; disconnecting to reroute"
                        );
                        return Ok(SourceTransition::IncarnationChanged(
                            SourceCutoff::WrongOwner,
                        ));
                    }
                    Err(LineageBuildError::IncompatibleTopology(e)) => {
                        tracing::warn!(
                            stream = %sref,
                            error = %e,
                            "sealed lineage is inconsistent; disconnecting"
                        );
                        return Ok(SourceTransition::IncarnationChanged(
                            SourceCutoff::IncompatibleTopology,
                        ));
                    }
                }
            }
            return Ok(SourceTransition::GenuineClose);
        }
        let Some(map) = &d.segments else {
            return Ok(SourceTransition::GenuineClose);
        };
        if map.pending.is_some() {
            // Transition in flight: AWAIT the resumable completion
            // under this permit (bounded by the ONE absolute
            // deadline), then re-read IMMEDIATELY — regardless of
            // resume's boolean (review round 5: an external actor may
            // win the completion; the boolean is not evidence that the
            // topology did not change). A COMPLETED resume always
            // earns its re-read, even at the deadline edge (round 7).
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(SourceTransition::RetryLater);
            }
            let _ = tokio::time::timeout(remaining, crate::scaler3::resume(state, &sref)).await;
            continue;
        }
        if map.segments.len() <= 1 {
            return Ok(SourceTransition::GenuineClose);
        }
        let next =
            match LineageSource::build(state.clone(), d, key.clone(), *epoch, rk_filter.clone())
                .await
            {
                Ok(n) => n,
                // Durable wrong-owner: disconnect-and-reroute NOW
                // (fleet posture).
                Err(LineageBuildError::WrongOwner(e)) => {
                    tracing::warn!(
                        stream = %sref,
                        error = %e,
                        "lineage source is owned elsewhere; disconnecting to reroute"
                    );
                    return Ok(SourceTransition::IncarnationChanged(
                        SourceCutoff::WrongOwner,
                    ));
                }
                Err(LineageBuildError::IncompatibleTopology(e)) => {
                    tracing::warn!(
                        stream = %sref,
                        error = %e,
                        "lineage topology is inconsistent; disconnecting"
                    );
                    return Ok(SourceTransition::IncarnationChanged(
                        SourceCutoff::IncompatibleTopology,
                    ));
                }
                // Transient (engine opening, anti-flap holdoff):
                // RETRY — never a feed-wide incarnation cutoff
                // (review round 5).
                Err(LineageBuildError::Transient(e)) => {
                    tracing::warn!(stream = %sref, error = %e, "lineage build deferred");
                    return Ok(SourceTransition::RetryLater);
                }
            };
        let new_sig = next.span_sig();
        if !sig_compatible(current_sig, &new_sig) {
            // The topology no longer contains this feed's cursor space —
            // not a swap. Sessions disconnect without a terminal control.
            return Ok(SourceTransition::IncarnationChanged(
                SourceCutoff::IncompatibleTopology,
            ));
        }
        if new_sig.len() == current_sig.len() {
            // Spurious wake on an unchanged map.
            return Ok(SourceTransition::RetryLater);
        }
        return Ok(SourceTransition::NewSource(next));
    }
    Ok(SourceTransition::RetryLater)
}

/// The linearization rule (engine-free, so the mapping itself is
/// unit-testable): a linearized one-past offset maps to the span
/// covering it; the boundary one-past a sealed span's cap belongs to
/// the NEXT span at local 0.
fn locate_in_spans(spans: &[(u32, u64, Option<u64>)], logical_after: u64) -> WirePosition {
    for (i, (seg, start, cap)) in spans.iter().enumerate() {
        let last = i + 1 == spans.len();
        match cap.map(|c| start + c) {
            Some(e) if logical_after >= e && !last => continue,
            Some(e) if logical_after > e => continue,
            _ => {
                return WirePosition {
                    seg_id: *seg,
                    local_after: logical_after - start,
                };
            }
        }
    }
    let (seg_id, start, _) = spans.last().copied().expect("non-empty lineage");
    WirePosition {
        seg_id,
        local_after: logical_after.saturating_sub(start),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The linearization rule maps linearized offsets back to
    /// (seg_id, segment-local), with sealed-cap boundaries owned by
    /// the NEXT span.
    #[test]
    fn linearized_cursor_space_roundtrips() {
        let spans = [(0u32, 0u64, Some(5)), (1, 5, Some(3)), (2, 8, None)];
        // Inside the first span.
        let wp = |seg, local| WirePosition {
            seg_id: seg,
            local_after: local,
        };
        assert_eq!(locate_in_spans(&spans, 0), wp(0, 0));
        assert_eq!(locate_in_spans(&spans, 4), wp(0, 4));
        // The cap boundary belongs to the next span at local 0.
        assert_eq!(locate_in_spans(&spans, 5), wp(1, 0));
        assert_eq!(locate_in_spans(&spans, 7), wp(1, 2));
        assert_eq!(locate_in_spans(&spans, 8), wp(2, 0));
        // The live tail is open-ended.
        assert_eq!(locate_in_spans(&spans, 100), wp(2, 92));
    }

    #[test]
    fn sig_compatibility_rules() {
        // A live span may gain its sealed cap; spans may be appended.
        let old = [(0u32, 0u64, None)];
        let new = [(0u32, 0u64, Some(5)), (1, 5, None)];
        assert!(sig_compatible(&old, &new));
        // A different segment id is never a continuation.
        let bad_seg = [(1u32, 0u64, Some(5)), (2, 5, None)];
        assert!(!sig_compatible(&old, &bad_seg));
        // A span may not vanish.
        let old2 = [(0u32, 0u64, Some(5)), (1, 5, None)];
        let shrunk = [(0u32, 0u64, Some(5))];
        assert!(!sig_compatible(&old2, &shrunk));
        // A sealed cap may not change.
        let changed = [(0u32, 0u64, Some(6)), (1, 6, None)];
        assert!(!sig_compatible(&old2, &changed));
        // Identity.
        assert!(sig_compatible(&old2, &old2));
    }
}
