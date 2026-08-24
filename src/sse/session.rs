//! The single SSE session over a LiveFeed (LIVE-FEED).
//!
//! One state machine serves every concurrency level and every covered
//! shape. Canonical framing (LIVE-FEED.md §Wire semantics):
//!
//!   * prepared DATA frames never carry upToDate/sealed;
//!   * every record is followed by its own bare cursor control
//!     (composed per session around the shared data event);
//!   * at a verified durable frontier the session emits ONE standalone
//!     upToDate control (deduped by reported position);
//!   * at genuine closure it emits exactly ONE sealed/streamClosed
//!     terminal control, then EOF — transitions disconnect WITHOUT a
//!     terminal control.
//!
//! Phases:
//!   A  INITIAL CATCH-UP — private durable reads bounded by the
//!      CAPTURED join head (never chases a moving frontier).
//!   B  LIVE — shared consumption: one batch per hand-off; contended
//!      drivers park on the version watch.

use super::auth::{GatedSseBody, LeaseWatch, SseLease};
use super::feed::{DriveOutcome, FeedKey, FeedSourceRead, LiveFeed, Take};
use crate::http::{AppState, ReadParams, SseSlot, err_resp, sse_heartbeat, sse_send};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Per-session wire vocabulary.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    Product,
    /// Raw scalar offsets on single-segment streams and forks.
    RawScalar,
}

#[derive(Clone)]
pub(crate) struct SessionCtx {
    surface: Surface,
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    rk_hash: [u8; 16],
    /// The ACTUAL live segment this lane resolves to (finding 7): a
    /// pruned-to-one segment map can leave a nonzero live segment ID,
    /// and product cursors must name it, not hard-coded zero.
    seg_id: u32,
}

impl SessionCtx {
    /// Bare cursor control (no status flags) for one record boundary.
    fn record_ctl(&self, offset_after: u64) -> Bytes {
        match self.surface {
            Surface::Product => {
                let tok = crate::product_cursor::KeyCursor {
                    epoch: self.epoch,
                    key_hash: self.rk_hash,
                    seg_id: self.seg_id,
                    offset: offset_after,
                }
                .encode(&self.desc.project_id, &self.key);
                Bytes::from(crate::sse::wire::sse_control_product(&tok, false, false))
            }
            Surface::RawScalar => Bytes::from(crate::sse::wire::sse_control(
                offset_after,
                None,
                false,
                false,
            )),
        }
    }

    /// Standalone STATUS control — the ONLY frame carrying flags.
    fn status_ctl(&self, offset_after: u64, closed: bool) -> Bytes {
        match self.surface {
            Surface::Product => {
                let tok = crate::product_cursor::KeyCursor {
                    epoch: self.epoch,
                    key_hash: self.rk_hash,
                    seg_id: self.seg_id,
                    offset: offset_after,
                }
                .encode(&self.desc.project_id, &self.key);
                Bytes::from(crate::sse::wire::sse_control_product(
                    &tok,
                    true,
                    sealed_of(closed),
                ))
            }
            Surface::RawScalar => Bytes::from(crate::sse::wire::sse_control(
                offset_after,
                None,
                true,
                sealed_of(closed),
            )),
        }
    }

    /// Shared data event + this session's bare cursor control = ONE
    /// wire chunk (small local concat over the shared payload).
    fn compose_record(&self, data: &Bytes, offset_after: u64) -> Bytes {
        let ctl = self.record_ctl(offset_after);
        let mut out = BytesMut::with_capacity(data.len() + ctl.len());
        out.extend_from_slice(data);
        out.extend_from_slice(&ctl);
        out.freeze()
    }
}

fn sealed_of(closed: bool) -> bool {
    closed // product vocabulary names it `sealed`; raw uses streamClosed
}

/// Entry point for ALL product/raw SSE when the instance runs the
/// livefeed engine. The slot is acquired ONCE by the caller and moved
/// here (finding 9).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve(
    state: Arc<AppState>,
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    handle: Arc<crate::shard::StreamHandle>,
    engine: Arc<crate::shard::ShardEngine>,
    start: crate::http::StartPos,
    params: ReadParams,
    rk_filter: Option<String>,
    surface: crate::http::SseSurface,
    slot: SseSlot,
) -> axum::response::Response {
    // Round-4 finding 1 discipline: subscribe BEFORE the initial proof.
    let gen_rx = state.auth.generation_watch();
    let term = Arc::new(super::auth::TerminateOnce::default());
    let body_watch = match LeaseWatch::new_checked(&state, SseLease::of(&params), term.clone()) {
        Ok(w) => w,
        Err(reason) => return super::auth::lease_refusal_response(reason),
    };

    let lane_rk = rk_filter.clone().unwrap_or_default();
    let fkey = feed_key_of(&desc, &rk_filter);
    let src = Arc::new(super::source::SingleSource {
        state: state.clone(),
        rk_filter: Some(lane_rk.clone()),
        desc: desc.clone(),
        key: key.clone(),
        epoch,
        engine: engine.clone(),
        handle: handle.clone(),
    });
    // RAII subscription (finding 3): atomic create-or-join under the
    // registry lock; Drop detaches, clears retention at one and evicts
    // at zero. Finding 6-mem: entering SHARED mode reserves this feed's
    // ring allowance from the process-global budget — exhaustion
    // rejects THE NEW subscriber with a typed capacity refusal while
    // the existing singleton continues normally.
    let subscription = match state.live_feeds.subscribe(fkey.clone(), || {
        LiveFeed::new(fkey.clone(), src.clone(), crate::livehub::feed_ring_bytes())
    }) {
        Ok(sub) => sub,
        Err(_) => {
            use axum::response::IntoResponse;
            return err_resp(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "subscription_capacity",
                "the process retention budget cannot host another shared subscription",
            )
            .into_response();
        }
    };
    let feed = subscription.feed();
    // subscribe() already incremented the count (registry RAII owns
    // attach/detach — no second join here, finding 3's double-count).
    // The handoff state CAPTURED under the subscribe lock is the one a
    // session must use (finding 8): re-reading head/version-watch after
    // the fact reopens the race where a pre-existing subscriber
    // advances the feed between attach and session start, and Phase A
    // would then privately deliver records the shared ring also holds.
    let mut ver_rx = subscription.version_rx();
    let mut cursor = match start {
        crate::http::StartPos::At(p) => p,
        crate::http::StartPos::Now => src.frontier(),
    };
    // PHASE A handoff bound (finding 8): catch up only to the head
    // captured at subscribe time — never chase the moving frontier.
    let join_head = subscription.join_head();

    let ctx = SessionCtx {
        surface: match surface {
            crate::http::SseSurface::Raw => Surface::RawScalar,
            crate::http::SseSurface::Product => Surface::Product,
        },
        rk_hash: crate::crypto::stream_hash(&lane_rk),
        // The ACTUAL segment this lane resolves to (finding 7): a map
        // pruned to one live segment can carry a nonzero segment ID.
        seg_id: desc.resolve_segment(&lane_rk).seg_id,
        epoch,
        key: key.clone(),
        desc: desc.clone(),
    };
    let binary = {
        let mt = crate::registry::media_type(&desc.content_type);
        mt != "application/json" && !mt.starts_with("text/")
    };
    let usage = crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let mut hb = sse_heartbeat();
    crate::billing::meter_read(&state, &desc, 0, 0);
    let body_state = state.clone();

    let task_state = state.clone();
    let task_desc = desc.clone();
    let task_lease = SseLease::of(&params);
    let task_subscription = subscription;
    tokio::spawn(async move {
        let _subscription = task_subscription; // RAII detach on any exit
        let mut lease_watch = match LeaseWatch::new_checked(&task_state, task_lease, term) {
            Ok(w) => w,
            Err(reason) => {
                tracing::warn!(reason = %reason.as_str(), "livefeed lease dead at construction");
                return;
            }
        };
        let sref = task_desc.sref();
        let mut need_status = true;
        // Exact status machine (finding 4): dedupe by reported frontier
        // position; the terminal control is emitted once, then EOF.
        let mut last_reported: Option<u64> = None;

        // PHASE A: catch up to the CAPTURED join head only.
        while cursor < join_head {
            if lease_watch.revoked(&task_state) {
                return;
            }
            match src.read_batch(cursor, 1024 * 1024).await {
                Ok(batch) if batch.scan_to > cursor => {
                    for r in &batch.records {
                        // NEVER emit beyond the captured handoff bound
                        // (finding 8): records at/after join_head are
                        // delivered through the shared ring in Phase B —
                        // sending them here too would duplicate them.
                        if r.off >= join_head {
                            break;
                        }
                        if r.off < cursor {
                            continue;
                        }
                        let frame = ctx.compose_record(&src.prepare_data(r), r.off + 1);
                        if !sse_send(&tx, frame).await || lease_watch.revoked(&task_state) {
                            return;
                        }
                        crate::sse::auth::sse_stats::DELIVERED_RECORDS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        crate::billing::meter_read_chunk(
                            &task_state.billing_reads,
                            &crate::billing::identity_of(&task_state, &task_desc),
                            r.payload.len() as u64,
                            1,
                        );
                        cursor = cursor.max(r.off + 1);
                    }
                    // Match-free scanned range still progresses.
                    cursor = cursor.max(batch.scan_to.min(join_head));
                }
                _ => break,
            }
        }

        // PHASE B: shared live consumption.
        loop {
            if lease_watch.revoked(&task_state) {
                return;
            }
            // Wakeups registered at LOOP TOP (finding 7): every state
            // read below is covered by these futures. `ver_rx` is THE
            // persistent receiver — changed() consumes each publication
            // exactly once (no clone-echo loops).
            let mut ver_wait = Box::pin(async {
                let _ = ver_rx.changed().await;
            });
            // The source waiter must be REGISTERED now, not at the
            // final select! (finding: registration window): an unpolled
            // `Notify::notified()` future has no waiter, so an append
            // committing between the frontier read below and the park
            // would be missed until the heartbeat. `enable()` registers
            // the waiter eagerly; every state read in this iteration is
            // covered, and the state reads themselves cover the gap
            // since the previous iteration's registration.
            let cur_src = feed.current_source();
            let src_wait = cur_src.advance_notify().notified();
            tokio::pin!(src_wait);
            src_wait.as_mut().enable();
            match feed.take_visible(cursor) {
                Take::Lagged { floor } => {
                    tracing::warn!(cursor, floor, "lag disconnect below feed floor");
                    return;
                }
                Take::Batch { batch, start_index } => {
                    for r in batch.records[start_index..].iter() {
                        let frame = ctx.compose_record(&r.data_event, r.offset + 1);
                        if !sse_send(&tx, frame).await || lease_watch.revoked(&task_state) {
                            return;
                        }
                        crate::sse::auth::sse_stats::DELIVERED_RECORDS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        crate::billing::meter_read_chunk(
                            &task_state.billing_reads,
                            &crate::billing::identity_of(&task_state, &task_desc),
                            u64::from(r.payload_len),
                            1,
                        );
                    }
                    cursor = cursor.max(batch.scan_to);
                    need_status = true;
                }
                Take::Progress { next } => {
                    // Match-free scanned range: pure cursor progress.
                    cursor = next.max(cursor);
                    need_status = true;
                }
                Take::AtHead => {}
            }
            let frontier = src.frontier();
            let closed = src.closed();
            if cursor >= frontier {
                // TERMINAL: at genuine close exactly ONE sealed control
                // is emitted, then EOF — never a preceding duplicate
                // upToDate at the same position (finding 7).
                let genuine = closed && genuine_closure_checked(&task_state, &sref).await;
                if genuine {
                    if !sse_send(&tx, ctx.status_ctl(cursor, true)).await {
                        return;
                    }
                    return; // EOF after THE final control
                }
                // Transition (closed but NOT genuine): disconnect
                // without a terminal control — the client resumes from
                // its cursor into the new topology via the legacy
                // lineage path (finding 5 minimum safe behavior).
                if closed {
                    tracing::info!("topology transition detected: disconnecting without terminal");
                    return;
                }
                // Open frontier: one upToDate per position (deduped).
                if need_status && last_reported != Some(cursor) {
                    if !sse_send(&tx, ctx.status_ctl(cursor, false)).await
                        || lease_watch.revoked(&task_state)
                    {
                        return;
                    }
                    need_status = false;
                    last_reported = Some(cursor);
                }
            } else {
                need_status = true;
            }

            // Drive when progress is needed. Contended callers fall to
            // the park below (ver_wait registered at loop top).
            if cursor < frontier {
                match feed.drive_once().await {
                    Some(DriveOutcome::Solo { records, scan_to }) => {
                        let at_cursor = cursor;
                        for r in records.iter().filter(|r| r.offset >= at_cursor) {
                            let frame = ctx.compose_record(&r.data_event, r.offset + 1);
                            if !sse_send(&tx, frame).await || lease_watch.revoked(&task_state) {
                                return;
                            }
                            crate::sse::auth::sse_stats::DELIVERED_RECORDS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            crate::billing::meter_read_chunk(
                                &task_state.billing_reads,
                                &crate::billing::identity_of(&task_state, &task_desc),
                                u64::from(r.payload_len),
                                1,
                            );
                            cursor = cursor.max(r.offset + 1);
                        }
                        // ADVANCE TO SCANNED BATCH END ONLY (findings
                        // 1+2): never jump to the live frontier and never
                        // stop at the last MATCHING record — match-free
                        // ranges are consumed progress.
                        cursor = cursor.max(scan_to);
                        need_status = true;
                        continue;
                    }
                    // A publication or closure changed feed state (and
                    // bumped the version): loop and consume it.
                    Some(DriveOutcome::Published) | Some(DriveOutcome::Closed) => continue,
                    // No-progress page or source failure: feed state did
                    // NOT change and the version was NOT bumped — park
                    // rather than spin (finding 6). The next durable
                    // advance or the heartbeat retries.
                    Some(DriveOutcome::NoProgress) => {}
                    Some(DriveOutcome::SourceFailed) => {
                        tracing::warn!("livefeed source read failed; parking until next wake");
                    }
                    // Head already covers the frontier: nothing to do.
                    Some(DriveOutcome::Idle) => {}
                    // Contended: park; the winner's publication bumps
                    // ver_wait (registered at loop top).
                    None => {}
                }
            }
            // Park.
            tokio::select! {
                _ = &mut ver_wait => {}
                _ = &mut src_wait => {}
                _ = tx.closed() => {
                    crate::sse::auth::sse_stats::DISCONNECT_CLIENT_CLOSED
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
                _ = tokio::time::sleep(lease_watch.nap()) => {
                    if lease_watch.revoked(&task_state) {
                        return;
                    }
                }
                _ = hb.changed() => {
                    if !sse_send(&tx, Bytes::from(": keep-alive\n\n")).await {
                        return;
                    }
                }
            }
        }
    });

    let stream = futures_util::StreamExt::map(
        GatedSseBody::new(body_state, rx, slot, body_watch, gen_rx),
        move |item| item,
    );
    response_from_stream(stream, binary, &usage)
}

/// Stream-incarnation identity + selector lane — the feed registry
/// key derivation shared with tests. The identity leg is the
/// domain-separated STORAGE identity (`storage_hash`: layout domain +
/// project + name + epoch), NOT an ad hoc hash of the same inputs
/// (follow-up review finding 7).
pub(crate) fn feed_key_of(
    desc: &crate::registry::StreamDesc,
    rk_filter: &Option<String>,
) -> FeedKey {
    let identity = desc.storage_hash();
    match rk_filter.as_deref() {
        None | Some("") => FeedKey::default_lane(identity),
        Some(rk) => FeedKey::keyed(identity, rk),
    }
}

async fn genuine_closure_checked(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> bool {
    crate::http::genuine_closure(state, sref, true).await
}

fn response_from_stream(
    stream: impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    binary: bool,
    usage: &Arc<crate::usage::Counters>,
) -> axum::response::Response {
    use axum::http::header;
    let usage = Arc::clone(usage);
    let stream = futures_util::StreamExt::map(stream, move |item| {
        if let Ok(b) = &item {
            usage.bytes_out.fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        item
    });
    let mut builder = axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header("x-accel-buffering", "no")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if binary {
        builder = builder.header("Stream-SSE-Data-Encoding", "base64");
    }
    builder.body(axum::body::Body::from_stream(stream)).unwrap()
}

// ==================================================================
// Unit tests (follow-up review finding 7): the product cursor must
// name the ACTUAL live segment, not hard-coded zero.
// ==================================================================
#[cfg(test)]
mod tests {
    use super::*;

    /// A materialized one-segment map whose only live segment has a
    /// NONZERO id (a lineage pruned down to a later segment) meets the
    /// LiveFeed eligibility condition — and its cursors must name that
    /// segment.
    #[test]
    fn product_cursor_names_the_actual_live_segment() {
        let mut desc = crate::sse::feed::tests::test_desc("segtest");
        desc.segments = Some(crate::segmap::SegmentMap {
            version: 7,
            next_seg_id: 6,
            pending: None,
            segments: vec![crate::segmap::SegmentDesc {
                seg_id: 5,
                lo: 0,
                hi: crate::segmap::KEYSPACE_END,
                shard_prefix: String::new(),
                route_hash: [0u8; 16],
                created_ms: 1,
                predecessors: vec![0],
                successors: Vec::new(),
                sealed_ms: None,
                sealed_next_offset: None,
            }],
        });
        let key = crate::crypto::StreamKey([9u8; 32]);
        let epoch = [3u8; 16];
        let lane_rk = String::new();
        let ctx = SessionCtx {
            surface: Surface::Product,
            rk_hash: crate::crypto::stream_hash(&lane_rk),
            seg_id: desc.resolve_segment(&lane_rk).seg_id,
            epoch,
            key: key.clone(),
            desc: desc.clone(),
        };
        assert_eq!(ctx.seg_id, 5, "resolve must find the nonzero live segment");

        // The emitted bare cursor control carries a KeyCursor naming
        // segment 5, decodable and authenticated.
        let ctl = ctx.record_ctl(42);
        let text = String::from_utf8(ctl.to_vec()).unwrap();
        let tok = text
            .split("\"nextCursor\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("product control carries nextCursor");
        let kc = crate::product_cursor::KeyCursor::decode(
            tok,
            &desc.project_id,
            &key,
            &epoch,
            &ctx.rk_hash,
        )
        .expect("cursor decodes");
        assert_eq!(kc.seg_id, 5, "the cursor names the actual segment");
        assert_eq!(kc.offset, 42);

        // And the standalone status control names it too.
        let status = ctx.status_ctl(42, false);
        let text = String::from_utf8(status.to_vec()).unwrap();
        let tok = text
            .split("\"nextCursor\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("status control carries nextCursor");
        let kc = crate::product_cursor::KeyCursor::decode(
            tok,
            &desc.project_id,
            &key,
            &epoch,
            &ctx.rk_hash,
        )
        .expect("status cursor decodes");
        assert_eq!(kc.seg_id, 5);
    }

    /// Finding 7 (identity): the feed identity leg IS the domain-
    /// separated storage hash — distinct incarnations never share a
    /// feed, and the derivation matches the storage keyspace identity.
    #[test]
    fn feed_identity_is_the_storage_hash() {
        let desc = crate::sse::feed::tests::test_desc("ident");
        let key = feed_key_of(&desc, &None);
        assert_eq!(key.identity, desc.storage_hash());
        let mut other = crate::sse::feed::tests::test_desc("ident");
        other.stream_epoch = "ffffffffffffffffffffffffffffffff".into();
        assert_ne!(
            feed_key_of(&other, &None).identity,
            key.identity,
            "a recreated stream (new epoch) is a new feed identity"
        );
    }
}
