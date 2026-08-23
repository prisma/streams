//! The single SSE session over a LiveFeed (LIVE-FEED Stage 3+).
//!
//! One state machine serves every concurrency level. Phases:
//!
//!   A  INITIAL CATCH-UP — the session reads durable history for its
//!      own cursor directly (allowed from any cursor on CONNECT).
//!   B  LIVE — shared consumption: `take_visible` serves prepared
//!      data events; when progress is needed the session attempts
//!      `drive_once`; contended sessions park on the version watch.
//!
//! Framing: the feed prepares DATA events once per lane; each session
//! composes its own surface control onto them (one chunk on the wire)
//! and folds lane-global `upToDate`/`sealed` facts into the batch-last
//! frame exactly like the legacy direct producer did.
//!
//! Lag contract: a session that has reached live and later falls
//! below the retention floor disconnects (typed), it never becomes a
//! private historical reader.

use super::auth::{GatedSseBody, LeaseWatch, SseLease};
use super::feed::{DriveOutcome, FeedKey, FeedSourceRead, LiveFeed, Take};
use super::source::SingleSource;
use crate::http::{AppState, ReadParams, SseSlot, sse_acquire, sse_heartbeat, sse_send};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// Per-session wire context. v0 serves the product surface; raw
/// vocabulary joins through the same composition point.
#[derive(Clone)]
pub(crate) struct SessionCtx {
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    rk_hash: [u8; 16],
    seg_id: u32,
}

impl SessionCtx {
    fn ctl(&self, offset_after: u64, up_to_date: bool, sealed: bool) -> Bytes {
        let tok = crate::product_cursor::KeyCursor {
            epoch: self.epoch,
            key_hash: self.rk_hash,
            seg_id: self.seg_id,
            offset: offset_after,
        }
        .encode(&self.desc.project_id, &self.key);
        Bytes::from(crate::sse::wire::sse_control_product(
            &tok, up_to_date, sealed,
        ))
    }

    /// Shared data event + this session's control = ONE frame.
    fn compose(&self, data: &Bytes, offset_after: u64, up_to_date: bool, sealed: bool) -> Bytes {
        let ctl = self.ctl(offset_after, up_to_date, sealed);
        let mut out = BytesMut::with_capacity(data.len() + ctl.len());
        out.extend_from_slice(data);
        out.extend_from_slice(&ctl);
        out.freeze()
    }
}

/// Entry point replacing sse_response/sse_hub_response for the
/// eligible shape when the instance runs the livefeed engine.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve(
    state: Arc<AppState>,
    mut desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    handle: Arc<crate::shard::StreamHandle>,
    engine: Arc<crate::shard::ShardEngine>,
    start: crate::http::StartPos,
    params: ReadParams,
    rk_filter: Option<String>,
) -> axum::response::Response {
    let slot: SseSlot = match sse_acquire(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    // Round-4 finding 1 discipline: subscribe to the generation watch
    // BEFORE the initial proof so nothing published between prove and
    // park is lost; then prove generation-stably.
    let gen_rx = state.auth.generation_watch();
    let term = Arc::new(super::auth::TerminateOnce::default());
    let body_watch = match LeaseWatch::new_checked(&state, SseLease::of(&params), term.clone()) {
        Ok(w) => w,
        Err(reason) => return super::auth::lease_refusal_response(reason),
    };

    let lane_rk = rk_filter.unwrap_or_default();
    let seg_id = desc.resolve_segment(&lane_rk).seg_id;
    let identity = desc.dynamic_segment_identity(seg_id);
    let src = Arc::new(SingleSource {
        state: state.clone(),
        desc: desc.clone(),
        key: key.clone(),
        epoch,
        engine: engine.clone(),
        handle: handle.clone(),
        rk_filter: Some(lane_rk.clone()),
    });
    let feed = state
        .live_feeds
        .get_or_create(FeedKey::keyed(identity, &lane_rk), || {
            LiveFeed::new(
                FeedKey::keyed(identity, &lane_rk),
                src.clone(),
                crate::livehub::hub_ring_bytes(),
            )
        });
    let (join_head, ver_rx) = feed.join();

    let mut cursor = match start {
        crate::http::StartPos::At(p) => p,
        crate::http::StartPos::Now => src.frontier(),
    };

    let ctx = SessionCtx {
        rk_hash: crate::crypto::stream_hash(&lane_rk),
        epoch,
        key: key.clone(),
        seg_id,
        desc: desc.clone(),
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let mut hb = sse_heartbeat();
    // One subscribe operation; delivered bytes meter per chunk.
    crate::billing::meter_read(&state, &desc, 0, 0);
    let body_state = state.clone();

    let task_state = state.clone();
    let task_desc = desc.clone();
    let task_lease = SseLease::of(&params);
    tokio::spawn(async move {
        // Producer-side watch: same exactly-once termination record as
        // the body gate; a lease already dead here never schedules.
        let mut lease_watch = match LeaseWatch::new_checked(&task_state, task_lease, term) {
            Ok(w) => w,
            Err(_) => {
                feed.leave();
                task_state
                    .live_feeds
                    .evict_if_unsubscribed(&FeedKey::keyed(identity, &lane_rk));
                return;
            }
        };
        let sref = task_desc.sref();
        let mut need_status = true;
        let mut emitted_closed = false;

        // PHASE A: initial catch-up from the connecting cursor.
        while cursor < join_head.max(src.frontier()) {
            if lease_watch.revoked(&task_state) {
                return;
            }
            match src.read(cursor, 1024 * 1024).await {
                Ok((recs, _)) if !recs.is_empty() => {
                    let n = recs.len();
                    for (i, r) in recs.iter().enumerate() {
                        let last_of_window = i + 1 == n && cursor.max(r.off + 1) >= src.frontier();
                        let frame =
                            ctx.compose(&src.prepare_data(r), r.off + 1, last_of_window, false);
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
                }
                _ => break,
            }
        }

        // PHASE B: shared live consumption.
        let mut ver_rx = ver_rx;
        loop {
            if lease_watch.revoked(&task_state) {
                return;
            }
            match feed.take_visible(cursor) {
                Take::Lagged { floor } => {
                    tracing::warn!(
                        cursor,
                        floor,
                        "lag disconnect: subscriber fell below feed floor"
                    );
                    return;
                }
                Take::Records { records, next } => {
                    let drained = next >= src.frontier() && src.closed();
                    let n = records.len();
                    for (i, (off, data, plen)) in records.iter().enumerate() {
                        let last = i + 1 == n && drained;
                        let frame = ctx.compose(data, off + 1, last, last);
                        if !sse_send(&tx, frame).await || lease_watch.revoked(&task_state) {
                            return;
                        }
                        crate::sse::auth::sse_stats::DELIVERED_RECORDS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        crate::billing::meter_read_chunk(
                            &task_state.billing_reads,
                            &crate::billing::identity_of(&task_state, &task_desc),
                            u64::from(*plen),
                            1,
                        );
                    }
                    cursor = next;
                    need_status = false;
                }
                Take::AtHead => {}
            }
            // Status decision against the DURABLE frontier at send
            // time. A closure flip re-opens the emission even with no
            // new records — that is what turns "upToDate" into the one
            // sealed terminal.
            let frontier = src.frontier();
            let closed = src.closed();
            if cursor >= frontier {
                if need_status || (closed && !emitted_closed) {
                    let report_closed = closed && genuine_closure_checked(&task_state, &sref).await;
                    if !sse_send(&tx, ctx.ctl(cursor, true, report_closed)).await
                        || lease_watch.revoked(&task_state)
                    {
                        return;
                    }
                    need_status = false;
                    if report_closed {
                        return; // exactly ONE final control, then EOF
                    }
                    // Open stream: only report when something changed.
                    emitted_closed = false;
                }
            } else {
                need_status = true;
            }

            // Register wakeups BEFORE the frontier check: an append
            // committing between check and park must not be missed.
            let mut ver_rx2 = ver_rx.clone();
            let ver_wait = wait_version(&mut ver_rx2);
            let src_wait = src.advance_notify().notified();
            if cursor < frontier {
                drop(ver_wait);
                drop(src_wait);
                match feed.drive_once().await {
                    // SOLO retention: these records belong to THIS
                    // driver — consume them directly.
                    Some(DriveOutcome::Solo(records)) => {
                        let frontier_now = src.frontier();
                        let closed_now = src.closed();
                        for r in records.iter().filter(|r| r.offset >= cursor) {
                            let last = closed_now && r.offset + 1 >= frontier_now;
                            let frame = ctx.compose(&r.data_event, r.offset + 1, last, last);
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
                        cursor = cursor.max(frontier_now);
                        need_status = true;
                        if closed_now {
                            emitted_closed = true;
                        }
                        continue;
                    }
                    Some(_) => {
                        continue;
                    }
                    None => {
                        // Another session holds the permit; its
                        // publication wakes us on the version watch.
                        tokio::task::yield_now().await;
                        continue;
                    }
                }
            }
            // Park: durable advance, feed publication, heartbeat,
            // lease deadline, client disconnect.
            tokio::select! {
                _ = ver_wait => {}
                _ = src_wait => {}
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
    response_from_stream(stream)
}

async fn genuine_closure_checked(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> bool {
    crate::http::genuine_closure(state, sref, true).await
}

async fn wait_version(rx: &mut tokio::sync::watch::Receiver<u64>) {
    let _ = rx.changed().await;
}

fn response_from_stream(
    stream: impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
) -> axum::response::Response {
    use axum::http::header;
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header("x-accel-buffering", "no")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("Cross-Origin-Resource-Policy", "cross-origin")
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}
