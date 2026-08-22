//! The single SSE session over a LiveFeed (LIVE-FEED Stage 3).
//!
//! One state machine serves every concurrency level. Phases:
//!
//!   A  INITIAL CATCH-UP — the session reads durable history for its
//!      own cursor directly (allowed from any cursor on CONNECT).
//!   B  LIVE — shared consumption: `take_visible` serves prepared
//!      bytes; when progress is needed the session attempts
//!      `drive_once`; contended sessions park on the version watch.
//!
//! Lag contract: a session that has reached live and later falls
//! below the retention floor disconnects (typed), it never becomes a
//! private historical reader.

use super::auth::{GatedSseBody, LeaseWatch, SseLease};
use super::feed::{DriveOutcome, FeedKey, FeedSourceRead, LiveFeed, Take};
use super::registry::FeedRegistry;
use super::source::SingleSource;
use crate::http::{AppState, PlainRec, ReadParams, SseSlot, sse_acquire, sse_heartbeat, sse_send};
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct SessionCtx {
    pub(crate) desc: crate::registry::StreamDesc,
    pub(crate) key: crate::crypto::StreamKey,
    pub(crate) epoch: [u8; 16],
    pub(crate) rk_hash: [u8; 16],
    pub(crate) seg_id: u32,
}

fn product_ctl(ctx: &SessionCtx, offset: u64, up_to_date: bool, sealed: bool) -> Bytes {
    let tok = crate::product_cursor::KeyCursor {
        epoch: ctx.epoch,
        key_hash: ctx.rk_hash,
        seg_id: ctx.seg_id,
        offset,
    }
    .encode(&ctx.desc.project_id, &ctx.key);
    Bytes::from(crate::sse::wire::sse_control_product(
        &tok, up_to_date, sealed,
    ))
}

/// Entry point replacing sse_response/sse_hub_response for the
/// eligible shape (product, unforked, default-key lane, one segment)
/// when the instance runs the livefeed engine.
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

    let seg_id = desc.resolve_segment("").seg_id;
    let identity = desc.dynamic_segment_identity(seg_id);
    let src = Arc::new(SingleSource {
        state: state.clone(),
        desc: desc.clone(),
        key: key.clone(),
        epoch,
        engine,
        handle,
    });
    let feed = state.live_feeds.get_or_create(FeedKey::of(identity), || {
        LiveFeed::new(
            FeedKey::of(identity),
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
        rk_hash: crate::crypto::stream_hash(""),
        epoch,
        key,
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
    let task_params_lease = SseLease::of(&params);
    tokio::spawn(async move {
        // Producer-side watch: same exactly-once termination record as
        // the body gate; a lease already dead here never schedules.
        let mut lease_watch = match LeaseWatch::new_checked(&task_state, task_params_lease, term) {
            Ok(w) => w,
            Err(_) => {
                feed.leave();
                task_state
                    .live_feeds
                    .evict_if_unsubscribed(&FeedKey::of(identity));
                return;
            }
        };
        let sref = task_desc.sref();
        // PHASE A: initial catch-up from the connecting cursor.
        while cursor < join_head.max(src.frontier()) && !src.closed() || cursor < join_head {
            if lease_watch.revoked(&task_state) {
                return;
            }
            match src.read(cursor, 1024 * 1024).await {
                Ok((recs, _)) if !recs.is_empty() => {
                    for r in &recs {
                        let data =
                            Bytes::from(crate::sse::wire::sse_data_event(&task_desc, &r.payload));
                        if !sse_send(&tx, data).await || lease_watch.revoked(&task_state) {
                            return;
                        }
                        if !sse_send(&tx, product_ctl(&ctx, r.off + 1, false, false)).await {
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
        let mut need_status = true;
        let mut emitted_closed = false;
        let mut emitted_closed = false;
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
                    for (off, frame, plen, sealed) in &records {
                        if !sse_send(&tx, frame.clone()).await || lease_watch.revoked(&task_state) {
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
                        if *sealed {
                            return; // the terminal control rode this frame
                        }
                    }
                    cursor = next;
                    need_status = false;
                }
                Take::AtHead { .. } => {}
            }
            // Empty-drain statuses: connect-at-tail answers ONE
            // upToDate control; a closure flip MUST re-report even
            // without new records — it is what turns "upToDate" into
            // the one sealed terminal. Record-bearing batches carry
            // their lane-global flags INSIDE the prepared frame.
            let frontier = src.frontier();
            let closed = src.closed();
            if cursor >= frontier {
                if need_status || closed != emitted_closed {
                    let report_closed = closed && genuine_closure_checked(&task_state, &sref).await;
                    let ctl = product_ctl(&ctx, cursor, true, report_closed);
                    if !sse_send(&tx, ctl).await || lease_watch.revoked(&task_state) {
                        return;
                    }
                    need_status = false;
                    emitted_closed = closed;
                    if report_closed {
                        return; // exactly ONE final control, then EOF
                    }
                }
            } else {
                need_status = true;
            }
            // Register wakeups BEFORE the frontier check: an append
            // committing between check and park must not be missed.
            let mut ver_rx2 = ver_rx.clone();
            let ver_wait = wait_version(&mut ver_rx2);
            let src_wait = src.advance_notify().notified();
            // Need progress? Drive; contended drivers wait for the
            // winner's publication.
            if cursor < frontier {
                drop(ver_wait);
                drop(src_wait);
                match feed.drive_once().await {
                    // SOLO retention: this batch was handed back to its
                    // driver — consume it directly instead of re-reading
                    // the (empty) ring, which would misreport lag.
                    Some(DriveOutcome::Solo(b)) => {
                        for r in b.records.iter().filter(|r| r.offset >= cursor) {
                            if !sse_send(&tx, r.data_event.clone()).await
                                || lease_watch.revoked(&task_state)
                            {
                                return;
                            }
                            if !sse_send(&tx, product_ctl(&ctx, r.offset + 1, false, false)).await {
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
                        cursor = cursor.max(b.scan_to);
                        need_status = true;
                        continue;
                    }
                    Some(_) => {
                        need_status = true;
                        continue;
                    }
                    None => {
                        // Another session holds the permit; its
                        // publication will wake us via the version watch.
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

    drop(desc);

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

// Registry re-export for AppState wiring.
pub(crate) type LiveFeeds = FeedRegistry;

// Keep imports referenced regardless of cfg folds.
#[allow(unused)]
fn _imports(_: &ReadParams, _: &PlainRec, _: &LiveFeed) {}
