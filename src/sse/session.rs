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
//!     terminal control, then EOF; a topology transition swaps the
//!     feed's source in place (product surface) or disconnects WITHOUT
//!     a terminal control (raw fallback, incarnation change).
//!
//! Phases:
//!   A  INITIAL CATCH-UP — private durable reads bounded by the
//!      CAPTURED join head (never chases a moving frontier); re-entered
//!      when the ring overtakes a not-yet-live session.
//!   B  LIVE — shared consumption: one batch per hand-off; contended
//!      drivers park on the version watch; a source swap parks on the
//!      source-generation watch and re-snapshots.

use super::auth::{GatedSseBody, LeaseWatch, SseLease};
use super::feed::{DriveOutcome, FeedKey, LiveFeed, Take};
use crate::http::{AppState, ReadParams, SseSlot, err_resp, sse_send, sse_send_billed};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Per-session wire vocabulary.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    Product,
    /// Raw epoch/segment offset tokens (round-11.3): byte-compatible
    /// with the scalar encoding at segment 0, segment-aware on
    /// successors — the ONE raw vocabulary across every lineage.
    RawToken,
}

#[derive(Clone)]
pub(crate) struct SessionCtx {
    surface: Surface,
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    rk_hash: [u8; 16],
    /// The client's `?cursor=` on the RAW surface — the pinned
    /// protocol's collision-jitter input: an emitted streamCursor
    /// must exceed a presented numeric cursor (round 11.8: livefeed
    /// serves the conformance surface directly).
    raw_cursor: Option<String>,
}

impl SessionCtx {
    /// Cursor control for one record boundary. `pos` is the wire
    /// position bound AT PREPARATION (or located with the source that
    /// owns the position) — segment id AND segment-local offset,
    /// never the linearized offset (review round 4, blocker 1).
    ///
    /// PRODUCT: always BARE (the canonical framing — flags ride the
    /// standalone status control only). RAW: the pinned conformance
    /// protocol pairs each data event with ONE control that carries
    /// the flags, so `at_head` folds upToDate into the LAST record's
    /// control (round 11.8: livefeed serves the conformance surface).
    fn record_ctl(&self, pos: super::feed::WirePosition, at_head: bool) -> Bytes {
        match self.surface {
            Surface::Product => {
                let tok = crate::product_cursor::KeyCursor {
                    epoch: self.epoch,
                    key_hash: self.rk_hash,
                    seg_id: pos.seg_id,
                    offset: pos.local_after,
                }
                .encode(&self.desc.project_id, &self.key);
                Bytes::from(crate::sse::wire::sse_control_product(&tok, false, false))
            }
            Surface::RawToken => Bytes::from(crate::sse::wire::sse_control_ep(
                pos.seg_id,
                pos.local_after,
                self.raw_cursor.as_deref(),
                at_head,
                false,
            )),
        }
    }

    /// Standalone STATUS control — the ONLY frame carrying flags.
    fn status_ctl(&self, pos: super::feed::WirePosition, closed: bool) -> Bytes {
        match self.surface {
            Surface::Product => {
                let tok = crate::product_cursor::KeyCursor {
                    epoch: self.epoch,
                    key_hash: self.rk_hash,
                    seg_id: pos.seg_id,
                    offset: pos.local_after,
                }
                .encode(&self.desc.project_id, &self.key);
                Bytes::from(crate::sse::wire::sse_control_product(
                    &tok,
                    true,
                    sealed_of(closed),
                ))
            }
            Surface::RawToken => Bytes::from(crate::sse::wire::sse_control_ep(
                pos.seg_id,
                pos.local_after,
                self.raw_cursor.as_deref(),
                true,
                sealed_of(closed),
            )),
        }
    }

    /// `at_head` = this is the LAST record of a delivered batch AND
    /// the cursor reached the durable frontier — the RAW surface's
    /// paired control carries the upToDate flag (Product stays bare).
    fn compose_record_flagged(
        &self,
        data: &Bytes,
        pos: super::feed::WirePosition,
        at_head: bool,
    ) -> Bytes {
        let ctl = self.record_ctl(pos, at_head);
        let mut out = BytesMut::with_capacity(data.len() + ctl.len());
        out.extend_from_slice(data);
        out.extend_from_slice(&ctl);
        out.freeze()
    }
}

fn sealed_of(closed: bool) -> bool {
    closed // product vocabulary names it `sealed`; raw uses streamClosed
}

/// Typed cutoff accounting (Stage 7 canary telemetry): one counter
/// per `SourceCutoff` reason.
fn count_cutoff(reason: super::feed::SourceCutoff) {
    use super::feed::SourceCutoff;
    let c = match reason {
        SourceCutoff::IncarnationChanged => &crate::sse::auth::sse_stats::FEED_CUTOFF_INCARNATION,
        SourceCutoff::WrongOwner => &crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER,
        SourceCutoff::IncompatibleTopology => {
            &crate::sse::auth::sse_stats::FEED_CUTOFF_INCOMPATIBLE
        }
        SourceCutoff::TargetMismatch => &crate::sse::auth::sse_stats::FEED_CUTOFF_TARGET_MISMATCH,
        SourceCutoff::FleetAuth => &crate::sse::auth::sse_stats::FEED_CUTOFF_FLEET_AUTH,
        SourceCutoff::RedirectLoop => &crate::sse::auth::sse_stats::FEED_CUTOFF_REDIRECT_LOOP,
    };
    c.fetch_add(1, Ordering::Relaxed);
}

/// Entry point for ALL product/raw SSE when the instance runs the
/// livefeed engine. The slot is acquired ONCE by the caller and moved
/// here (finding 9). The caller builds the initial read source
/// (`SingleSource` for single-segment/fork streams, `LineageSource`
/// for connect-time lineage, Stage 7A).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve(
    state: Arc<AppState>,
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    src: Arc<dyn super::feed::FeedSourceRead>,
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
    // Round-11.3: KEYED raw rides every lineage via epoch/segment
    // tokens; KEYLESS raw remains scalar-total-order only — a keyless
    // subscriber semantically wants EVERY record, which no single
    // lane linearizes once a stream splits. Mid-flight splits and
    // post-swap attaches take the typed disconnect (the reconnect is
    // refused upstream with 400 keyless_live).
    let raw_keyless = matches!(surface, crate::http::SseSurface::Raw)
        && rk_filter.as_deref().is_none_or(str::is_empty);
    let fkey = feed_key_of(&desc, &rk_filter);
    // Round-11.3: the raw capability gate is GONE — raw controls carry
    // epoch/segment tokens, so every lineage shape is representable on
    // the raw surface and a source swap needs no raw disconnect.
    // Test failpoint: BEFORE the atomic attach — the window in which a
    // feed can swap generations between dispatch and attach.
    #[cfg(test)]
    crate::failpoints::pause(crate::failpoints::Fp::SseFeedBeforeSubscribe, &desc.name).await;
    // RAII subscription (finding 3): atomic create-or-join under the
    // registry lock; Drop detaches, clears retention at one and evicts
    // at zero. Finding 6-mem: entering SHARED mode reserves this feed's
    // ring allowance from the process-global budget — exhaustion
    // rejects THE NEW subscriber with a typed capacity refusal while
    // the existing singleton continues normally.
    let subscription = match state.live_feeds.subscribe(
        fkey.clone(),
        || {
            let feed = LiveFeed::new_with_budget(
                fkey.clone(),
                src.clone(),
                state
                    .feed_ring_bytes
                    .load(std::sync::atomic::Ordering::Relaxed),
                state.feed_budget.clone(),
                desc.project_id.clone(),
            );
            // Round-13: bind the project's admission pressure entry —
            // in the CREATION closure, so the feed charges its static
            // weight exactly once regardless of racing first
            // subscribers, and the retention mirror is live before
            // the first publication reserves bytes.
            if let Some(adm) = state.quotas.pressure_handle(&desc.project_id) {
                feed.bind_pressure(adm);
            }
            feed
        },
        Some(&src),
    ) {
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
    // Test failpoint: AFTER the atomic attach, BEFORE the session reads
    // any feed state — the exact window of the join-head handoff race.
    #[cfg(test)]
    crate::failpoints::pause(crate::failpoints::Fp::SseFeedAfterSubscribe, &desc.name).await;
    let feed = subscription.feed();
    if raw_keyless
        && feed.current_source().cursor_capability() != super::feed::CursorCapability::Scalar
    {
        // A keyless raw subscriber cannot ride a segmented lineage:
        // typed disconnect as an EMPTY SSE stream (immediate EOF; the
        // reconnect dispatch answers 400 keyless_live) — a plain 400
        // here would ride a keep-alive connection without EOF.
        crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        drop(subscription);
        let usage = crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0);
        let mt = crate::registry::media_type(&desc.content_type);
        return response_from_stream(
            futures_util::stream::empty::<Result<Bytes, std::io::Error>>(),
            mt != "application/json" && !mt.starts_with("text/"),
            &usage,
        );
    }
    // subscribe() already incremented the count (registry RAII owns
    // attach/detach — no second join here, finding 3's double-count).
    // The handoff state CAPTURED under the subscribe lock is the one a
    // session must use (finding 8): re-reading head/version-watch after
    // the fact reopens the race where a pre-existing subscriber
    // advances the feed between attach and session start, and Phase A
    // would then privately deliver records the shared ring also holds.
    let mut ver_rx = subscription.version_rx();
    // Source generation + receiver CAPTURED atomically with the attach
    // (review round 4: reading them after the fact reopens the
    // construction race).
    let mut src_gen_rx = subscription.gen_rx();
    let join_gen = subscription.join_gen();
    let mut cursor = match start {
        crate::http::StartPos::At(p) => p,
        crate::http::StartPos::Now => feed.current_source().frontier(),
    };
    // PHASE A handoff bound (finding 8): catch up only to the head
    // captured at subscribe time — never chase the moving frontier.
    let join_head = subscription.join_head();

    let ctx = SessionCtx {
        surface: match surface {
            crate::http::SseSurface::Raw => Surface::RawToken,
            crate::http::SseSurface::Product => Surface::Product,
        },
        rk_hash: crate::crypto::stream_hash(&lane_rk),
        epoch,
        key: key.clone(),
        desc: desc.clone(),
        raw_cursor: params.cursor.clone(),
    };
    let binary = {
        let mt = crate::registry::media_type(&desc.content_type);
        mt != "application/json" && !mt.starts_with("text/")
    };
    let usage = crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0);
    let (tx, rx) = tokio::sync::mpsc::channel::<crate::sse::auth::SseChunk>(4);
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
        // Handoff state (finding 2): only a session that HAS reached
        // live may be lag-disconnected. A session still in its initial
        // handoff that the ring overtakes performs durable catch-up
        // again — connecting from an old cursor is NEVER a lag.
        let mut reached_live = false;
        let mut catchup_bound = join_head;
        // Transition retry bound (Stage 6.4): a pending topology change
        // retries briefly; past the bound the session takes the typed
        // disconnect-and-resume fallback.
        let mut transition_retries = 0u32;

        // The handoff loop: durable catch-up to `catchup_bound`, then
        // live consumption. Re-entered when the ring overtakes a
        // not-yet-live session (the catch-up bound refreshes to the
        // current feed head).
        'handoff: loop {
            // DURABLE CATCH-UP: private reads bounded by the catch-up
            // bound — never emit at/after it (finding 8: those records
            // arrive through the shared ring in the live phase). One
            // snapshot per pass: a mid-catch-up swap is picked up on
            // the next pass (the old snapshot stays correct for its
            // own spans).
            let csrc = feed.current_source();
            while cursor < catchup_bound {
                if lease_watch.revoked(&task_state) {
                    return;
                }
                // Round-11.1: the private catch-up read races the
                // client/body — a dropped body (or the body gate's
                // cutoff, which closes the channel) cancels a blocked
                // read instead of letting it run to completion.
                let read = tokio::select! {
                    r = csrc.read_batch(cursor, 1024 * 1024) => r,
                    _ = tx.closed() => return,
                };
                match read {
                    Ok(batch) if batch.scan_to > cursor => {
                        for r in &batch.records {
                            if r.off >= catchup_bound {
                                break;
                            }
                            if r.off < cursor {
                                continue;
                            }
                            // RAW pairing (round 11.8): the pinned
                            // conformance protocol pairs each data
                            // event with ONE flag-carrying control —
                            // the LAST catch-up record that lands
                            // exactly on the durable frontier folds
                            // upToDate into its paired control, and
                            // the live phase suppresses the duplicate
                            // standalone status. Product stays bare.
                            let next = r.off + 1;
                            let at_head = ctx.surface == Surface::RawToken
                                && next >= catchup_bound
                                && batch.scan_to.min(catchup_bound) <= next
                                && next >= csrc.frontier()
                                && !csrc.closed();
                            let frame = ctx.compose_record_flagged(
                                &csrc.prepare_data(r),
                                csrc.locate(next),
                                at_head,
                            );
                            // Test failpoint: authorization-cutoff legs
                            // park the producer before the next send
                            // (every data-send site, all engines).
                            #[cfg(test)]
                            crate::failpoints::pause(
                                crate::failpoints::Fp::SseBeforeSend,
                                &task_desc.name,
                            )
                            .await;
                            if !sse_send_billed(&tx, frame, r.payload.len() as u64, 1).await
                                || lease_watch.revoked(&task_state)
                            {
                                return;
                            }
                            cursor = cursor.max(next);
                            if at_head {
                                need_status = false;
                                last_reported = Some(cursor);
                                reached_live = true;
                            }
                        }
                        // Match-free scanned range still progresses.
                        cursor = cursor.max(batch.scan_to.min(catchup_bound));
                    }
                    // This source's spans are exhausted below the bound
                    // (a swap happened mid-catch-up): the live loop
                    // re-snapshots and, if the ring moved, re-catches-up
                    // through the 'handoff path.
                    Ok(_) => break,
                    Err(e) => {
                        // Round-11.2: fatal span outcomes disconnect
                        // with the typed reason (no terminal) instead
                        // of retrying forever.
                        if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
                            count_cutoff(cut.0);
                            crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::info!(reason = ?cut.0, "livefeed catch-up fatal cutoff");
                            return;
                        }
                        // Source failure mid-catch-up: bounded backoff,
                        // then retry the SAME bound — never a hot loop
                        // (finding 6 discipline applies here too).
                        crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }

            // LIVE phase: shared consumption.
            loop {
                if lease_watch.revoked(&task_state) {
                    return;
                }
                // Wakeups registered at LOOP TOP (finding 7): every
                // state read below is covered by these futures.
                // `ver_rx` is THE persistent receiver — changed()
                // consumes each publication exactly once.
                let mut ver_wait = Box::pin(async {
                    let _ = ver_rx.changed().await;
                });
                let mut gen_wait = Box::pin(async {
                    let _ = src_gen_rx.changed().await;
                });
                // The source snapshot for THIS iteration (Stage 6.1):
                // the waiter must be REGISTERED now, not at the final
                // select! — `enable()` registers eagerly; a source swap
                // mid-iteration is caught by gen_wait, and the next
                // iteration re-snapshots.
                let snap = feed.source_snapshot();
                let cur_src = snap.source.clone();
                if raw_keyless && snap.generation != join_gen {
                    crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!("keyless raw session: source swap; disconnecting to resume");
                    return;
                }
                let src_wait = cur_src.advance_notify().notified();
                tokio::pin!(src_wait);
                src_wait.as_mut().enable();
                match feed.take_visible(cursor) {
                    Take::Lagged { floor } => {
                        if reached_live {
                            crate::sse::auth::sse_stats::FEED_LAG_DISCONNECTS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::warn!(cursor, floor, "lag disconnect below feed floor");
                            return;
                        }
                        // Still in the initial handoff and the ring
                        // overtook us (finding 2): NOT lag — durable
                        // catch-up resumes from our cursor to the
                        // CURRENT feed head, then the live phase
                        // re-enters. No records are skipped.
                        crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        catchup_bound = feed.head();
                        continue 'handoff;
                    }
                    Take::Batch { batch, start_index } => {
                        // RAW pairing (round 11.8): the LAST record's
                        // control carries upToDate when the batch ends
                        // at the durable frontier — the pinned
                        // conformance protocol pairs each data event
                        // with ONE flag-carrying control. Product
                        // framing stays bare + standalone status.
                        let after = cursor.max(batch.scan_to);
                        let head_here = after >= cur_src.frontier() && !cur_src.closed();
                        let last_i = batch.records.len();
                        for (i, r) in batch.records[start_index..].iter().enumerate() {
                            let at_head = head_here && start_index + i + 1 == last_i;
                            let frame = ctx.compose_record_flagged(&r.data_event, r.pos, at_head);
                            #[cfg(test)]
                            crate::failpoints::pause(
                                crate::failpoints::Fp::SseBeforeSend,
                                &task_desc.name,
                            )
                            .await;
                            if !sse_send_billed(&tx, frame, u64::from(r.payload_len), 1).await
                                || lease_watch.revoked(&task_state)
                            {
                                return;
                            }
                        }
                        cursor = after;
                        if head_here && ctx.surface == Surface::RawToken {
                            // The paired control already reported the
                            // head — the standalone status would be a
                            // duplicate the pinned protocol forbids.
                            need_status = false;
                            last_reported = Some(cursor);
                            reached_live = true;
                        } else {
                            need_status = true;
                        }
                        // DRAIN (finding 5): more retained batches may
                        // already be visible — loop and consume them
                        // immediately instead of driving or parking
                        // between batches.
                        continue;
                    }
                    Take::AtHead => {}
                }
                let frontier = cur_src.frontier();
                let closed = cur_src.closed();
                if cursor >= frontier {
                    if closed {
                        // Test failpoint: pause the transition drive as
                        // well (a test can hold a session across a
                        // split+seal before any refresh happens).
                        #[cfg(test)]
                        crate::failpoints::pause(
                            crate::failpoints::Fp::SseFeedBeforeDrive,
                            &task_desc.name,
                        )
                        .await;
                        // Genuine close OR topology transition — ONLY
                        // the drive (driver permit → descriptor refresh)
                        // decides, and it installs the successor source
                        // on a transition (Stage 6.3).
                        let drive = feed.drive_once().await;
                        if drive.is_none() {
                            // CONTENDED at a CLOSED source (round-11.1
                            // herd leg): the lifecycle transition bumps
                            // the version EXACTLY ONCE, and it fires
                            // while the resolving drive still holds the
                            // permit — a session woken by that bump can
                            // lose the permit race, park having already
                            // consumed the only bump, and never wake
                            // (the producer heartbeat that papered
                            // over this is gone). The window is
                            // transient: re-check on a short bound.
                            tokio::select! {
                                _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
                                _ = tx.closed() => return,
                            }
                            continue;
                        }
                        if let Some(outcome) = drive {
                            match outcome {
                                DriveOutcome::Closed => {
                                    // DRAIN BEFORE TERMINAL (round-5
                                    // flake): another session may have
                                    // published a batch AND closed the
                                    // lifecycle between this session's
                                    // take_visible and its drive — a
                                    // terminal here would skip the
                                    // retained records. Loop back and
                                    // consume first.
                                    if !matches!(feed.take_visible(cursor), Take::AtHead) {
                                        continue;
                                    }
                                    // TERMINAL: exactly ONE sealed
                                    // control, then EOF. The terminal
                                    // position is located against the
                                    // CURRENT source — the drive may
                                    // have installed a newer one inside
                                    // this very call (review round 5:
                                    // never emit a terminal cursor
                                    // computed on the predecessor).
                                    let snap3 = feed.source_snapshot();
                                    if !sse_send(
                                        &tx,
                                        ctx.status_ctl(snap3.source.locate(cursor), true),
                                    )
                                    .await
                                    {
                                        return;
                                    }
                                    return; // EOF after THE final control
                                }
                                DriveOutcome::IncarnationClosed(reason) => {
                                    count_cutoff(reason);
                                    crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    tracing::info!(
                                        stream = %sref,
                                        ?reason,
                                        "livefeed source cutoff; disconnecting without terminal"
                                    );
                                    return;
                                }
                                // Swapped and published: loop with
                                // the new source.
                                DriveOutcome::Published => {
                                    transition_retries = 0;
                                    continue;
                                }
                                // Swapped and delivered records to
                                // THIS driver (singleton): they are
                                // ours to emit — dropping them
                                // would silently lose them (the
                                // feed head already advanced).
                                DriveOutcome::Solo { records, scan_to } => {
                                    transition_retries = 0;
                                    // The wire positions are bound in
                                    // the prepared records (located
                                    // with the READING source) — emit
                                    // them as-is. RAW pairing: the
                                    // last record's control carries
                                    // upToDate at the frontier
                                    // (round 11.8, see Take::Batch).
                                    let at_cursor = cursor;
                                    let after = cursor.max(scan_to);
                                    let head_here =
                                        after >= cur_src.frontier() && !cur_src.closed();
                                    let last_off = records
                                        .iter()
                                        .filter(|r| r.offset >= at_cursor)
                                        .map(|r| r.offset)
                                        .max();
                                    for r in records.iter().filter(|r| r.offset >= at_cursor) {
                                        let at_head = head_here && Some(r.offset) == last_off;
                                        let frame = ctx.compose_record_flagged(
                                            &r.data_event,
                                            r.pos,
                                            at_head,
                                        );
                                        #[cfg(test)]
                                        crate::failpoints::pause(
                                            crate::failpoints::Fp::SseBeforeSend,
                                            &task_desc.name,
                                        )
                                        .await;
                                        if !sse_send_billed(&tx, frame, u64::from(r.payload_len), 1)
                                            .await
                                            || lease_watch.revoked(&task_state)
                                        {
                                            return;
                                        }
                                        cursor = cursor.max(r.offset + 1);
                                    }
                                    cursor = after.max(cursor);
                                    if head_here
                                        && last_off.is_some()
                                        && ctx.surface == Surface::RawToken
                                    {
                                        need_status = false;
                                        last_reported = Some(cursor);
                                        reached_live = true;
                                    } else {
                                        need_status = true;
                                    }
                                    continue;
                                }
                                DriveOutcome::Cancelled => return,
                                DriveOutcome::Idle
                                | DriveOutcome::NoProgress
                                | DriveOutcome::SourceFailed => {
                                    // ONE retry task per feed drives
                                    // the unresolved transition; this
                                    // session parks on the version
                                    // watch (round-11.1: no per-
                                    // session timer herd).
                                    feed.schedule_transition_retry();
                                    transition_retries += 1;
                                    if transition_retries > 64 {
                                        crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        tracing::warn!(
                                            stream = %sref,
                                            "topology transition did not settle; disconnecting to resume"
                                        );
                                        return;
                                    }
                                    // Fall through to the park: the resume
                                    // spawn or the next append/heartbeat
                                    // wakes us.
                                }
                            }
                        }
                    } else {
                        transition_retries = 0;
                        // Open frontier: one upToDate per position
                        // (deduped).
                        if need_status && last_reported != Some(cursor) {
                            if !sse_send(&tx, ctx.status_ctl(cur_src.locate(cursor), false)).await
                                || lease_watch.revoked(&task_state)
                            {
                                return;
                            }
                            need_status = false;
                            last_reported = Some(cursor);
                            // An HONEST upToDate was emitted: from here
                            // on, falling below the floor is genuine lag.
                            reached_live = true;
                        }
                    }
                } else {
                    need_status = true;
                    transition_retries = 0;
                }

                // Drive when progress is needed. Contended callers fall
                // to the park below (ver_wait registered at loop top).
                if cursor < frontier {
                    // Test failpoint: pause driving so a test can make
                    // a whole window durable before any read occurs.
                    #[cfg(test)]
                    crate::failpoints::pause(
                        crate::failpoints::Fp::SseFeedBeforeDrive,
                        &task_desc.name,
                    )
                    .await;
                    match feed.drive_once().await {
                        Some(DriveOutcome::Solo { records, scan_to }) => {
                            // RAW pairing at the frontier — see
                            // Take::Batch (round 11.8).
                            let at_cursor = cursor;
                            let after = cursor.max(scan_to);
                            let head_here = after >= cur_src.frontier() && !cur_src.closed();
                            let last_off = records
                                .iter()
                                .filter(|r| r.offset >= at_cursor)
                                .map(|r| r.offset)
                                .max();
                            for r in records.iter().filter(|r| r.offset >= at_cursor) {
                                let at_head = head_here && Some(r.offset) == last_off;
                                let frame =
                                    ctx.compose_record_flagged(&r.data_event, r.pos, at_head);
                                #[cfg(test)]
                                crate::failpoints::pause(
                                    crate::failpoints::Fp::SseBeforeSend,
                                    &task_desc.name,
                                )
                                .await;
                                if !sse_send_billed(&tx, frame, u64::from(r.payload_len), 1).await
                                    || lease_watch.revoked(&task_state)
                                {
                                    return;
                                }
                                cursor = cursor.max(r.offset + 1);
                            }
                            // ADVANCE TO SCANNED BATCH END ONLY (findings
                            // 1+2): never jump to the live frontier and
                            // never stop at the last MATCHING record —
                            // match-free ranges are consumed progress.
                            cursor = after.max(cursor);
                            if head_here && last_off.is_some() && ctx.surface == Surface::RawToken {
                                need_status = false;
                                last_reported = Some(cursor);
                                reached_live = true;
                            } else {
                                need_status = true;
                            }
                            continue;
                        }
                        // A publication or closure changed feed state
                        // (and bumped the version): loop and consume it.
                        Some(DriveOutcome::Published) | Some(DriveOutcome::Closed) => continue,
                        Some(DriveOutcome::IncarnationClosed(reason)) => {
                            count_cutoff(reason);
                            crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::info!(
                                stream = %sref,
                                ?reason,
                                "livefeed source cutoff mid-stream; disconnecting"
                            );
                            return;
                        }
                        // No-progress page or source failure: feed state
                        // did NOT change and the version was NOT bumped —
                        // park rather than spin (finding 6). The next
                        // durable advance or the heartbeat retries.
                        Some(DriveOutcome::NoProgress) => {}
                        Some(DriveOutcome::SourceFailed) => {
                            tracing::warn!("livefeed source read failed; parking until next wake");
                        }
                        // Head already covers the frontier: nothing to do.
                        Some(DriveOutcome::Idle) => {}
                        Some(DriveOutcome::Cancelled) => return,
                        // Contended: park; the winner's publication bumps
                        // ver_wait (registered at loop top).
                        None => {}
                    }
                }
                // Round-11.4 fleet finding: an at-tail session has no
                // read to surface an ownership move (read_batch's check
                // never runs), so the park is guarded. Ordering is
                // register → check → park: src_wait was registered at
                // loop top, and the loser's engine close fires the
                // advance notify — a move landing after this check
                // wakes the park, and the next iteration's check cuts.
                if let Some(reason) = cur_src.cut_off() {
                    count_cutoff(reason);
                    crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!(
                        stream = %sref,
                        ?reason,
                        "livefeed ownership moved under a parked session; disconnecting"
                    );
                    return;
                }
                // Park. Seal-publication convergence is the feed's ONE
                // retry task (round-11.1) — its resolving drive bumps
                // the version and wakes every parked session; no
                // per-session timer exists.
                tokio::select! {
                    _ = &mut ver_wait => {}
                    _ = &mut gen_wait => {}
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
                }
            }
        }
    });

    let stream = futures_util::StreamExt::map(
        GatedSseBody::new(body_state, rx, desc, slot, body_watch, gen_rx),
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
            epoch,
            key: key.clone(),
            desc: desc.clone(),
            raw_cursor: None,
        };
        let seg_id = desc.resolve_segment(&lane_rk).seg_id;
        assert_eq!(seg_id, 5, "resolve must find the nonzero live segment");

        // The emitted bare cursor control carries a KeyCursor naming
        // segment 5, decodable and authenticated.
        let ctl = ctx.record_ctl(
            crate::sse::feed::WirePosition {
                seg_id,
                local_after: 42,
            },
            false,
        );
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
        let status = ctx.status_ctl(
            crate::sse::feed::WirePosition {
                seg_id,
                local_after: 42,
            },
            false,
        );
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

    /// Finding 10 coverage: bytes_out accounts EXACTLY the emitted
    /// frame bytes — one counter increment per body chunk, no more,
    /// no less (deterministic: a private Counters, no global state).
    #[tokio::test]
    async fn bytes_out_accounts_exactly_the_emitted_frames() {
        let usage = std::sync::Arc::new(crate::usage::Counters::default());
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from_static(b"event: data\ndata:AAEC\n\n")),
            Ok(Bytes::from_static(b"event: control\ndata:{}\n\n")),
            Ok(Bytes::from_static(b": keep-alive\n\n")),
        ];
        let want: usize = chunks.iter().map(|c| c.as_ref().unwrap().len()).sum();
        let resp = response_from_stream(futures_util::stream::iter(chunks), true, &usage);
        assert_eq!(
            resp.headers()
                .get("stream-sse-data-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("base64"),
            "binary bodies carry the encoding header"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.len(), want);
        assert_eq!(
            usage.bytes_out.load(Ordering::Relaxed),
            want as u64,
            "bytes_out IS the emitted frame bytes, exactly"
        );
    }
}
