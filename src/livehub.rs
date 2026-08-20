//! #268 SSE Phase 2: shared live fanout (bench/WORKLOAD-CERT-PLAN.md,
//! SSE execution-model program).
//!
//! One append used to wake N subscriber tasks that each re-read the
//! same range, re-derived the same subkey, re-decrypted the same
//! payload and re-formatted the same SSE text. A LiveHub does that
//! work ONCE per stream: a single pump task (per stream WITH live
//! subscribers — hubs exist only while subscribed) reads on durable
//! advance through the ordinary read pipeline (ring-preferring), and
//! publishes immutable prepared batches whose event text is shared,
//! reference-counted `Bytes`. Subscribers hold a cursor into the hub
//! ring; a subscriber that falls behind the ring floor is DISCONNECTED
//! and resumes from its durable cursor — never a private replay
//! buffer.
//!
//! Scope (v1, env-gated by SSE_LIVE_HUB): PRODUCT surface, unforked
//! single-segment streams, no routing-key filter, durable delivery.
//! Raw-surface controls echo the subscriber's own request cursor and
//! fall back to the Phase-1 path, as do lineage/fork shapes. The
//! up-to-date/closed flags cannot ride shared bytes (a mid-ring reader
//! must not see a stale upToDate), so each batch also carries a
//! FLAGGED variant of its last event, sent only by subscribers that
//! park at the hub head after it.

use crate::http::AppState;
use crate::shard::ShardEngine;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Per-stream cap on prepared-event bytes retained for laggards.
/// Beyond it the floor advances and slower subscribers re-enter
/// durable catch-up (or disconnect). Env SSE_HUB_RING_BYTES.
pub(crate) fn hub_ring_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SSE_HUB_RING_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024 * 1024)
    })
}

/// #273 F7: process-wide prepared-event bytes across ALL hubs. A
/// per-stream cap alone lets 1,000 subscribed streams pin 1,000 ring
/// allowances — the whole 1-GiB host. Publish charges, eviction and
/// hub drop credit; past SSE_HUB_TOTAL_BYTES the publish path goes
/// uncached (advance the head, retain nothing — parked subscribers
/// re-read durably).
static HUB_TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn hub_total_bytes() -> u64 {
    HUB_TOTAL_BYTES.load(Ordering::Relaxed)
}

/// Per-batch read budget for the hub pump. Must sit WELL under the
/// per-hub ring allowance or every live batch would trip the
/// uncached posture (#273). Env SSE_HUB_BATCH_BYTES.
pub(crate) fn hub_batch_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SSE_HUB_BATCH_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256 * 1024)
    })
}

/// Process cap on prepared-event bytes. Env SSE_HUB_TOTAL_BYTES.
pub(crate) fn hub_total_cap() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SSE_HUB_TOTAL_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64 * 1024 * 1024)
    })
}

pub(crate) struct PreparedBatch {
    /// SCANNED range (#272): the keyed reader advances over ranges
    /// containing no matching records, and that consumed position is
    /// first-class — batches cover contiguous scanned ranges
    /// (prev.scan_next == next.scan_first), not just the offsets of
    /// matching events. Losing it lagged the hub head behind the pump
    /// and stalled default-lane cursors under foreign-key appends.
    pub scan_first: u64,
    /// Offset after the last SCANNED record (out.last + 1).
    pub scan_next: u64,
    /// (offset, combined data+control SSE bytes, payload len for the
    /// per-subscriber delivered-bytes meter) — control carries NO
    /// up-to-date/closed flags.
    pub events: Vec<(u64, Bytes, u32)>,
    /// The last event of this batch with the flags the pump observed
    /// at prepare time (upToDate, and closed when genuine). Sent
    /// instead of the plain last event by a subscriber that parks at
    /// the hub head right after this batch.
    pub last_flagged: Bytes,
    /// (up_to_date, closed) carried by `last_flagged` — the subscriber
    /// cannot see inside the shared bytes, and it must know which
    /// terminal facts were already conveyed to avoid both silence and
    /// duplicate final controls (#270).
    pub flagged_flags: (bool, bool),
    pub bytes: usize,
}

#[derive(Default)]
struct HubRing {
    batches: VecDeque<Arc<PreparedBatch>>,
    bytes: usize,
    /// First offset still covered by `batches`
    /// (== batches.front().scan_first; == head when empty).
    floor: u64,
    /// The SCAN head: offset after the newest scanned record. May be
    /// ahead of the last batch's scan_next (scanned-no-match progress
    /// advances it without a batch).
    head: u64,
}

/// Hub lifecycle (#271). ACTIVE hubs accept joins; RETIRING is the
/// pump's last-subscriber exit window (a racing join backs out and
/// creates a replacement); CLOSED hubs stay drainable; DEAD hubs
/// disconnect. Stored as one atomic so subscribe's
/// increment-then-recheck and the pump's CAS handshake close the
/// join-vs-exit race without a lock.
pub(crate) const HUB_ACTIVE: u8 = 0;
pub(crate) const HUB_RETIRING: u8 = 1;
pub(crate) const HUB_CLOSED: u8 = 2;
pub(crate) const HUB_DEAD: u8 = 3;

/// #273 F5: the ONE per-stream read context. Subscribers hold the
/// hub Arc, a cursor and small connection state — never their own
/// descriptor/key/engine/handle clones (a StreamDesc carries many
/// String/Vec allocations; N subscribers used to carry N copies).
pub(crate) struct HubContext {
    pub(crate) desc: crate::registry::StreamDesc,
    pub(crate) key: crate::crypto::StreamKey,
    pub(crate) epoch: [u8; 16],
    pub(crate) engine: Arc<ShardEngine>,
    pub(crate) handle: Arc<crate::shard::StreamHandle>,
    pub(crate) rk_hash: [u8; 16],
    pub(crate) seg_id: u32,
}

pub(crate) struct LiveHub {
    /// Registry key: the stream INCARNATION (handle.hash — the segment
    /// identity), never the name-stable route. A recreated stream
    /// mints a new identity, so its subscribers can never attach to a
    /// previous incarnation's hub (#271).
    pub(crate) id: [u8; 16],
    ring: Mutex<HubRing>,
    pub(crate) notify: tokio::sync::Notify,
    pub(crate) subscribers: AtomicU64,
    lifecycle: std::sync::atomic::AtomicU8,
    /// Process-total accounting target — the global gauge in
    /// production, a private counter in unit tests (the global is
    /// shared across the parallel test binary).
    total: &'static AtomicU64,
    /// None only in unit-test hubs that never touch a pump or
    /// subscriber path.
    pub(crate) ctx: Option<Arc<HubContext>>,
}

impl LiveHub {
    fn lifecycle(&self) -> u8 {
        self.lifecycle.load(Ordering::SeqCst)
    }
}

impl Drop for LiveHub {
    fn drop(&mut self) {
        // #273 F7: the last reference releases whatever the ring still
        // retains against the process cap.
        let bytes = self.ring.lock().unwrap().bytes;
        if bytes > 0 {
            self.total.fetch_sub(bytes as u64, Ordering::Relaxed);
        }
    }
}

/// What a subscriber gets when it asks for everything from `from`.
pub(crate) enum HubRead {
    /// Prepared (event bytes, payload len) at and after `from`, plus
    /// the head after them, plus Some((up_to_date, closed)) when the
    /// LAST returned event is the flagged variant (None = plain
    /// mid-ring event; the subscriber has NOT been told it is at
    /// head).
    Events(Vec<(Bytes, u32)>, u64, Option<(bool, bool)>),
    /// Scanned progress with no deliverable events between `from` and
    /// the returned scan head: advance the cursor (the subscriber's
    /// next status control conveys it) without sending data (#272).
    Progress(u64),
    /// `from` is below the ring floor — re-run durable catch-up.
    BelowFloor,
    /// Nothing new; park on notify. Bool = stream closed (end after
    /// draining).
    AtHead(bool),
    /// Pump died; disconnect and let the client resume by cursor.
    Dead,
}

impl LiveHub {
    pub(crate) fn snapshot_head(&self) -> u64 {
        self.ring.lock().unwrap().head
    }

    pub(crate) fn read_from(&self, from: u64) -> HubRead {
        let lc = self.lifecycle();
        // RETIRING is visible only to a join backing out mid-handshake;
        // treat it like DEAD defensively — the caller re-subscribes.
        if lc == HUB_DEAD || lc == HUB_RETIRING {
            return HubRead::Dead;
        }
        let r = self.ring.lock().unwrap();
        if from >= r.head {
            return HubRead::AtHead(lc == HUB_CLOSED);
        }
        if from < r.floor {
            return HubRead::BelowFloor;
        }
        // #273 F6: AT MOST ONE batch per call. The whole-suffix Vec
        // gave every waking subscriber a private clone of the entire
        // ring's metadata held across bounded-deadline sends; one
        // batch bounds the private allocation by the per-batch cap,
        // and the caller simply asks again from its new cursor.
        let mut out = Vec::new();
        let mut flagged: Option<(bool, bool)> = None;
        let mut next = r.head;
        for (bi, b) in r.batches.iter().enumerate() {
            if b.scan_next <= from {
                continue;
            }
            let last_batch = bi + 1 == r.batches.len();
            let n = b.events.len();
            for (ei, (off, ev, plen)) in b.events.iter().enumerate() {
                if *off < from {
                    continue;
                }
                let last_event = last_batch && ei + 1 == n;
                if last_event {
                    // Ring-last: the flagged variant, and the caller
                    // learns exactly which terminal facts it conveyed.
                    out.push((b.last_flagged.clone(), *plen));
                    flagged = Some(b.flagged_flags);
                } else {
                    out.push((ev.clone(), *plen));
                }
            }
            // The cursor lands after THIS batch's scanned range; only
            // the ring-last batch jumps to the (possibly further)
            // scan head.
            next = if last_batch { r.head } else { b.scan_next };
            if !out.is_empty() {
                break;
            }
        }
        if out.is_empty() {
            // `from` lies in scanned-no-match territory (foreign-key
            // ranges, or head advanced without a batch): pure cursor
            // progress (#272).
            return HubRead::Progress(r.head);
        }
        HubRead::Events(out, next, flagged)
    }

    fn publish(&self, batch: PreparedBatch) {
        let mut r = self.ring.lock().unwrap();
        // Contiguous scanned coverage: the pump publishes every scan
        // range in order (batches or bare head advances), so a batch
        // always starts at the current scan head.
        debug_assert!(
            batch.scan_first <= r.head,
            "scan gap: batch {} vs head {}",
            batch.scan_first,
            r.head
        );
        // #273 F7: a batch above the per-hub allowance, or one that
        // would break the PROCESS cap, is never retained — the range
        // goes UNCACHED: the head jumps past it, the floor follows,
        // and parked subscribers re-read it durably (BelowFloor).
        // Anything previously retained is credited and dropped with
        // it (it sits behind the new floor anyway).
        let over_hub = batch.bytes > hub_ring_bytes();
        let over_global = self
            .total
            .load(Ordering::Relaxed)
            .saturating_add(batch.bytes as u64)
            > hub_total_cap();
        if over_hub || over_global {
            self.total.fetch_sub(r.bytes as u64, Ordering::Relaxed);
            r.bytes = 0;
            r.batches.clear();
            r.head = r.head.max(batch.scan_next);
            r.floor = r.head;
            drop(r);
            self.notify.notify_waiters();
            return;
        }
        r.head = r.head.max(batch.scan_next);
        r.bytes += batch.bytes;
        self.total.fetch_add(batch.bytes as u64, Ordering::Relaxed);
        if r.batches.is_empty() {
            r.floor = batch.scan_first;
        }
        r.batches.push_back(Arc::new(batch));
        let cap = hub_ring_bytes();
        while r.bytes > cap && r.batches.len() > 1 {
            if let Some(front) = r.batches.pop_front() {
                r.bytes -= front.bytes;
                self.total.fetch_sub(front.bytes as u64, Ordering::Relaxed);
                r.floor = front.scan_next;
            }
        }
        drop(r);
        self.notify.notify_waiters();
    }

    /// Scanned-no-match progress: advance the scan head with no batch
    /// (#272) — parked subscribers wake and convey the new cursor.
    fn advance_head(&self, new_head: u64) {
        let mut r = self.ring.lock().unwrap();
        if new_head > r.head {
            r.head = new_head;
            if r.batches.is_empty() {
                r.floor = new_head;
            }
            drop(r);
            self.notify.notify_waiters();
        }
    }

    fn mark_closed(&self) {
        self.lifecycle.store(HUB_CLOSED, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn mark_dead(&self) {
        self.lifecycle.store(HUB_DEAD, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

/// Process-wide registry: stream hash -> live hub. Hubs are created on
/// the first eligible subscriber and removed when their pump exits
/// (last subscriber gone, closure, or fence).
pub(crate) struct HubRegistry {
    map: Mutex<HashMap<[u8; 16], Arc<LiveHub>>>,
    /// #274 F8: DIRECT (Phase-1 path) subscriber counts per stream
    /// incarnation. The FIRST subscriber rides direct — a hub + pump
    /// per single-subscriber stream is strictly more machinery than
    /// Phase 1 (the 100k x 1 shape). The second concurrent subscriber
    /// promotes the stream to a hub; the existing direct one stays
    /// direct until reconnect (one duplicate reader on a fanned-out
    /// stream is negligible).
    direct: Mutex<HashMap<[u8; 16], u32>>,
}

/// RAII registration for a DIRECT (non-hub) subscriber. Held on the
/// response body; dropping it un-registers the stream's direct count.
pub(crate) struct DirectGuard {
    state: Arc<AppState>,
    id: [u8; 16],
}

impl Drop for DirectGuard {
    fn drop(&mut self) {
        let mut d = self.state.live_hubs.direct.lock().unwrap();
        if let Some(n) = d.get_mut(&self.id) {
            *n -= 1;
            if *n == 0 {
                d.remove(&self.id);
            }
        }
    }
}

/// F8 promotion decision for one arriving subscriber: Some(guard)
/// means ride the DIRECT path (this is the stream's first live
/// subscriber and no hub exists); None means join/create the hub.
pub(crate) fn join_direct_or_promote(state: &Arc<AppState>, id: [u8; 16]) -> Option<DirectGuard> {
    let reg = &state.live_hubs;
    if reg.map.lock().unwrap().contains_key(&id) {
        return None;
    }
    let mut d = reg.direct.lock().unwrap();
    match d.get(&id) {
        None => {
            d.insert(id, 1);
            Some(DirectGuard {
                state: state.clone(),
                id,
            })
        }
        Some(_) => None,
    }
}

impl HubRegistry {
    pub(crate) fn new() -> Self {
        HubRegistry {
            map: Mutex::new(HashMap::new()),
            direct: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn hub_count(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Get or create the hub for a stream INCARNATION, spawning its
    /// pump on creation. Keyed by handle.hash (#271): a recreated
    /// stream's new identity can never resolve to a previous
    /// incarnation's hub. Joins increment-then-recheck against the
    /// lifecycle so a pump retiring on last-subscriber either serves
    /// the racing join (CAS back to ACTIVE) or the join backs out and
    /// installs a replacement — it never receives a retiring hub.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn subscribe(
        state: &Arc<AppState>,
        desc: &crate::registry::StreamDesc,
        key: crate::crypto::StreamKey,
        epoch: [u8; 16],
        engine: Arc<ShardEngine>,
        handle: Arc<crate::shard::StreamHandle>,
    ) -> Arc<LiveHub> {
        let id = handle.hash;
        loop {
            let mut map = state.live_hubs.map.lock().unwrap();
            if let Some(h) = map.get(&id) {
                let h = h.clone();
                drop(map);
                if h.lifecycle() == HUB_ACTIVE {
                    h.subscribers.fetch_add(1, Ordering::SeqCst);
                    if h.lifecycle() == HUB_ACTIVE {
                        return h;
                    }
                    // Lost the race with the pump's retirement CAS:
                    // back out and install a replacement.
                    h.subscribers.fetch_sub(1, Ordering::SeqCst);
                }
                state.live_hubs.remove_if_same(id, &h);
                continue;
            }
            let ctx = Arc::new(HubContext {
                desc: desc.clone(),
                key: key.clone(),
                epoch,
                engine: engine.clone(),
                handle: handle.clone(),
                rk_hash: crate::postings::rk_hash("").0,
                seg_id: desc.resolve_segment("").seg_id,
            });
            let hub = Arc::new(LiveHub {
                id,
                ring: Mutex::new(HubRing {
                    head: handle.state.lock().unwrap().durable.next,
                    ..Default::default()
                }),
                notify: tokio::sync::Notify::new(),
                subscribers: AtomicU64::new(1),
                lifecycle: std::sync::atomic::AtomicU8::new(HUB_ACTIVE),
                total: &HUB_TOTAL_BYTES,
                ctx: Some(ctx),
            });
            {
                let mut r = hub.ring.lock().unwrap();
                r.floor = r.head;
            }
            map.insert(id, hub.clone());
            drop(map);
            let pump_hub = hub.clone();
            let pump_state = state.clone();
            tokio::spawn(async move {
                hub_pump(pump_state, pump_hub).await;
            });
            return hub;
        }
    }

    /// Remove ONLY if the entry is still this exact hub — an
    /// unconditional remove could delete a replacement installed after
    /// this hub retired (#271 ABA).
    fn remove_if_same(&self, id: [u8; 16], expected: &Arc<LiveHub>) {
        let mut map = self.map.lock().unwrap();
        if map
            .get(&id)
            .is_some_and(|actual| Arc::ptr_eq(actual, expected))
        {
            map.remove(&id);
        }
    }
}

/// Why a pump stopped (#270). A CLOSED hub and a DEAD hub are
/// different protocol states with different subscriber obligations —
/// closed hubs stay drainable (subscribers finish with the sealed
/// control); dead hubs disconnect (transition or fence: the client
/// resumes by cursor). They must never share a cleanup path: the
/// original code marked every exiting hub dead, and read_from checks
/// dead FIRST, so notified subscribers woke into Dead and dropped the
/// final batch and sealed control with no await in between to let
/// them drain.
enum PumpExit {
    Closed,
    Transition,
    ReadError,
    NoSubscribers,
}

/// One pump per subscribed stream: wake on durable advance, read once
/// through the ordinary pipeline (ring-preferring), decrypt once,
/// format once, publish shared bytes. Exits when the last subscriber
/// leaves, on genuine closure, or on a read error (fence).
async fn hub_pump(state: Arc<AppState>, hub: Arc<LiveHub>) {
    let ctx = hub.ctx.clone().expect("production hubs carry a context");
    let (desc, key, epoch, engine, handle) =
        (&ctx.desc, &ctx.key, ctx.epoch, &ctx.engine, &ctx.handle);
    let (rk_hash, seg_id) = (ctx.rk_hash, ctx.seg_id);
    let mut pos = hub.snapshot_head();
    let exit = loop {
        // Last-subscriber handshake (#271): claim RETIRING first, then
        // re-check — a join that incremented before the CAS keeps the
        // hub (we CAS back); a join after the CAS sees RETIRING on its
        // recheck and installs a replacement.
        if hub.subscribers.load(Ordering::SeqCst) == 0
            && hub
                .lifecycle
                .compare_exchange(HUB_ACTIVE, HUB_RETIRING, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            if hub.subscribers.load(Ordering::SeqCst) == 0 {
                break PumpExit::NoSubscribers;
            }
            let _ = hub.lifecycle.compare_exchange(
                HUB_RETIRING,
                HUB_ACTIVE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
        let (end, closed) = {
            let st = handle.state.lock().unwrap();
            (st.durable.next, st.durable.closed)
        };
        if pos < end {
            // #273: per-batch budget well under the ring allowance —
            // an 8 MiB read would trip the uncached posture on every
            // live batch.
            let read = Box::pin(crate::http::read_records_for_hub(
                &state,
                desc,
                key,
                &epoch,
                handle,
                engine,
                pos,
                hub_batch_bytes(),
            ))
            .await;
            match read {
                Ok(out) => {
                    // Scanned progress is first-class (#272): the keyed
                    // reader advances over non-matching ranges and that
                    // consumed position IS the batch boundary.
                    let scan_next = out.last.map(|l| l + 1).unwrap_or(pos).max(pos);
                    if !out.recs.is_empty() {
                        let will_end = out.completed && scan_next >= end;
                        let report_closed = closed
                            && will_end
                            && crate::http::genuine_closure(&state, &desc.sref(), true).await;
                        let _ = ();
                        let n = out.recs.len();
                        let mut events = Vec::with_capacity(n);
                        let mut bytes = 0usize;
                        let mut last_flagged = Bytes::new();
                        for (i, r) in out.recs.iter().enumerate() {
                            let last_rec = i + 1 == n;
                            // The LAST event's control names scan_next
                            // — not off+1 — so a resuming client skips
                            // trailing non-matching records (#272).
                            let ctl_at = if last_rec { scan_next } else { r.off + 1 };
                            let ev = crate::http::hub_event_at(
                                desc, key, epoch, rk_hash, seg_id, r, ctl_at, false, false,
                            );
                            if last_rec {
                                last_flagged = Bytes::from(crate::http::hub_event_at(
                                    desc,
                                    key,
                                    epoch,
                                    rk_hash,
                                    seg_id,
                                    r,
                                    ctl_at,
                                    will_end,
                                    report_closed,
                                ));
                            }
                            let b = Bytes::from(ev);
                            bytes += b.len();
                            events.push((r.off, b, r.payload.len() as u32));
                        }
                        hub.publish(PreparedBatch {
                            scan_first: pos,
                            scan_next,
                            events,
                            last_flagged,
                            flagged_flags: (will_end, report_closed),
                            bytes,
                        });
                        pos = scan_next;
                        if report_closed {
                            break PumpExit::Closed;
                        }
                        continue;
                    }
                    if scan_next > pos {
                        // Foreign-key-only range: pure cursor progress.
                        hub.advance_head(scan_next);
                        pos = scan_next;
                        continue;
                    }
                }
                Err(_) => {
                    break PumpExit::ReadError;
                }
            }
        }
        if closed && pos >= end {
            if crate::http::genuine_closure(&state, &desc.sref(), true).await {
                break PumpExit::Closed;
            }
            break PumpExit::Transition;
        }
        let notified = handle.notify.notified();
        let cur = handle.state.lock().unwrap().durable.next;
        if cur > pos {
            continue;
        }
        // #274 F9: the last SubGuard drop notifies handle.notify, and
        // `notified` was created BEFORE this re-check, so a drop
        // landing after it still wakes the select — no lost-wakeup
        // window, no per-pump poll timer. The long sleep is a pure
        // belt-and-braces fallback.
        if hub.subscribers.load(Ordering::SeqCst) == 0 {
            continue;
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
        }
    };
    // #270: closed and dead are DIFFERENT protocol states. A closed
    // hub stays drainable — subscribers finish with the sealed
    // control; only transitions and read failures disconnect for
    // cursor resume. Removal is CONDITIONAL (#271): a replacement hub
    // installed after this one retired must survive this cleanup.
    state.live_hubs.remove_if_same(hub.id, &hub);
    match exit {
        PumpExit::Closed => hub.mark_closed(),
        PumpExit::Transition | PumpExit::ReadError => hub.mark_dead(),
        PumpExit::NoSubscribers => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub(head: u64) -> LiveHub {
        LiveHub {
            id: [7u8; 16],
            ring: Mutex::new(HubRing {
                head,
                floor: head,
                ..Default::default()
            }),
            notify: tokio::sync::Notify::new(),
            subscribers: AtomicU64::new(1),
            lifecycle: std::sync::atomic::AtomicU8::new(HUB_ACTIVE),
            total: Box::leak(Box::new(AtomicU64::new(0))),
            ctx: None,
        }
    }

    fn batch(first: u64, n: u64, ev_len: usize) -> PreparedBatch {
        let mut events = Vec::new();
        for off in first..first + n {
            events.push((off, Bytes::from(vec![b'e'; ev_len]), ev_len as u32));
        }
        PreparedBatch {
            scan_first: first,
            scan_next: first + n,
            events,
            last_flagged: Bytes::from_static(b"FLAGGED"),
            flagged_flags: (true, false),
            bytes: n as usize * ev_len,
        }
    }

    /// Cursor mechanics: at-head parks, mid-ring returns exactly the
    /// suffix, ring-last returns the flagged variant, below-floor
    /// demands durable catch-up, dead disconnects.
    #[test]
    fn read_from_cursor_mechanics() {
        let h = hub(10);
        assert!(matches!(h.read_from(10), HubRead::AtHead(false)));
        h.publish(batch(10, 4, 8));
        // Full drain: 4 events, last one flagged.
        let HubRead::Events(evs, head, flagged) = h.read_from(10) else {
            panic!("expected events");
        };
        assert_eq!((evs.len(), head, flagged), (4, 14, Some((true, false))));
        assert_eq!(&evs[3].0[..], b"FLAGGED");
        assert_eq!(&evs[0].0[..], &[b'e'; 8][..]);
        // Mid-ring suffix.
        let HubRead::Events(evs, head, _) = h.read_from(12) else {
            panic!("expected events");
        };
        assert_eq!((evs.len(), head), (2, 14));
        // Below the floor after eviction-by-construction.
        assert!(matches!(h.read_from(9), HubRead::BelowFloor));
        h.mark_dead();
        assert!(matches!(h.read_from(12), HubRead::Dead));
    }

    /// Laggards never pin memory: past the byte cap the floor advances
    /// and a subscriber behind it is told to re-run durable catch-up —
    /// the ring keeps at most ~cap bytes regardless of subscriber
    /// speed.
    #[test]
    fn eviction_advances_floor_and_bounds_bytes() {
        let h = hub(0);
        let cap = hub_ring_bytes();
        let per = cap / 4;
        for i in 0..8u64 {
            h.publish(batch(i * 10, 10, per / 10));
        }
        let r = h.ring.lock().unwrap();
        assert!(
            r.bytes <= cap + per,
            "ring bytes {} way over cap {cap}",
            r.bytes
        );
        assert!(r.floor > 0, "floor must have advanced");
        let floor = r.floor;
        drop(r);
        assert!(matches!(h.read_from(0), HubRead::BelowFloor));
        // At or past the floor still serves from the ring.
        assert!(matches!(h.read_from(floor), HubRead::Events(..)));
    }

    /// Closed propagates to parked readers; a subscriber at head sees
    /// AtHead(true) and can emit its per-subscriber final control.
    #[test]
    fn closed_reaches_parked_readers() {
        let h = hub(5);
        h.mark_closed();
        assert!(matches!(h.read_from(5), HubRead::AtHead(true)));
    }
}

#[cfg(test)]
mod tests_memory {
    use super::*;

    fn hub(head: u64) -> LiveHub {
        LiveHub {
            id: [8u8; 16],
            ring: Mutex::new(HubRing {
                head,
                floor: head,
                ..Default::default()
            }),
            notify: tokio::sync::Notify::new(),
            subscribers: AtomicU64::new(1),
            lifecycle: std::sync::atomic::AtomicU8::new(HUB_ACTIVE),
            total: Box::leak(Box::new(AtomicU64::new(0))),
            ctx: None,
        }
    }

    fn batch(first: u64, n: u64, ev_len: usize) -> PreparedBatch {
        let mut events = Vec::new();
        for off in first..first + n {
            events.push((off, Bytes::from(vec![b'e'; ev_len]), ev_len as u32));
        }
        PreparedBatch {
            scan_first: first,
            scan_next: first + n,
            events,
            last_flagged: Bytes::from_static(b"FLAGGED"),
            flagged_flags: (true, false),
            bytes: n as usize * ev_len,
        }
    }

    /// F6 red: read_from must return AT MOST ONE batch per call — the
    /// whole-suffix Vec gave every waking subscriber a private clone
    /// of the entire ring's metadata, held across up-to-10s sends.
    #[test]
    fn read_from_returns_one_batch_per_call() {
        let h = hub(0);
        h.publish(batch(0, 4, 8));
        h.publish(batch(4, 4, 8));
        let HubRead::Events(evs, next, flagged) = h.read_from(0) else {
            panic!("expected events");
        };
        assert_eq!(evs.len(), 4, "ONE batch per call, not the whole suffix");
        assert_eq!(next, 4, "cursor advances to the batch's scan_next");
        assert!(flagged.is_none(), "mid-ring batch carries no flags");
        let HubRead::Events(evs, next, flagged) = h.read_from(next) else {
            panic!("expected second batch");
        };
        assert_eq!(evs.len(), 4);
        assert_eq!(next, 8, "ring-last batch advances to head");
        assert_eq!(flagged, Some((true, false)), "ring-last carries flags");
    }

    /// F7 red: a batch larger than the per-hub allowance must NOT be
    /// retained — the range goes uncached (floor == head jumps past
    /// it) and parked subscribers re-read durably; today the ring
    /// keeps any-size batches (the 8 MiB read can pin 8x the cap).
    #[test]
    fn oversized_batch_is_not_retained() {
        let h = hub(0);
        let cap = hub_ring_bytes();
        h.publish(batch(0, 2, cap)); // 2x cap in one batch
        let r = h.ring.lock().unwrap();
        assert_eq!(r.bytes, 0, "oversized batch must not be retained");
        assert!(r.batches.is_empty());
        assert_eq!(
            (r.floor, r.head),
            (2, 2),
            "range uncached: floor==head past it"
        );
        drop(r);
        assert!(matches!(h.read_from(0), HubRead::BelowFloor));
    }

    /// F7: prepared-bytes accounting moves up on publish, down on
    /// eviction, and to zero when the hub drops (asserted against the
    /// hub's own counter — the process global is shared across the
    /// parallel test binary).
    #[test]
    fn global_prepared_bytes_accounting() {
        let h = hub(0);
        let total = h.total;
        h.publish(batch(0, 4, 64));
        assert_eq!(total.load(Ordering::Relaxed), 4 * 64);
        let cap = hub_ring_bytes();
        let per = cap / 2;
        h.publish(batch(4, 2, per)); // evicts the first batch
        assert!(total.load(Ordering::Relaxed) <= (2 * per) as u64);
        drop(h);
        assert_eq!(
            total.load(Ordering::Relaxed),
            0,
            "dropping the hub credits its retained bytes"
        );
    }
}
