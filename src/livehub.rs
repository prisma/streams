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

pub(crate) struct PreparedBatch {
    pub first: u64,
    /// Offset after the last prepared record.
    pub next: u64,
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
    /// First offset still covered by `batches` (== batches.front().first).
    floor: u64,
    /// Offset after the newest prepared record.
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
}

impl LiveHub {
    fn lifecycle(&self) -> u8 {
        self.lifecycle.load(Ordering::SeqCst)
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
        let mut out = Vec::new();
        let mut flagged: Option<(bool, bool)> = None;
        for (bi, b) in r.batches.iter().enumerate() {
            if b.next <= from {
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
        }
        HubRead::Events(out, r.head, flagged)
    }

    fn publish(&self, batch: PreparedBatch) {
        let mut r = self.ring.lock().unwrap();
        debug_assert!(r.head == 0 || batch.first <= r.head);
        r.head = batch.next;
        r.bytes += batch.bytes;
        if r.floor == 0 && r.batches.is_empty() {
            r.floor = batch.first;
        }
        r.batches.push_back(Arc::new(batch));
        let cap = hub_ring_bytes();
        while r.bytes > cap && r.batches.len() > 1 {
            if let Some(front) = r.batches.pop_front() {
                r.bytes -= front.bytes;
                r.floor = front.next;
            }
        }
        drop(r);
        self.notify.notify_waiters();
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
}

impl HubRegistry {
    pub(crate) fn new() -> Self {
        HubRegistry {
            map: Mutex::new(HashMap::new()),
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
            let hub = Arc::new(LiveHub {
                id,
                ring: Mutex::new(HubRing {
                    head: handle.state.lock().unwrap().durable.next,
                    ..Default::default()
                }),
                notify: tokio::sync::Notify::new(),
                subscribers: AtomicU64::new(1),
                lifecycle: std::sync::atomic::AtomicU8::new(HUB_ACTIVE),
            });
            {
                let mut r = hub.ring.lock().unwrap();
                r.floor = r.head;
            }
            map.insert(id, hub.clone());
            drop(map);
            let pump_hub = hub.clone();
            let pump_state = state.clone();
            let desc = desc.clone();
            let key = key.clone();
            let engine = engine.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                hub_pump(pump_state, pump_hub, desc, key, epoch, engine, handle).await;
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
async fn hub_pump(
    state: Arc<AppState>,
    hub: Arc<LiveHub>,
    desc: crate::registry::StreamDesc,
    key: crate::crypto::StreamKey,
    epoch: [u8; 16],
    engine: Arc<ShardEngine>,
    handle: Arc<crate::shard::StreamHandle>,
) {
    let mut pos = hub.snapshot_head();
    let rk_hash = crate::postings::rk_hash("").0;
    let seg_id = desc.resolve_segment("").seg_id;
    let bill_id = crate::billing::identity_of(&state, &desc);
    let exit = loop {
        // Last-subscriber handshake (#271): claim RETIRING first, then
        // re-check — a join that incremented before the CAS keeps the
        // hub (we CAS back); a join after the CAS sees RETIRING on its
        // recheck and installs a replacement.
        if hub.subscribers.load(Ordering::SeqCst) == 0 {
            if hub
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
        }
        let (end, closed) = {
            let st = handle.state.lock().unwrap();
            (st.durable.next, st.durable.closed)
        };
        if pos < end {
            let read = Box::pin(crate::http::read_records_for_hub(
                &state,
                &desc,
                &key,
                &epoch,
                &handle,
                &engine,
                pos,
                crate::http::MAX_READ_BYTES_PUB,
            ))
            .await;
            match read {
                Ok(out) => {
                    if !out.recs.is_empty() {
                        let will_end =
                            out.completed && out.last.map(|l| l + 1).unwrap_or(pos) >= end;
                        let report_closed = closed
                            && will_end
                            && crate::http::genuine_closure(&state, &desc.sref(), true).await;
                        let n = out.recs.len();
                        let mut events = Vec::with_capacity(n);
                        let mut bytes = 0usize;
                        let mut last_flagged = Bytes::new();
                        let first = out.recs[0].off;
                        let mut next = pos;
                        for (i, r) in out.recs.iter().enumerate() {
                            let ev = crate::http::hub_event(
                                &desc, &key, epoch, rk_hash, seg_id, r, false, false,
                            );
                            if i + 1 == n {
                                last_flagged = Bytes::from(crate::http::hub_event(
                                    &desc,
                                    &key,
                                    epoch,
                                    rk_hash,
                                    seg_id,
                                    r,
                                    will_end,
                                    report_closed,
                                ));
                            }
                            let b = Bytes::from(ev);
                            bytes += b.len();
                            events.push((r.off, b, r.payload.len() as u32));
                            next = r.off + 1;
                            // Delivered-bytes metering happens per
                            // SUBSCRIBER at send time; the pump meters
                            // nothing (decrypt/format is not delivery).
                            let _ = &bill_id;
                        }
                        hub.publish(PreparedBatch {
                            first,
                            next,
                            events,
                            last_flagged,
                            flagged_flags: (will_end, report_closed),
                            bytes,
                        });
                        pos = next;
                        if report_closed {
                            break PumpExit::Closed;
                        }
                        continue;
                    }
                    if let Some(last) = out.last {
                        pos = last + 1;
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
        // Idle poll doubles as the last-subscriber sweep.
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
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
        }
    }

    fn batch(first: u64, n: u64, ev_len: usize) -> PreparedBatch {
        let mut events = Vec::new();
        for off in first..first + n {
            events.push((off, Bytes::from(vec![b'e'; ev_len]), ev_len as u32));
        }
        PreparedBatch {
            first,
            next: first + n,
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
