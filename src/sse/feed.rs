//! LiveFeed — the one per-stream subscription engine (LIVE-FEED
//! Stage 2). Replaces both the direct reader and the LiveHub pump:
//!
//! * NO dedicated pump task. When progress is needed, one session
//!   acquires the feed's driver permit (`drive_once`), reads at most
//!   one bounded source batch, formats each payload event ONCE, and
//!   publishes it to the shared ring — releasing the permit BEFORE any
//!   socket write. If the driving session disappears the permit drops.
//! * Adaptive retention: with a single subscriber nothing is retained
//!   (the batch is handed straight back); with two or more, prepared
//!   batches are retained in a bounded ring so every consumer reads
//!   shared prepared bytes without touching durable history again.
//! * Wakeups ride a `watch` generation, so a publication landing
//!   between a session's state check and its park can never be missed.
//!
//! The feed tracks the LIVE TAIL only: its scan head seeds from the
//! durable frontier at creation. A subscriber connecting BEHIND that
//! point performs its own durable catch-up (the session's initial
//! phase), per the lag contract in docs/LIVE-FEED.md §Lag policy.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Identity of a feed: the stream INCARNATION plus the record
/// selector lane. Delete/recreate mints a new epoch and therefore a
/// new feed; the default-key lane never mixes with a keyed lane.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct FeedKey {
    pub(crate) identity: [u8; 16],
}

impl FeedKey {
    pub(crate) fn of(identity: [u8; 16]) -> Self {
        Self { identity }
    }
}

/// One formatted record: the expensive frame is produced ONCE. The
/// per-record cursor control folds into the frame (lane-global);
/// `upToDate`/`sealed` are folded too when the scan reached the
/// durable end — they are lane-global facts, never per-session.
/// The standalone session status remains only for the empty
/// connect-at-tail case.
pub(crate) struct PreparedRecord {
    pub(crate) offset: u64,
    pub(crate) data_event: Bytes,
    pub(crate) payload_len: u32,
    /// True when THIS frame carried the terminal sealed control.
    pub(crate) sealed: bool,
}

pub(crate) struct PreparedBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Arc<[PreparedRecord]>,
    /// Reserved-memory charge GOVERNING retention (event bytes +
    /// element metadata + fixed allowance), mirroring the hub's
    /// conservative accounting.
    pub(crate) charge: usize,
}

/// Where reads come from. v0 wraps the existing single-segment read
/// pipeline; fork/lineage adapters join later with the same shape.
#[async_trait::async_trait]
pub(crate) trait FeedSourceRead: Send + Sync {
    /// Read up to `max_bytes` of DURABLE records from `from`. Returns
    /// them in order plus whether this scan reached the durable end.
    async fn read(
        &self,
        from: u64,
        max_bytes: usize,
    ) -> anyhow::Result<(Vec<crate::http::PlainRec>, bool)>;
    /// The durable frontier (next offset) right now.
    fn frontier(&self) -> u64;
    /// Whether the segment is closed for appends.
    fn closed(&self) -> bool;
    /// The descriptor (media type drives data-event encoding).
    fn desc(&self) -> &crate::registry::StreamDesc;
    /// Encode one record's DATA event (control composed by the feed,
    /// which owns the lane-global flag decision).
    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes;
    /// The lane's cursor token naming `offset_after`.
    fn ctl_token(&self, offset_after: u64) -> String;
    /// Wake source: fired on every durable advance and close. Sessions
    /// park on this BESIDES the feed version watch — with no pump task,
    /// appends must be what wakes the first driver.
    fn advance_notify(&self) -> &tokio::sync::Notify;
}

/// Conservative reservation charge for a prepared batch.
fn charge_for(events: &[PreparedRecord]) -> usize {
    let ev: usize = events.iter().map(|r| r.data_event.len()).sum();
    ev + events.len() * 64 + 256
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Active,
    /// Genuine close observed AND fully drained to the frontier.
    Closed,
}

struct FeedState {
    head: u64,
    floor: u64,
    version: u64,
    batches: VecDeque<Arc<PreparedBatch>>,
    charge: usize,
    lifecycle: Lifecycle,
}

pub(crate) struct LiveFeed {
    key: FeedKey,
    src: Arc<dyn FeedSourceRead>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    retained_charge: AtomicUsize,
    /// Metric / entered-proof: how many SOURCE reads were issued.
    /// Shared preparation means fanout N consumes with ~1 read per
    /// batch window instead of N.
    source_reads: AtomicU64,
    ring_budget: usize,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

impl LiveFeed {
    pub(crate) fn new(key: FeedKey, src: Arc<dyn FeedSourceRead>, ring_budget: usize) -> Arc<Self> {
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            key,
            src,
            st: Mutex::new(FeedState {
                head,
                floor: head,
                version: 0,
                batches: VecDeque::new(),
                charge: 0,
                lifecycle: Lifecycle::Active,
            }),
            changed,
            driving: AtomicBool::new(false),
            subscribers: AtomicU64::new(0),
            retained_charge: AtomicUsize::new(0),
            source_reads: AtomicU64::new(0),
            ring_budget,
        })
    }

    /// Subscribe FIRST (returns the current head cursor and a version
    /// receiver), so no publication can slip between joining and the
    /// session's first state check.
    pub(crate) fn join(&self) -> (u64, tokio::sync::watch::Receiver<u64>) {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        let rx = self.changed.subscribe();
        let head = self.st.lock().unwrap().head;
        (head, rx)
    }

    pub(crate) fn leave(&self) {
        self.subscribers.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn lifecycle(&self) -> LifecycleState {
        if self.st.lock().unwrap().lifecycle == Lifecycle::Closed {
            LifecycleState::Closed
        } else {
            LifecycleState::Active
        }
    }

    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    pub(crate) fn source_read_count(&self) -> u64 {
        self.source_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn retained(&self) -> usize {
        self.retained_charge.load(Ordering::Relaxed)
    }

    /// Consume retained records at/after `cursor`. `Lagged` means the
    /// cursor fell below the retention floor (lag contract:
    /// disconnect-and-resume, never private historical re-reading).
    pub(crate) fn take_visible(&self, cursor: u64) -> Take {
        let st = self.st.lock().unwrap();
        if cursor < st.floor {
            return Take::Lagged { floor: st.floor };
        }
        let mut out = Vec::new();
        let mut next = cursor;
        for b in &st.batches {
            if b.scan_to <= cursor {
                continue;
            }
            for r in b.records.iter().filter(|r| r.offset >= cursor) {
                out.push((r.offset, r.data_event.clone(), r.payload_len, r.sealed));
                next = next.max(r.offset + 1);
            }
        }
        if next > cursor {
            Take::Records { records: out, next }
        } else {
            Take::AtHead {
                closed: matches!(st.lifecycle, Lifecycle::Closed),
            }
        }
    }

    /// Single-flight drive: ONE session reads the next bounded source
    /// batch and formats each payload once. With zero-or-one
    /// subscribers nothing is retained — the batch is returned to its
    /// caller directly (`Solo`). Otherwise the batch publishes to the
    /// ring, the floor advances under the budget, and every parked
    /// session wakes on the version bump.
    ///
    /// Returns `None` immediately when another session already holds
    /// the permit (that driver will publish + wake everyone shortly).
    pub(crate) async fn drive_once(&self) -> Option<DriveOutcome> {
        if self
            .driving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        let out = self.drive_under_permit().await;
        // Release BEFORE any socket write by any consumer of the
        // outcome: the caller only receives this value afterwards.
        self.driving.store(false, Ordering::SeqCst);
        Some(out)
    }

    /// Test hook: hold the driver permit to prove contention paths.
    #[cfg(test)]
    pub(crate) fn hold_permit_for_test(&self) -> PermitGuard<'_> {
        self.driving.store(true, Ordering::SeqCst);
        PermitGuard(self)
    }

    #[cfg(test)]
    pub(crate) fn permit_held(&self) -> bool {
        self.driving.load(Ordering::SeqCst)
    }

    async fn drive_under_permit(&self) -> DriveOutcome {
        let (head, active) = {
            let st = self.st.lock().unwrap();
            (st.head, st.lifecycle == Lifecycle::Active)
        };
        if !active {
            return DriveOutcome::Idle;
        }
        if head >= self.src.frontier() {
            if self.src.closed() {
                let mut st = self.st.lock().unwrap();
                st.lifecycle = Lifecycle::Closed;
                st.version += 1;
                let _ = self.changed.send(st.version);
                return DriveOutcome::Closed;
            }
            return DriveOutcome::Idle;
        }
        self.source_reads.fetch_add(1, Ordering::Relaxed);
        let outcome = self.read_and_publish(head).await;
        // Publish the version bump for EVERY outcome that touched the
        // ring/head so parked sessions re-check.
        let ver = {
            let mut st = self.st.lock().unwrap();
            st.version += 1;
            st.version
        };
        let _ = self.changed.send(ver);
        outcome
    }

    async fn read_and_publish(&self, head: u64) -> DriveOutcome {
        let (recs, _) = match self.src.read(head, MAX_DRIVER_BATCH_BYTES).await {
            Ok(x) => x,
            Err(_) => return DriveOutcome::SourceFailed,
        };
        let scan_from = head;
        let frontier_after = self.src.frontier();
        let closed_now = self.src.closed();
        let mut prepared: Vec<PreparedRecord> = Vec::with_capacity(recs.len());
        let mut last = scan_from;
        let n = recs.len();
        for (i, r) in recs.iter().enumerate() {
            let data = self.src.prepare_data(r);
            last = last.max(r.off + 1);
            // Lane-global flags ride the batch-LAST record when the
            // scan reached the durable end (legacy-direct semantics):
            // identical for every subscriber of the lane, so they fold
            // into the shared frame instead of costing a second chunk.
            let reached_end = i + 1 == n && last >= frontier_after;
            let sealed_i = reached_end && closed_now;
            let tok = self.src.ctl_token(r.off + 1);
            let mut frame = bytes::BytesMut::from(self.src.prepare_data(r).as_ref());
            frame.extend_from_slice(
                crate::sse::wire::sse_control_product(&tok, reached_end, sealed_i).as_bytes(),
            );
            prepared.push(PreparedRecord {
                offset: r.off,
                payload_len: r.payload.len() as u32,
                data_event: frame.freeze(),
                sealed: sealed_i,
            });
        }
        let drained = last >= self.src.frontier() && self.src.closed();
        let batch = Arc::new(PreparedBatch {
            scan_from,
            scan_to: last,
            charge: charge_for(&prepared),
            records: prepared.into(),
        });
        let solo = self.subscribers.load(Ordering::Relaxed) <= 1;
        let mut st = self.st.lock().unwrap();
        st.head = st.head.max(last);
        if solo {
            // Retain NOTHING: floor tracks head so a racing joiner sees
            // an honest empty window and drives forward from head.
            st.floor = st.head;
        } else {
            st.charge += batch.charge;
            st.batches.push_back(batch.clone());
            while st.charge > self.ring_budget {
                match st.batches.pop_front() {
                    Some(b) => {
                        st.charge -= b.charge;
                        st.floor = st.floor.max(b.scan_to);
                    }
                    None => break,
                }
            }
            self.retained_charge.store(st.charge, Ordering::Relaxed);
        }
        if drained {
            st.lifecycle = Lifecycle::Closed;
        }
        if solo {
            DriveOutcome::Solo(batch)
        } else {
            DriveOutcome::Published
        }
    }
}

#[cfg(test)]
pub(crate) struct PermitGuard<'a>(&'a LiveFeed);
#[cfg(test)]
impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        self.0.driving.store(false, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleState {
    Active,
    Closed,
}

#[derive(Debug)]
pub(crate) enum Take {
    Records {
        records: Vec<(u64, Bytes, u32, bool)>,
        next: u64,
    },
    AtHead {
        closed: bool,
    },
    Lagged {
        floor: u64,
    },
}

pub(crate) enum DriveOutcome {
    /// Retention was zero: the batch belongs to the driving session.
    Solo(Arc<PreparedBatch>),
    /// Published to the ring for all subscribers.
    Published,
    /// Nothing durable ahead (or already terminal / not driving).
    Idle,
    Closed,
    SourceFailed,
}

impl std::fmt::Debug for DriveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Solo(_) => "Solo(..)",
            Self::Published => "Published",
            Self::Idle => "Idle",
            Self::Closed => "Closed",
            Self::SourceFailed => "SourceFailed",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::StreamDesc;

    struct FakeSrc {
        desc: StreamDesc,
        recs: Mutex<Vec<crate::http::PlainRec>>,
        frontier: AtomicU64,
        closed: AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl FakeSrc {
        /// Simulate one committed append: record lands, frontier moves,
        /// parked sessions are woken.
        fn append(&self, off: u64, tag: &str) {
            self.recs.lock().unwrap().push(rec(off, tag));
            self.frontier.store(off + 1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl FeedSourceRead for FakeSrc {
        async fn read(
            &self,
            from: u64,
            _max_bytes: usize,
        ) -> anyhow::Result<(Vec<crate::http::PlainRec>, bool)> {
            let all = self.recs.lock().unwrap();
            let v: Vec<_> = all.iter().filter(|r| r.off >= from).cloned().collect();
            Ok((v, true))
        }
        fn frontier(&self) -> u64 {
            self.frontier.load(Ordering::SeqCst)
        }
        fn closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }
        fn desc(&self) -> &StreamDesc {
            &self.desc
        }

        fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes {
            Bytes::from(crate::sse::wire::sse_data_event(&self.desc, &rec.payload))
        }

        fn ctl_token(&self, offset_after: u64) -> String {
            offset_after.to_string()
        }

        fn advance_notify(&self) -> &tokio::sync::Notify {
            &self.notify
        }
    }

    fn fake_desc() -> StreamDesc {
        StreamDesc {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("proj-test").unwrap(),
            name: "f".into(),
            stream_epoch: "00".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    fn rec(off: u64, tag: &str) -> crate::http::PlainRec {
        crate::http::PlainRec {
            off,
            payload: Bytes::from(format!(r#"{{"t":"{tag}"}}"#)),
            rkey: String::new(),
        }
    }

    fn mk_feed(budget: usize, closed: bool) -> (Arc<LiveFeed>, Arc<FakeSrc>) {
        let src = Arc::new(FakeSrc {
            desc: fake_desc(),
            recs: Mutex::new(Vec::new()),
            frontier: AtomicU64::new(0),
            closed: AtomicBool::new(closed),
            notify: tokio::sync::Notify::new(),
        });
        (
            LiveFeed::new(FeedKey::of([7u8; 16]), src.clone(), budget),
            src,
        )
    }

    /// E3/SOLO: one subscriber, zero retention — the batch is handed
    /// straight back and NOTHING is retained.
    #[tokio::test]
    async fn solo_drive_hands_batch_back_and_retains_nothing() {
        let (f, src) = mk_feed(1024 * 1024, false);
        f.join();
        assert_eq!(f.subscriber_count(), 1);
        src.append(0, "a");
        src.append(1, "b");
        let out = f.drive_once().await.unwrap();
        match out {
            DriveOutcome::Solo(b) => {
                assert_eq!(b.records.len(), 2);
                assert_eq!(b.scan_to, 2);
            }
            other => panic!("expected solo handoff, got {other:?}"),
        }
        assert_eq!(f.retained(), 0);
        assert_eq!(f.source_read_count(), 1);
    }

    /// SHARED: two subscribers, ONE source read — both consume the
    /// same prepared bytes without touching durable storage again.
    #[tokio::test]
    async fn shared_mode_prepares_once_for_both_subscribers() {
        let (f, src) = mk_feed(1024 * 1024, false);
        f.join();
        f.join(); // second subscriber BEFORE the drive
        assert_eq!(f.subscriber_count(), 2);
        src.append(0, "a");
        src.append(1, "b");
        src.append(2, "c");
        assert!(matches!(
            f.drive_once().await,
            Some(DriveOutcome::Published)
        ));
        assert_eq!(f.source_read_count(), 1, "one read serves both");
        match f.take_visible(0) {
            Take::Records { records, next } => {
                assert_eq!(records.len(), 3);
                assert_eq!(next, 3);
                let f = std::str::from_utf8(records[0].1.as_ref()).unwrap();
                assert!(
                    f.starts_with("event: data\ndata:[{\"t\":\"a\"}]\n\n")
                        && f.contains("nextCursor"),
                    "frame must pair data with its cursor control:\n{f}"
                );
            }
            other => panic!("expected records, got {other:?}"),
        }
        // Second subscriber consumes the SAME prepared bytes...
        match f.take_visible(0) {
            Take::Records { records, .. } => assert_eq!(records.len(), 3),
            other => panic!("expected records, got {other:?}"),
        }
        // ...and NO additional source read was issued for either.
        assert_eq!(f.source_read_count(), 1);
        assert!(f.retained() > 0);
    }

    /// CONTENTION: a held permit makes drive_once return None without
    /// publishing or issuing a source read; releasing lets it through.
    #[tokio::test]
    async fn contended_drive_returns_none_and_never_leaks_the_permit() {
        let (f, src) = mk_feed(1024 * 1024, false);
        f.join();
        src.append(0, "a");
        {
            let _guard = f.hold_permit_for_test();
            assert!(f.drive_once().await.is_none());
            assert_eq!(f.source_read_count(), 0);
        }
        // Guard dropped → permit released → drive succeeds.
        assert!(!f.permit_held());
        assert!(f.drive_once().await.is_some());
        assert_eq!(f.source_read_count(), 1);
    }

    /// LAG CONTRACT: a cursor below the retention floor reports Lagged
    /// (disconnect-and-resume), never private historical re-reading.
    #[tokio::test]
    async fn cursor_below_floor_is_lagged_not_silently_refilled() {
        let (f, src) = mk_feed(256 /* tiny budget forces trim */, false);
        f.join();
        f.join();
        for i in 0..50u64 {
            src.append(i, "x");
        }
        assert!(f.drive_once().await.is_some());
        match f.take_visible(10) {
            Take::Lagged { floor } => assert_eq!(floor, 50),
            other => panic!("expected lagged, got {other:?}"),
        }
        assert_eq!(f.retained(), 0);
    }

    /// TERMINAL: when the drain reaches the frontier of a CLOSED
    /// stream the feed transitions to Closed; visibility still serves
    /// the final batch, then reports closed at head.
    #[tokio::test]
    async fn closed_segment_transitions_the_feed_to_terminal() {
        let (f, src) = mk_feed(1024 * 1024, true /* closed */);
        f.join();
        f.join();
        src.append(0, "last");
        // Close-observed-mid-batch reports Published; the terminal
        // fact is what visibility reports afterwards.
        assert!(matches!(
            f.drive_once().await,
            Some(DriveOutcome::Closed) | Some(DriveOutcome::Published)
        ));
        match f.take_visible(0) {
            Take::Records { records, next } => {
                assert_eq!(records.len(), 1);
                assert_eq!(next, 1);
            }
            other => panic!("expected terminal records, got {other:?}"),
        }
        match f.take_visible(1) {
            Take::AtHead { closed: true } => {}
            other => panic!("expected closed-at-head, got {other:?}"),
        }
    }

    /// WAKEUP: a publication landing AFTER a session subscribed fires
    /// that session's version watch — no missed wakeup between check
    /// and park.
    #[tokio::test]
    async fn version_watch_fires_for_publications_after_join() {
        let (f, src) = mk_feed(1024 * 1024, false);
        let (_head, mut rx) = f.join();
        let before = rx.borrow().to_owned();
        src.append(0, "a");
        f.drive_once().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.changed())
            .await
            .expect("watch must fire")
            .expect("sender alive");
        assert!(rx.borrow().to_owned() > before);
    }
}
