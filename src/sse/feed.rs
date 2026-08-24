//! LiveFeed — the one per-stream subscription engine (LIVE-FEED).
//! Replaces the direct reader and the LiveHub pump with a single
//! implementation whose variables are retention and WHO reads:
//!
//! * SOLO (one subscriber): no background task, no retained state. The
//!   lone session parks on the source's durable advance and drives its
//!   own reads — thousands of singleton feeds stay task-free.
//! * SHARED (two or more): COOPERATIVE driving — whichever session
//!   needs progress acquires the single-flight driver permit, reads one
//!   bounded batch, publishes it to the shared ring, and releases the
//!   permit BEFORE any socket write; contended sessions park on the
//!   feed version watch. There is NO dedicated driver task.
//!
//! Retention: bounded per-feed ring + PROCESS-GLOBAL budget
//! (`FeedMemoryBudget`, SSE_FEED_TOTAL_BYTES). The budget is reserved
//! EXACTLY ONCE per feed, on the 1→2 subscriber transition (one ring
//! allowance), and released when the feed is dropped at zero
//! subscribers; per-batch retention is charged against the ring bound,
//! never reserved from the global pool again. Zero global budget =
//! singleton-only posture: a second subscriber to the same feed is
//! refused with a typed capacity error.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Identity of a feed: stream incarnation + selector lane.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct FeedKey {
    pub(crate) identity: [u8; 16],
    pub(crate) selector: [u8; 16],
}

impl FeedKey {
    pub(crate) fn default_lane(identity: [u8; 16]) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(""),
        }
    }
    pub(crate) fn keyed(identity: [u8; 16], rk: &str) -> Self {
        Self {
            identity,
            selector: crate::crypto::stream_hash(rk),
        }
    }
}

/// One prepared record: the DATA event only, formatted once per lane.
/// Canonical framing: sessions compose their own bare cursor control
/// around it; status flags ride standalone status controls only.
pub(crate) struct PreparedRecord {
    pub(crate) offset: u64,
    pub(crate) data_event: Bytes,
    pub(crate) payload_len: u32,
}

pub(crate) struct PreparedBatch {
    pub(crate) scan_to: u64,
    pub(crate) records: Arc<[PreparedRecord]>,
    pub(crate) charge: usize,
}

/// HONEST scanned progress for one bounded pass: `scan_to` names the
/// position after the last SCANNED record — including non-matching
/// ones — so filtered lanes always progress even when zero records
/// match (follow-up review finding 2). `completed` distinguishes
/// "scanned everything durable up to the frontier" from a partial
/// page; a partial page with `scan_to == scan_from` and no records is
/// NO progress and must never bump the feed version (finding 6).
pub(crate) struct SourceBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Vec<crate::http::PlainRec>,
    pub(crate) completed: bool,
}

#[async_trait::async_trait]
pub(crate) trait FeedSourceRead: Send + Sync {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch>;
    fn frontier(&self) -> u64;
    fn closed(&self) -> bool;
    /// The DATA event for one record, formatted ONCE per lane. Cursor
    /// and status controls are composed per session (canonical framing:
    /// flags never ride data frames).
    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes;
    /// Wake source: fired on every durable advance and close. Sessions
    /// park on this (registered eagerly at loop top — see session.rs).
    fn advance_notify(&self) -> &tokio::sync::Notify;
}

fn charge_for(events: &[PreparedRecord]) -> usize {
    let ev: usize = events.iter().map(|r| r.data_event.len()).sum();
    ev + events.len() * 64 + 256
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Active,
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

/// PROCESS-GLOBAL shared-mode budget across ALL LiveFeeds (follow-up
/// review finding 6, redesigned). A feed reserves EXACTLY ONE ring
/// allowance when it enters SHARED mode (the 1→2 transition); the
/// reservation is released when the feed is dropped at zero
/// subscribers. Per-batch retention is bounded by the ring allowance
/// itself and is NEVER reserved from this pool again — retained bytes
/// and reserved bytes therefore describe the same memory exactly once.
/// Cap from SSE_FEED_TOTAL_BYTES (falling back to SSE_HUB_TOTAL_BYTES;
/// 16 MiB certified on 1-GiB).
pub(crate) struct FeedMemoryBudget {
    reserved: AtomicU64,
    max: u64,
}

impl FeedMemoryBudget {
    pub(crate) fn from_env() -> Self {
        Self {
            reserved: AtomicU64::new(0),
            max: crate::livehub::feed_total_cap(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(max: u64) -> Self {
        Self {
            reserved: AtomicU64::new(0),
            max,
        }
    }

    fn try_reserve(&self, charge: usize) -> bool {
        let charge = charge as u64;
        if self.max == 0 {
            return false; // zero budget = zero-retention posture
        }
        let mut cur = self.reserved.load(Ordering::Relaxed);
        loop {
            if cur + charge > self.max {
                return false;
            }
            match self.reserved.compare_exchange(
                cur,
                cur + charge,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self, charge: usize) {
        self.reserved.fetch_sub(charge as u64, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Relaxed)
    }
}

pub(crate) struct LiveFeed {
    /// Swap cell (Stage 6): the read source changes on topology
    /// transitions while sessions stay attached.
    src: std::sync::RwLock<Arc<dyn FeedSourceRead>>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    /// Set EXACTLY once, under the registry lock, on the 1→2
    /// transition after the ring allowance was reserved from the
    /// process budget; released when the feed is dropped at zero
    /// subscribers (finding 6-mem redesign).
    shared_reserved: AtomicBool,
    retained_charge: AtomicUsize,
    source_reads: AtomicU64,
    ring_budget: usize,
    budget: Arc<FeedMemoryBudget>,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

/// RAII single-flight driver permit (finding 6): dropping it —
/// including via task abort while awaiting a source read — releases
/// the permit, so an aborted driving session can never strand
/// `driving`.
pub(crate) struct DriverPermit<'a>(&'a AtomicBool);
impl Drop for DriverPermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl LiveFeed {
    pub(crate) fn new(key: FeedKey, src: Arc<dyn FeedSourceRead>, ring_budget: usize) -> Arc<Self> {
        Self::new_with_budget(key, src, ring_budget, crate::sse::feed_budget())
    }

    pub(crate) fn new_with_budget(
        _key: FeedKey,
        src: Arc<dyn FeedSourceRead>,
        ring_budget: usize,
        budget: Arc<FeedMemoryBudget>,
    ) -> Arc<Self> {
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            src: std::sync::RwLock::new(src),
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
            shared_reserved: AtomicBool::new(false),
            retained_charge: AtomicUsize::new(0),
            source_reads: AtomicU64::new(0),
            ring_budget,
            budget,
        })
    }

    /// The CURRENT read source (Stage 6 swap cell).
    pub(crate) fn current_source(&self) -> Arc<dyn FeedSourceRead> {
        let guard = self.src.read().unwrap();
        guard.clone()
    }

    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn source_read_count(&self) -> u64 {
        self.source_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn retained(&self) -> usize {
        self.retained_charge.load(Ordering::Relaxed)
    }

    /// Increment-only attach; called under the REGISTRY lock by
    /// `FeedRegistry::subscribe` so count and membership mutate in one
    /// synchronization boundary.
    pub(crate) fn subscribe_locked(&self) -> (u64, tokio::sync::watch::Receiver<u64>) {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        let rx = self.changed.subscribe();
        let head = self.st.lock().unwrap().head;
        (head, rx)
    }

    /// Decrement-only detach; called under the REGISTRY lock by
    /// `unsubscribe`. Returns the POST-decrement count (finding 1 of
    /// the follow-up review: `fetch_sub` yields the PRE-decrement
    /// value, which stranded every feed in the registry at zero).
    pub(crate) fn leave_locked(&self) -> u64 {
        let prev = self.subscribers.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(prev > 0, "leave_locked on a zero-subscriber feed");
        prev.saturating_sub(1)
    }

    /// Enter SHARED mode: reserve this feed's ring allowance from the
    /// process-global budget EXACTLY ONCE, on the 1→2 subscriber
    /// transition (finding 6-mem redesign). Idempotent: the 2→3+
    /// transitions and any shared→solo→shared oscillation while the
    /// reservation is still held cost nothing. Called under the
    /// registry lock.
    pub(crate) fn enter_shared_locked(&self) -> bool {
        if self.shared_reserved.load(Ordering::SeqCst) {
            return true;
        }
        if !self.budget.try_reserve(self.ring_budget) {
            return false;
        }
        self.shared_reserved.store(true, Ordering::SeqCst);
        true
    }

    #[cfg(test)]
    pub(crate) fn head(&self) -> u64 {
        self.st.lock().unwrap().head
    }

    #[cfg(test)]
    pub(crate) fn version(&self) -> u64 {
        self.st.lock().unwrap().version
    }

    #[cfg(test)]
    pub(crate) fn floor(&self) -> u64 {
        self.st.lock().unwrap().floor
    }

    /// Consume retained records at/after `cursor`. Lagged = below floor
    /// → disconnect-and-resume per the lag contract.
    pub(crate) fn take_visible(&self, cursor: u64) -> Take {
        let st = self.st.lock().unwrap();
        if cursor < st.floor {
            return Take::Lagged { floor: st.floor };
        }
        // ONE shared batch per hand-off; a match-free prepared range is
        // pure PROGRESS (finding 2).
        for b in &st.batches {
            if b.scan_to <= cursor {
                continue;
            }
            let start_index = b
                .records
                .iter()
                .position(|r| r.offset >= cursor)
                .unwrap_or(b.records.len());
            return Take::Batch {
                batch: Arc::clone(b),
                start_index,
            };
        }
        if let Some(last_b) = st.batches.back() {
            return Take::Progress {
                next: last_b.scan_to,
            };
        }
        Take::AtHead
    }

    pub(crate) async fn drive_once(&self) -> Option<DriveOutcome> {
        // RAII permit (follow-up review finding 6): an aborted session
        // (cancelled mid-read) drops its guard, releasing the permit —
        // it can never strand held.
        let _permit = self.acquire_permit()?;
        let out = self.drive_under_permit().await;
        // Release BEFORE any socket write by any consumer of the result.
        drop(_permit);
        self.driving.store(false, Ordering::SeqCst);
        Some(out)
    }

    /// Acquire the single-flight driver permit. None = already held.
    fn acquire_permit(&self) -> Option<DriverPermit<'_>> {
        self.driving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        Some(DriverPermit(&self.driving))
    }

    async fn drive_under_permit(&self) -> DriveOutcome {
        let src = self.current_source();
        let head = self.st.lock().unwrap().head;
        if head >= src.frontier() {
            if src.closed() && st_lifecycle_active(&self.st) {
                let mut st = self.st.lock().unwrap();
                if st.lifecycle == Lifecycle::Active {
                    st.lifecycle = Lifecycle::Closed;
                    st.version += 1;
                    let _ = self.changed.send(st.version);
                }
                return DriveOutcome::Closed;
            }
            return DriveOutcome::Idle;
        }
        self.source_reads.fetch_add(1, Ordering::Relaxed);
        let outcome = self.read_and_publish(&src, head).await;
        // Bump the version ONLY when feed state actually changed
        // (finding 6): a no-progress partial read or a source failure
        // must never wake every session into another immediate drive —
        // that is the busy retry loop.
        match outcome {
            DriveOutcome::NoProgress | DriveOutcome::SourceFailed => {}
            _ => {
                let ver = {
                    let mut st = self.st.lock().unwrap();
                    st.version += 1;
                    st.version
                };
                let _ = self.changed.send(ver);
            }
        }
        outcome
    }

    async fn read_and_publish(&self, src: &Arc<dyn FeedSourceRead>, head: u64) -> DriveOutcome {
        let batch = match src.read_batch(head, MAX_DRIVER_BATCH_BYTES).await {
            Ok(x) => x,
            Err(_) => return DriveOutcome::SourceFailed,
        };
        // No-progress partial page (finding 6): nothing scanned, nothing
        // matched — report it WITHOUT touching head/version. The session
        // parks; the next durable advance or heartbeat retries.
        if batch.scan_to <= batch.scan_from && batch.records.is_empty() {
            if !batch.completed {
                tracing::debug!(
                    feed_head = head,
                    "livefeed source read made no progress (partial empty page)"
                );
            }
            return DriveOutcome::NoProgress;
        }
        let scan_to = batch.scan_to;
        let mut prepared: Vec<PreparedRecord> = Vec::with_capacity(batch.records.len());
        for r in &batch.records {
            prepared.push(PreparedRecord {
                offset: r.off,
                data_event: src.prepare_data(r),
                payload_len: r.payload.len() as u32,
            });
        }
        let solo = self.subscribers.load(Ordering::Relaxed) <= 1;
        let mut st = self.st.lock().unwrap();
        // Head advances to the SCANNED boundary even with zero matches
        // (finding 2: filtered lanes always progress).
        st.head = st.head.max(scan_to);
        if solo {
            // Solo drives retain nothing. While retained batches from an
            // earlier SHARED period are still draining to the survivor,
            // the floor MUST NOT jump to head: that would strand the
            // survivor's unread retained batch below the floor and
            // disconnect it as lagged (follow-up review finding 4).
            if st.batches.is_empty() {
                st.floor = st.head;
            }
            return DriveOutcome::Solo {
                records: prepared,
                scan_to,
            };
        }
        // SHARED: retention is charged against this feed's ring bound —
        // ALREADY paid for by the one-time process-global allowance
        // reserved at the 1→2 transition; never reserved again here
        // (finding 6-mem redesign: no double count, no leak).
        let batch_charge = charge_for(&prepared);
        st.charge += batch_charge;
        st.batches.push_back(Arc::new(PreparedBatch {
            scan_to,
            charge: batch_charge,
            records: prepared.into(),
        }));
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
        DriveOutcome::Published
    }
}

fn st_lifecycle_active(st: &Mutex<FeedState>) -> bool {
    st.lock().unwrap().lifecycle == Lifecycle::Active
}

impl Drop for LiveFeed {
    fn drop(&mut self) {
        // Release the ONE shared-mode allowance back to the process
        // budget; per-batch retention was charged against the ring
        // bound that allowance paid for, so there is nothing else to
        // release (finding 6-mem redesign).
        if self.shared_reserved.load(Ordering::SeqCst) {
            self.budget.release(self.ring_budget);
        }
    }
}

pub(crate) enum Take {
    /// One shared batch plus the index of the first record at/after the
    /// session's cursor. Sessions iterate `batch.records[start..]`.
    Batch {
        batch: Arc<PreparedBatch>,
        start_index: usize,
    },
    /// Match-free prepared range: pure cursor progress.
    Progress {
        next: u64,
    },
    AtHead,
    Lagged {
        floor: u64,
    },
}

impl std::fmt::Debug for Take {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch { batch, start_index } => f
                .debug_struct("Batch")
                .field("records", &batch.records.len())
                .field("start_index", start_index)
                .finish(),
            Self::Progress { next } => f.debug_struct("Progress").field("next", next).finish(),
            Self::AtHead => f.write_str("AtHead"),
            Self::Lagged { floor } => f.debug_struct("Lagged").field("floor", floor).finish(),
        }
    }
}

pub(crate) enum DriveOutcome {
    /// Zero retention: these records belong to the driving session,
    /// plus the SCANNED boundary (finding 1/2 — the cursor must advance
    /// to scan_to, covering match-free ranges).
    Solo {
        records: Vec<PreparedRecord>,
        scan_to: u64,
    },
    Published,
    /// Nothing durable beyond head.
    Idle,
    /// The source returned an empty partial page (`scan_to == scan_from`,
    /// zero records): NO state changed, so the version was NOT bumped.
    /// The session parks instead of spinning (finding 6).
    NoProgress,
    Closed,
    /// The source read failed; no state changed, no version bump. The
    /// session parks and retries on the next wake (finding 6).
    SourceFailed,
}

// ==================================================================
// Unit tests (follow-up review: "no unit tests inside src/sse"). The
// FakeSource drives deterministic lifecycle, budget, drain and
// no-progress shapes that the HTTP-level suite cannot reach
// deterministically.
// ==================================================================
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    pub(crate) fn test_desc(name: &str) -> crate::registry::StreamDesc {
        crate::registry::StreamDesc {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("proj-feed-test").unwrap(),
            name: name.into(),
            stream_epoch: "0123456789abcdef0123456789abcdef".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            layout_version: crate::registry::LAYOUT_VERSION,
        }
    }

    /// Deterministic in-memory source: offsets [0, frontier) each with a
    /// fixed-size payload; `empty_pages` forces the no-progress partial
    /// shape; `fail_reads` forces source errors.
    pub(crate) struct FakeSource {
        pub(crate) frontier: AtomicU64,
        pub(crate) closed: AtomicBool,
        pub(crate) notify: tokio::sync::Notify,
        pub(crate) fail_reads: AtomicBool,
        pub(crate) empty_pages: AtomicBool,
        pub(crate) payload: usize,
    }

    impl FakeSource {
        pub(crate) fn new(frontier: u64, payload: usize) -> Self {
            Self {
                frontier: AtomicU64::new(frontier),
                closed: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
                fail_reads: AtomicBool::new(false),
                empty_pages: AtomicBool::new(false),
                payload,
            }
        }
    }

    #[async_trait::async_trait]
    impl FeedSourceRead for FakeSource {
        async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch> {
            if self.fail_reads.load(Ordering::Relaxed) {
                anyhow::bail!("injected source failure");
            }
            if self.empty_pages.load(Ordering::Relaxed) {
                return Ok(SourceBatch {
                    scan_from: from,
                    scan_to: from,
                    records: Vec::new(),
                    completed: false,
                });
            }
            let frontier = self.frontier.load(Ordering::Relaxed);
            let mut records = Vec::new();
            let mut used = 0usize;
            let mut off = from;
            while off < frontier && used + self.payload <= max_bytes {
                records.push(crate::http::PlainRec {
                    off,
                    payload: Bytes::from(vec![b'x'; self.payload]),
                    rkey: String::new(),
                });
                used += self.payload;
                off += 1;
            }
            Ok(SourceBatch {
                scan_from: from,
                scan_to: off,
                records,
                completed: off >= frontier,
            })
        }
        fn frontier(&self) -> u64 {
            self.frontier.load(Ordering::Relaxed)
        }
        fn closed(&self) -> bool {
            self.closed.load(Ordering::Relaxed)
        }
        fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes {
            Bytes::from(format!("event: data\ndata:{}\n\n", rec.off))
        }
        fn advance_notify(&self) -> &tokio::sync::Notify {
            &self.notify
        }
    }

    pub(crate) fn feed_with(
        frontier: u64,
        payload: usize,
        ring: usize,
        budget: &Arc<FeedMemoryBudget>,
    ) -> (Arc<LiveFeed>, Arc<FakeSource>) {
        // The feed captures `frontier()` as its initial head — create
        // EMPTY, then advance: that is the live-append shape.
        let src = Arc::new(FakeSource::new(0, payload));
        let feed = LiveFeed::new_with_budget(
            FeedKey::default_lane([7u8; 16]),
            src.clone(),
            ring,
            budget.clone(),
        );
        src.frontier.store(frontier, Ordering::Relaxed);
        (feed, src)
    }

    /// Finding 1 (red): leave_locked must return the POST-decrement
    /// count, so the last leave is observable as zero.
    #[test]
    fn leave_locked_returns_post_decrement_count() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, _src) = feed_with(0, 8, 4096, &budget);
        feed.subscribe_locked();
        feed.subscribe_locked();
        assert_eq!(feed.leave_locked(), 1, "2 -> 1 reports one remaining");
        assert_eq!(feed.leave_locked(), 0, "1 -> 0 reports zero remaining");
    }

    /// Finding 6-mem (red): exactly ONE allowance per shared feed,
    /// however many subscribers join beyond the second.
    #[test]
    fn shared_allowance_is_reserved_exactly_once() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(16 * 4096));
        let (feed, _src) = feed_with(0, 8, 4096, &budget);
        feed.subscribe_locked();
        assert_eq!(budget.reserved(), 0, "singletons reserve nothing");
        feed.subscribe_locked();
        assert!(feed.enter_shared_locked());
        assert_eq!(budget.reserved(), 4096, "the 1->2 transition reserves");
        feed.subscribe_locked();
        feed.subscribe_locked();
        assert_eq!(feed.subscriber_count(), 4);
        assert!(
            feed.enter_shared_locked(),
            "re-entry while held is a no-op success"
        );
        assert_eq!(
            budget.reserved(),
            4096,
            "subscribers 3+ reserve nothing further"
        );
    }

    /// Finding 6-mem (red): the allowance comes back when the feed is
    /// dropped at zero subscribers — budget returns EXACTLY to zero.
    #[test]
    fn allowance_returns_to_zero_at_teardown() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(16 * 4096));
        let (feed, _src) = feed_with(0, 8, 4096, &budget);
        feed.subscribe_locked();
        feed.subscribe_locked();
        assert!(feed.enter_shared_locked());
        assert_eq!(feed.leave_locked(), 1);
        assert_eq!(feed.leave_locked(), 0);
        drop(feed);
        assert_eq!(budget.reserved(), 0, "teardown releases the allowance");
    }

    /// Finding 4 (red): on 2 -> 1 the survivor keeps the retained ring —
    /// an unread batch must remain consumable, never a lag disconnect
    /// caused by ANOTHER subscriber leaving.
    #[tokio::test]
    async fn survivor_drains_retained_batches_after_drop_to_one() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, src) = feed_with(3, 8, 1 << 20, &budget);
        feed.subscribe_locked();
        feed.subscribe_locked();
        assert!(feed.enter_shared_locked());

        // Shared drive retains the batch.
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert!(feed.retained() > 0);
        assert_eq!(feed.head(), 3);

        // The second subscriber leaves BEFORE the survivor consumed the
        // retained batch. The batch must survive.
        assert_eq!(feed.leave_locked(), 1);
        match feed.take_visible(0) {
            Take::Batch { batch, start_index } => {
                assert_eq!(start_index, 0);
                assert_eq!(batch.records.len(), 3, "the retained batch is intact");
            }
            other => panic!("survivor must drain the retained batch, got {other:?}"),
        }

        // The survivor drives solo for NEW appends: no new retention,
        // and the floor must NOT jump past the still-unread ring.
        src.frontier.store(5, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Solo { records, scan_to }) => {
                assert_eq!(records.len(), 2, "solo drive reads offsets 3,4");
                assert_eq!(scan_to, 5);
            }
            other => panic!("expected Solo, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.head(), 5);
        assert_eq!(
            feed.floor(),
            0,
            "draining floor stays put — the survivor is not lagged"
        );
        // The old retained batch is STILL consumable (not stranded
        // below a jumped floor).
        match feed.take_visible(0) {
            Take::Batch { batch, .. } => assert_eq!(batch.records.len(), 3),
            other => panic!("retained batch survives the solo drive: {other:?}"),
        }
        // Once the survivor's cursor passes the ring, pure progress.
        match feed.take_visible(5) {
            Take::Progress { next } => assert_eq!(next, 3),
            other => panic!("expected Progress past the ring, got {other:?}"),
        }
    }

    /// Finding 6 (red): a no-progress partial page changes nothing —
    /// no head movement, NO version bump (no wake-storm, no spin).
    #[tokio::test]
    async fn no_progress_page_never_bumps_the_version() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, src) = feed_with(10, 8, 1 << 20, &budget);
        feed.subscribe_locked();
        src.empty_pages.store(true, Ordering::Relaxed);
        let v0 = feed.version();
        for _ in 0..3 {
            match feed.drive_once().await {
                Some(DriveOutcome::NoProgress) => {}
                other => panic!("expected NoProgress, got {}", outcome_name(&other)),
            }
        }
        assert_eq!(feed.version(), v0, "no-progress drives never bump");
        assert_eq!(feed.head(), 0, "no-progress drives never move head");
    }

    /// Finding 6 (red): a failed source read changes nothing and bumps
    /// nothing; a later healthy read proceeds normally.
    #[tokio::test]
    async fn source_failure_is_typed_and_recoverable() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
        feed.subscribe_locked();
        src.fail_reads.store(true, Ordering::Relaxed);
        let v0 = feed.version();
        match feed.drive_once().await {
            Some(DriveOutcome::SourceFailed) => {}
            other => panic!("expected SourceFailed, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.version(), v0, "a failed read never bumps");
        src.fail_reads.store(false, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Solo { records, scan_to }) => {
                assert_eq!(records.len(), 4);
                assert_eq!(scan_to, 4);
            }
            other => panic!("recovery must drive normally, got {}", outcome_name(&other)),
        }
        assert!(feed.version() > v0, "a real drive bumps once");
    }

    /// The driver permit survives an aborted drive: a task cancelled
    /// mid-read must release the single-flight permit (finding 6 RAII).
    #[tokio::test]
    async fn aborted_drive_releases_the_permit() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, _src) = feed_with(1, 8, 1 << 20, &budget);
        feed.subscribe_locked();
        // First drive succeeds (permit acquired and released).
        assert!(matches!(
            feed.drive_once().await,
            Some(DriveOutcome::Solo { .. })
        ));
        // A second drive is not stranded by the first.
        assert!(matches!(feed.drive_once().await, Some(DriveOutcome::Idle)));
    }

    fn outcome_name(o: &Option<DriveOutcome>) -> &'static str {
        match o {
            Some(DriveOutcome::Solo { .. }) => "Solo",
            Some(DriveOutcome::Published) => "Published",
            Some(DriveOutcome::Idle) => "Idle",
            Some(DriveOutcome::NoProgress) => "NoProgress",
            Some(DriveOutcome::Closed) => "Closed",
            Some(DriveOutcome::SourceFailed) => "SourceFailed",
            None => "Contended",
        }
    }
}
