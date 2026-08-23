//! LiveFeed — the one per-stream subscription engine (LIVE-FEED).
//! Replaces the direct reader and the LiveHub pump with a single
//! implementation whose variables are retention and WHO reads:
//!
//! * SOLO (one subscriber): no background task, no retained state. The
//!   lone session parks on the source's durable advance and drives its
//!   own reads — thousands of singleton feeds stay task-free.
//! * SHARED (two or more): ONE dedicated driver task owns reading
//!   (scheduling parity with the legacy hub pump — one read + one
//!   publish per window; subscribers wake once on the version bump).
//!   The driver exists only while subscribers >= 2 and uses a Weak
//!   handle so it cannot keep the feed alive.
//!
//! Retention: bounded per-feed ring + PROCESS-GLOBAL budget
//! (`FeedMemoryBudget`, SSE_FEED_TOTAL_BYTES). Zero global budget =
//! zero-retention posture (publish-and-continue, same code).

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
    /// True when this frame carried THE terminal sealed control.
    pub(crate) sealed: bool,
}

pub(crate) struct PreparedBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Arc<[PreparedRecord]>,
    pub(crate) charge: usize,
}

/// HONEST scanned progress for one bounded pass: `scan_to` names the
/// position after the last SCANNED record — including non-matching
/// ones — so filtered lanes always progress even when zero records
/// match (follow-up review finding 2).
pub(crate) struct SourceBatch {
    pub(crate) scan_from: u64,
    pub(crate) scan_to: u64,
    pub(crate) records: Vec<crate::http::PlainRec>,
}

#[async_trait::async_trait]
pub(crate) trait FeedSourceRead: Send + Sync {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch>;
    fn frontier(&self) -> u64;
    fn closed(&self) -> bool;
    fn desc(&self) -> &crate::registry::StreamDesc;
    /// The DATA event for one record, formatted ONCE per lane. Cursor
    /// and status controls are composed per session (canonical framing:
    /// flags never ride data frames).
    fn prepare_data(&self, rec: &crate::http::PlainRec) -> Bytes;
    /// Wake source: fired on every durable advance and close. Sessions
    /// (solo) and the shared driver park on this.
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

/// PROCESS-GLOBAL retained-charge budget across ALL LiveFeeds
/// (follow-up review finding 6). A batch is retained only after one
/// exact reservation; eviction, drop-to-one and feed teardown release
/// it. Cap from SSE_FEED_TOTAL_BYTES (16 MiB certified on 1-GiB).
pub(crate) struct FeedMemoryBudget {
    reserved: AtomicU64,
    max: u64,
}

impl FeedMemoryBudget {
    pub(crate) fn from_env() -> Self {
        Self {
            reserved: AtomicU64::new(0),
            max: crate::livehub::hub_total_cap(),
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

    pub(crate) fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Relaxed)
    }
}

pub(crate) struct LiveFeed {
    key: FeedKey,
    src: std::sync::RwLock<Arc<dyn FeedSourceRead>>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    retained_charge: AtomicUsize,
    source_reads: AtomicU64,
    ring_budget: usize,
    budget: Arc<FeedMemoryBudget>,
    driver_abort: Mutex<Option<tokio::task::AbortHandle>>,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

/// RAII single-flight driver permit (finding 6): dropping it —
/// including via task abort while awaiting a source read — releases
/// the permit, so an aborted shared driver can never strand `driving`.
pub(crate) struct DriverPermit<'a>(&'a AtomicBool);
impl Drop for DriverPermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl LiveFeed {
    /// SHARED-mode driver: the only reader while fanned out; aborted
    /// when the crowd drops below two. Weak handle so it cannot keep
    /// the feed alive after teardown.
    fn spawn_shared_driver(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                if this.subscriber_count() < 2 {
                    return;
                }
                let _ = this.drive_once().await;
                drop(this);
                // Park on the CURRENT source's advance; bounded repoll
                // re-arms across source swaps and subscriber churn.
                let src_wait = {
                    let Some(this) = weak.upgrade() else { return };
                    let src = this.current_source();
                    Box::pin(async move {
                        let n = src.advance_notify().notified();
                        n.await;
                    })
                };
                tokio::pin!(src_wait);
                let poll = async {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    }
                };
                tokio::select! {
                    _ = &mut src_wait => {}
                    _ = poll => {}
                }
            }
        });
        *self.driver_abort.lock().unwrap() = Some(handle.abort_handle());
    }

    pub(crate) fn new(key: FeedKey, src: Arc<dyn FeedSourceRead>, ring_budget: usize) -> Arc<Self> {
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            key,
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
            retained_charge: AtomicUsize::new(0),
            source_reads: AtomicU64::new(0),
            ring_budget,
            budget: crate::sse::feed_budget(),
            driver_abort: Mutex::new(None),
        })
    }

    fn current_source(&self) -> Arc<dyn FeedSourceRead> {
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
    /// `unsubscribe`.
    pub(crate) fn leave_locked(&self) -> u64 {
        self.subscribers.fetch_sub(1, Ordering::SeqCst)
    }

    /// Reserve this feed's ring allowance from the process-global
    /// budget — REQUIRED before a second subscriber may enter shared
    /// mode (finding 6-mem option A).
    pub(crate) fn reserve_shared_allowance(&self) -> bool {
        self.budget.try_reserve(self.ring_budget)
    }

    /// Idempotent: spawn the SHARED driver when subscribers >= 2 and no
    /// driver exists (closes the subscribe/spawn race from session side).
    pub(crate) fn ensure_shared_driver(self: &Arc<Self>) {
        if self.subscriber_count() < 2 {
            return;
        }
        let mut slot = self.driver_abort.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            loop {
                let Some(this) = weak.upgrade() else { return };
                if this.subscriber_count() < 2 {
                    return;
                }
                let _ = this.drive_once().await;
                drop(this);
                // Park on the CURRENT source's advance; the bounded
                // timer re-arms across source swaps (finding: the old
                // repoll future never completed).
                let src_wait = {
                    let Some(this) = weak.upgrade() else { return };
                    let src = this.current_source();
                    Box::pin(async move {
                        let n = src.advance_notify().notified();
                        n.await;
                    })
                };
                tokio::pin!(src_wait);
                let repoll = async {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                };
                tokio::select! {
                    _ = &mut src_wait => {}
                    _ = repoll => {}
                }
            }
        });
        *slot = Some(handle.abort_handle());
    }

    /// Shared → solo cleanup: retained bytes can never be consumed by a
    /// second reader that no longer exists.
    pub(crate) fn clear_retention(&self) {
        let mut st = self.st.lock().unwrap();
        if !st.batches.is_empty() {
            self.budget.release(st.charge);
            st.charge = 0;
            st.batches.clear();
            st.floor = st.head;
            self.retained_charge.store(0, Ordering::Relaxed);
        }
    }

    pub(crate) fn version_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) fn head(&self) -> u64 {
        self.st.lock().unwrap().head
    }

    /// Solo-mode park: the CURRENT source's durable advance.
    pub(crate) fn park_advance(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let src = self.current_source();
        Box::pin(async move {
            let n = src.advance_notify().notified();
            n.await;
        })
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
        // RAII permit (follow-up review finding 6): an aborted task
        // (shared-driver abort mid-read) drops its guard, releasing the
        // permit — it can never strand held.
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
        let ver = {
            let mut st = self.st.lock().unwrap();
            st.version += 1;
            st.version
        };
        let _ = self.changed.send(ver);
        outcome
    }

    async fn read_and_publish(&self, src: &Arc<dyn FeedSourceRead>, head: u64) -> DriveOutcome {
        let batch = match src.read_batch(head, MAX_DRIVER_BATCH_BYTES).await {
            Ok(x) => x,
            Err(_) => return DriveOutcome::SourceFailed,
        };
        let scan_from = batch.scan_from;
        let scan_to = batch.scan_to;
        let mut prepared: Vec<PreparedRecord> = Vec::with_capacity(batch.records.len());
        for r in &batch.records {
            prepared.push(PreparedRecord {
                offset: r.off,
                data_event: src.prepare_data(r),
                payload_len: r.payload.len() as u32,
                sealed: false,
            });
        }
        let solo = self.subscribers.load(Ordering::Relaxed) <= 1;
        let mut st = self.st.lock().unwrap();
        // Head advances to the SCANNED boundary even with zero matches
        // (finding 2: filtered lanes always progress).
        st.head = st.head.max(scan_to);
        if solo {
            st.floor = st.head;
            return DriveOutcome::Solo {
                records: prepared,
                scan_to,
            };
        }
        let batch_charge = charge_for(&prepared);
        // Global-budget reservation (finding 6): retain only after one
        // exact process-wide reservation; exhausted → publish WITHOUT
        // retention (zero-retention posture, same code path).
        if !self.budget.try_reserve(batch_charge) {
            st.floor = st.head;
            return DriveOutcome::Published;
        }
        st.charge += batch_charge;
        st.batches.push_back(Arc::new(PreparedBatch {
            scan_from,
            scan_to,
            charge: batch_charge,
            records: prepared.into(),
        }));
        while st.charge > self.ring_budget {
            match st.batches.pop_front() {
                Some(b) => {
                    st.charge -= b.charge;
                    st.floor = st.floor.max(b.scan_to);
                    self.budget.release(b.charge);
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
        // Release retained charge back to the process budget; the feed
        // itself is being discarded.
        let charge = self.st.get_mut().unwrap().charge;
        if charge > 0 {
            self.budget.release(charge);
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
    Idle,
    Closed,
    SourceFailed,
}
