//! Feed retention and driver lifecycle fixtures.
#![cfg(test)]

mod fixture;
use super::*;
use std::sync::atomic::AtomicBool;

pub(crate) fn test_desc(name: &str) -> crate::registry::StreamDesc {
    crate::registry::PersistedDescriptor {
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
    .try_into()
    .expect("valid descriptor fixture")
}

/// Deterministic in-memory source: offsets [0, frontier) each with a
/// fixed-size payload; `empty_pages` forces the no-progress partial
/// shape; `fail_reads` forces source errors; `block_reads` parks a
/// read between `read_started` and `read_release` (abort leg).
pub(crate) struct FakeSource {
    pub(crate) frontier: AtomicU64,
    pub(crate) closed: AtomicBool,
    pub(crate) notify: tokio::sync::Notify,
    pub(crate) fail_reads: AtomicBool,
    pub(crate) empty_pages: AtomicBool,
    pub(crate) block_reads: AtomicBool,
    pub(crate) read_started: tokio::sync::Notify,
    pub(crate) read_release: tokio::sync::Notify,
    pub(crate) payload: usize,
    /// Single-flight instrumentation: reads in flight right now,
    /// and the maximum ever observed concurrently.
    pub(crate) reads_in_flight: AtomicU64,
    pub(crate) max_concurrent_reads: AtomicU64,
    /// `next_source()` control: park between `next_started` and
    /// `next_release` when blocked; return `NewSource` of
    /// `next_result` when set (else the default GenuineClose).
    pub(crate) next_source_block: AtomicBool,
    pub(crate) next_started: tokio::sync::Notify,
    pub(crate) next_release: tokio::sync::Notify,
    pub(crate) next_result: Mutex<Option<Arc<dyn FeedSourceRead>>>,
    /// Overrides `span_sig()` (default: one open span).
    pub(crate) sig_override: SigOverride,
}

pub(crate) type SigOverride = Mutex<Option<Vec<(u32, u64, Option<u64>)>>>;

impl FakeSource {
    pub(crate) fn new(frontier: u64, payload: usize) -> Self {
        Self {
            frontier: AtomicU64::new(frontier),
            closed: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            fail_reads: AtomicBool::new(false),
            empty_pages: AtomicBool::new(false),
            block_reads: AtomicBool::new(false),
            read_started: tokio::sync::Notify::new(),
            read_release: tokio::sync::Notify::new(),
            payload,
            reads_in_flight: AtomicU64::new(0),
            max_concurrent_reads: AtomicU64::new(0),
            next_source_block: AtomicBool::new(false),
            next_started: tokio::sync::Notify::new(),
            next_release: tokio::sync::Notify::new(),
            next_result: Mutex::new(None),
            sig_override: Mutex::new(None),
        }
    }
}

/// Decrements `reads_in_flight` on every exit path of
/// `read_batch`, aborts included.
struct ReadInFlight<'a>(&'a FakeSource);
impl Drop for ReadInFlight<'_> {
    fn drop(&mut self) {
        self.0.reads_in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl FeedSourceRead for FakeSource {
    async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch> {
        let cur = self.reads_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent_reads.fetch_max(cur, Ordering::SeqCst);
        let _in_flight = ReadInFlight(self);
        if self.fail_reads.load(Ordering::Relaxed) {
            anyhow::bail!("injected source failure");
        }
        if self.block_reads.load(Ordering::Relaxed) {
            fixture::hold_until_release(&self.read_started, &self.read_release).await;
        }
        if self.empty_pages.load(Ordering::Relaxed) {
            return Ok(SourceBatch {
                scan_from: from,
                scan_to: from,
                records: crate::application::read::PlainBatch::default(),
                completed: false,
            });
        }
        let frontier = self.frontier.load(Ordering::Relaxed);
        let mut records = crate::application::read::PlainBatch::default();
        let mut budget = crate::application::read_budget::PageBudget::new(max_bytes);
        let mut off = from;
        while off < frontier {
            if !records.admit_owned(off, vec![b'x'; self.payload], String::new(), &mut budget) {
                break;
            }
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
    fn prepare_data(&self, rec: &crate::application::read::PlainRec) -> Bytes {
        Bytes::from(format!("event: data\ndata:{}\n\n", rec.off))
    }
    fn advance_notify(&self) -> &tokio::sync::Notify {
        &self.notify
    }
    fn locate(&self, logical_after: u64) -> WirePosition {
        WirePosition {
            seg_id: 0,
            local_after: logical_after,
        }
    }
    fn logicalize(&self, pos: WirePosition) -> Option<u64> {
        (pos.seg_id == 0).then_some(pos.local_after)
    }
    fn cursor_capability(&self) -> CursorCapability {
        CursorCapability::Scalar
    }
    fn span_sig(&self) -> Vec<(u32, u64, Option<u64>)> {
        if let Some(s) = self.sig_override.lock().unwrap().clone() {
            return s;
        }
        vec![(0, 0, None)]
    }
    async fn next_source(&self) -> anyhow::Result<SourceTransition> {
        if self.next_source_block.load(Ordering::Relaxed) {
            fixture::hold_until_release(&self.next_started, &self.next_release).await;
        }
        if let Some(next) = self.next_result.lock().unwrap().take() {
            return Ok(SourceTransition::NewSource(next));
        }
        // The fake source has no topology: a closed tail is a
        // genuine close.
        Ok(SourceTransition::GenuineClose)
    }
}

pub(crate) fn tpid() -> crate::tenant::ProjectId {
    crate::tenant::ProjectId::new("proj-feed-test").unwrap()
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
        tpid(),
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

/// Budget model B (red): retention reserves the ACTUAL retained
/// bytes; extra subscribers cost nothing; teardown returns the
/// budget to exactly zero.
#[tokio::test]
async fn shared_retention_reserves_actual_bytes_only() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, _src) = feed_with(3, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    assert_eq!(budget.reserved(), 0, "singletons reserve nothing");
    feed.subscribe_locked();
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert_eq!(feed.subscriber_count(), 4);
    assert_eq!(
        budget.reserved(),
        0,
        "subscribers themselves never reserve — only retained batches do"
    );
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    let retained = feed.retained();
    assert!(retained > 0);
    assert_eq!(
        budget.reserved(),
        retained as u64,
        "the reservation IS the retained charge, exactly"
    );
}

/// Model B: retained bytes come back when the feed is dropped at
/// zero subscribers — budget returns EXACTLY to zero.
#[tokio::test]
async fn retained_bytes_return_to_zero_at_teardown() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, _src) = feed_with(3, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert!(budget.reserved() > 0);
    assert_eq!(feed.leave_locked(), 1);
    assert_eq!(feed.leave_locked(), 0);
    drop(feed);
    assert_eq!(budget.reserved(), 0, "teardown releases the charge");
}

/// Model B: a process budget too small for the batch admits the
/// shared feed but retains NOTHING — the honest uncached posture,
/// never a phantom reservation.
#[tokio::test]
async fn budget_exhaustion_publishes_without_retention() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(16));
    let (feed, _src) = feed_with(2, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(feed.retained(), 0, "nothing retained at exhaustion");
    assert_eq!(feed.head(), 2, "the head still advances");
    assert_eq!(feed.floor(), 2, "the floor follows the head");
    assert_eq!(budget.reserved(), 0, "no phantom reservation");
}

/// Finding 4 (red): a batch larger than the whole ring is advanced
/// WITHOUT retention — it can never be evicted before anyone
/// consumed it. Subscribers below the new floor take the typed lag
/// path and resume durably.
#[tokio::test]
async fn oversized_batch_is_never_self_evicted() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    // Ring far below one batch's prepared charge (508 > 500).
    let (feed, _src) = feed_with(3, 8, 500, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(feed.retained(), 0, "the oversized batch is not retained");
    assert_eq!(feed.head(), 3);
    assert_eq!(feed.floor(), 3, "floor advanced with the head");
    assert_eq!(budget.reserved(), 0);
    assert!(matches!(feed.take_visible(0), Take::Lagged { floor: 3 }));
}

/// Eviction releases exactly the evicted batch's reservation; the
/// surviving batch stays reserved and consumable.
#[tokio::test]
async fn eviction_releases_exactly_the_evicted_charge() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    // Ring fits roughly one small batch.
    let (feed, src) = feed_with(2, 8, 700, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    let first = budget.reserved();
    assert!(first > 0);
    src.frontier.store(4, Ordering::Relaxed);
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    // The first batch was evicted by the second; only the second's
    // charge remains reserved.
    assert_eq!(feed.retained(), usize::try_from(budget.reserved()).unwrap());
    assert!(budget.reserved() < first + first, "no accumulation");
    assert_eq!(feed.floor(), 2, "floor moved to the evicted scan_to");
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

    // Shared drive retains the batch.
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert!(feed.retained() > 0);
    assert_eq!(feed.head(), 3);

    // The second subscriber leaves BEFORE the survivor consumed the
    // retained batch. The batch must survive.
    assert_eq!(feed.leave_locked(), 1);
    assert!(
        matches!(
            feed.take_visible(0),
            Take::Batch { batch, start_index: 0 } if batch.records.len() == 3
        ),
        "survivor must drain the intact retained batch"
    );
    // The survivor drives solo for NEW appends: no new retention,
    // and the floor must NOT jump past the still-unread ring.
    src.frontier.store(5, Ordering::Relaxed);
    match feed.drive_once().await {
        Some(DriveOutcome::Solo {
            records, scan_to, ..
        }) => {
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
    assert!(
        matches!(
            feed.take_visible(0), Take::Batch { batch, .. } if batch.records.len() == 3
        ),
        "retained batch survives the solo drive"
    );
    // Once the survivor's cursor passes the ring, it is AT the
    // head — and the drain-release popped the passed batch,
    // returning its reservation to the process budget.
    assert!(matches!(feed.take_visible(5), Take::AtHead));
    assert_eq!(feed.retained(), 0, "the passed batch was released");
    assert_eq!(
        budget.reserved(),
        0,
        "drain-release returns the reservation without waiting for feed drop"
    );
    assert_eq!(feed.floor(), 5, "the floor follows the survivor");
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
        assert!(matches!(
            feed.drive_once().await,
            Some(DriveOutcome::NoProgress)
        ));
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
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::SourceFailed)
    ));
    assert_eq!(feed.version(), v0, "a failed read never bumps");
    src.fail_reads.store(false, Ordering::Relaxed);
    match feed.drive_once().await {
        Some(DriveOutcome::Solo {
            records, scan_to, ..
        }) => {
            assert_eq!(records.len(), 4);
            assert_eq!(scan_to, 4);
        }
        other => panic!("recovery must drive normally, got {}", outcome_name(&other)),
    }
    assert!(feed.version() > v0, "a real drive bumps once");
}

/// Finding 5 (red): Idle and Closed version semantics. Nothing
/// durable beyond the head means NO state change and NO bump; the
/// close transition bumps EXACTLY once.
#[tokio::test]
async fn idle_never_bumps_and_closed_bumps_exactly_once() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(2, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    match feed.drive_once().await {
        Some(DriveOutcome::Solo { .. }) => {}
        other => panic!("expected Solo, got {}", outcome_name(&other)),
    }
    let v = feed.version();
    // Idle: head already covers the frontier — no change, no bump.
    for _ in 0..3 {
        assert!(matches!(feed.drive_once().await, Some(DriveOutcome::Idle)));
    }
    assert_eq!(feed.version(), v, "Idle never bumps");
    // Closed: exactly ONE bump across the transition; the outcome
    // stays OBSERVABLE for every later drive (parked sessions must
    // each see it) but never bumps again.
    src.closed.store(true, Ordering::Relaxed);
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Closed)
    ));
    assert_eq!(feed.version(), v + 1, "the close bumps exactly once");
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Closed)
    ));
    assert_eq!(feed.version(), v + 1, "no second bump after the close");
}

/// Finding 5 (red): retained batches drain back-to-back — the
/// session loop consumes every visible batch with NO further source
/// reads and exactly one version bump per real publication.
#[tokio::test]
async fn retained_batches_drain_without_extra_reads() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(0, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    // Three publications, one record each.
    for i in 1..=3u64 {
        src.frontier.store(i, Ordering::Relaxed);
        assert!(matches!(
            feed.drive_once().await,
            Some(DriveOutcome::Published)
        ));
    }
    assert_eq!(feed.source_read_count(), 3);
    assert_eq!(feed.version(), 3, "one bump per real publication");
    // Drain: take_visible must hand out all three batches with no
    // further source read.
    let mut cursor = 0u64;
    for _ in 0..3 {
        let Take::Batch { batch, .. } = feed.take_visible(cursor) else {
            panic!("each publication must have a retained batch");
        };
        cursor = cursor.max(batch.scan_to);
    }
    assert!(matches!(feed.take_visible(cursor), Take::AtHead));
    assert_eq!(cursor, 3, "cursor reached the head");
    assert_eq!(
        feed.source_read_count(),
        3,
        "draining never touches the source"
    );
}

/// The driver permit survives a REAL abort: a task cancelled
/// mid-read must release the single-flight permit, and the next
/// drive proceeds (finding 6 RAII, actually aborted this time).
#[expect(
    clippy::disallowed_methods,
    reason = "abort fixture owns the driver; cancellation is observed through its joined handle; supervised service tasks cannot exercise direct task abort"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_drive_releases_the_permit() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    src.block_reads.store(true, Ordering::Relaxed);
    // Register the started-signal BEFORE spawning (no lost wake).
    let started = src.read_started.notified();
    tokio::pin!(started);
    started.as_mut().enable();
    let driving = feed.clone();
    let task = tokio::spawn(async move { driving.drive_once().await });
    started.await; // the drive is INSIDE the source read now
    task.abort();
    assert!(
        matches!(task.await, Err(error) if error.is_cancelled()),
        "the drive was aborted"
    );
    src.block_reads.store(false, Ordering::Relaxed);
    src.read_release.notify_waiters();
    // The permit must be free: a new drive proceeds to completion.
    match feed.drive_once().await {
        Some(DriveOutcome::Solo {
            records, scan_to, ..
        }) => {
            assert_eq!(records.len(), 4);
            assert_eq!(scan_to, 4);
        }
        other => panic!(
            "the post-abort drive must proceed, got {}",
            outcome_name(&other)
        ),
    }
}

/// Round-9 review blocker (red): the driver permit must be
/// released EXACTLY once. drive_once carried a redundant
/// `driving.store(false)` after the RAII drop; between the two
/// clears another driver legitimately acquires, and the redundant
/// clear then frees THAT driver's permit, admitting a third
/// mid-drive. Deterministic: A parks at the post-release
/// failpoint, B acquires and blocks inside a source read, A
/// resumes (on the buggy tree its second clear lands now), and a
/// third acquisition must still be refused.
#[expect(
    clippy::disallowed_methods,
    reason = "permit fixture owns both drivers; both handles are joined after the controlled interleaving; serial drives cannot exercise permit theft"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_permit_releases_exactly_once() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(1, 8, 4096, &budget);
    feed.fp_name.set("permit-once".into()).unwrap();
    let release = PermitReleaseGuard;
    crate::failpoints::arm(crate::failpoints::Fp::FeedAfterPermitRelease, "permit-once");
    // A: drives one record, releases the permit, parks at the hook.
    let feed_a = feed.clone();
    let a = tokio::spawn(async move { feed_a.drive_once().await });
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while crate::failpoints::parked(
            crate::failpoints::Fp::FeedAfterPermitRelease,
            "permit-once",
        ) == 0
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("A must park after its release");
    // B: acquires the freed permit and blocks INSIDE the read.
    src.frontier.store(2, Ordering::Relaxed);
    src.block_reads.store(true, Ordering::Relaxed);
    let started = src.read_started.notified();
    tokio::pin!(started);
    started.as_mut().enable();
    let feed_b = feed.clone();
    let b = tokio::spawn(async move { feed_b.drive_once().await });
    started.await; // B holds the permit inside the source read
    // A resumes and returns: no second release may occur.
    drop(release);
    let a_out = a.await.unwrap();
    assert!(a_out.is_some(), "A's drive completed");
    // C: the permit must still be HELD by B.
    assert!(
        feed.acquire_permit().is_none(),
        "third driver admitted while B is mid-drive: the permit was released twice"
    );
    src.block_reads.store(false, Ordering::Relaxed);
    src.read_release.notify_waiters();
    assert!(b.await.unwrap().is_some(), "B's drive completed");
    assert_eq!(
        src.max_concurrent_reads.load(Ordering::SeqCst),
        1,
        "source reads must stay single-flight"
    );
}

/// Round-10e review (red): the per-project retention tracker is
/// BOUNDED — churning feeds across many distinct projects must
/// return the map to its steady state (entries live only while a
/// project has live feeds), and an ACTIVE project's entry is never
/// evicted by other projects' churn.
#[tokio::test]
async fn project_retention_tracker_is_bounded_under_churn() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    // A long-lived feed pins its project's entry across the churn.
    let pinned_src = Arc::new(FakeSource::new(0, 8));
    let pinned = LiveFeed::new_with_budget(
        FeedKey::default_lane([9u8; 16]),
        pinned_src,
        4096,
        budget.clone(),
        crate::tenant::ProjectId::new("proj-pinned").unwrap(),
    );
    // Churn: far more distinct projects than any plausible steady
    // state, each feed dropped immediately.
    for i in 0..300u16 {
        let src = Arc::new(FakeSource::new(0, 8));
        let feed = LiveFeed::new_with_budget(
            FeedKey::default_lane([i.to_le_bytes()[0]; 16]),
            src,
            4096,
            budget.clone(),
            crate::tenant::ProjectId::new(&format!("proj-churn-{i}")).unwrap(),
        );
        drop(feed);
    }
    assert_eq!(
        budget.project_entries_for_test(),
        1,
        "churned projects must leave the tracker; only the pinned project remains"
    );
    assert_eq!(budget.reserved(), 0, "no retention outlives its feeds");
    let rows = budget.project_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].0, "proj-pinned",
        "the ACTIVE project is never evicted"
    );
    drop(pinned);
    assert_eq!(
        budget.project_entries_for_test(),
        0,
        "the last feed retires the last entry"
    );
}

/// Round-9 review (red): subscribe-time reconciliation and an
/// active driver racing to install the SAME extension must bump
/// the source generation exactly ONCE. The driver is held inside
/// next_source(), the reconciliation installs first, and the
/// driver's completion of the identical transition must be an
/// idempotent no-op (AlreadyCurrent), never a second bump.
#[expect(
    clippy::disallowed_methods,
    reason = "reconciliation fixture owns the parked driver; its handle is joined after releasing the source; serial execution cannot exercise competing installation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_and_driver_install_bump_generation_once() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(0, 8, 4096, &budget);
    // The extension: sealed predecessor + live successor — a
    // strictly longer compatible continuation of [(0, 0, None)].
    let ext = Arc::new(FakeSource::new(0, 8));
    *ext.sig_override.lock().unwrap() = Some(vec![(0, 0, Some(0)), (1, 0, None)]);
    // Driver: closed tail -> next_source() parks, then returns the
    // SAME extension.
    src.closed.store(true, Ordering::Relaxed);
    src.next_source_block.store(true, Ordering::Relaxed);
    *src.next_result.lock().unwrap() = Some(ext.clone() as Arc<dyn FeedSourceRead>);
    let started = src.next_started.notified();
    tokio::pin!(started);
    started.as_mut().enable();
    let feed_a = feed.clone();
    let a = tokio::spawn(async move { feed_a.drive_once().await });
    started.await; // the driver is INSIDE next_source()
    // Subscribe-time reconciliation installs the extension FIRST.
    let req: Arc<dyn FeedSourceRead> = ext.clone();
    feed.reconcile_locked(&req);
    assert_eq!(feed.source_snapshot().generation, 1, "reconcile installed");
    // The driver completes the identical transition: no re-bump.
    src.next_release.notify_waiters();
    let out = a.await.unwrap();
    assert!(out.is_some(), "the driver's drive completed");
    assert_eq!(
        feed.source_snapshot().generation,
        1,
        "the losing installer must be an idempotent no-op"
    );
    // And the installed source IS the extension.
    assert_eq!(
        feed.current_source().span_sig(),
        vec![(0, 0, Some(0)), (1, 0, None)]
    );
}

/// Rollover (red): a full ring at a full global cap rolls forward —
/// the new publication replaces the old batch's reservation
/// net-of-release; the cap is never exceeded and no uncached
/// publication occurs.
#[tokio::test]
async fn full_ring_rolls_forward_at_full_global_cap() {
    // One 1-record batch charges 340; ring 500 fits exactly one.
    let budget = Arc::new(FeedMemoryBudget::new_for_test(340));
    let (feed, src) = feed_with(1, 8, 500, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(budget.reserved(), 340, "the first batch fills the cap");
    src.frontier.store(2, Ordering::Relaxed);
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(
        budget.reserved(),
        340,
        "net replacement: cap never exceeded"
    );
    assert_eq!(feed.floor(), 1, "the replaced batch moved the floor");
    assert!(
        matches!(
            feed.take_visible(1), Take::Batch { batch, .. } if batch.scan_to == 2
        ),
        "the new batch must be retained"
    );
}

/// Rollover (red): EXTERNAL budget exhaustion — the feed's next
/// publication cannot reserve, so its now-unreachable retained ring
/// is cleared and released; another feed's retention is untouched.
#[tokio::test]
async fn external_exhaustion_clears_unreachable_ring() {
    // Room for exactly two 340 batches, held by two DIFFERENT feeds.
    let budget = Arc::new(FeedMemoryBudget::new_for_test(680));
    let src_a = Arc::new(FakeSource::new(0, 8));
    let feed_a = LiveFeed::new_with_budget(
        FeedKey::default_lane([1u8; 16]),
        src_a.clone(),
        1 << 20,
        budget.clone(),
        tpid(),
    );
    src_a.frontier.store(1, Ordering::Relaxed);
    let src_b = Arc::new(FakeSource::new(0, 8));
    let feed_b = LiveFeed::new_with_budget(
        FeedKey::default_lane([2u8; 16]),
        src_b.clone(),
        1 << 20,
        budget.clone(),
        tpid(),
    );
    src_b.frontier.store(1, Ordering::Relaxed);
    for f in [&feed_a, &feed_b] {
        f.subscribe_locked();
        f.subscribe_locked();
    }
    for f in [&feed_a, &feed_b] {
        assert!(matches!(
            f.drive_once().await,
            Some(DriveOutcome::Published)
        ));
    }
    assert_eq!(budget.reserved(), 680, "both feeds fill the cap");

    // Feed A publishes again: no room (and A's huge ring evicts
    // nothing of its own), so A's now-unreachable old ring must be
    // cleared and released.
    src_a.frontier.store(2, Ordering::Relaxed);
    assert!(matches!(
        feed_a.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(feed_a.retained(), 0, "A's stale ring was cleared");
    assert_eq!(
        budget.reserved(),
        340,
        "A's reservation was released; B's is untouched"
    );
    assert_eq!(feed_a.floor(), 2, "A's floor advanced with the head");
    assert!(matches!(feed_a.take_visible(0), Take::Lagged { floor: 2 }));
    // B's retention is fully intact and consumable.
    assert!(
        matches!(
            feed_b.take_visible(0), Take::Batch { batch, .. } if batch.records.len() == 1
        ),
        "B's ring is intact"
    );
}

/// Rollover (red): an oversized batch after ordinary retained
/// batches clears the old ring and its reservation too — not just
/// the new batch.
#[tokio::test]
async fn oversized_batch_clears_the_old_ring() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    // Ring 500: a 1-record batch (340) fits; a 3-record batch (588)
    // does not.
    let (feed, src) = feed_with(1, 8, 500, &budget);
    feed.subscribe_locked();
    feed.subscribe_locked();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(feed.retained(), 340, "the ordinary batch is retained");
    src.frontier.store(4, Ordering::Relaxed);
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::Published)
    ));
    assert_eq!(feed.retained(), 0, "the old ring was cleared too");
    assert_eq!(budget.reserved(), 0, "its reservation was released");
    assert_eq!(feed.head(), 4);
    assert_eq!(feed.floor(), 4);
}

/// Rollover (red): 32 shared feeds publishing CONCURRENTLY against
/// one budget — the hard cap holds at every instant, no underflow,
/// and teardown returns the budget to exactly zero.
#[expect(
    clippy::disallowed_methods,
    reason = "retention fixture owns all 32 drivers; all handles are joined before assertions; sequential futures cannot exercise simultaneous reservation pressure"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_retention_never_exceeds_cap() {
    const MAX: u64 = 8 * 340; // room for 8 of 32 concurrent batches
    let budget = Arc::new(FeedMemoryBudget::new_for_test(MAX));
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut feeds = Vec::new();
    let mut handles = Vec::new();
    for _ in 0..32 {
        let (feed, _src) = feed_with(1, 8, 1 << 20, &budget);
        feed.subscribe_locked();
        feed.subscribe_locked();
        let b = barrier.clone();
        let f = feed.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            f.drive_once().await
        }));
        feeds.push(feed);
    }
    // Join every driver before inspecting results, even if one driver panicked.
    let outcomes = futures_util::future::join_all(handles).await;
    let retained_n = outcomes
        .into_iter()
        .map(Result::unwrap)
        .filter(|outcome| matches!(outcome, Some(DriveOutcome::Published)))
        .count();
    assert!(retained_n > 0, "some publications retained");
    assert!(budget.reserved() <= MAX, "the hard cap held under a herd");
    let retained_sum: usize = feeds.iter().map(|f| f.retained()).sum();
    assert_eq!(
        budget.reserved(),
        retained_sum as u64,
        "reserved == the sum of ACTUAL retained bytes (no phantom, no underflow)"
    );
    drop(feeds);
    assert_eq!(budget.reserved(), 0, "teardown returns exactly to zero");
}

fn outcome_name(o: &Option<DriveOutcome>) -> &'static str {
    match o {
        Some(DriveOutcome::Solo { .. }) => "Solo",
        Some(DriveOutcome::Published) => "Published",
        Some(DriveOutcome::Idle) => "Idle",
        Some(DriveOutcome::NoProgress) => "NoProgress",
        Some(DriveOutcome::Closed) => "Closed",
        Some(DriveOutcome::IncarnationClosed(_)) => "IncarnationClosed",
        Some(DriveOutcome::SourceFailed) => "SourceFailed",
        Some(DriveOutcome::Cancelled) => "Cancelled",
        None => "Contended",
    }
}

/// Releases the permit fixture's named hold even when its assertions unwind.
struct PermitReleaseGuard;
impl Drop for PermitReleaseGuard {
    fn drop(&mut self) {
        crate::failpoints::release(crate::failpoints::Fp::FeedAfterPermitRelease, "permit-once");
    }
}
