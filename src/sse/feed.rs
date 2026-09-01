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
//! (`FeedMemoryBudget`, SSE_FEED_TOTAL_BYTES). The budget reserves the
//! ACTUAL retained bytes — one exact reservation per retained batch,
//! released on eviction and at feed drop (the LiveHub accounting
//! model): idle shared feeds cost nothing, busy feeds consume real
//! bytes. A publication that cannot reserve advances WITHOUT
//! retention; sessions below the new floor take the typed lag path
//! and resume durably. Zero global budget (or a zero ring) =
//! singleton-only posture: a second subscriber to the same feed is
//! refused with a typed capacity error before it attaches.

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

/// One prepared record: the DATA event only, formatted once per lane,
/// plus the WIRE position after it — bound at preparation time to the
/// exact source the driver read from (review round 4: a session must
/// never locate a record against a newer source than its batch).
pub(crate) struct PreparedRecord {
    /// LINEARIZED logical offset (feed cursor space).
    pub(crate) offset: u64,
    /// The wire position AFTER this record (segment id + segment-local
    /// offset), located with the reading source.
    pub(crate) pos: WirePosition,
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
    /// Translate a LINEARIZED logical offset (one-past-a-record) into
    /// the wire position (segment + segment-local offset) — Stage 6:
    /// the feed's cursor space is linearized across sealed predecessor
    /// caps; the wire names segments with segment-local offsets.
    fn locate(&self, logical_after: u64) -> WirePosition;
    /// The inverse of `locate` (Stage 7A): convert a wire cursor
    /// position into this source's linearized space. None = the cursor
    /// names a segment outside this lineage, or a local offset beyond
    /// what the segment can prove (past a sealed cap / the durable
    /// frontier) — an invalid cursor, never a silent clamp.
    fn logicalize(&self, pos: WirePosition) -> Option<u64>;
    /// Which cursor vocabulary this source honors (raw gate).
    fn cursor_capability(&self) -> CursorCapability;
    /// Span signature `(seg_id, logical_start, sealed cap)` for swap
    /// validation: an installed replacement must carry the CURRENT
    /// source's signature as an exact prefix, or the cursor space
    /// would shift underneath parked sessions.
    fn span_sig(&self) -> Vec<(u32, u64, Option<u64>)>;
    /// Stage 6: refresh the descriptor and decide the source's future
    /// (called ONLY under the feed's driver permit).
    async fn next_source(&self) -> anyhow::Result<SourceTransition>;
    /// Round-11.4: is this source's LIVE tail no longer servable here?
    /// An at-tail parked session has no read to surface an ownership
    /// move (read_batch's check never runs), so the park re-checks
    /// this after every wake — the loser's engine close fires the
    /// advance notify, and the woken session takes the typed cutoff
    /// instead of sleeping through the fence on keep-alives.
    fn cut_off(&self) -> Option<SourceCutoff> {
        None
    }
}

/// Why a feed's source was cut off (typed for canary telemetry —
/// the wire behavior is disconnect-and-resume in all cases, but the
/// reasons must be distinguishable in metrics and logs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceCutoff {
    /// Delete/recreate or descriptor gone: a DIFFERENT incarnation.
    IncarnationChanged,
    /// A lineage span is owned by another instance (409-class).
    WrongOwner,
    /// The topology no longer contains this feed's cursor space
    /// (incompatible spans, malformed lineage).
    IncompatibleTopology,
    /// Round-11.2: the remote owner refused the incarnation-bound
    /// target (epoch/identity/project mismatch).
    TargetMismatch,
    /// Round-11.2: 401 AFTER the one forced workload-token refresh —
    /// a fleet-auth failure, never a generic source stall.
    FleetAuth,
    /// Round-11.2: a second ownership redirect in one operation.
    RedirectLoop,
}

/// What a descriptor refresh decided about the current source.
pub(crate) enum SourceTransition {
    /// A validated newer source (longer lineage, same prefix).
    NewSource(Arc<dyn FeedSourceRead>),
    /// Genuine collection closure: exactly one terminal control, EOF.
    GenuineClose,
    /// The source cannot continue here (typed reason): sessions
    /// disconnect WITHOUT a terminal control; clients resume from
    /// their cursors.
    IncarnationChanged(SourceCutoff),
    /// Transition still in flight: retry on the next wake.
    RetryLater,
}

/// The wire cursor identity for one linearized logical offset
/// (one-past-a-record): the segment containing the position and the
/// SEGMENT-LOCAL offset after it. Product cursors are consumed as
/// segment-local positions on resume — emitting the linearized offset
/// here would skip records on reconnect (review round 4, blocker 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WirePosition {
    pub(crate) seg_id: u32,
    pub(crate) local_after: u64,
}

/// Which cursor vocabulary a source can honor (Stage 7A): raw scalar
/// offsets work only on a single-segment source; a segmented lineage
/// needs epoch/segment (or signed product) cursors. Raw sessions may
/// join ONLY Scalar sources — generation alone cannot prove this once
/// connect-time lineage exists (a lineage source may be generation 0).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CursorCapability {
    Scalar,
    Segmented,
}

/// A feed-owned source plus its generation (Stage 6.1). Every session
/// observation carries the generation; a swap bumps it and publishes
/// one `source_changed` wake.
#[derive(Clone)]
pub(crate) struct SourceSnapshot {
    pub(crate) generation: u64,
    pub(crate) source: Arc<dyn FeedSourceRead>,
}

/// Span-signature compatibility for a source swap: the CURRENT
/// signature must be an exact prefix of the replacement's, where a
/// previously-live span (cap `None`) may gain its sealed cap — that is
/// the transition itself. Anything else shifts the cursor space
/// underneath parked sessions and is NOT a swap.
pub(crate) fn sig_compatible(
    old: &[(u32, u64, Option<u64>)],
    new: &[(u32, u64, Option<u64>)],
) -> bool {
    old.len() <= new.len()
        && old
            .iter()
            .zip(new.iter())
            .all(|(a, b)| a.0 == b.0 && a.1 == b.1 && (a.2.is_none() || a.2 == b.2))
}

fn charge_for(events: &[PreparedRecord]) -> usize {
    let ev: usize = events.iter().map(|r| r.data_event.len()).sum();
    ev + events.len() * 64 + 256
}

/// Worst-case retained charge of ONE prepared record of `payload`
/// bytes, over EVERY encoding (round-10e review — base64's 4/3 was
/// not the worst case):
///
///   * text/*: each line gains a `data:` prefix + newline, so a
///     payload of newline bytes expands ~6x ("data:\n" per byte);
///     lossy UTF-8 replacement is 3x per byte and stays under that;
///   * binary: base64 4/3;
///   * JSON: 1x plus brackets.
///
/// 6x + framing/accounting overheads bounds them all. CHECKED: an
/// absurd configured ceiling must fail validation, never wrap. The
/// release posture requires the bound to fit the feed ring AND both
/// retention caps.
pub(crate) fn worst_prepared_charge(payload: usize) -> Option<usize> {
    payload.checked_mul(6)?.checked_add(64 + 64 + 256)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Active,
    /// Genuine collection close: one terminal control, then EOF.
    Closed,
    /// The source cannot continue here (typed reason, Stage 6/7):
    /// sessions disconnect WITHOUT a terminal control.
    Gone(SourceCutoff),
}

struct FeedState {
    head: u64,
    floor: u64,
    version: u64,
    batches: VecDeque<Arc<PreparedBatch>>,
    charge: usize,
    lifecycle: Lifecycle,
}

/// PROCESS-GLOBAL retained-bytes budget across ALL LiveFeeds (budget
/// model B, per the follow-up review's capacity finding): ONE exact
/// reservation per retained batch, released on eviction and at feed
/// drop. Idle shared feeds cost nothing; busy feeds consume real
/// bytes. Cap from SSE_FEED_TOTAL_BYTES (16 MiB certified on 1-GiB).
pub(crate) struct FeedMemoryBudget {
    reserved: AtomicU64,
    max: AtomicU64,
    /// Project backstop (round-10 review): the process budget is the
    /// CELL safety ceiling; no single project may consume all of it.
    /// A project exceeding its own allowance takes the uncached
    /// posture ITSELF — it can never force an unrelated project to.
    project_cap: AtomicU64,
    by_project: Mutex<std::collections::HashMap<crate::tenant::ProjectId, Arc<ProjectRetention>>>,
}

/// One project's live retention entry. BOUNDED lifetime (round-10e
/// review): feeds hold the Arc; the LAST feed of a project removes
/// the entry on drop once its reservation is zero — the map tracks
/// projects WITH live feeds, never every project ever observed.
pub(crate) struct ProjectRetention {
    reserved: AtomicU64,
    /// Publications this project pushed into the uncached posture by
    /// ITS OWN allowance (per-project cap-hit observability without
    /// unbounded metric cardinality: rows exist only while feeds do).
    cap_hits: AtomicU64,
    /// Round-13: the ONE canonical project admission entry. The exact
    /// reservation above is MIRRORED into its retained_sse_bytes at
    /// the same mutation sites (final try_replace success + release) —
    /// one accounting policy, one truth; the pressure model never
    /// re-estimates retention.
    admission: std::sync::OnceLock<Arc<crate::quota::ProjectAdmission>>,
}

impl ProjectRetention {
    pub(crate) fn bind_admission(&self, adm: Arc<crate::quota::ProjectAdmission>) {
        let _ = self.admission.set(adm);
    }
    fn mirror_add(&self, bytes: u64) {
        if let Some(a) = self.admission.get() {
            a.retained_sse_add(bytes);
        }
    }
    fn mirror_sub(&self, bytes: u64) {
        if let Some(a) = self.admission.get() {
            a.retained_sse_sub(bytes);
        }
    }
}

/// The project allowance: SSE_FEED_PROJECT_BYTES, defaulting to a
/// QUARTER of the cell ceiling. Strict form for release validation
/// (an unparseable value must fail boot, round-10e).
pub(crate) fn configured_project_cap(global: u64) -> Result<u64, String> {
    match crate::config::current()
        .sse
        .feed_project_bytes_raw
        .as_deref()
    {
        None => Ok(global / 4),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|_| format!("SSE_FEED_PROJECT_BYTES={raw:?} does not parse as a byte count")),
    }
}

/// Outcome of a retention reservation, split by WHICH ceiling refused
/// (round-10: the two refusals carry different product meanings and
/// separate counters).
pub(crate) enum ReserveOutcome {
    Reserved,
    /// The project's own allowance is exhausted: the offender takes
    /// the uncached posture.
    ProjectOver,
    /// The cell ceiling is exhausted: many projects collectively
    /// reached a real shared limit.
    GlobalOver,
}

impl FeedMemoryBudget {
    pub(crate) fn from_env() -> Self {
        let max = crate::sse::budget::feed_total_cap();
        // One noisy project can never evict every other project's
        // retention, while a handful of busy projects can still fill
        // the cell — a defensible shared-cell contract (round-10
        // review). Release boot validates this STRICTLY (unparseable
        // or isolation-defeating values refuse to boot); the dev
        // fallback warns.
        let project_cap = match configured_project_cap(max) {
            Ok(v) => v,
            Err(m) => {
                tracing::warn!("{m}; falling back to the quarter-of-cell default");
                max / 4
            }
        };
        Self {
            reserved: AtomicU64::new(0),
            max: AtomicU64::new(max),
            project_cap: AtomicU64::new(project_cap),
            by_project: Mutex::new(std::collections::HashMap::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(max: u64) -> Self {
        Self {
            reserved: AtomicU64::new(0),
            max: AtomicU64::new(max),
            // Unit rigs are single-project: the project backstop
            // equals the ceiling unless a test narrows it.
            project_cap: AtomicU64::new(max),
            by_project: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Test-only: shrink the cell ceiling so exhaustion scenarios stay
    /// cheap (per-rig — every AppState builds its own budget). The
    /// project backstop follows at the default quarter.
    #[cfg(test)]
    pub(crate) fn set_max_for_test(&self, max: u64) {
        self.max.store(max, Ordering::SeqCst);
        self.project_cap.store(max / 4, Ordering::SeqCst);
    }

    /// The per-project retention entry, shared by every feed of the
    /// project in this process; created on first use, removed by the
    /// LAST feed's drop (see `release_project_entry`).
    pub(crate) fn project_retention(
        &self,
        project: &crate::tenant::ProjectId,
    ) -> Arc<ProjectRetention> {
        self.by_project
            .lock()
            .unwrap()
            .entry(project.clone())
            .or_insert_with(|| {
                Arc::new(ProjectRetention {
                    reserved: AtomicU64::new(0),
                    cap_hits: AtomicU64::new(0),
                    admission: std::sync::OnceLock::new(),
                })
            })
            .clone()
    }

    /// Bounded-lifetime cleanup (round-10e review): called from a
    /// dropping feed AFTER it released its retained charge. Removes
    /// the project's entry when this feed is the LAST holder (map +
    /// caller = 2 strong refs) and nothing is reserved; `ptr_eq`
    /// guards against removing a replacement entry a concurrent
    /// subscribe raced in.
    fn release_project_entry(
        &self,
        project: &crate::tenant::ProjectId,
        entry: &Arc<ProjectRetention>,
    ) {
        let mut map = self.by_project.lock().unwrap();
        if let Some(cur) = map.get(project)
            && Arc::ptr_eq(cur, entry)
            && Arc::strong_count(entry) == 2
            && entry.reserved.load(Ordering::SeqCst) == 0
        {
            map.remove(project);
        }
    }

    /// Live per-project retention rows (debug surface): bounded by
    /// projects WITH live feeds.
    pub(crate) fn project_rows(&self) -> Vec<(String, u64, u64)> {
        self.by_project
            .lock()
            .unwrap()
            .iter()
            .map(|(p, e)| {
                (
                    p.as_str().to_string(),
                    e.reserved.load(Ordering::Relaxed),
                    e.cap_hits.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn project_entries_for_test(&self) -> usize {
        self.by_project.lock().unwrap().len()
    }

    /// Test-only exhaustion: reserve everything that remains, so the
    /// next publication takes the uncached path. Returns the amount to
    /// hand back via `release_for_test`.
    #[cfg(test)]
    pub(crate) fn exhaust_for_test(&self) -> u64 {
        loop {
            let cur = self.reserved.load(Ordering::SeqCst);
            let take = self.max.load(Ordering::SeqCst).saturating_sub(cur);
            if self
                .reserved
                .compare_exchange(cur, cur + take, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return take;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn release_for_test(&self, n: u64) {
        self.reserved.fetch_sub(n, Ordering::SeqCst);
    }

    /// Zero budget = singleton-only posture (docs/LIVE-FEED.md): no
    /// feed may admit a second subscriber.
    pub(crate) fn admits_shared(&self) -> bool {
        self.max.load(Ordering::Relaxed) > 0
    }

    /// Net-of-replacement reservation (review round 3): a publication
    /// that will REMOVE `released` bytes of this feed's own retained
    /// batches replaces them with `add` new bytes ATOMICALLY — the
    /// post-replacement total is what the cap checks, so a full ring
    /// rolls forward at a full global cap. On success the counter
    /// ALREADY reflects both sides: the caller must NOT release the
    /// replaced batches again. Checked arithmetic: any accounting
    /// drift fails CLOSED rather than wrapping.
    fn try_replace(&self, proj: &ProjectRetention, released: usize, add: usize) -> ReserveOutcome {
        let (rel, add) = (released as u64, add as u64);
        // PROJECT leg first (round-10 isolation): the offending
        // project fails its OWN reservation; unrelated projects never
        // see this refusal. The replaced bytes are this feed's own
        // prior reservation, hence part of this project's counter.
        let cap = self.project_cap.load(Ordering::Relaxed);
        let mut pcur = proj.reserved.load(Ordering::Relaxed);
        loop {
            let Some(after) = pcur.checked_sub(rel).and_then(|b| b.checked_add(add)) else {
                proj.cap_hits.fetch_add(1, Ordering::Relaxed);
                return ReserveOutcome::ProjectOver; // drift: fail closed
            };
            if after > cap {
                proj.cap_hits.fetch_add(1, Ordering::Relaxed);
                return ReserveOutcome::ProjectOver;
            }
            match proj
                .reserved
                .compare_exchange(pcur, after, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => pcur = actual,
            }
        }
        // GLOBAL leg: the cell safety ceiling. Roll the project leg
        // back on refusal (add first, then sub — never a transient
        // underflow).
        let mut cur = self.reserved.load(Ordering::Relaxed);
        loop {
            let Some(after) = cur.checked_sub(rel).and_then(|b| b.checked_add(add)) else {
                proj.reserved.fetch_add(rel, Ordering::SeqCst);
                proj.reserved.fetch_sub(add, Ordering::SeqCst);
                return ReserveOutcome::GlobalOver; // drift: fail closed
            };
            if after > self.max.load(Ordering::Relaxed) {
                proj.reserved.fetch_add(rel, Ordering::SeqCst);
                proj.reserved.fetch_sub(add, Ordering::SeqCst);
                return ReserveOutcome::GlobalOver;
            }
            match self
                .reserved
                .compare_exchange(cur, after, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => {
                    // Mirror the settled net delta into the project's
                    // admission pressure (round-13). Rolled-back and
                    // refused paths never mirror.
                    if add > rel {
                        proj.mirror_add(add - rel);
                    } else {
                        proj.mirror_sub(rel - add);
                    }
                    return ReserveOutcome::Reserved;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self, proj: &ProjectRetention, charge: usize) {
        let charge = charge as u64;
        proj.mirror_sub(charge);
        for counter in [&self.reserved, &proj.reserved] {
            let mut cur = counter.load(Ordering::Relaxed);
            loop {
                let Some(next) = cur.checked_sub(charge) else {
                    debug_assert!(false, "budget release underflow: {charge} > {cur}");
                    break; // accounting drift: refuse to wrap
                };
                match counter.compare_exchange(cur, next, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
    }

    pub(crate) fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Relaxed)
    }
}

pub(crate) struct LiveFeed {
    /// Swap cell (Stage 6): the read source + its generation change on
    /// topology transitions while sessions stay attached.
    src: std::sync::RwLock<SourceSnapshot>,
    st: Mutex<FeedState>,
    changed: tokio::sync::watch::Sender<u64>,
    /// Source-generation watch: ONE publication per installed source.
    source_changed: tokio::sync::watch::Sender<u64>,
    driving: AtomicBool,
    subscribers: AtomicU64,
    retained_charge: AtomicUsize,
    source_reads: AtomicU64,
    ring_budget: usize,
    /// Driver read bound: derived from the ring so a prepared batch
    /// (base64 worst case 4/3 + per-record overhead) almost always
    /// fits; a single record that still exceeds the ring takes the
    /// honest no-retention path (review finding 4).
    read_cap: usize,
    budget: Arc<FeedMemoryBudget>,
    /// This feed's project's shared retention entry (round-10
    /// isolation) — every reservation and release pairs the global
    /// counter with it; the last feed's drop removes it (round-10e
    /// bounded lifetime).
    project: crate::tenant::ProjectId,
    project_reserved: Arc<ProjectRetention>,
    /// Round-13: the feed's static pressure charge (16 KiB model
    /// weight), held for the feed's lifetime.
    pressure_guard: std::sync::OnceLock<crate::quota::FeedPressureGuard>,
    /// Round-11.1: teardown cancellation + the ONE transition-retry
    /// scheduler per feed (never a per-session timer herd).
    pub(crate) cancel: CancelFlag,
    retry_scheduled: AtomicBool,
    #[cfg(test)]
    pub(crate) retry_spawns: AtomicU64,
    /// Test-only: names this feed for the post-release drive failpoint
    /// (`Fp::FeedAfterPermitRelease`); unset = the hook is inert.
    #[cfg(test)]
    pub(crate) fp_name: std::sync::OnceLock<String>,
}

const MAX_DRIVER_BATCH_BYTES: usize = 256 * 1024;

/// Round-11.1 teardown token: fired by the registry when the LAST
/// subscriber leaves — every in-flight source operation (local read,
/// remote page, refresh) selects against it, so no source work
/// survives feed teardown. A single subscriber's disconnect never
/// fires it.
pub(crate) struct CancelFlag {
    fired: AtomicBool,
    notify: tokio::sync::Notify,
}

impl CancelFlag {
    fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }
    pub(crate) fn fire(&self) {
        self.fired.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
    pub(crate) fn is_fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
    pub(crate) async fn cancelled(&self) {
        loop {
            if self.is_fired() {
                return;
            }
            let n = self.notify.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            if self.is_fired() {
                return;
            }
            n.await;
        }
    }
}

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
    /// Round-13: bind this feed to the project's admission entry —
    /// charges the static feed weight exactly once (guard held by the
    /// feed; released at feed drop) and wires the retention mirror.
    /// Called inside the registry's CREATION closure only, so joiners
    /// never double-charge.
    pub(crate) fn bind_pressure(&self, adm: Arc<crate::quota::ProjectAdmission>) {
        self.project_reserved.bind_admission(adm.clone());
        let _ = self
            .pressure_guard
            .set(crate::quota::FeedPressureGuard::acquire(adm));
    }

    pub(crate) fn new_with_budget(
        _key: FeedKey,
        src: Arc<dyn FeedSourceRead>,
        ring_budget: usize,
        budget: Arc<FeedMemoryBudget>,
        project: crate::tenant::ProjectId,
    ) -> Arc<Self> {
        let project_reserved = budget.project_retention(&project);
        let head = src.frontier();
        let (changed, _) = tokio::sync::watch::channel(0u64);
        let (source_changed, _) = tokio::sync::watch::channel(0u64);
        Arc::new(Self {
            src: std::sync::RwLock::new(SourceSnapshot {
                generation: 0,
                source: src,
            }),
            st: Mutex::new(FeedState {
                head,
                floor: head,
                version: 0,
                batches: VecDeque::new(),
                charge: 0,
                lifecycle: Lifecycle::Active,
            }),
            changed,
            source_changed,
            driving: AtomicBool::new(false),
            subscribers: AtomicU64::new(0),
            retained_charge: AtomicUsize::new(0),
            source_reads: AtomicU64::new(0),
            ring_budget,
            // Prepared charge ≈ payload·4/3 (base64) + 64/record + 256,
            // so a read bounded at 2/3 of the ring prepares a batch
            // that fits the ring in the ordinary case.
            read_cap: (ring_budget.saturating_mul(2) / 3).clamp(1024, MAX_DRIVER_BATCH_BYTES),
            budget,
            project,
            project_reserved,
            pressure_guard: std::sync::OnceLock::new(),
            cancel: CancelFlag::new(),
            retry_scheduled: AtomicBool::new(false),
            #[cfg(test)]
            retry_spawns: AtomicU64::new(0),
            #[cfg(test)]
            fp_name: std::sync::OnceLock::new(),
        })
    }

    /// May this feed admit a SECOND subscriber? Static configuration
    /// only (nonzero ring AND nonzero global budget) — checked BEFORE
    /// the attach under the registry lock, so a shared admission never
    /// exposes a subscriber count whose memory posture is not already
    /// valid (follow-up review finding 1).
    pub(crate) fn can_share(&self) -> bool {
        self.ring_budget > 0 && self.budget.admits_shared()
    }

    /// The CURRENT read source (Stage 6 swap cell).
    pub(crate) fn current_source(&self) -> Arc<dyn FeedSourceRead> {
        self.source_snapshot().source
    }

    /// The current source WITH its generation (Stage 6.1): every
    /// session observation carries a generation, and a swap publishes
    /// exactly one `source_changed` wake.
    pub(crate) fn source_snapshot(&self) -> SourceSnapshot {
        self.src.read().unwrap().clone()
    }

    /// Install a VALIDATED newer source (Stage 6.3): only a
    /// replacement whose span signature carries the CURRENT source's
    /// signature as a compatible prefix AND is strictly longer —
    /// otherwise the cursor space would shift underneath parked
    /// sessions (incarnation change, not a swap). The decision and
    /// the install are ONE atomic step under the source write lock
    /// (round-9 review): a driver completing a transition and a
    /// subscribe-time reconciliation may race to install the same
    /// extension, and the loser must be an idempotent no-op — never a
    /// second generation bump.
    fn install_source(&self, next: Arc<dyn FeedSourceRead>) -> InstallOutcome {
        let mut w = self.src.write().unwrap();
        let old_sig = w.source.span_sig();
        let new_sig = next.span_sig();
        if !sig_compatible(&old_sig, &new_sig) {
            return InstallOutcome::Incompatible;
        }
        if new_sig.len() <= old_sig.len() {
            // Compatible but not a strict extension: this transition
            // is already installed (a lost install race), or a
            // same-length cap refresh the refresh path deliberately
            // never installs (same spans = RetryLater/GenuineClose).
            return InstallOutcome::AlreadyCurrent;
        }
        w.generation += 1;
        w.source = next;
        let g = w.generation;
        drop(w);
        let _ = self.source_changed.send(g);
        InstallOutcome::Installed
    }

    /// Subscribe-time reconciliation (Stage 7A static concern): a feed
    /// that exists from BEFORE a topology change must not hand a new
    /// subscriber a stale source. The REQUESTED source — built from
    /// the current descriptor — is installed when it is a strictly
    /// longer compatible extension of the current one; anything else
    /// keeps the existing source (the feed's own refresh converges).
    /// Called under the registry lock, BEFORE the join state is
    /// captured, so `join_head`/generation/`now` all bind to the
    /// reconciled source. The extension check lives INSIDE
    /// install_source, under the source write lock — checking here
    /// and installing there was the reconcile-vs-driver TOCTOU
    /// (round-9 review).
    pub(crate) fn reconcile_locked(&self, requested: &Arc<dyn FeedSourceRead>) {
        let _ = self.install_source(requested.clone());
    }

    pub(crate) fn subscriber_count(&self) -> u64 {
        self.subscribers.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn source_read_count(&self) -> u64 {
        self.source_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_for_test(&self) -> &'static str {
        match self.st.lock().unwrap().lifecycle {
            Lifecycle::Active => "Active",
            Lifecycle::Closed => "Closed",
            Lifecycle::Gone(_) => "Gone",
        }
    }

    #[cfg(test)]
    pub(crate) fn retained(&self) -> usize {
        self.retained_charge.load(Ordering::Relaxed)
    }

    /// Increment-only attach; called under the REGISTRY lock by
    /// `FeedRegistry::subscribe` so count, membership, captured head,
    /// version receiver, and SOURCE GENERATION all bind in one
    /// synchronization boundary (review round 4: reading the generation
    /// after the fact reopens the construction race).
    pub(crate) fn subscribe_locked(
        &self,
    ) -> (
        u64,
        tokio::sync::watch::Receiver<u64>,
        u64,
        tokio::sync::watch::Receiver<u64>,
    ) {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        let rx = self.changed.subscribe();
        let grx = self.source_changed.subscribe();
        let head = self.st.lock().unwrap().head;
        let generation = self.src.read().unwrap().generation;
        (head, rx, generation, grx)
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

    /// Current feed head (session handoff re-catch-up bound; tests).
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

    #[cfg(test)]
    pub(crate) fn floor_for_test(&self) -> u64 {
        self.st.lock().unwrap().floor
    }

    /// Consume retained records at/after `cursor`. Lagged = below floor
    /// → disconnect-and-resume per the lag contract.
    pub(crate) fn take_visible(&self, cursor: u64) -> Take {
        let mut st = self.st.lock().unwrap();
        if cursor < st.floor {
            return Take::Lagged { floor: st.floor };
        }
        // SOLO drain-release (review: the budget must not stay pinned
        // after shared use): with ONE subscriber left, a batch it has
        // fully passed can never be needed again — pop it and release
        // its reservation immediately instead of at feed drop.
        if self.subscribers.load(Ordering::Relaxed) == 1 {
            let mut popped = false;
            while let Some(b) = st.batches.front() {
                if b.scan_to > cursor {
                    break;
                }
                let b = st.batches.pop_front().expect("front checked");
                st.charge -= b.charge;
                self.budget.release(&self.project_reserved, b.charge);
                popped = true;
            }
            if popped {
                if st.batches.is_empty() {
                    // Nothing below the survivor's own cursor is owed
                    // to anyone: the floor may follow it.
                    st.floor = st.floor.max(cursor.min(st.head));
                }
                self.retained_charge.store(st.charge, Ordering::Relaxed);
            }
        }
        // ONE shared batch per hand-off; a match-free prepared range is
        // pure PROGRESS (finding 2). When every retained batch is
        // already consumed the session is AT the head — there is no
        // second "progress" shape to drain (finding 5: sessions loop
        // on this directly; it must be total).
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
        Take::AtHead
    }

    /// Round-11.1: ONE transient-transition retry scheduler per feed.
    /// A parked fan-out never creates a timer herd: the first session
    /// that observes an unresolved closed source arms one task; the
    /// task re-drives at 250 ms until the transition resolves, the
    /// subscribers leave, or the feed tears down. Resolution bumps
    /// the version, waking every parked session.
    pub(crate) fn schedule_transition_retry(self: &Arc<Self>) {
        if self
            .retry_scheduled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        #[cfg(test)]
        self.retry_spawns.fetch_add(1, Ordering::Relaxed);
        let feed = self.clone();
        tokio::spawn(async move {
            // Bounded: 20 minutes of 250 ms attempts, far beyond any
            // real transition; sessions' own caps disconnect earlier.
            for _ in 0..4800u32 {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                    _ = feed.cancel.cancelled() => break,
                }
                if feed.subscriber_count() == 0 || feed.cancel.is_fired() {
                    break;
                }
                match feed.drive_once().await {
                    Some(DriveOutcome::Idle)
                    | Some(DriveOutcome::NoProgress)
                    | Some(DriveOutcome::SourceFailed)
                    | None => {
                        let unresolved = feed.current_source().closed()
                            && matches!(feed.st.lock().unwrap().lifecycle, Lifecycle::Active);
                        if !unresolved {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            feed.retry_scheduled.store(false, Ordering::SeqCst);
        });
    }

    pub(crate) async fn drive_once(&self) -> Option<DriveOutcome> {
        // RAII permit (follow-up review finding 6): an aborted session
        // (cancelled mid-read) drops its guard, releasing the permit —
        // it can never strand held.
        let permit = self.acquire_permit()?;
        let out = self.drive_under_permit().await;
        // Release BEFORE any socket write by any consumer of the
        // result — and EXACTLY ONCE, via the RAII drop alone. A second
        // `driving.store(false)` here reopened the single-flight
        // window (round-9 review blocker): after the drop freed the
        // permit and another driver acquired it, the redundant clear
        // released THAT driver's permit, admitting a third mid-drive.
        drop(permit);
        // Test hook: park AFTER the release so a race leg can prove
        // the permit changes hands exactly once (armed per feed via
        // `fp_name`; inert everywhere else).
        #[cfg(test)]
        if let Some(n) = self.fp_name.get() {
            crate::failpoints::pause(crate::failpoints::Fp::FeedAfterPermitRelease, n).await;
        }
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
        let mut swap_attempts = 0u8;
        // Lifecycle outcomes are REPEATABLE (every parked session must
        // observe them on its own drive), but the version bump happens
        // only on the drive that performs the transition itself.
        let mut transitioned = false;
        let outcome = loop {
            let snap = self.source_snapshot();
            let src = snap.source.clone();
            let head = self.st.lock().unwrap().head;
            if head < src.frontier() {
                self.source_reads.fetch_add(1, Ordering::Relaxed);
                break self.read_and_publish(&src, head).await;
            }
            // Nothing durable beyond the head. A closed tail is either
            // a genuine collection close or a topology transition —
            // only the descriptor refresh (under THIS permit) decides
            // and installs the successor source (Stage 6.3).
            let lifecycle = self.st.lock().unwrap().lifecycle;
            match lifecycle {
                Lifecycle::Closed => break DriveOutcome::Closed,
                Lifecycle::Gone(reason) => break DriveOutcome::IncarnationClosed(reason),
                Lifecycle::Active => {}
            }
            if !src.closed() {
                break DriveOutcome::Idle;
            }
            match src.next_source().await {
                Ok(SourceTransition::NewSource(next)) => {
                    match self.install_source(next) {
                        // Validated continuation — installed by us, or
                        // already installed by a racing reconciliation
                        // (AlreadyCurrent is a LOST RACE, never an
                        // incarnation change): re-evaluate with the
                        // current source (its live tail may already
                        // have records for this head).
                        InstallOutcome::Installed | InstallOutcome::AlreadyCurrent => {
                            swap_attempts += 1;
                            if swap_attempts >= 4 {
                                break DriveOutcome::Idle;
                            }
                            continue;
                        }
                        // Incompatible topology: NOT a swap — sessions
                        // disconnect without a terminal control.
                        InstallOutcome::Incompatible => {
                            let mut st = self.st.lock().unwrap();
                            st.lifecycle = Lifecycle::Gone(SourceCutoff::IncompatibleTopology);
                            transitioned = true;
                            break DriveOutcome::IncarnationClosed(
                                SourceCutoff::IncompatibleTopology,
                            );
                        }
                    }
                }
                Ok(SourceTransition::GenuineClose) => {
                    let mut st = self.st.lock().unwrap();
                    st.lifecycle = Lifecycle::Closed;
                    transitioned = true;
                    break DriveOutcome::Closed;
                }
                Ok(SourceTransition::IncarnationChanged(reason)) => {
                    let mut st = self.st.lock().unwrap();
                    st.lifecycle = Lifecycle::Gone(reason);
                    transitioned = true;
                    break DriveOutcome::IncarnationClosed(reason);
                }
                Ok(SourceTransition::RetryLater) => break DriveOutcome::Idle,
                Err(_) => {
                    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
                    break DriveOutcome::Idle;
                }
            }
        };
        // Bump the version EXACTLY when feed state actually changed
        // (findings 5+6): a delivery, a publication, a swap, or the
        // lifecycle transition ITSELF (its repeated observation is not
        // a change). Idle, no-progress and source failures changed
        // nothing — bumping would wake every parked session into
        // another immediate drive (the busy retry loop).
        let bump = match outcome {
            DriveOutcome::Solo { .. } | DriveOutcome::Published => true,
            DriveOutcome::Closed | DriveOutcome::IncarnationClosed(_) => transitioned,
            DriveOutcome::Idle
            | DriveOutcome::NoProgress
            | DriveOutcome::SourceFailed
            | DriveOutcome::Cancelled => false,
        };
        if bump {
            let ver = {
                let mut st = self.st.lock().unwrap();
                st.version += 1;
                st.version
            };
            crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.fetch_add(1, Ordering::Relaxed);
            let _ = self.changed.send(ver);
        }
        outcome
    }

    async fn read_and_publish(&self, src: &Arc<dyn FeedSourceRead>, head: u64) -> DriveOutcome {
        let read = tokio::select! {
            r = src.read_batch(head, self.read_cap) => r,
            // Round-11.1: feed teardown cancels in-flight source work
            // (the dropped future aborts a remote page's request).
            _ = self.cancel.cancelled() => return DriveOutcome::Cancelled,
        };
        let batch = match read {
            Ok(x) => x,
            Err(e) => {
                // Round-11.2: FATAL span outcomes become the typed
                // lifecycle cutoff — never an endless retry.
                if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
                    let reason = cut.0;
                    let mut st = self.st.lock().unwrap();
                    st.lifecycle = Lifecycle::Gone(reason);
                    st.version += 1;
                    let ver = st.version;
                    drop(st);
                    crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.fetch_add(1, Ordering::Relaxed);
                    let _ = self.changed.send(ver);
                    return DriveOutcome::IncarnationClosed(reason);
                }
                crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
                return DriveOutcome::SourceFailed;
            }
        };
        // No-progress partial page (finding 6): nothing scanned, nothing
        // matched — report it WITHOUT touching head/version. The session
        // parks; the next durable advance or heartbeat retries.
        if batch.scan_to <= batch.scan_from && batch.records.is_empty() {
            if !batch.completed {
                crate::sse::auth::sse_stats::FEED_NO_PROGRESS.fetch_add(1, Ordering::Relaxed);
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
                // The wire position is located with THE READING SOURCE
                // and bound into the record — sessions never re-locate
                // against a newer source (review round 4, blocker 2).
                pos: src.locate(r.off + 1),
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
        // SHARED: retention reserves the ACTUAL retained bytes from
        // the process-global budget (budget model B) — one exact
        // reservation per retained batch, released on eviction and at
        // feed drop.
        let batch_charge = charge_for(&prepared);
        // UNCACHED posture (oversized batch, or a net reservation the
        // process budget cannot host): release and clear EVERY retained
        // batch — after `floor = head` that ring is unreachable anyway,
        // and keeping it would pin the global budget (review round 3,
        // retained-ring rollover). The head/floor still advance;
        // sessions below the floor take the typed lag path and resume
        // durably.
        if batch_charge > self.ring_budget {
            crate::sse::auth::sse_stats::FEED_OVERSIZE_DROPPED.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                batch_charge,
                ring = self.ring_budget,
                "livefeed batch exceeds the feed ring; published without retention"
            );
            clear_ring(
                &self.budget,
                &self.project_reserved,
                &mut st,
                &self.retained_charge,
            );
            st.floor = st.head;
            return DriveOutcome::Published;
        }
        // Which OLD batches must this publication evict to fit the
        // ring? Determined FIRST, so the reservation can be the NET of
        // replacement — a full ring rolls forward at a full global cap
        // (review round 3).
        let mut evict_n = 0usize;
        let mut evict_charge = 0usize;
        {
            let mut projected = st.charge + batch_charge;
            for b in &st.batches {
                if projected <= self.ring_budget {
                    break;
                }
                projected -= b.charge;
                evict_charge += b.charge;
                evict_n += 1;
            }
            debug_assert!(
                projected <= self.ring_budget,
                "batch fits the ring, so evictions always settle"
            );
        }
        match self
            .budget
            .try_replace(&self.project_reserved, evict_charge, batch_charge)
        {
            ReserveOutcome::Reserved => {}
            over => {
                match over {
                    ReserveOutcome::ProjectOver => {
                        crate::sse::auth::sse_stats::FEED_PROJECT_CAP_UNCACHED
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        crate::sse::auth::sse_stats::FEED_UNCACHED_PUBLISH
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                clear_ring(
                    &self.budget,
                    &self.project_reserved,
                    &mut st,
                    &self.retained_charge,
                );
                st.floor = st.head;
                return DriveOutcome::Published;
            }
        }
        // The replacement ALREADY netted the evicted charges out of the
        // global counter — pop them WITHOUT releasing again.
        for _ in 0..evict_n {
            let b = st.batches.pop_front().expect("eviction set pre-counted");
            st.charge -= b.charge;
            st.floor = st.floor.max(b.scan_to);
        }
        st.charge += batch_charge;
        st.batches.push_back(Arc::new(PreparedBatch {
            scan_to,
            charge: batch_charge,
            records: prepared.into(),
        }));
        self.retained_charge.store(st.charge, Ordering::Relaxed);
        DriveOutcome::Published
    }
}

/// Release and clear EVERY retained batch (uncached posture): nothing
/// unreachable may keep a global reservation.
fn clear_ring(
    budget: &Arc<FeedMemoryBudget>,
    proj: &ProjectRetention,
    st: &mut FeedState,
    gauge: &AtomicUsize,
) {
    for b in st.batches.drain(..) {
        budget.release(proj, b.charge);
    }
    st.charge = 0;
    gauge.store(0, Ordering::Relaxed);
}

impl Drop for LiveFeed {
    fn drop(&mut self) {
        // Release the ACTUAL retained bytes back to the process budget
        // (model B); the feed itself is being discarded.
        let charge = self.st.get_mut().unwrap().charge;
        if charge > 0 {
            self.budget.release(&self.project_reserved, charge);
        }
        // Bounded project-tracker lifetime (round-10e): the last feed
        // of a project retires its retention entry.
        self.budget
            .release_project_entry(&self.project, &self.project_reserved);
    }
}

pub(crate) enum Take {
    /// One shared batch plus the index of the first record at/after the
    /// session's cursor. Sessions iterate `batch.records[start..]`.
    Batch {
        batch: Arc<PreparedBatch>,
        start_index: usize,
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
            Self::AtHead => f.write_str("AtHead"),
            Self::Lagged { floor } => f.debug_struct("Lagged").field("floor", floor).finish(),
        }
    }
}

/// The one atomic install decision (round-9 review): taken under the
/// source write lock so racing installers of the same extension
/// resolve to exactly one generation bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallOutcome {
    /// Strictly longer compatible extension: installed, generation +1.
    Installed,
    /// Compatible but not strictly longer: the transition is already
    /// installed (or a same-length refresh that never installs) —
    /// idempotent no-op, NO generation bump.
    AlreadyCurrent,
    /// Not a continuation of this feed's cursor space.
    Incompatible,
}

pub(crate) enum DriveOutcome {
    /// Zero retention: these records belong to the driving session,
    /// plus the SCANNED boundary (finding 1/2 — the cursor must advance
    /// to scan_to, covering match-free ranges). Wire positions are
    /// bound per record at preparation (round-11.3: the raw
    /// generation binding is gone — tokens name segments directly).
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
    /// The source cannot continue here (typed reason): sessions
    /// disconnect WITHOUT a terminal control (Stage 6; clients resume
    /// from their cursors through the legacy lineage path).
    IncarnationClosed(SourceCutoff),
    /// The source read failed; no state changed, no version bump. The
    /// session parks and retries on the next wake (finding 6).
    SourceFailed,
    /// The feed is tearing down (last subscriber left): the read was
    /// cancelled mid-flight. Nothing to deliver, nothing to retry.
    Cancelled,
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
                self.read_started.notify_waiters();
                self.read_release.notified().await;
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
                self.next_started.notify_waiters();
                self.next_release.notified().await;
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.retained(), 0, "the oversized batch is not retained");
        assert_eq!(feed.head(), 3);
        assert_eq!(feed.floor(), 3, "floor advanced with the head");
        assert_eq!(budget.reserved(), 0);
        match feed.take_visible(0) {
            Take::Lagged { floor } => assert_eq!(floor, 3),
            other => panic!("a cursor below the new floor is typed lag: {other:?}"),
        }
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        let first = budget.reserved();
        assert!(first > 0);
        src.frontier.store(4, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        // The first batch was evicted by the second; only the second's
        // charge remains reserved.
        assert_eq!(feed.retained(), budget.reserved() as usize);
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
        match feed.take_visible(0) {
            Take::Batch { batch, .. } => assert_eq!(batch.records.len(), 3),
            other => panic!("retained batch survives the solo drive: {other:?}"),
        }
        // Once the survivor's cursor passes the ring, it is AT the
        // head — and the drain-release popped the passed batch,
        // returning its reservation to the process budget.
        match feed.take_visible(5) {
            Take::AtHead => {}
            other => panic!("expected AtHead past the ring, got {other:?}"),
        }
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
            match feed.drive_once().await {
                Some(DriveOutcome::Idle) => {}
                other => panic!("expected Idle, got {}", outcome_name(&other)),
            }
        }
        assert_eq!(feed.version(), v, "Idle never bumps");
        // Closed: exactly ONE bump across the transition; the outcome
        // stays OBSERVABLE for every later drive (parked sessions must
        // each see it) but never bumps again.
        src.closed.store(true, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Closed) => {}
            other => panic!("expected Closed, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.version(), v + 1, "the close bumps exactly once");
        match feed.drive_once().await {
            Some(DriveOutcome::Closed) => {}
            other => panic!(
                "the close outcome must stay observable, got {}",
                outcome_name(&other)
            ),
        }
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
            match feed.drive_once().await {
                Some(DriveOutcome::Published) => {}
                other => panic!("expected Published, got {}", outcome_name(&other)),
            }
        }
        assert_eq!(feed.source_read_count(), 3);
        assert_eq!(feed.version(), 3, "one bump per real publication");
        // Drain: take_visible must hand out all three batches with no
        // further source read.
        let mut cursor = 0u64;
        let mut batches = 0;
        loop {
            match feed.take_visible(cursor) {
                Take::Batch { batch, .. } => {
                    batches += 1;
                    cursor = cursor.max(batch.scan_to);
                }
                Take::AtHead => break,
                Take::Lagged { floor } => panic!("unexpected lag: floor {floor}"),
            }
        }
        assert_eq!(batches, 3, "all three retained batches drained");
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
        let join = task.await;
        assert!(join.is_err(), "the drive was aborted");
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn driver_permit_releases_exactly_once() {
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let (feed, src) = feed_with(1, 8, 4096, &budget);
        feed.fp_name.set("permit-once".into()).unwrap();
        crate::failpoints::arm(crate::failpoints::Fp::FeedAfterPermitRelease, "permit-once");
        // A: drives one record, releases the permit, parks at the hook.
        let feed_a = feed.clone();
        let a = tokio::spawn(async move { feed_a.drive_once().await });
        let mut parked = 0;
        for _ in 0..400 {
            parked = crate::failpoints::parked(
                crate::failpoints::Fp::FeedAfterPermitRelease,
                "permit-once",
            );
            if parked >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(parked >= 1, "A must park after its release");
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
        crate::failpoints::release(crate::failpoints::Fp::FeedAfterPermitRelease, "permit-once");
        let a_out = a.await.unwrap();
        assert!(a_out.is_some(), "A's drive completed");
        // C: the permit must still be HELD by B.
        assert!(
            feed.acquire_permit().is_none(),
            "third driver admitted while B is mid-drive: the permit was released twice"
        );
        src.block_reads.store(false, Ordering::Relaxed);
        src.read_release.notify_waiters();
        let _ = b.await.unwrap();
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
        for i in 0..300 {
            let src = Arc::new(FakeSource::new(0, 8));
            let feed = LiveFeed::new_with_budget(
                FeedKey::default_lane([i as u8; 16]),
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert_eq!(budget.reserved(), 340, "the first batch fills the cap");
        src.frontier.store(2, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!(
                "the ring must roll forward at a full cap, got {}",
                outcome_name(&other)
            ),
        }
        assert_eq!(
            budget.reserved(),
            340,
            "net replacement: cap never exceeded"
        );
        assert_eq!(feed.floor(), 1, "the replaced batch moved the floor");
        match feed.take_visible(1) {
            Take::Batch { batch, .. } => {
                assert_eq!(batch.scan_to, 2, "the NEW batch is the one retained")
            }
            other => panic!("the new batch must be retained: {other:?}"),
        }
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
            match f.drive_once().await {
                Some(DriveOutcome::Published) => {}
                other => panic!("expected Published, got {}", outcome_name(&other)),
            }
        }
        assert_eq!(budget.reserved(), 680, "both feeds fill the cap");

        // Feed A publishes again: no room (and A's huge ring evicts
        // nothing of its own), so A's now-unreachable old ring must be
        // cleared and released.
        src_a.frontier.store(2, Ordering::Relaxed);
        match feed_a.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert_eq!(feed_a.retained(), 0, "A's stale ring was cleared");
        assert_eq!(
            budget.reserved(),
            340,
            "A's reservation was released; B's is untouched"
        );
        assert_eq!(feed_a.floor(), 2, "A's floor advanced with the head");
        match feed_a.take_visible(0) {
            Take::Lagged { floor } => assert_eq!(floor, 2),
            other => panic!("A's stale cursor is typed lag: {other:?}"),
        }
        // B's retention is fully intact and consumable.
        match feed_b.take_visible(0) {
            Take::Batch { batch, .. } => assert_eq!(batch.records.len(), 1),
            other => panic!("B's ring is intact: {other:?}"),
        }
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
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.retained(), 340, "the ordinary batch is retained");
        src.frontier.store(4, Ordering::Relaxed);
        match feed.drive_once().await {
            Some(DriveOutcome::Published) => {}
            other => panic!("expected Published, got {}", outcome_name(&other)),
        }
        assert_eq!(feed.retained(), 0, "the old ring was cleared too");
        assert_eq!(budget.reserved(), 0, "its reservation was released");
        assert_eq!(feed.head(), 4);
        assert_eq!(feed.floor(), 4);
    }

    /// Rollover (red): 32 shared feeds publishing CONCURRENTLY against
    /// one budget — the hard cap holds at every instant, no underflow,
    /// and teardown returns the budget to exactly zero.
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
        let mut retained_n = 0usize;
        for h in handles {
            if matches!(h.await.unwrap(), Some(DriveOutcome::Published)) {
                retained_n += 1;
            }
        }
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
}
