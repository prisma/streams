//! The shard's native-queue maintenance (docs/QUEUES-DESIGN.md §6.8, §8.2):
//! a sweeper that serves every queue whose roster deadline has come, and a
//! collector that turns deletion debt into physically freed storage with
//! an explicit compaction of the shard log (layout 7: queue rows under
//! `'Q' ‖ gen` and stream rows under `'S'` share one tree, `crate::keyspace`)
//! — every L0 file and sorted run into the last run, so tombstones drop —
//! then waits for garbage collection to delete what the compaction
//! superseded before it clears the debt it covered.
//!
//! A pass that names the L0s holds the log's L0 lane (SlateDB runs one L0
//! compaction at a time), and a flush into a log at `l0_max_ssts` stalls
//! every write of the shard (DESIGN-6A F5). A pass whose rewrite would let
//! the L0s arriving meanwhile reach the line is split: the L0s into a fresh
//! run first, then the runs into the last run, which holds no L0 lane.
use super::*;
use crate::native_queue::rows::StateRow;
use crate::native_queue::{NqOp, keys};

/// How often the sweeper reads the roster.
const SWEEP_TICK: std::time::Duration = std::time::Duration::from_secs(1);
/// How often the collector considers a pass.
const COLLECT_TICK: std::time::Duration = std::time::Duration::from_secs(30);
/// Debt that starts a pass at once (efficient under load).
const FORCE_BYTES: u64 = 256 << 20;
/// The longest any debt waits for a pass, however small: a pass rewrites
/// the whole shard log, stream rows included (layout 7), so small debt
/// waits for a day rather than costing a whole-log rewrite every hour
/// (QUEUE-TREE-DECISION §2; QUEUES-DESIGN §5.7's bound).
const MAX_INTERVAL_MS: i64 = 24 * crate::native_queue::HOUR_MS;
/// How long a pass waits for the compactor to hold no live job before it
/// defers to the next tick, and how often it looks.
const QUIET_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
const QUIET_POLL: std::time::Duration = std::time::Duration::from_millis(250);
/// How long a pass waits for its compaction, and for GC afterwards.
const COMPACTION_WAIT: std::time::Duration = std::time::Duration::from_secs(600);
const GC_WAIT: std::time::Duration = std::time::Duration::from_mins(45);
/// Roster entries read per tick.
const ROSTER_PAGE: usize = 4_096;
/// The shard log's L0 line (`l0_max_ssts`, the CLI default the
/// shard log runs with): a flush into a tree holding this many L0s stalls.
const L0_LINE: usize = 32;
/// L0 slots a pass leaves free below the line.
const L0_MARGIN: usize = 4;
/// The shard's memtable flush cadence (the engine's flush ticker): one L0
/// arrives this often while a pass runs (at most: the seal cadence flushes
/// less often).
const FLUSH_SECS: u64 = 5;
/// The rewrite rate a pass is planned with before one has been measured.
const INITIAL_RATE_BPS: u64 = 30 << 20;

/// The measured rewrite rate of the last passes (bytes per second, a
/// running mean), instance-wide like the permit.
static PASS_RATE_BPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(INITIAL_RATE_BPS);

/// One instance-wide pass at a time (§8.2 "one pass at a time per
/// instance"): SlateDB budgets each database's compactor on its own, so
/// the queue passes of every shard share this permit.
static PASS_PERMIT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// A queue's debt as a pass captured it.
struct Captured {
    hash: [u8; 16],
    generation: u64,
    bytes: u64,
}

/// The debt a due pass covers: every queue's share, their total, and when
/// it was captured.
struct DueDebt {
    captured: Vec<Captured>,
    total: u64,
    at: i64,
}

/// Whether captured debt calls for a pass (§8.2 trigger): `total` bytes
/// whose oldest was recorded at `oldest`, at `now`.
fn pass_due(total: u64, oldest: Option<i64>, now: i64) -> bool {
    oldest.is_some_and(|oldest| total >= FORCE_BYTES || now - oldest >= MAX_INTERVAL_MS)
}

/// Whether the compactor holds no live job. A database whose compactor has
/// not yet written its compactions file is not quiet: nothing would take a
/// submission.
fn quiet(view: &slatedb::compactor::CompactorStateView) -> bool {
    view.compactions()
        .is_some_and(|file| !file.recent_compactions().any(|c| c.active()))
}

/// The shard log's one tree: its L0 views and sorted runs.
struct Tree<'a> {
    l0: &'a std::collections::VecDeque<slatedb::manifest::SsTableView>,
    runs: &'a [slatedb::manifest::SortedRun],
}

fn shard_tree(manifest: &slatedb::VersionedManifest) -> Tree<'_> {
    Tree {
        l0: manifest.l0(),
        runs: manifest.compacted(),
    }
}

/// The SST ids a set of L0 views and runs supersede.
fn superseded_ids<'a>(
    l0: impl Iterator<Item = &'a slatedb::manifest::SsTableView>,
    runs: &[slatedb::manifest::SortedRun],
) -> Vec<String> {
    let mut ids: Vec<String> = l0
        .map(|v| v.sst.id.unwrap_compacted_id().to_string())
        .collect();
    for run in runs {
        let tables = run.sst_views().iter();
        ids.extend(tables.map(|v| v.sst.id.unwrap_compacted_id().to_string()));
    }
    ids
}

/// One compaction of a pass over the shard log.
struct Step {
    sources: Vec<slatedb::compactor::SourceId>,
    destination: u32,
    superseded: Vec<String>,
    /// The bytes it reads (and at most rewrites).
    bytes: u64,
    l0s: usize,
    runs: usize,
}

/// A fresh sorted-run id: above every run and every live compaction's
/// destination.
fn fresh_run_id(view: &slatedb::compactor::CompactorStateView) -> u32 {
    let committed = view.manifest().compacted().iter().map(|run| run.id);
    let live = view.compactions().into_iter().flat_map(|file| {
        file.recent_compactions()
            .filter(|c| c.active())
            .filter_map(|c| c.spec().destination())
            .collect::<Vec<_>>()
    });
    committed
        .chain(live)
        .max()
        .map_or(0, |id| id.saturating_add(1))
}

/// The coverage rule's sources in the shard log: every L0 view in tree
/// order, then every sorted run, into the last run (a fresh run when the
/// log has none, which is then its last).
fn everything(view: &slatedb::compactor::CompactorStateView) -> Option<Step> {
    use slatedb::compactor::SourceId;
    let tree = shard_tree(view.manifest());
    let mut sources: Vec<SourceId> = tree.l0.iter().map(|v| SourceId::SstView(v.id)).collect();
    sources.extend(tree.runs.iter().map(|run| SourceId::SortedRun(run.id)));
    if sources.is_empty() {
        return None;
    }
    let destination = match tree.runs.last() {
        Some(run) => run.id,
        None => fresh_run_id(view),
    };
    Some(Step {
        sources,
        destination,
        superseded: superseded_ids(tree.l0.iter(), tree.runs),
        bytes: tree.l0.iter().map(|v| v.estimate_size()).sum::<u64>()
            + tree.runs.iter().map(|r| r.estimate_size()).sum::<u64>(),
        l0s: tree.l0.len(),
        runs: tree.runs.len(),
    })
}

/// The first half of a split pass: every L0 view of the shard log into a
/// fresh run.
fn l0s_only(view: &slatedb::compactor::CompactorStateView) -> Option<Step> {
    use slatedb::compactor::SourceId;
    let tree = shard_tree(view.manifest());
    if tree.l0.is_empty() {
        return None;
    }
    Some(Step {
        sources: tree.l0.iter().map(|v| SourceId::SstView(v.id)).collect(),
        destination: fresh_run_id(view),
        superseded: superseded_ids(tree.l0.iter(), &[]),
        bytes: tree.l0.iter().map(|v| v.estimate_size()).sum(),
        l0s: tree.l0.len(),
        runs: 0,
    })
}

/// The second half of a split pass: every sorted run of the shard log into
/// its last run. It names no L0, so the scheduler keeps compacting the
/// log's new L0s beside it.
fn runs_only(view: &slatedb::compactor::CompactorStateView) -> Option<Step> {
    use slatedb::compactor::SourceId;
    let tree = shard_tree(view.manifest());
    let last = tree.runs.last()?;
    Some(Step {
        sources: tree
            .runs
            .iter()
            .map(|r| SourceId::SortedRun(r.id))
            .collect(),
        destination: last.id,
        superseded: superseded_ids(std::iter::empty(), tree.runs),
        bytes: tree.runs.iter().map(|r| r.estimate_size()).sum(),
        l0s: 0,
        runs: tree.runs.len(),
    })
}

/// Whether one pass naming `l0s` L0s and reading `bytes` at `rate_bps`
/// keeps the shard log under the L0 line: the L0s it names stay in the
/// log until it commits, and one more arrives every flush meanwhile.
fn single_pass_fits(l0s: usize, bytes: u64, rate_bps: u64) -> bool {
    if rate_bps == 0 {
        return false;
    }
    let secs = bytes.div_ceil(rate_bps);
    let arrivals = usize::try_from(secs.div_ceil(FLUSH_SECS)).unwrap_or(usize::MAX);
    l0s.saturating_add(arrivals).saturating_add(L0_MARGIN) <= L0_LINE
}

/// What a pass did, for its log line and the rate it measured.
#[derive(Default)]
struct PassReport {
    superseded: Vec<String>,
    rewritten_bytes: u64,
    l0s: usize,
    runs: usize,
    steps: usize,
    split: bool,
}

#[cfg(test)]
mod native_queue_collector_tests;

impl ShardEngine {
    pub(super) fn spawn_queue_maintenance(self: &Arc<Self>) {
        let sweeper = self.clone();
        self.spawn_required("queue-sweeper", async move { sweeper.sweeper_loop().await });
        let collector = self.clone();
        self.spawn_required("queue-collector", async move {
            collector.collector_loop().await
        });
        let sealer = self.clone();
        self.spawn_required("sealer", async move { sealer.seal_loop().await });
        let deadlines = self.clone();
        self.spawn_required(
            "seal-deadlines",
            async move { deadlines.deadline_loop().await },
        );
        let folder = self.clone();
        self.spawn_required("seal-fold", async move { folder.fold_loop().await });
        let consolidator = self.clone();
        self.spawn_required("seal-consolidate", async move {
            consolidator.consolidate_loop().await
        });
        let collector = self.clone();
        self.spawn_required("seal-gc", async move { collector.gc_loop().await });
        let watch = self.clone();
        self.spawn_required("l0-watch", async move { watch.l0_watch_loop().await });
    }

    /// Waits for the next tick; false once the engine closes.
    async fn queue_tick(
        &self,
        interval: &mut tokio::time::Interval,
        closed: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        tokio::select! {
            _ = closed.changed() => {}
            _ = interval.tick() => {}
        }
        !*closed.borrow() && !self.is_closed()
    }

    async fn sweeper_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(SWEEP_TICK);
        let mut closed = self.close_tx.subscribe();
        while self.queue_tick(&mut interval, &mut closed).await {
            self.sweep_due_queues().await;
        }
    }

    async fn collector_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(COLLECT_TICK);
        let mut closed = self.close_tx.subscribe();
        while self.queue_tick(&mut interval, &mut closed).await {
            if let Err(error) = self.maybe_collect().await {
                tracing::warn!(shard = %self.prefix, "queue collection pass deferred: {error}");
            }
        }
    }

    /// The roster: (queue hash, next deadline).
    async fn queue_roster(&self) -> Result<Vec<([u8; 16], i64)>, String> {
        let mut it = self
            .db
            .scan_prefix_with_options(
                &keys::ROSTER_PREFIX[..],
                ..,
                &crate::native_queue::scan_options(),
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while out.len() < ROSTER_PAGE {
            let Some(kv) = it.next().await.map_err(|e| e.to_string())? else {
                break;
            };
            let hash: Option<[u8; 16]> = kv
                .key
                .get(keys::ROSTER_PREFIX.len()..)
                .and_then(|h| h.try_into().ok());
            let at: Option<[u8; 8]> = kv.value.get(..8).and_then(|v| v.try_into().ok());
            if let (Some(hash), Some(at)) = (hash, at) {
                out.push((hash, i64::from_be_bytes(at)));
            }
        }
        Ok(out)
    }

    async fn sweep_due_queues(&self) {
        let roster = match self.queue_roster().await {
            Ok(r) => r,
            Err(error) => {
                tracing::warn!(shard = %self.prefix, "queue roster unreadable: {error}");
                return;
            }
        };
        let now = now_ms();
        for (hash, _) in roster.into_iter().filter(|(_, at)| *at <= now) {
            match self.submit_native_queue(hash, NqOp::Sweep).await {
                Ok(crate::native_queue::NqOut::Swept {
                    wake: Some(wake), ..
                }) => self.notify(hash, wake).await,
                Ok(_) => {}
                Err(refusal) => {
                    tracing::debug!(shard = %self.prefix, "queue sweep refused: {refusal:?}")
                }
            }
        }
    }

    /// Post a wake-up notification off the sweeper's path (§9.2): a hint,
    /// never a delivery, so a failure costs latency only; the backoff the
    /// sweep recorded paces the next one.
    async fn notify(&self, hash: [u8; 16], wake: crate::native_queue::WakeRequest) {
        let Some(root) = crate::native_queue::wakeup::root() else {
            return;
        };
        let secret = crate::native_queue::wakeup::wakeup_secret(&root, &hash, wake.version);
        let note = crate::native_queue::wakeup::Notification {
            queue: wake.name,
            group: "default",
            due: wake.due,
            oldest_due_age_ms: wake.oldest_due_age_ms,
            nonce: crate::crypto::hex(&rand::random::<[u8; 12]>()),
            sent_at: now_ms(),
        };
        match crate::native_queue::wakeup::deliver(&wake.url, &secret, &note).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(shard = %self.prefix, "wake-up notification refused by its receiver")
            }
            Err(error) => {
                tracing::debug!(shard = %self.prefix, "wake-up notification failed: {error}")
            }
        }
    }

    /// Start a pass when the debt calls for one (§8.2 trigger). The debt is
    /// captured again once the pass holds the instance's permit: a pass that
    /// waited out another collects what is left then, and nothing when the
    /// other cleared it, so it never rewrites the run that pass just wrote
    /// and then waits on garbage collection for it.
    ///
    /// No pass starts while the shard log's L0s are at the watermark's
    /// yield mark (checked before it waits for the instance's permit, and
    /// again once it holds it, after waiting out another pass): the
    /// scheduler has the compaction slot to itself until they drain
    /// (`layout::l0_watermark`).
    async fn maybe_collect(&self) -> Result<(), String> {
        if self.layout.l0.collector_yields() {
            return Ok(());
        }
        if self.due_debt().await?.is_none() {
            return Ok(());
        }
        let _permit = PASS_PERMIT.acquire().await.map_err(|e| e.to_string())?;
        if self.layout.l0.collector_yields() {
            return Ok(());
        }
        let Some(due) = self.due_debt().await? else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let pass = self.compact_shard_log().await?;
        let compacted = started.elapsed();
        Self::note_rate(pass.rewritten_bytes, compacted);
        let compacted_secs = compacted.as_secs_f64();
        let superseded = pass.superseded;
        self.await_collection(&superseded).await?;
        let (captured, total) = (due.captured, due.total);
        for c in &captured {
            let op = NqOp::ClearDebt {
                generation: c.generation,
                bytes: c.bytes,
                captured_at: due.at,
            };
            // A queue deleted meanwhile has no debt left to clear.
            match self.submit_native_queue(c.hash, op).await {
                Ok(_) | Err(crate::native_queue::NqRefusal::NotFound) => {}
                Err(e) => return Err(format!("{e:?}")),
            }
        }
        tracing::info!(
            shard = %self.prefix,
            queues = captured.len(),
            bytes = total,
            superseded = superseded.len(),
            rewritten_bytes = pass.rewritten_bytes,
            l0s = pass.l0s,
            runs = pass.runs,
            steps = pass.steps,
            split = pass.split,
            compacted_secs,
            collected_secs = started.elapsed().as_secs_f64(),
            "queue collection pass complete"
        );
        Ok(())
    }

    /// The roster's debt now when it calls for a pass (§8.2 trigger).
    async fn due_debt(&self) -> Result<Option<DueDebt>, String> {
        let captured = self.capture_debt().await?;
        let total: u64 = captured.iter().map(|c| c.0.bytes).sum();
        let oldest = captured.iter().map(|c| c.1).min();
        let at = now_ms();
        if !pass_due(total, oldest, at) {
            return Ok(None);
        }
        let captured = captured.into_iter().map(|(c, _)| c).collect();
        Ok(Some(DueDebt {
            captured,
            total,
            at,
        }))
    }

    /// Every roster queue's debt now, with its oldest debt time. Captured
    /// before the flush, so every deletion it covers is in the flush.
    async fn capture_debt(&self) -> Result<Vec<(Captured, i64)>, String> {
        let mut out = Vec::new();
        for (hash, _) in self.queue_roster().await? {
            let Some(raw) = self
                .db
                .get(keys::state(&hash))
                .await
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            let s = StateRow::decode(&raw)?;
            if s.debt_bytes > 0 {
                out.push((
                    Captured {
                        hash,
                        generation: s.debt_generation,
                        bytes: s.debt_bytes,
                    },
                    s.debt_oldest_at,
                ));
            }
        }
        Ok(out)
    }

    /// Fold a pass's measured rewrite rate into the running mean; a pass
    /// under 64 MiB measures its fixed costs, not its rate.
    fn note_rate(bytes: u64, elapsed: std::time::Duration) {
        use std::sync::atomic::Ordering;
        let millis = elapsed.as_millis();
        if bytes < (64 << 20) || millis == 0 {
            return;
        }
        let measured = u64::try_from(u128::from(bytes) * 1000 / millis).unwrap_or(u64::MAX);
        let old = PASS_RATE_BPS.load(Ordering::Relaxed);
        PASS_RATE_BPS.store(old / 2 + measured / 2, Ordering::Relaxed);
    }

    fn queue_admin(&self) -> slatedb::admin::Admin {
        slatedb::admin::AdminBuilder::new(
            crate::sharddir::shard_db_path(&self.prefix),
            self.data_store.clone(),
        )
        .build()
    }

    /// Steps 4-5 of the coverage rule: flush the memtable so every captured
    /// deletion is in an SST, wait until the compactor holds no live job,
    /// then compact every L0 view and every sorted run of the shard log into
    /// its last run — in one compaction when the L0s arriving meanwhile keep
    /// the log under the L0 line, else the L0s into a fresh run first and
    /// then the runs into the last run. Returns the superseded
    /// SST ids and what was rewritten. A pass whose compactor stays busy,
    /// whose spec is refused, or whose compaction fails or outlasts its wait
    /// returns an error, and the next tick starts over: a pass never submits
    /// beside a live job, its own earlier one included.
    async fn compact_shard_log(&self) -> Result<PassReport, String> {
        use std::sync::atomic::Ordering;
        self.flush_memtable().await?;
        self.compact_flushed_shard_log(PASS_RATE_BPS.load(Ordering::Relaxed))
            .await
    }

    /// The pass after its flush, planned at `rate` bytes per second.
    async fn compact_flushed_shard_log(&self, rate: u64) -> Result<PassReport, String> {
        let admin = self.queue_admin();
        let view = Self::await_quiet(&admin)
            .await
            .ok_or("the compactor held a live job throughout the wait")?;
        let mut report = PassReport::default();
        let Some(whole) = everything(&view) else {
            return Ok(report);
        };
        if whole.runs == 0 || single_pass_fits(whole.l0s, whole.bytes, rate) {
            self.run_step(&admin, whole, &mut report).await?;
            return Ok(report);
        }
        report.split = true;
        if let Some(step) = l0s_only(&view) {
            self.run_step(&admin, step, &mut report).await?;
        }
        let view = Self::await_quiet(&admin)
            .await
            .ok_or("the compactor held a live job throughout the wait")?;
        if let Some(step) = runs_only(&view) {
            self.run_step(&admin, step, &mut report).await?;
        }
        Ok(report)
    }

    /// Submit one step of a pass and wait for it.
    async fn run_step(
        &self,
        admin: &slatedb::admin::Admin,
        step: Step,
        report: &mut PassReport,
    ) -> Result<(), String> {
        let spec = slatedb::compactor::CompactionSpec::new(step.sources, step.destination);
        let compaction = admin
            .submit_compaction(spec)
            .await
            .map_err(|error| format!("queue pass spec refused: {error}"))?;
        if !Self::await_compaction(admin, &compaction).await {
            return Err("the collection compaction did not complete".to_owned());
        }
        report.superseded.extend(step.superseded);
        report.rewritten_bytes += step.bytes;
        report.l0s += step.l0s;
        report.runs += step.runs;
        report.steps += 1;
        Ok(())
    }

    /// The compactor's state (its compactions file, then the manifest) once
    /// no job is live in it: none submitted, scheduled, running or awaiting
    /// its commit; `None` when one stayed live for `QUIET_WAIT`. SlateDB
    /// checks a submitted spec against no other job's sources or
    /// destination, and its worker asserts that no two running jobs write
    /// one run: a pass beside a regular compaction into the last run
    /// panicked the compaction worker and closed the shard log on every
    /// reopen (100x run of 2026-10-24). A job the compactor schedules
    /// between this read and the submission still conflicts; only the
    /// compactor's own validation can exclude that.
    async fn await_quiet(
        admin: &slatedb::admin::Admin,
    ) -> Option<slatedb::compactor::CompactorStateView> {
        let poll = Self::poll_quiet(admin);
        tokio::time::timeout(QUIET_WAIT, poll).await.ok()
    }

    /// Read the compactor's state every `QUIET_POLL` until it is quiet.
    async fn poll_quiet(admin: &slatedb::admin::Admin) -> slatedb::compactor::CompactorStateView {
        loop {
            if let Ok(view) = admin.read_compactor_state_view().await
                && quiet(&view)
            {
                return view;
            }
            tokio::time::sleep(QUIET_POLL).await;
        }
    }

    /// Whether the submitted compaction completed within the wait.
    async fn await_compaction(
        admin: &slatedb::admin::Admin,
        compaction: &slatedb::compactor::Compaction,
    ) -> bool {
        let poll = Self::poll_compaction(admin, compaction);
        tokio::time::timeout(COMPACTION_WAIT, poll)
            .await
            .unwrap_or(false)
    }

    /// Read the compaction every half second until it finishes.
    async fn poll_compaction(
        admin: &slatedb::admin::Admin,
        compaction: &slatedb::compactor::Compaction,
    ) -> bool {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if let Some(completed) = Self::compaction_outcome(admin, compaction).await {
                return completed;
            }
        }
    }

    /// A finished compaction's outcome (a record already trimmed away
    /// finished), or `None` while it runs or cannot be read.
    async fn compaction_outcome(
        admin: &slatedb::admin::Admin,
        compaction: &slatedb::compactor::Compaction,
    ) -> Option<bool> {
        use slatedb::compactor::CompactionStatus;
        match admin.read_compaction(compaction.id(), None).await {
            Ok(Some(c)) if c.status() == CompactionStatus::Completed => Some(true),
            Ok(None) => Some(true),
            Ok(Some(c)) if c.status() == CompactionStatus::Failed => Some(false),
            _ => None,
        }
    }

    /// Superseded SSTs still in the bucket, the residual excepted.
    async fn superseded_left(&self, superseded: &[String], residual: Option<&String>) -> usize {
        let base = crate::sharddir::shard_db_path(&self.prefix);
        let mut left = 0usize;
        for id in superseded.iter().filter(|id| Some(*id) != residual) {
            let path = object_store::path::Path::from(format!("{base}/compacted/{id}.sst"));
            if object_store::ObjectStoreExt::head(&*self.data_store, &path)
                .await
                .is_ok()
            {
                left += 1;
            }
        }
        left
    }

    /// Stage 4: every superseded SST is gone from the bucket, except the
    /// one the manifest names as the last compacted L0 (D22's residual: GC
    /// keeps the newest compacted L0 until a newer L0 exists). An unsegmented
    /// manifest records that L0's SST id itself (layout 7).
    async fn await_collection(&self, superseded: &[String]) -> Result<(), String> {
        let deadline = std::time::Instant::now() + GC_WAIT;
        loop {
            let manifest = self.queue_admin().read_manifest(None).await.ok().flatten();
            let residual = manifest
                .as_ref()
                .and_then(|m| m.last_compacted_l0_sst_id())
                .map(|u| u.to_string());
            let left = self.superseded_left(superseded, residual.as_ref()).await;
            if left == 0 {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!("{left} superseded SSTs still present"));
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}
