//! The shard's native-queue maintenance (docs/QUEUES-DESIGN.md §6.8, §8.2):
//! a sweeper that serves every queue whose roster deadline has come, and a
//! collector that turns deletion debt into physically freed storage with
//! one explicit compaction over every L0 file and sorted run into the last
//! run (so tombstones drop), then waits for garbage collection to delete
//! what the compaction superseded before it clears the debt it covered.
use super::*;
use crate::native_queue::rows::StateRow;
use crate::native_queue::{NqOp, keys};

/// How often the sweeper reads the roster.
const SWEEP_TICK: std::time::Duration = std::time::Duration::from_secs(1);
/// How often the collector considers a pass.
const COLLECT_TICK: std::time::Duration = std::time::Duration::from_secs(30);
/// Debt that starts a pass at once (efficient under load).
const FORCE_BYTES: u64 = 256 << 20;
/// The trigger floor (§8.2, Fable rev-7 H1): below it the age trigger
/// waits for `MAX_INTERVAL_MS`.
const MIN_PASS_BYTES: u64 = 16 << 20;
/// Launch age: how old the oldest debt may get before a pass starts.
const LAUNCH_AGE_MS: i64 = 15 * 60_000;
/// The longest any debt waits for a pass, however small (Fable code
/// review H5: a pass rewrites every run of the shard, so small debt waits
/// for a day rather than costing a full rewrite every two hours).
const MAX_INTERVAL_MS: i64 = 24 * crate::native_queue::HOUR_MS;
/// Below the size trigger, the age trigger also needs debt worth this
/// fraction (1/RELATIVE_DENOM) of the shard's compacted bytes.
const RELATIVE_DENOM: u64 = 20;
/// How long a pass waits for the compactor to hold no live job before it
/// defers to the next tick, and how often it looks.
const QUIET_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
const QUIET_POLL: std::time::Duration = std::time::Duration::from_millis(250);
/// How long a pass waits for its compaction, and for GC afterwards.
const COMPACTION_WAIT: std::time::Duration = std::time::Duration::from_secs(600);
const GC_WAIT: std::time::Duration = std::time::Duration::from_mins(45);
/// Roster entries read per tick.
const ROSTER_PAGE: usize = 4_096;

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
/// whose oldest was recorded at `oldest`, on a shard log of `compacted`
/// compacted bytes, at `now`.
fn pass_due(total: u64, oldest: Option<i64>, now: i64, compacted: u64) -> bool {
    let floor = MIN_PASS_BYTES.max(compacted / RELATIVE_DENOM);
    oldest.is_some_and(|oldest| {
        total >= FORCE_BYTES
            || (total >= floor && now - oldest >= LAUNCH_AGE_MS)
            || now - oldest >= MAX_INTERVAL_MS
    })
}

/// Whether the compactor holds no live job. A database whose compactor has
/// not yet written its compactions file is not quiet: nothing would take a
/// submission.
fn quiet(view: &slatedb::compactor::CompactorStateView) -> bool {
    view.compactions()
        .is_some_and(|file| !file.recent_compactions().any(|c| c.active()))
}

/// The coverage rule's sources in `manifest`: every L0 view in tree order,
/// then every sorted run; and the SST ids they supersede.
fn everything(
    manifest: &slatedb::VersionedManifest,
) -> (Vec<slatedb::compactor::SourceId>, Vec<String>) {
    use slatedb::compactor::SourceId;
    let l0 = manifest.l0().iter();
    let mut sources: Vec<SourceId> = l0.clone().map(|v| SourceId::SstView(v.id)).collect();
    let mut superseded: Vec<String> = l0
        .map(|v| v.sst.id.unwrap_compacted_id().to_string())
        .collect();
    for run in manifest.compacted() {
        sources.push(SourceId::SortedRun(run.id));
        let tables = run.sst_views().iter();
        superseded.extend(tables.map(|v| v.sst.id.unwrap_compacted_id().to_string()));
    }
    (sources, superseded)
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
    async fn maybe_collect(&self) -> Result<(), String> {
        if self.due_debt().await?.is_none() {
            return Ok(());
        }
        let _permit = PASS_PERMIT.acquire().await.map_err(|e| e.to_string())?;
        let Some(due) = self.due_debt().await? else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let superseded = self.compact_everything().await?;
        let compacted_secs = started.elapsed().as_secs_f64();
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
        if !pass_due(total, oldest, at, self.compacted_bytes()) {
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

    /// The shard log's compacted bytes, from its manifest.
    fn compacted_bytes(&self) -> u64 {
        self.db
            .manifest()
            .compacted()
            .iter()
            .map(|run| run.estimate_size())
            .sum()
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
    /// then compact every L0 view and every sorted run of the manifest it
    /// read into the last run. Returns the superseded SST ids. A pass whose
    /// compactor stays busy, whose spec is refused, or whose compaction
    /// fails or outlasts its wait returns an error, and the next tick starts
    /// over: a pass submits one spec, and never beside a live job, its own
    /// earlier one included.
    async fn compact_everything(&self) -> Result<Vec<String>, String> {
        use slatedb::compactor::CompactionSpec;
        use slatedb::config::{FlushOptions, FlushType};
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(|e| e.to_string())?;
        let admin = self.queue_admin();
        let quiet = Self::await_quiet(&admin).await;
        let view = quiet.ok_or("the compactor held a live job throughout the wait")?;
        let (sources, superseded) = everything(view.manifest());
        if sources.is_empty() {
            return Ok(superseded);
        }
        let destination = view.manifest().compacted().last().map_or(0, |run| run.id);
        let compaction = admin
            .submit_compaction(CompactionSpec::new(sources, destination))
            .await
            .map_err(|error| format!("queue pass spec refused: {error}"))?;
        if Self::await_compaction(&admin, &compaction).await {
            return Ok(superseded);
        }
        Err("the collection compaction did not complete".to_owned())
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
    /// one the manifest names as its last compacted L0 (D22's residual).
    async fn await_collection(&self, superseded: &[String]) -> Result<(), String> {
        let deadline = std::time::Instant::now() + GC_WAIT;
        loop {
            let residual = self
                .queue_admin()
                .read_manifest(None)
                .await
                .ok()
                .flatten()
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
