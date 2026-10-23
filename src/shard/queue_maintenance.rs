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
/// The longest any debt waits for a pass, however small.
const MAX_INTERVAL_MS: i64 = 2 * crate::native_queue::HOUR_MS;
/// How long a pass waits for its compaction, and for GC afterwards.
const COMPACTION_WAIT: std::time::Duration = std::time::Duration::from_secs(600);
const GC_WAIT: std::time::Duration = std::time::Duration::from_secs(45 * 60);
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
            .scan_prefix(&keys::ROSTER_PREFIX[..], ..)
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

    /// Start a pass when the debt calls for one (§8.2 trigger).
    async fn maybe_collect(&self) -> Result<(), String> {
        let captured = self.capture_debt().await?;
        let total: u64 = captured.iter().map(|c| c.0.bytes).sum();
        let oldest = captured.iter().map(|c| c.1).min();
        let now = now_ms();
        let due = match oldest {
            None => false,
            Some(oldest) => {
                total >= FORCE_BYTES
                    || (total >= MIN_PASS_BYTES && now - oldest >= LAUNCH_AGE_MS)
                    || now - oldest >= MAX_INTERVAL_MS
            }
        };
        if !due {
            return Ok(());
        }
        let _permit = PASS_PERMIT.acquire().await.map_err(|e| e.to_string())?;
        let started = std::time::Instant::now();
        let superseded = self.compact_everything().await?;
        let compacted_secs = started.elapsed().as_secs_f64();
        self.await_collection(&superseded).await?;
        for (c, _) in &captured {
            let op = NqOp::ClearDebt {
                generation: c.generation,
                bytes: c.bytes,
            };
            self.submit_native_queue(c.hash, op)
                .await
                .map_err(|e| format!("{e:?}"))?;
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

    fn queue_admin(&self) -> slatedb::admin::Admin {
        slatedb::admin::AdminBuilder::new(
            crate::sharddir::shard_db_path(&self.prefix),
            self.data_store.clone(),
        )
        .build()
    }

    /// Steps 4-5 of the coverage rule: flush the memtable so every captured
    /// deletion is in an SST, then compact every L0 view and every sorted
    /// run into the last run. Returns the superseded SST ids.
    async fn compact_everything(&self) -> Result<Vec<String>, String> {
        use slatedb::compactor::{CompactionSpec, SourceId};
        use slatedb::config::{FlushOptions, FlushType};
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(|e| e.to_string())?;
        let admin = self.queue_admin();
        for attempt in 0..5u32 {
            let manifest = self.db.manifest();
            let mut sources: Vec<SourceId> = manifest
                .l0()
                .iter()
                .map(|v| SourceId::SstView(v.id))
                .collect();
            let mut superseded: Vec<String> = manifest
                .l0()
                .iter()
                .map(|v| v.sst.id.unwrap_compacted_id().to_string())
                .collect();
            for run in manifest.compacted() {
                sources.push(SourceId::SortedRun(run.id));
                superseded.extend(
                    run.sst_views()
                        .iter()
                        .map(|v| v.sst.id.unwrap_compacted_id().to_string()),
                );
            }
            if sources.is_empty() {
                return Ok(superseded);
            }
            let destination = manifest.compacted().last().map_or(0, |run| run.id);
            let compaction = match admin
                .submit_compaction(CompactionSpec::new(sources, destination))
                .await
            {
                Ok(c) => c,
                Err(error) => {
                    tracing::debug!(shard = %self.prefix, attempt, "queue pass spec refused: {error}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            if Self::await_compaction(&admin, &compaction).await {
                return Ok(superseded);
            }
        }
        Err("the collection compaction did not complete".to_owned())
    }

    /// Whether the submitted compaction completed within the wait.
    async fn await_compaction(
        admin: &slatedb::admin::Admin,
        compaction: &slatedb::compactor::Compaction,
    ) -> bool {
        let id = compaction.id();
        use slatedb::compactor::CompactionStatus;
        let deadline = std::time::Instant::now() + COMPACTION_WAIT;
        while std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            match admin.read_compaction(id, None).await {
                Ok(Some(c)) if c.status() == CompactionStatus::Completed => return true,
                Ok(None) => return true,
                Ok(Some(c)) if c.status() == CompactionStatus::Failed => return false,
                _ => {}
            }
        }
        false
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
