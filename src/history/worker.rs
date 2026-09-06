//! The absorber's owned worker future, shared by owned and legacy test spawns.
use super::{
    ABSORB_ZERO_ROUTE_DROPPED, AbsorbSignal, Absorber, GATHER_LAST_RESERVED, MAX_PENDING_STREAMS,
    PendingAbsorb, SegmentHash, absorb_error_is_fence, due_streams,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
impl Absorber {
    pub(super) async fn run(self, mut rx: mpsc::Receiver<AbsorbSignal>) {
        let absorber = self;

        let mut pending: HashMap<[u8; 16], PendingAbsorb> = HashMap::new();
        // Dropping the active pass releases owned reservations and reads.
        // Dirty markers and committed absorbed frontiers remain the
        // restart source of truth, including accepted-but-unflushed work.
        tokio::select! {
            biased;
            _ = absorber.shard.closed() => {}
            _ = async {
        let mut classify_after: Option<[u8; 16]> = None;
        // Restart rediscovery (static audit P1, hardened round 4):
        // seed from the durable dirty-stream index so work left
        // outstanding by a previous owner converges WITHOUT the
        // customer ever touching those streams again. The scan runs
        // inside the tick loop so signals keep flowing while it
        // retries: a failed startup scan with no retry permanently
        // stranded pre-restart streams (no signal, no handle, no
        // pending entry — no rediscovery path at all). Once seeded, a
        // low-cadence rescan re-merges anything runtime handle
        // eviction or dropped signals let slip. The resident-handle
        // sweep below remains as belt-and-braces.
        let mut seeded = false;
        let mut seed_failures: u32 = 0;
        let mut seed_next_tick: u32 = 0;
        const RESCAN_EVERY: u32 = 120; // ~10 min at the 5 s tick
        // OOM review item 2: co-opened shards must not become due
        // and flush together — seed each absorber's phase from its
        // shard prefix, exactly like the WAL tick stagger. Sixteen
        // absorbers with identical phases synchronized their gather
        // peaks; staggered phases spread them across the tick.
        let phase = {
            let h = crate::crypto::stream_hash(&absorber.shard.prefix);
            let tick_ms = absorber.cfg.tick.as_millis().max(1) as u64;
            Duration::from_millis(u64::from_le_bytes(h[..8].try_into().unwrap()) % tick_ms)
        };
        let mut tick =
            tokio::time::interval_at(tokio::time::Instant::now() + phase, absorber.cfg.tick);
        let mut tick_n: u32 = 0;
        loop {
            tokio::select! {
                sig = rx.recv() => {
                    let Some(sig) = sig else { return };
                    if pending.len() >= MAX_PENDING_STREAMS && !pending.contains_key(&sig.hash) {
                        // Its durable dirty marker is the bounded discovery fallback.
                        continue;
                    }
                    let e = pending.entry(sig.hash).or_insert(PendingAbsorb {
                        bytes: 0,
                        since: Instant::now(),
                        failures: 0,
                        retry_after: None,
                    });
                    e.bytes += sig.appended_bytes;
                }
                _ = tick.tick() => {
                    let now = Instant::now();
                    tick_n = tick_n.wrapping_add(1);
                    // Durable-index discovery: retry with exponential
                    // backoff until the FIRST scan succeeds, then
                    // rescan at low cadence as a safety net.
                    if (!seeded && tick_n >= seed_next_tick)
                        || (seeded && tick_n.is_multiple_of(RESCAN_EVERY))
                        || absorber.discovery_after.lock().unwrap().is_some()
                    {
                        match absorber.seed_from_dirty_index(&mut pending).await {
                            Ok(n) => {
                                if !seeded && n > 0 {
                                    tracing::info!(
                                        "absorber seeded {} dirty streams from the durable index ({})",
                                        n,
                                        absorber.shard.prefix
                                    );
                                }
                                seeded = absorber.discovery_after.lock().unwrap().is_none();
                            }
                            Err(e) => {
                                if seeded {
                                    tracing::warn!("dirty-stream index rescan failed: {e}");
                                } else {
                                    seed_failures = seed_failures.saturating_add(1);
                                    let shift = seed_failures.min(6);
                                    seed_next_tick = tick_n
                                        .saturating_add(2u32.saturating_pow(shift));
                                    tracing::warn!(
                                        failures = seed_failures,
                                        "dirty-stream index scan failed at absorber start (retrying): {e}"
                                    );
                                }
                            }
                        }
                    }
                    // Re-discovery sweep: signals are the fast path;
                    // this closes their gaps (the bounded channel's
                    // try_send drops under a wide backlog, and a
                    // restarted instance has no signals for pre-crash
                    // data). Thin backlogs (a few records) enter as
                    // small-lane entries due by AGE — a re-discovered
                    // wide backlog must trickle through the capped
                    // lanes, not stampede them (the uncapped first
                    // version opened a history DB per stream faster
                    // than anything evicted: 2.3 GB RSS in seven
                    // minutes). Fat backlogs enter due-now and big.
                    if tick_n.is_multiple_of(absorber.cfg.sweep_every.max(1)) {
                        // Prune the submitted high-water map (it
                        // otherwise grows with every stream ever
                        // absorbed): an entry is only load-bearing
                        // while a re-gather could still observe a
                        // stale durable boundary — i.e. while the
                        // stream is pending or its resident absorbed
                        // boundary trails the submitted mark. Frames
                        // are deterministic and boundary submits are
                        // guarded, so over-pruning merely costs an
                        // idempotent rewrite.
                        absorber.submitted.lock().unwrap().retain(|h, v| {
                            pending.contains_key(h)
                                || absorber
                                    .shard
                                    .resident_absorbed(h)
                                    .is_some_and(|a| a < v.0)
                        });
                    }
                    // Publish absorption lag (scale-out signal).
                    // Every pending stream is eligible: the interim
                    // sparse-deferral mode (age absorption gated on
                    // min_age_bytes) was DELETED in R26-1 — shared
                    // history v2 made absorb-all economical, and a
                    // sub-threshold residual that never retired kept
                    // the durable no-progress clock ticking until
                    // the LagSecs latch shed the whole instance
                    // while the residual could never grow eligible
                    // (the 2026-08-11 soak measured 938 s of stall
                    // on a 154 KiB residual — one tick from that
                    // deadlock).
                    let mut eligible: u64 = 0;
                    let mut oldest_eligible: u64 = 0;
                    for (h, p) in pending.iter() {
                        let age = p.since.elapsed().as_secs();
                        eligible += 1;
                        oldest_eligible = oldest_eligible.max(age);
                        absorber.shard.usage.set_absorb_lag(crate::crypto::SegmentHash(*h), age);
                    }
                    absorber.shard.usage.set_absorb_pending_summary(
                        &absorber.shard.prefix,
                        eligible,
                        oldest_eligible,
                    );
                    // Per-shard lag: the rebalancer picks its victim
                    // from THIS, keyed by the shard we actually serve.
                    absorber.shard.usage.set_shard_lag(&absorber.shard.prefix, oldest_eligible);
                    // Test hook (SCALING.md D3): pause absorption so
                    // lag grows while the tick keeps publishing it.
                    // RUNTIME-togglable: pausing via env needs a
                    // restart, and a restart hands the instance's
                    // shards to its peers — the paused instance then
                    // has no absorber to lag (ladder p8 D3).
                    if absorber.shard.history_resources.paused.load(std::sync::atomic::Ordering::Relaxed) {
                        continue;
                    }
                    let v2_lane = absorber.classify_due(&mut pending, now, &mut classify_after).await;
                    absorber.gather_due(&mut pending, now, &v2_lane).await;

                }
            }
        }
            } => {}
        }
        tracing::info!(
            shard = %absorber.shard.prefix,
            pending_bytes = pending.values().map(|item| item.bytes).sum::<u64>(),
            "absorber exited; durable debt belongs to the next owner"
        );
        // Clear transient accounting on every exit, so a moved shard's
        // previous owner cannot leave phantom backlog in runtime views.
        for hash in pending.keys() {
            absorber.shard.usage.clear_absorb_lag(SegmentHash(*hash));
        }
        absorber.shard.usage.clear_shard_lag(&absorber.shard.prefix);
        absorber
            .shard
            .usage
            .clear_absorb_pending_summary(&absorber.shard.prefix);
    }
    async fn classify_due(
        &self,
        pending: &mut HashMap<[u8; 16], PendingAbsorb>,
        now: Instant,
        classify_after: &mut Option<[u8; 16]>,
    ) -> Vec<[u8; 16]> {
        // Due = byte threshold OR old enough. Age
        // absorbs EVERYTHING — no sparse floor (R26-1):
        // any durable residual left to sit forever is a
        // no-progress stall to the maintenance latch.
        let due = due_streams(pending, &self.cfg, now, *classify_after);
        // Three lanes. V2 (shared partition, one flush for
        // the whole lane) takes every stream whose
        // history lives — or will live — in the shared
        // partition: the history_v2 flag, or a stream
        // that has never absorbed. Legacy v1 streams keep
        // the two per-stream lanes (docs/COST-WIDE1.md §1:
        // the serial grind was the ceiling; the
        // concurrent small lane its repair). ALL lanes
        // are capped per tick: the tick must return to
        // the select loop, and v1 eviction must run often
        // enough that open_dbs stays near the LRU — an
        // uncapped tick once grew 2.3 GB of open history
        // DBs before its first eviction. Leftover due
        // entries simply run next tick. Classification
        // reads resident handle state (map lookup) and is
        // itself capped.
        const V2_LANE_PER_TICK: usize = 1024;
        const CLASSIFY_PER_TICK: usize = 4096;
        let mut v2_lane: Vec<[u8; 16]> = Vec::new();
        for (hash, bytes) in due.into_iter().take(CLASSIFY_PER_TICK) {
            if v2_lane.len() >= V2_LANE_PER_TICK {
                break;
            }
            *classify_after = Some(hash);
            let Ok(handle) = self.shard.stream_handle(hash).await else {
                continue;
            };
            // Lane eligibility reads the APPLIED tail, not
            // the durable one (round-4 root cause): a
            // signal can arrive before its append's batch
            // DISPATCHES, so the durable snapshot briefly
            // shows route==0 / absorbed==0 and the lane
            // decision flaps — the zero-route guard sent
            // fresh routed streams down v1, and one tick
            // later a stale absorbed==0 re-admitted v2.
            // `applied` is updated synchronously at commit,
            // strictly before any signal for that batch
            // exists, so it cannot race the classifier.
            // (The committer-side layout seal remains the
            // hard correctness backstop.)
            let (absorbed, v2flag, route) = {
                let st = handle.state.lock().unwrap();
                (st.applied.absorbed, st.applied.history_v2, st.applied.route)
            };
            // Zero-route guard (static audit): a legacy
            // stream with no name-level route must NOT
            // enter v2 — its records would be keyed under
            // route 0x00.. and a future route-range split
            // would classify them into the wrong range.
            // Such streams keep the v1 per-stream layout.
            // The v1 per-stream layout was DELETED in the
            // pre-launch clean switch: every stream in a
            // fresh namespace carries a route from its
            // first append. A zero-route tail with data is
            // a bug, not a layout — count it, drop it, and
            // never write the deleted format.
            let v2_eligible = v2flag || (absorbed == 0 && route != [0u8; 16]);
            #[cfg(test)]
            if std::env::var("DST_DRAIN_TRACE").is_ok() {
                eprintln!(
                    "CLASSIFY {} absorbed={absorbed} v2flag={v2flag} route_set={} eligible={v2_eligible} bytes={bytes}",
                    crate::crypto::hex(&hash[..4]),
                    route != [0u8; 16],
                );
            }
            let _ = bytes;
            if v2_eligible {
                v2_lane.push(hash);
            } else {
                ABSORB_ZERO_ROUTE_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    "zero-route tail {} has unabsorbed data; the v1 layout \
                                 no longer exists — dropping from the absorb queue",
                    crate::crypto::hex(&hash[..4]),
                );
                pending.remove(&hash);
            }
        }
        v2_lane
    }
    async fn gather_due(
        &self,
        pending: &mut HashMap<[u8; 16], PendingAbsorb>,
        now: Instant,
        v2_lane: &[[u8; 16]],
    ) {
        if !v2_lane.is_empty() && !self.shard.is_closed() {
            // OOM review item 1: reserve the PROCESS-WIDE
            // budget before a single frame is read. The
            // estimate is this gather's packing cap times
            // the build multiplier; waiting here is the
            // intended backpressure when other shards'
            // gathers hold the budget.
            // #266: adaptive estimate — the decaying
            // max of observed transients, not the
            // worst case. CHAOS-3 measured the fixed
            // 96 MiB reservation against 6 MB actual
            // gathers; held per in-flight gather it
            // crossed the RSS shed line and SHED
            // APPENDS for the hold duration (L1
            // ladder, bench/WORKLOAD-CERT-PLAN.md).
            // grow() inside the build keeps the OOM
            // bound exact when reality outruns the
            // estimate.
            let est = self.adaptive_gather_est();
            let mut _reservation = self.shard.history_resources.budget.reserve(est).await;
            GATHER_LAST_RESERVED.store(
                _reservation.granted() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            if self.shard.is_closed() {
                return; // fenced while waiting for budget
            }
            match self
                .absorb_gather_v2_with(&v2_lane, &mut _reservation)
                .await
            {
                Ok(outcome) => {
                    // Retire ONLY what the gather settled:
                    // covered streams advanced; no_work had
                    // nothing durable to absorb (residues
                    // and new data re-arrive via
                    // signals/sweep). Budget-deferred
                    // streams KEEP their pending entry, lag
                    // and age — they gather next tick
                    // without needing a new signal or the
                    // ~60 s handle sweep (review round 4:
                    // removing them silently stranded
                    // their backlog for up to a minute and
                    // blinded the fleet lag view).
                    let partial: std::collections::HashMap<[u8; 16], u64> =
                        outcome.partial.iter().copied().collect();
                    for (h, _, _) in &outcome.advanced {
                        // A PARTIAL advance is progress, not
                        // completion: keep it pending so the
                        // next tick continues immediately.
                        if partial.contains_key(h) {
                            continue;
                        }
                        pending.remove(h);
                        self.shard
                            .usage
                            .clear_absorb_lag(crate::crypto::SegmentHash(*h));
                    }
                    for (h, remaining) in &partial {
                        let est = remaining.saturating_mul(1024);
                        pending
                            .entry(*h)
                            .and_modify(|p| {
                                p.bytes = p.bytes.max(est);
                                // Progress clears the failure
                                // backoff; the age is left
                                // alone so age-based due keeps
                                // its original meaning.
                                p.failures = 0;
                                p.retry_after = None;
                            })
                            .or_insert(PendingAbsorb {
                                bytes: est,
                                since: Instant::now(),
                                failures: 0,
                                retry_after: None,
                            });
                    }
                    for h in &outcome.no_work {
                        pending.remove(h);
                        self.shard
                            .usage
                            .clear_absorb_lag(crate::crypto::SegmentHash(*h));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    if absorb_error_is_fence(&e) {
                        tracing::warn!("v2 gather fence-class ({} streams): {msg}", v2_lane.len());
                        // Engine is dying; the exit path
                        // clears pending.
                    } else {
                        tracing::warn!("v2 gather failed ({} streams): {msg}", v2_lane.len());
                        for h in v2_lane {
                            if let Some(p) = pending.get_mut(h) {
                                p.failures = p.failures.saturating_add(1);
                                let shift = p.failures.min(6);
                                p.retry_after = Some(now + self.cfg.tick * 2u32.pow(shift));
                            }
                        }
                    }
                }
            }
        }
    }
}
