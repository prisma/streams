//! The absorber's per-tick work: seeding its pending roster from the
//! durable dirty index, and the shared-partition gather pass (history
//! v2) that reads MANY streams' raw encrypted frames from the shard log,
//! puts them all into ONE WriteBatch on the shard's shared partition,
//! flushes ONCE, then advances every covered boundary. No decryption, no
//! KeyCache, no per-stream DB — the per-stream request tax this replaced
//! was ~43 Class A per one-record stream (docs/COST-WIDE1.md §1).
use super::{
    ABSORB_BUILD_MULTIPLIER, ABSORB_BYTES_TOTAL, AbsorbReservation, Absorber,
    CANONICAL_BYTES_WRITTEN, DISCOVERY_PAGE_STREAMS, GATHER_LAST_ACTUAL, GATHER_LAST_FLUSH_MS,
    GATHER_LAST_PACE_MS, GATHER_LAST_READ_MS, GATHER_LAST_WRITE_MS, GATHER_PER_STREAM_CAP,
    HISTORY_FLUSH_STALL_MS, HISTORY_FLUSH_WAIT_MS_MAX, MAX_PENDING_STREAMS, POSTINGS_BYTES_WRITTEN,
    POSTINGS_PAGES_WRITTEN, POSTINGS_RUNS_WRITTEN, PendingAbsorb, hist2_record_key,
};
use crate::crypto::{RouteHash, SegmentHash};
use crate::postings::{AbsRun, PageBuilder};
use crate::shard::record::{RangeReadError, RecordCorruption};
use crate::shard::{CopiedBytes, FrameReadResult, StreamHandle, read_frames_range};
use bytes::Bytes;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Rough WriteBatch bookkeeping cost per entry, on top of key+value.
const ENTRY_OVERHEAD: usize = 64;

/// One stream's planned read (#266 phase A): resident-map lookups and
/// lock reads only. The per-stream frame reads are the latency-bound part
/// of the gather (store round trips), and the L1 ladder showed append
/// shed scales with read-phase WALL TIME (L1d8: stretching the phase via
/// pacing amplified shed 10x), so the reads run in bounded-concurrency
/// waves while the WriteBatch build stays serial and deterministic in
/// lane order.
struct ReadPlan {
    hash: [u8; 16],
    handle: Arc<StreamHandle>,
    from: u64,
    upto: u64,
    route: RouteHash,
}

/// One chunk's postings runs per routing-key hash.
type KeyRuns = Vec<([u8; 16], Vec<AbsRun>)>;

/// (segment, chunk_from, chunk_to, per-key runs) for write-through cache
/// warming — installed only after the batch flush succeeds.
type WarmChunk = (SegmentHash, u64, u64, KeyRuns);

/// Per-stream classification of one v2 gather (review round 4, P1): the
/// pump must retire ONLY what the gather settled. `advanced` carries
/// (hash, new upto, raw frame bytes copied — the committer's
/// unabsorbed_bytes decrement); `no_work` had nothing durable to absorb;
/// `deferred_budget` did not fit this batch's byte budget and MUST stay
/// pending — with lag and age intact — for the next tick.
#[derive(Default)]
pub(crate) struct GatherOutcome {
    pub(crate) advanced: Vec<([u8; 16], u64, u64)>,
    pub(crate) no_work: Vec<[u8; 16]>,
    pub(crate) deferred_budget: Vec<[u8; 16]>,
    /// Streams whose gather ADVANCED but did not reach the stream's
    /// durable end — the per-stream byte cap truncated the chunk, so
    /// data remains. `(hash, remaining offsets)`. These MUST stay
    /// pending: retiring them (they are also in `advanced`) strands the
    /// remainder until some unrelated event re-discovers it, and for a
    /// stream whose next record exceeds the cap that is effectively
    /// never — 8x100 KiB behind a 64 KiB cap absorbed exactly one
    /// record and then stopped forever (chaos campaign, 2026-08-09).
    pub(crate) partial: Vec<([u8; 16], u64)>,
    /// Streams left out for their own stored bytes, with the typed reason.
    /// They MUST stay pending with their own backoff: retiring them strands
    /// their backlog; retrying them every tick re-reads a chunk that fails
    /// the same way.
    pub(crate) failed: Vec<([u8; 16], StreamGatherFailure)>,
}

/// Why a gather left one stream out (item 35). Each verdict is a pure
/// function of that stream's own durable bytes, so the same read fails the
/// same way on every retry: it costs that stream its turn and its backoff,
/// never its lane-mates'. Store, fence, partition and flush errors are not
/// here: they still abort the whole gather.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StreamGatherFailure {
    /// A stored row failed canonical admission (key width, namespace,
    /// frame or offset).
    Corrupt(RecordCorruption),
    /// The chunk's postings pages did not decode back to what was encoded.
    PostingsSelfDecode,
    /// The chunk's postings pages decoded to overlapping runs.
    PostingsOverlap,
}

impl std::fmt::Display for StreamGatherFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(corruption) => write!(f, "{corruption}"),
            Self::PostingsSelfDecode => f.write_str("postings page failed its self-decode"),
            Self::PostingsOverlap => f.write_str("postings pages overlap"),
        }
    }
}

/// The batch under construction: its rows, their modeled size, and what
/// the flush must prove durable before it is published.
struct Staged {
    wb: WriteBatch,
    bytes: usize,
    /// Batch bytes the pool REFUSED to cover (0 = never refused): the
    /// batch as it would have stood with the chunk try_grow() turned
    /// away. Non-zero closes the batch — every later chunk defers — and
    /// sizes the next tick's reserve(), which waits holding no bytes.
    refused: usize,
    warm_installs: Vec<WarmChunk>,
    out: GatherOutcome,
    /// The committer's view of `out.advanced`: each advance with the
    /// offset its copy starts at, which is where it may retire from.
    copies: Vec<([u8; 16], u64, CopiedBytes)>,
}

/// #266: optional duty cycle between read waves — see the
/// gather_pace_window field doc. L1d8 falsified pacing as a shed fix
/// (default now 0); the knob remains for field experiments.
struct Pacing {
    paced: Duration,
    last_park: Instant,
}

/// Whole milliseconds of `elapsed`, saturating at `u64::MAX`.
fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// This chunk's batch contribution and its raw frame bytes (the tail's
/// unabsorbed_bytes gauge). A canonical row costs the frame plus a
/// conservative per-record postings allowance (~key 65 B amortized + a
/// few varints); the full-frame keyed duplicate is GONE (ROUTING-V3 §3).
fn chunk_cost(chunk: &FrameReadResult) -> (usize, u64) {
    let mut chunk_bytes = 0usize;
    let mut chunk_raw = 0u64;
    for raw in &chunk.frames {
        chunk_raw += raw.len() as u64;
        chunk_bytes += raw.len() + 41 + ENTRY_OVERHEAD + 24;
    }
    (chunk_bytes, chunk_raw)
}

/// Note every frame's routing key for the chunk's postings pages, staging
/// nothing yet: a chunk whose pages fail their self-check must leave no
/// canonical row in the shared batch. Returns the chunk's last offset.
fn note_frames(plan: &ReadPlan, chunk: &FrameReadResult, pages: &mut PageBuilder) -> u64 {
    let mut last = plan.from;
    for raw in &chunk.frames {
        let frame = raw.view();
        let off = frame.header.offset;
        pages.note_frame(
            crate::postings::rk_hash(frame.header.routing_key),
            off,
            raw.len() as u64,
        );
        last = off;
    }
    last
}

/// A chunk's postings pages after they proved their own round trip. Only
/// `check_postings` builds one and only `stage_checked` spends it, so the
/// batch can only ever receive a checked chunk.
struct ChunkPostings {
    pages: crate::postings::Pages,
    runs: KeyRuns,
    bytes: u64,
}

/// The chunk's postings pages must decode back to runs that do not
/// overlap. The runs decoded back from what was encoded (cheap varints)
/// are exactly the runs a reader would load, so write-through warming
/// (spec §7) makes first-read-after-absorb skip the index round trip.
/// Touches no batch and counts nothing.
fn check_postings(pages: PageBuilder) -> Result<ChunkPostings, StreamGatherFailure> {
    let (pages, bytes) = pages.finish();
    let mut runs: HashMap<[u8; 16], Vec<AbsRun>> = HashMap::new();
    for (kh, _, first, value) in &pages {
        let abs = crate::postings::decode_page_abs(*first, value)
            .ok_or(StreamGatherFailure::PostingsSelfDecode)?;
        crate::postings::append_page_runs(runs.entry(kh.0).or_default(), abs)
            .ok_or(StreamGatherFailure::PostingsOverlap)?;
    }
    let runs = runs.into_iter().collect();
    Ok(ChunkPostings { pages, runs, bytes })
}

/// Stage a checked chunk: its canonical rows — the frame is stored once
/// under its canonical offset — then its postings pages (ROUTING-V3 §3):
/// every routing key, INCLUDING the empty/default key, gets compact
/// offset-run pages in the SAME WriteBatch, so the index adds no request,
/// manifest, database, namespace or GC surface of its own. Returns the
/// chunk's per-key runs.
fn stage_checked(
    wb: &mut WriteBatch,
    plan: &ReadPlan,
    chunk: &FrameReadResult,
    checked: ChunkPostings,
) -> KeyRuns {
    let inc = SegmentHash(plan.hash);
    for raw in &chunk.frames {
        wb.put(
            hist2_record_key(plan.route, inc, raw.view().header.offset),
            Bytes::from(raw.clone()),
        );
    }
    let ChunkPostings { pages, runs, bytes } = checked;
    POSTINGS_PAGES_WRITTEN.fetch_add(pages.len() as u64, Ordering::Relaxed);
    let run_count: usize = runs.iter().map(|(_, key_runs)| key_runs.len()).sum();
    POSTINGS_RUNS_WRITTEN.fetch_add(run_count as u64, Ordering::Relaxed);
    for (kh, bucket, first, value) in pages {
        wb.put(
            crate::postings::postings_key(plan.route, inc, &kh, bucket, first),
            value,
        );
    }
    POSTINGS_BYTES_WRITTEN.fetch_add(bytes, Ordering::Relaxed);
    runs
}

/// Drain trace for the DST harness: which frames this gather staged.
#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "trace_gather; this test-only drain trace is switched on by the DST harness through the process environment; carrying a debugging switch in the absorber's configuration would put it on the production surface"
)]
fn trace_gather(plan: &ReadPlan, chunk: &FrameReadResult) {
    if std::env::var("DST_DRAIN_TRACE").is_ok() {
        let offs: Vec<u64> = chunk
            .frames
            .iter()
            .map(|raw| raw.view().header.offset)
            .collect();
        eprintln!(
            "GATHER {} from={} upto={} frames={offs:?}",
            crate::crypto::hex(&plan.hash[..4]),
            plan.from,
            plan.upto,
        );
    }
}

impl Absorber {
    /// Scan the durable dirty index and merge outstanding work:
    /// unabsorbed streams into `pending`, trim debt into the engine's
    /// maintenance set. Pending bytes come from the tail's EXACT
    /// `unabsorbed_bytes` gauge — the old records × 1 KiB estimate
    /// under-sized a single 32 MiB record by 32,000×, putting it below
    /// both default absorption thresholds forever (review round 4).
    /// `or_insert` merge: live entries always win over the scan's view.
    #[expect(
        clippy::unwrap_used,
        reason = "Absorber::seed_from_dirty_index; a poisoned discovery cursor may hold a half-advanced page position; recovering it could rescan the same dirty page forever or skip one that still holds backlog"
    )]
    pub(super) async fn seed_from_dirty_index(
        &self,
        pending: &mut HashMap<[u8; 16], PendingAbsorb>,
    ) -> anyhow::Result<usize> {
        // R25-A: maintenance state is loaded SYNCHRONOUSLY by the
        // engine opener, before the engine is published. Restoring it
        // here — asynchronously, after the engine is already serving —
        // was the R24 defect: the first request after a restart could be
        // admitted before the backlog was known, and a late restore
        // could overwrite state a new append had already advanced.
        let after = *self.discovery_after.lock().unwrap();
        let (dirty, more) = self
            .shard
            .scan_dirty_streams_page(after, DISCOVERY_PAGE_STREAMS)
            .await?;
        let last = dirty.last().map(|entry| entry.0);
        let mut absorb_seeded = 0usize;
        for (h, absorbed, next) in dirty {
            if pending.len() >= MAX_PENDING_STREAMS && !pending.contains_key(&h) {
                continue;
            }
            let (recs, bytes) = self.backlog_of(h, absorbed, next).await;
            if recs == 0 {
                continue;
            }
            self.roll_back_stranded_mark(h, absorbed);
            // Backdate by the age threshold so recovered work is eligible
            // promptly rather than a full window later.
            let since = Instant::now()
                .checked_sub(self.cfg.threshold_age)
                .unwrap_or_else(Instant::now);
            pending.entry(h).or_insert(PendingAbsorb {
                bytes,
                since,
                failures: 0,
                retry_after: None,
            });
            absorb_seeded += 1;
        }
        *self.discovery_after.lock().unwrap() = if more { last } else { None };
        Ok(absorb_seeded)
    }

    /// The stream's unabsorbed backlog as (records, bytes) from its
    /// durable tail — the exact gauge, or the legacy records × 1 KiB
    /// estimate for a tail without one — noting trim debt on the way. A
    /// tail unreadable right now falls back to the dirty marker's view
    /// rather than failing the whole seed pass.
    async fn backlog_of(&self, h: [u8; 16], absorbed: u64, next: u64) -> (u64, u64) {
        let Ok(Some(t)) = self.shard.tail_fields(&h).await else {
            let recs = next.saturating_sub(absorbed);
            return (recs, recs.saturating_mul(1024));
        };
        if t.trimmed < t.trim_safe_to {
            self.shard.note_trim_debt(h);
        }
        let recs = t.next.saturating_sub(t.absorbed);
        let bytes = if t.unabsorbed_bytes > 0 {
            t.unabsorbed_bytes
        } else {
            recs.saturating_mul(1024)
        };
        (recs, bytes)
    }

    /// R25-D: heal a stranded submitted-watermark. The gather records
    /// what it SUBMITTED (fire-and-forget) so an advance in flight to
    /// handle state is not re-sent — but if the committer group carrying
    /// that advance FAILED, the durable boundary never moved and the mark
    /// now fences the range off from every future gather: `from =
    /// max(mark, absorbed) >= upto` reads as no_work forever, and the
    /// backlog is stranded until a restart. The durable tail is the
    /// source of truth: a mark ahead of it at rescan time describes a
    /// submission that did not land, so roll it back. A genuine in-flight
    /// advance is NOT harmless to regather under: the regather starts
    /// below it, and the committer drops an advance that does not start
    /// at its boundary, so that gather's copy is wasted and its postings
    /// pages overlap the ones the stream's next gather writes.
    #[expect(
        clippy::unwrap_used,
        reason = "Absorber::roll_back_stranded_mark; a poisoned submitted-mark map may hold a partially raised lane mark; recovering it could fence a range off from every future gather or re-trust a mark the layout seal dropped"
    )]
    fn roll_back_stranded_mark(&self, h: [u8; 16], absorbed: u64) {
        let mut submitted = self.submitted.lock().unwrap();
        if let Some((mark, _v2)) = submitted.get(&h)
            && *mark > absorbed
        {
            tracing::warn!(
                "rolling back stranded absorb mark for {}: submitted={} durable absorbed={}",
                crate::crypto::hex(&h[..4]),
                mark,
                absorbed,
            );
            submitted.remove(&h);
        }
    }

    /// Test-facing wrapper: reserve adaptively, then gather. The pump
    /// loop calls absorb_gather_v2_with directly because its
    /// reservation must precede the post-budget fence re-check.
    #[cfg(test)]
    pub(crate) async fn absorb_gather_v2(
        &self,
        streams: &[[u8; 16]],
    ) -> anyhow::Result<GatherOutcome> {
        let mut reservation = self
            .shard
            .history_resources
            .budget
            .reserve(self.adaptive_gather_est())
            .await;
        self.absorb_gather_v2_with(streams, &mut reservation).await
    }

    /// One gather pass over `streams`. Classifies every requested stream:
    /// `advanced` covered by this flush (with new upto and the frame bytes
    /// copied), `no_work` had nothing durable to absorb, and
    /// `deferred_budget` did not fit the aggregate byte budget, or the
    /// process-wide pool refused to grow over it (try_grow) — the
    /// CALLER must keep those pending (with lag and age intact) so they
    /// gather on the next tick; dropping them used to strand their
    /// backlog until the ~60 s resident-handle sweep re-found it. A
    /// per-stream byte cap truncates fat streams mid-range — their
    /// boundary still advances over what was written, and the sweep or
    /// the next signal re-drives the remainder. A stream whose own stored
    /// bytes fail (a row refused admission, pages failing their self-check)
    /// is left out as `failed`, alone; every other error aborts the lane.
    pub(crate) async fn absorb_gather_v2_with(
        &self,
        streams: &[[u8; 16]],
        reservation: &mut AbsorbReservation<'_>,
    ) -> anyhow::Result<GatherOutcome> {
        let part = self.shard.history_partition().await?;
        let t_read = Instant::now();
        let mut staged = Staged {
            wb: WriteBatch::new(),
            bytes: 0,
            refused: 0,
            warm_installs: Vec::new(),
            out: GatherOutcome::default(),
            copies: Vec::new(),
        };
        let mut pacing = Pacing {
            paced: Duration::ZERO,
            last_park: Instant::now(),
        };
        let plans = self.plan_reads(streams, &mut staged.out).await?;
        let read_par = self.cfg.gather_read_par.max(1);
        let per_stream = GATHER_PER_STREAM_CAP.min(self.cfg.gather_max_bytes);
        let mut pi = 0usize;
        while pi < plans.len() {
            // Aggregate budget: the batch is held in memory until the one
            // flush below, so its size — not the lane's stream count — is
            // what a 1 GiB instance actually feels. Anything deferred here
            // stays in the pending set and gathers on a later tick; the
            // whole-remainder deferral also skips their reads. A batch the
            // process-wide pool refused to grow is closed the same way.
            if staged.bytes >= self.cfg.gather_max_bytes || staged.refused > 0 {
                staged
                    .out
                    .deferred_budget
                    .extend(plans[pi..].iter().map(|p| p.hash));
                break;
            }
            let wave_end = (pi + read_par).min(plans.len());
            let wave = &plans[pi..wave_end];
            pi = wave_end;
            let got = self.read_wave(wave, per_stream).await;
            self.pace_between_waves(&mut pacing).await;
            for (plan, read) in wave.iter().zip(got) {
                match read {
                    Ok(chunk) => self.stage_chunk(&mut staged, reservation, plan, &chunk),
                    // The row's own bytes failed admission: it costs this
                    // stream its turn, never its lane-mates'.
                    Err(RangeReadError::Corrupt(corruption)) => staged
                        .out
                        .failed
                        .push((plan.hash, StreamGatherFailure::Corrupt(corruption))),
                    Err(RangeReadError::Store(error)) => return Err(error.into()),
                }
            }
        }
        GATHER_LAST_PACE_MS.store(millis(pacing.paced), Ordering::Relaxed);
        self.observe_gather_transient(staged.bytes.max(staged.refused));
        if staged.out.advanced.is_empty() {
            return Ok(staged.out);
        }
        GATHER_LAST_READ_MS.store(millis(t_read.elapsed()), Ordering::Relaxed);
        GATHER_LAST_ACTUAL.store(staged.bytes as u64, Ordering::Relaxed);
        self.commit(&part, staged).await
    }

    /// Plan every requested stream's read; streams with nothing durable
    /// to absorb are classified `no_work` here.
    async fn plan_reads(
        &self,
        streams: &[[u8; 16]],
        out: &mut GatherOutcome,
    ) -> anyhow::Result<Vec<ReadPlan>> {
        let mut plans = Vec::new();
        for hash in streams {
            let handle = self.shard.stream_handle(*hash).await?;
            match self.plan_read(*hash, handle) {
                Some(plan) => plans.push(plan),
                None => out.no_work.push(*hash),
            }
        }
        Ok(plans)
    }

    /// The stream's read window: from its durable absorbed boundary —
    /// floored at OUR lane's submitted mark — up to its durable end. Lane-
    /// scoped floor: a v1 mark here may describe an advance the layout
    /// seal dropped, and skipping past it would hide that range from the
    /// partition.
    #[expect(
        clippy::unwrap_used,
        reason = "Absorber::plan_read; a poisoned handle state or submitted-mark map may hold a partially advanced boundary or lane mark; recovering it could plan a read from a boundary that was never committed"
    )]
    fn plan_read(&self, hash: [u8; 16], handle: Arc<StreamHandle>) -> Option<ReadPlan> {
        let (from, upto, route) = {
            let st = handle.state.lock().unwrap();
            (
                st.durable.absorbed,
                st.durable.next,
                RouteHash(st.durable.route),
            )
        };
        let from = self
            .submitted
            .lock()
            .unwrap()
            .get(&hash)
            .and_then(|(u, v2)| (*v2).then_some(*u))
            .unwrap_or(0)
            .max(from);
        (from < upto).then(|| ReadPlan {
            hash,
            handle,
            from,
            upto,
            route,
        })
    }

    /// Read one wave's frames concurrently. Transient memory: at most
    /// read_par chunks in flight, each capped at per_stream — read
    /// BEFORE stage_chunk funds them, so the adaptive reservation covers
    /// a chunk only once it is staged. The wave is already sized to read_par, so
    /// join_all IS the concurrency bound — no stream adapter needed — and
    /// it yields the chunks in lane order.
    async fn read_wave(
        &self,
        wave: &[ReadPlan],
        per_stream: usize,
    ) -> Vec<Result<FrameReadResult, RangeReadError>> {
        let reads = wave
            .iter()
            .map(|p| read_frames_range(&self.shard, &p.handle, p.from, p.upto, per_stream));
        futures_util::future::join_all(reads).await
    }

    /// The commit is untouched by pacing — never stretch the
    /// durability-critical section.
    async fn pace_between_waves(&self, pacing: &mut Pacing) {
        if !self.cfg.gather_pace.is_zero()
            && pacing.last_park.elapsed() >= self.cfg.gather_pace_window
        {
            tokio::time::sleep(self.cfg.gather_pace).await;
            pacing.paced += self.cfg.gather_pace;
            pacing.last_park = Instant::now();
        }
    }

    /// Admit one stream's chunk into the batch. A chunk that would blow
    /// the budget waits for a batch of its own — unless the batch is
    /// empty, in which case it proceeds alone (one oversized frame must
    /// still make progress; frame bodies can reach the 32 MiB API cap).
    /// Deliberately not async: staging never waits on the pool.
    fn stage_chunk(
        &self,
        staged: &mut Staged,
        reservation: &mut AbsorbReservation<'_>,
        plan: &ReadPlan,
        chunk: &FrameReadResult,
    ) {
        if chunk.frames.is_empty() {
            staged.out.no_work.push(plan.hash);
            return;
        }
        let (chunk_bytes, chunk_raw) = chunk_cost(chunk);
        let batch_bytes = staged.bytes + chunk_bytes;
        let over_packing = staged.bytes > 0 && batch_bytes > self.cfg.gather_max_bytes;
        if over_packing || staged.refused > 0 {
            staged.out.deferred_budget.push(plan.hash);
            return;
        }
        // #266 adaptive reservation: cover this chunk's modeled
        // transient BEFORE building it. On the steady path the
        // adaptive estimate already covers the batch and this is a
        // no-op. When a gather turns out fatter than recent history
        // it NEVER waits here — it holds bytes, and two such waiters
        // hold the pool between them forever. A refusal closes the
        // batch: what is staged flushes, the rest defers, and the
        // recorded need makes the next tick's reserve() — the one
        // wait on the pool, made holding nothing — ask for it up front.
        let needed = batch_bytes.saturating_mul(ABSORB_BUILD_MULTIPLIER);
        let short = needed.saturating_sub(reservation.granted());
        if !reservation.try_grow(short) {
            tracing::info!(
                "v2 gather {}: pool refused +{short} B at a {batch_bytes} B batch; deferring",
                self.shard.prefix,
            );
            staged.refused = batch_bytes;
            staged.out.deferred_budget.push(plan.hash);
            return;
        }
        let mut pages = PageBuilder::default();
        let last = note_frames(plan, chunk, &mut pages);
        // The pages prove their own round trip BEFORE any of the chunk
        // enters the shared batch: a refused chunk leaves nothing behind.
        let checked = match check_postings(pages) {
            Ok(checked) => checked,
            Err(failure) => {
                staged.out.failed.push((plan.hash, failure));
                return;
            }
        };
        staged.bytes = batch_bytes;
        #[cfg(test)]
        trace_gather(plan, chunk);
        let runs = stage_checked(&mut staged.wb, plan, chunk, checked);
        CANONICAL_BYTES_WRITTEN.fetch_add(chunk_raw, Ordering::Relaxed);
        staged
            .warm_installs
            .push((SegmentHash(plan.hash), plan.from, last + 1, runs));
        staged.out.advanced.push((plan.hash, last + 1, chunk_raw));
        staged
            .copies
            .push((plan.hash, last + 1, CopiedBytes::new(plan.from, chunk_raw)));
        // Truncated by the per-stream cap: more durable data sits
        // below `upto`. The caller must keep this stream pending.
        if last + 1 < plan.upto {
            staged.out.partial.push((plan.hash, plan.upto - (last + 1)));
        }
    }

    /// Make the batch durable — one write, one flush — then publish what
    /// the flush proved: warm the slice cache, submit the advanced
    /// boundaries and raise the lane marks.
    ///
    /// R25-B: NO maintenance retirement here. This task has proved
    /// the HISTORY COPY is durable — the backlog is not retired until
    /// the shard's absorbed boundary commits, which happens in the
    /// committer's common finalization when the AbsorbedBatch group
    /// lands (and stages the maintenance row in the same WriteBatch).
    /// Retiring here would claim progress a crash between this flush
    /// and that commit would revoke.
    ///
    /// R25-F: the per-gather read-amplification attribution was
    /// REMOVED. It snapshotted process-global GET deltas around the
    /// read phase, so concurrent customer/registry/billing/fleet
    /// traffic contaminated every sample — a number that looks like a
    /// measurement and is not is worse than no number. Operation-
    /// local attribution needs the metrics handle carried through
    /// SlateDB's scan and spawned fetch tasks (deferred fork patch);
    /// until then the process-wide transferred-byte counters in
    /// store_timing are the only honest read telemetry.
    async fn commit(&self, part: &Db, staged: Staged) -> anyhow::Result<GatherOutcome> {
        let Staged {
            wb,
            warm_installs,
            out,
            copies,
            ..
        } = staged;
        let ord = Ordering::Relaxed;
        let t_write = Instant::now();
        part.write_with_options(wb, &WriteOptions::default())
            .await?;
        GATHER_LAST_WRITE_MS.store(millis(t_write.elapsed()), ord);
        let t_flush = Instant::now();
        let stall = HISTORY_FLUSH_STALL_MS.load(ord);
        if stall > 0 {
            // Stalled-history-flush campaign lever: the stall sits ON
            // the real flush path with the reservation held, and it is
            // INSIDE the flush timing window — the flush-wait metrics
            // must report the delay the campaign injects, or the gate
            // could stall the path while its primary metric shows
            // nothing.
            tokio::time::sleep(Duration::from_millis(stall)).await;
        }
        part.flush().await?; // wal off => memtable -> L0, manifest published
        // Flush wait is the review's leading indicator: when history L0
        // approaches its cap, THIS is what starts blocking.
        let flush_ms = millis(t_flush.elapsed());
        GATHER_LAST_FLUSH_MS.store(flush_ms, ord);
        HISTORY_FLUSH_WAIT_MS_MAX.fetch_max(flush_ms, ord);
        let absorbed_bytes = out.advanced.iter().map(|(_, _, b)| *b).sum::<u64>();
        ABSORB_BYTES_TOTAL.fetch_add(absorbed_bytes, ord);
        // The pages are durable: warm the slice cache with the runs we
        // just wrote. Readers clip to their own durable boundary, so an
        // install racing the boundary advance can never over-serve.
        for (inc, chunk_from, chunk_to, per_key) in warm_installs {
            self.shard
                .postings_cache
                .install_chunk(inc, chunk_from, chunk_to, per_key);
        }
        self.shard.submit_absorbed_batch_v2(copies).await;
        self.raise_lane_marks(&out.advanced);
        tracing::info!(
            "v2 gather absorbed {} streams into {}/history2 ({} budget-deferred)",
            out.advanced.len(),
            self.shard.prefix,
            out.deferred_budget.len()
        );
        Ok(out)
    }

    /// Raise each advanced stream's v2 lane mark to what this gather
    /// submitted, so pacing off the published boundary alone cannot
    /// re-absorb a range whose committer batch has not dispatched yet.
    #[expect(
        clippy::unwrap_used,
        reason = "Absorber::raise_lane_marks; a poisoned submitted-mark map may hold a partially raised lane mark; recovering it could re-absorb or fence off a range whose submission is still in flight"
    )]
    fn raise_lane_marks(&self, advanced: &[([u8; 16], u64, u64)]) {
        let mut submitted = self.submitted.lock().unwrap();
        for (hash, upto, _) in advanced {
            let e = submitted.entry(*hash).or_insert((0, true));
            if e.1 {
                e.0 = e.0.max(*upto);
            } else {
                *e = (*upto, true);
            }
        }
    }
}

#[cfg(test)]
mod postings_refusal_tests;
