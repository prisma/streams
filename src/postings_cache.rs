//! Decoded postings-slice cache (spec §7): each shard engine owns one
//! bounded, weighted, single-flight cache of decoded postings runs.
//!
//! A slice is a large FORWARD section of one routing key's index for
//! one segment. Postings below the durable absorbed boundary are
//! immutable, so slices never invalidate — they only extend forward
//! (`indexed_to_offset` records proven coverage). A cold load reads up
//! to 64 buckets / 1 MiB of encoded pages / the requested boundary,
//! whichever comes first; with compact pages that is routinely
//! millions of offsets, so a key active repeatedly inside a window
//! pays the index read once.
//!
//! Loads are single-flight per (segment, key) and owned by a spawned
//! task — cancelling every waiter neither cancels the load nor stops
//! the finished slice from entering the cache. Eviction is by decoded
//! byte weight (global budget) plus a 10-minute idle sweep driven by
//! the engine's flush ticker. When a request consumes more than 75% of
//! its slice, the next section is prefetched asynchronously
//! (best-effort, never delaying the response).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use slatedb::Db;

use crate::crypto::{RouteHash, RoutingKeyHash, SegmentHash};
use crate::postings::{AbsRun, BUCKET_OFFSETS};

/// Default PROCESS-WIDE decoded-byte budget (spec §7.1; review finding
/// 7: one budget for the whole process — engines share one cache in
/// production via `process_cache`, sized by env POSTINGS_CACHE_BYTES).
pub const POSTINGS_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Estimated heap overhead per cache entry beyond the raw runs: the
/// 32-byte key, HashMap bucket, Entry, Arc<PostingsSlice> header and
/// allocator slack. The budget must account for what the process
/// actually holds, not just run payloads (review finding 7).
pub const ENTRY_OVERHEAD_BYTES: usize = 176;

/// Bound on tracked per-segment warm records (each ~120 B): past this,
/// the least-recent record is dropped — losing a warm record only
/// weakens future claims (fresh installs fall back to chunk-only), it
/// never breaks one already made.
pub const WARM_MAX_SEGMENTS: usize = 8_192;

static POSTINGS_CACHE_BYTES_INIT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(POSTINGS_CACHE_BYTES);

/// Composition-root seed (WP-01 PR 3.1): sized once from the owned
/// ServerConfig; un-seeded tests get the old env-unset default (64 MiB).
pub fn init_postings_cache(bytes: usize) {
    POSTINGS_CACHE_BYTES_INIT.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// The process-shared cache (production wiring). Tests build private
/// per-engine caches instead so their counters stay hermetic.
pub fn process_cache() -> Arc<PostingsCache> {
    static C: std::sync::OnceLock<Arc<PostingsCache>> = std::sync::OnceLock::new();
    C.get_or_init(|| {
        let bytes = POSTINGS_CACHE_BYTES_INIT.load(std::sync::atomic::Ordering::Relaxed);
        PostingsCache::new(bytes)
    })
    .clone()
}
/// Idle eviction horizon (spec §7.1).
pub const POSTINGS_CACHE_IDLE: Duration = Duration::from_secs(600);
/// Cold-load forward window (spec §7.2).
pub const LOAD_MAX_BUCKETS: u64 = 64;
pub const LOAD_MAX_ENCODED_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct PostingsSlice {
    pub first_bucket: u64,
    pub last_bucket_exclusive: u64,
    /// The slice's runs are COMPLETE over [covered_from, indexed_to_offset):
    /// a read below covered_from cannot be served from this slice (store
    /// loads prove coverage at bucket granularity; write-through installs
    /// at chunk granularity).
    pub covered_from: u64,
    /// The index provably covers [covered_from, indexed_to_offset):
    /// runs at or past this offset may exist but were not loaded.
    pub indexed_to_offset: u64,
    pub runs: Arc<[AbsRun]>,
    pub decoded_bytes: usize,
}

struct Entry {
    slice: Arc<PostingsSlice>,
    last_used: Instant,
}

type Key = ([u8; 16], [u8; 16]); // (segment identity, routing-key hash)

/// Write-through warm state for one segment (spec §7: the absorber
/// installs the runs it just encoded, so first-read-after-absorb skips
/// the index round trip). `clean` guards the ABSENCE proof: a fresh
/// install may claim "no matches in [from, chunk_start)" only while
/// every chunk since `from` was installed contiguously in this process
/// AND none of this segment's entries were evicted (an evicted entry's
/// key could re-appear and falsely claim its pre-eviction history was
/// empty).
struct SegWarm {
    from: u64,
    to: u64,
    clean: bool,
    /// True while EVERY key of every chunk since `from` was actually
    /// installed. Write-admission may skip cold keys once the cache
    /// passes its admission line — after the first skip, a FRESH
    /// install can no longer claim from-0 coverage (its key might have
    /// had skipped matches). Extends and the demand bridge stay valid:
    /// existing entries are always extended.
    admitted_all: bool,
    touched: Instant,
}

struct Inner {
    slices: HashMap<Key, Entry>,
    total_bytes: usize,
    inflight: HashMap<Key, tokio::sync::watch::Receiver<bool>>,
    warm: HashMap<[u8; 16], SegWarm>,
}

pub struct PostingsCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub coalesced: AtomicU64,
    pub evictions: AtomicU64,
    pub index_loads: AtomicU64,
    pub index_bytes_read: AtomicU64,
    pub prefetch_started: AtomicU64,
    pub prefetch_completed: AtomicU64,
    pub warm_installs: AtomicU64,
    pub warm_extends: AtomicU64,
}

/// Outcome of a cache consultation for one read.
pub enum CacheRuns {
    /// Runs (already clipped to cover the request range's buckets) plus
    /// how far the index provably covers. `provable_to >= upto` means
    /// the whole request range is index-verified.
    Runs { runs: Vec<AbsRun>, provable_to: u64 },
    /// A page in the range failed to decode: the caller must serve the
    /// range through the §8.6 canonical envelope and must NOT treat the
    /// index as authoritative.
    Corrupt,
}

impl PostingsCache {
    pub fn new(max_bytes: usize) -> Arc<PostingsCache> {
        Arc::new(PostingsCache {
            inner: Mutex::new(Inner {
                slices: HashMap::new(),
                total_bytes: 0,
                inflight: HashMap::new(),
                warm: HashMap::new(),
            }),
            max_bytes: max_bytes.max(1024 * 1024),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            index_loads: AtomicU64::new(0),
            index_bytes_read: AtomicU64::new(0),
            prefetch_started: AtomicU64::new(0),
            prefetch_completed: AtomicU64::new(0),
            warm_installs: AtomicU64::new(0),
            warm_extends: AtomicU64::new(0),
        })
    }

    pub fn stats(&self) -> serde_json::Value {
        let (bytes, entries) = {
            let g = self.inner.lock().unwrap();
            (g.total_bytes, g.slices.len())
        };
        serde_json::json!({
            "hits": self.hits.load(Ordering::Relaxed),
            "misses": self.misses.load(Ordering::Relaxed),
            "coalesced_waiters": self.coalesced.load(Ordering::Relaxed),
            "bytes": bytes,
            "entries": entries,
            "evictions": self.evictions.load(Ordering::Relaxed),
            "index_loads": self.index_loads.load(Ordering::Relaxed),
            "index_bytes_read": self.index_bytes_read.load(Ordering::Relaxed),
            "prefetch_started": self.prefetch_started.load(Ordering::Relaxed),
            "prefetch_completed": self.prefetch_completed.load(Ordering::Relaxed),
            "warm_installs": self.warm_installs.load(Ordering::Relaxed),
            "warm_extends": self.warm_extends.load(Ordering::Relaxed),
        })
    }

    /// Write-through install (spec §7): the absorber hands over the runs
    /// it just encoded for one contiguous gather chunk
    /// [chunk_from, chunk_to) of one segment, so the first read after
    /// absorption pays no index round trip. Coverage claims are exact:
    /// an EXTENDED entry keeps its covered_from; a FRESH entry claims
    /// from the segment's contiguous-warm base only while that base is 0
    /// (segment born in this process) and no entry of this segment was
    /// ever evicted — otherwise it claims only the chunk itself.
    pub fn install_chunk(
        &self,
        inc: SegmentHash,
        chunk_from: u64,
        chunk_to: u64,
        per_key: Vec<([u8; 16], Vec<AbsRun>)>,
    ) {
        if chunk_to <= chunk_from {
            return;
        }
        let now = Instant::now();
        let mut installs = 0u64;
        let mut extends = 0u64;
        let mut g = self.inner.lock().unwrap();
        // Write-admission line (review finding 7): a million cold keys
        // must not churn the cache to dodge one first-read miss each.
        // Under half the budget, admit every fresh install (small and
        // medium key populations stay fully warm — the campaign shape);
        // over it, only EXTEND existing entries. The first skipped
        // install permanently downgrades this segment's fresh-claim
        // strength.
        let admit_fresh = g.total_bytes < self.max_bytes / 2;
        if g.warm.len() >= WARM_MAX_SEGMENTS && !g.warm.contains_key(&inc.0) {
            // Bounded warm tracking: drop the least-recent record.
            // Losing one only weakens FUTURE claims (fresh installs fall
            // back to chunk-only), never an already-made one.
            if let Some(k) = g
                .warm
                .iter()
                .min_by_key(|(_, w)| w.touched)
                .map(|(k, _)| *k)
            {
                g.warm.remove(&k);
            }
        }
        let w = g.warm.entry(inc.0).or_insert(SegWarm {
            from: chunk_from,
            to: chunk_from,
            clean: true,
            admitted_all: true,
            touched: now,
        });
        if w.to != chunk_from {
            // A gap (restart, ownership move, or a chunk we never saw):
            // the contiguity claim restarts at this chunk.
            *w = SegWarm {
                from: chunk_from,
                to: chunk_from,
                clean: true,
                admitted_all: true,
                touched: now,
            };
        }
        w.to = chunk_to;
        w.touched = now;
        if !admit_fresh {
            w.admitted_all = false;
        }
        let (w_from, w_clean, w_admitted) = (w.from, w.clean, w.admitted_all);
        // Fresh installs claim absence-of-earlier-matches only from the
        // warm base, only from 0, and only while NO install was ever
        // skipped: a skipped key's matches were never recorded, so
        // absence stops being proof.
        let fresh_from = if w_clean && w_admitted && w_from == 0 {
            0
        } else {
            chunk_from
        };
        for (kh, runs) in per_key {
            let key: Key = (inc.0, kh);
            match g.slices.get(&key) {
                Some(e) => {
                    let s = &e.slice;
                    if s.indexed_to_offset >= chunk_to {
                        continue; // already covers us
                    }
                    // Adjacent chunks extend directly. A HOLE between the
                    // slice's coverage and this chunk is bridgeable iff
                    // the warm window contiguously installed every chunk
                    // across it with no evictions: this key's absence
                    // from those installs IS the proof the hole is
                    // match-free (a key active only intermittently would
                    // otherwise stop being warm forever — the campaign's
                    // warm_extends=0 finding).
                    let bridgeable = s.indexed_to_offset >= chunk_from
                        || (w_clean && s.indexed_to_offset >= w_from);
                    if !bridgeable {
                        continue;
                    }
                    let mut merged: Vec<AbsRun> = s.runs.to_vec();
                    let cut = s.indexed_to_offset;
                    // Round-13 CODE-RED: a run STRADDLING the cut must be
                    // SPLIT, never dropped — filtering on r.start >= cut
                    // discarded the [cut, end) tail of a straddler while
                    // the slice still claimed indexed_to = chunk_to, so
                    // every key whose match run crossed a prior
                    // extension boundary lost that tail from the proof
                    // FOREVER (the keyed history read then served a
                    // provably-covered gap: 11 durable records lost in
                    // field leg A1v2; cut_resume_never_skips_a_durable_
                    // record reproduces in ~70 s).
                    let fresh: Vec<AbsRun> = runs
                        .into_iter()
                        .filter_map(|r| {
                            let end = r.start + r.count as u64;
                            if end <= cut {
                                None
                            } else if r.start >= cut {
                                Some(r)
                            } else {
                                Some(AbsRun {
                                    start: cut,
                                    count: (end - cut) as u32,
                                    // The tail keeps the whole run's
                                    // byte weight (a safe OVER-estimate
                                    // for the scan planner) and an
                                    // unmeasured seam before it.
                                    matching_bytes: r.matching_bytes,
                                    gap_bytes_before: crate::postings::GAP_UNKNOWN,
                                })
                            }
                        })
                        .collect();
                    crate::postings::append_page_runs(&mut merged, fresh);
                    let decoded =
                        merged.len() * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES;
                    let slice = Arc::new(PostingsSlice {
                        first_bucket: s.first_bucket,
                        last_bucket_exclusive: chunk_to.div_ceil(BUCKET_OFFSETS),
                        covered_from: s.covered_from,
                        indexed_to_offset: chunk_to,
                        runs: merged.into(),
                        decoded_bytes: decoded,
                    });
                    g.total_bytes = g.total_bytes + decoded - s.decoded_bytes;
                    g.slices.insert(
                        key,
                        Entry {
                            slice,
                            last_used: now,
                        },
                    );
                    extends += 1;
                }
                None => {
                    if !admit_fresh {
                        continue; // over the admission line: extends only
                    }
                    let decoded = runs.len() * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES;
                    let slice = Arc::new(PostingsSlice {
                        first_bucket: fresh_from / BUCKET_OFFSETS,
                        last_bucket_exclusive: chunk_to.div_ceil(BUCKET_OFFSETS),
                        covered_from: fresh_from,
                        indexed_to_offset: chunk_to,
                        runs: runs.into(),
                        decoded_bytes: decoded,
                    });
                    g.total_bytes += decoded;
                    g.slices.insert(
                        key,
                        Entry {
                            slice,
                            last_used: now,
                        },
                    );
                    installs += 1;
                }
            }
        }
        // Weight eviction, poisoning each victim segment's absence proof.
        while g.total_bytes > self.max_bytes && g.slices.len() > 1 {
            let victim = g
                .slices
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k);
            match victim {
                Some(v) => {
                    if let Some(e) = g.slices.remove(&v) {
                        g.total_bytes -= e.slice.decoded_bytes;
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    if let Some(w) = g.warm.get_mut(&v.0) {
                        w.clean = false;
                    }
                }
                None => break,
            }
        }
        drop(g);
        self.warm_installs.fetch_add(installs, Ordering::Relaxed);
        self.warm_extends.fetch_add(extends, Ordering::Relaxed);
    }

    /// Runs covering [from, upto) for one (segment, key), through the
    /// cache. `absorbed` is the caller's durable boundary — the ceiling
    /// of what the index can prove and the prefetch target.
    #[cfg(test)]
    pub(crate) fn runs_for_test(&self, inc: SegmentHash, kh: RoutingKeyHash) -> Vec<AbsRun> {
        self.inner
            .lock()
            .unwrap()
            .slices
            .get(&(inc.0, kh.0))
            .map(|e| e.slice.runs.to_vec())
            .unwrap_or_default()
    }

    pub async fn runs_for(
        self: &Arc<Self>,
        part: &Arc<Db>,
        route: RouteHash,
        inc: SegmentHash,
        kh: RoutingKeyHash,
        from: u64,
        upto: u64,
        absorbed: u64,
    ) -> anyhow::Result<CacheRuns> {
        enum Decision {
            Hit(Arc<PostingsSlice>, u64),
            /// Cursor behind the slice: serve uncached, do not churn.
            Bypass,
            Wait(tokio::sync::watch::Receiver<bool>),
            Lead {
                tx: tokio::sync::watch::Sender<bool>,
                existing: Option<Arc<PostingsSlice>>,
            },
        }
        let key: Key = (inc.0, kh.0);
        let want_bucket = from / BUCKET_OFFSETS;
        // Bounded re-check loop (single-flight followers re-evaluate
        // after the leader publishes). Every lock scope is award-free:
        // decide under the lock, act after it drops.
        for _ in 0..4 {
            let decision = {
                let mut g = self.inner.lock().unwrap();
                // Warm-window bridge, demand side: a key whose slice ends
                // BEFORE the segment's contiguously-installed frontier is
                // still fully covered up to that frontier — its absence
                // from every install past indexed_to IS the proof of no
                // matches there. (The install-side bridge only fires on
                // the key's NEXT appearance; a key that never re-appears
                // would otherwise go cold at every new chunk.)
                let warm_to = g
                    .warm
                    .get(&key.0)
                    .filter(|w| w.clean)
                    .map(|w| (w.from, w.to));
                let covered = g.slices.get_mut(&key).and_then(|e| {
                    let mut effective_to = e.slice.indexed_to_offset;
                    if let Some((wf, wt)) = warm_to
                        && wf <= effective_to
                        && wt > effective_to
                    {
                        effective_to = wt;
                    }
                    if e.slice.covered_from <= from && upto <= effective_to {
                        e.last_used = Instant::now();
                        Some(Decision::Hit(e.slice.clone(), effective_to))
                    } else if e.slice.covered_from > from {
                        Some(Decision::Bypass)
                    } else {
                        None
                    }
                });
                match covered {
                    Some(d) => d,
                    None => match g.inflight.get(&key) {
                        Some(rx) => Decision::Wait(rx.clone()),
                        None => {
                            let (tx, rx) = tokio::sync::watch::channel(false);
                            g.inflight.insert(key, rx);
                            Decision::Lead {
                                tx,
                                existing: g.slices.get(&key).map(|e| e.slice.clone()),
                            }
                        }
                    },
                }
            };
            match decision {
                Decision::Hit(s, covered_to) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    self.maybe_prefetch(part, route, inc, kh, &s, upto, absorbed, covered_to);
                    let runs = clip_runs(&s.runs, from, upto);
                    return Ok(CacheRuns::Runs {
                        runs,
                        provable_to: upto,
                    });
                }
                Decision::Bypass => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    let (runs, _enc, provable_to, corrupt) =
                        load_runs(self, part, route, inc, kh, want_bucket, upto).await?;
                    if corrupt {
                        return Ok(CacheRuns::Corrupt);
                    }
                    let pt = provable_to.min(upto);
                    return Ok(CacheRuns::Runs {
                        runs: clip_runs(&runs, from, pt),
                        provable_to: pt,
                    });
                }
                Decision::Wait(mut rx) => {
                    self.coalesced.fetch_add(1, Ordering::Relaxed);
                    let _ = rx.changed().await;
                }
                Decision::Lead { tx, existing } => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    let target = upto.max(
                        absorbed.min(
                            want_bucket
                                .saturating_mul(BUCKET_OFFSETS)
                                .saturating_add(LOAD_MAX_BUCKETS * BUCKET_OFFSETS),
                        ),
                    );
                    self.spawn_load(
                        part.clone(),
                        route,
                        inc,
                        kh,
                        existing,
                        want_bucket,
                        target,
                        tx,
                        false,
                    );
                    let mut rx = {
                        let g = self.inner.lock().unwrap();
                        g.inflight.get(&key).cloned()
                    }
                    .unwrap_or_else(|| tokio::sync::watch::channel(true).1);
                    let _ = rx.changed().await;
                }
            }
            // Post-wake state: a covering slice means the next loop
            // iteration serves the hit; a finished-but-short load (or
            // corruption, which publishes nothing) resolves directly.
            let (ready, still_inflight) = {
                let g = self.inner.lock().unwrap();
                (
                    {
                        let warm_to = g
                            .warm
                            .get(&key.0)
                            .filter(|w| w.clean)
                            .map(|w| (w.from, w.to));
                        g.slices
                            .get(&key)
                            .map(|e| {
                                let mut eff = e.slice.indexed_to_offset;
                                if let Some((wf, wt)) = warm_to
                                    && wf <= eff
                                    && wt > eff
                                {
                                    eff = wt;
                                }
                                e.slice.covered_from <= from && upto <= eff
                            })
                            .unwrap_or(false)
                    },
                    g.inflight.contains_key(&key),
                )
            };
            if ready {
                continue;
            }
            if !still_inflight {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let (runs, _enc, provable_to, corrupt) =
                    load_runs(self, part, route, inc, kh, want_bucket, upto).await?;
                if corrupt {
                    return Ok(CacheRuns::Corrupt);
                }
                let pt = provable_to.min(upto);
                return Ok(CacheRuns::Runs {
                    runs: clip_runs(&runs, from, pt),
                    provable_to: pt,
                });
            }
        }
        // Persistent contention: honest uncached read.
        self.misses.fetch_add(1, Ordering::Relaxed);
        let (runs, _enc, provable_to, corrupt) =
            load_runs(self, part, route, inc, kh, want_bucket, upto).await?;
        if corrupt {
            return Ok(CacheRuns::Corrupt);
        }
        let pt = provable_to.min(upto);
        Ok(CacheRuns::Runs {
            runs: clip_runs(&runs, from, pt),
            provable_to: pt,
        })
    }

    /// Owned, cancellation-proof load: extends `existing` forward or
    /// cold-loads from `want_bucket`, publishes into the map, evicts to
    /// budget, then notifies waiters. Corruption publishes NOTHING (the
    /// waiters' direct load rediscovers it and serves the envelope).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn spawn_load(
        self: &Arc<Self>,
        part: Arc<Db>,
        route: RouteHash,
        inc: SegmentHash,
        kh: RoutingKeyHash,
        existing: Option<Arc<PostingsSlice>>,
        want_bucket: u64,
        target_offset: u64,
        tx: tokio::sync::watch::Sender<bool>,
        is_prefetch: bool,
    ) {
        let cache = self.clone();
        tokio::spawn(async move {
            let key: Key = (inc.0, kh.0);
            let start_bucket = match &existing {
                Some(s) if s.first_bucket <= want_bucket => s.indexed_to_offset / BUCKET_OFFSETS,
                _ => want_bucket,
            };
            let res = load_runs(&cache, &part, route, inc, kh, start_bucket, target_offset).await;
            let mut g = cache.inner.lock().unwrap();
            g.inflight.remove(&key);
            if let Ok((new_runs, _enc, provable_to, corrupt)) = res
                && !corrupt
            {
                let (runs, first_bucket, covered_from, decoded) = match &existing {
                    Some(s) if s.first_bucket <= want_bucket => {
                        // Forward extension: append past indexed_to.
                        let mut merged: Vec<AbsRun> = s.runs.to_vec();
                        let cut = s.indexed_to_offset;
                        let fresh: Vec<AbsRun> = new_runs
                            .iter()
                            .copied()
                            .filter(|r| r.start >= cut)
                            .collect();
                        crate::postings::append_page_runs(&mut merged, fresh);
                        let bytes =
                            merged.len() * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES;
                        (merged, s.first_bucket, s.covered_from, bytes)
                    }
                    _ => {
                        let bytes =
                            new_runs.len() * std::mem::size_of::<AbsRun>() + ENTRY_OVERHEAD_BYTES;
                        // Store loads prove coverage at bucket
                        // granularity: every bucket from start_bucket
                        // was scanned in full.
                        (new_runs, start_bucket, start_bucket * BUCKET_OFFSETS, bytes)
                    }
                };
                let old_bytes = g
                    .slices
                    .get(&key)
                    .map(|e| e.slice.decoded_bytes)
                    .unwrap_or(0);
                let slice = Arc::new(PostingsSlice {
                    first_bucket,
                    last_bucket_exclusive: provable_to.div_ceil(BUCKET_OFFSETS),
                    covered_from,
                    indexed_to_offset: provable_to,
                    runs: runs.into(),
                    decoded_bytes: decoded,
                });
                g.total_bytes = g.total_bytes + decoded - old_bytes;
                g.slices.insert(
                    key,
                    Entry {
                        slice,
                        last_used: Instant::now(),
                    },
                );
                // Weight eviction: drop least-recent entries (never
                // the one just inserted) until the budget holds.
                // Every victim poisons its segment's warm absence
                // proof (see install_chunk).
                while g.total_bytes > cache.max_bytes && g.slices.len() > 1 {
                    let victim = g
                        .slices
                        .iter()
                        .filter(|(k, _)| **k != key)
                        .min_by_key(|(_, e)| e.last_used)
                        .map(|(k, _)| *k);
                    match victim {
                        Some(v) => {
                            if let Some(e) = g.slices.remove(&v) {
                                g.total_bytes -= e.slice.decoded_bytes;
                                cache.evictions.fetch_add(1, Ordering::Relaxed);
                            }
                            if let Some(w) = g.warm.get_mut(&v.0) {
                                w.clean = false;
                            }
                        }
                        None => break,
                    }
                }
            }
            drop(g);
            if is_prefetch {
                cache.prefetch_completed.fetch_add(1, Ordering::Relaxed);
            }
            let _ = tx.send(true);
        });
    }

    /// Best-effort forward prefetch once 75% of the slice is consumed
    /// (spec §7.6). Single-flight via the same in-flight map; never
    /// blocks or delays the current response.
    #[allow(clippy::too_many_arguments)]
    fn maybe_prefetch(
        self: &Arc<Self>,
        part: &Arc<Db>,
        route: RouteHash,
        inc: SegmentHash,
        kh: RoutingKeyHash,
        slice: &Arc<PostingsSlice>,
        upto: u64,
        absorbed: u64,
        covered_to: u64,
    ) {
        if slice.indexed_to_offset >= absorbed || covered_to >= absorbed {
            return; // nothing further exists (or the warm window covers it)
        }
        let span = slice
            .indexed_to_offset
            .saturating_sub(slice.first_bucket * BUCKET_OFFSETS);
        let used = upto.saturating_sub(slice.first_bucket * BUCKET_OFFSETS);
        if span == 0 || used * 4 < span * 3 {
            return; // < 75% consumed
        }
        let key: Key = (inc.0, kh.0);
        let mut g = self.inner.lock().unwrap();
        if g.inflight.contains_key(&key) {
            return;
        }
        let (tx, rx) = tokio::sync::watch::channel(false);
        g.inflight.insert(key, rx);
        let existing = g.slices.get(&key).map(|e| e.slice.clone());
        drop(g);
        self.prefetch_started.fetch_add(1, Ordering::Relaxed);
        let target = absorbed.min(slice.indexed_to_offset + LOAD_MAX_BUCKETS * BUCKET_OFFSETS);
        self.spawn_load(
            part.clone(),
            route,
            inc,
            kh,
            existing,
            slice.first_bucket,
            target,
            tx,
            true,
        );
    }

    /// Test-only: the (covered_from, indexed_to, runs) of one cached
    /// slice, for flake diagnosis.
    #[cfg(test)]
    pub fn debug_slice(
        &self,
        inc: &SegmentHash,
        kh: &crate::crypto::RoutingKeyHash,
    ) -> Option<(u64, u64, usize)> {
        let g = self.inner.lock().unwrap();
        g.slices.get(&(inc.0, kh.0)).map(|e| {
            (
                e.slice.covered_from,
                e.slice.indexed_to_offset,
                e.slice.runs.len(),
            )
        })
    }

    /// Idle sweep (engine flush ticker): drop slices unused for the
    /// idle horizon. Sweeping an entry poisons its segment's warm
    /// absence proof (the swept key could re-appear and a fresh install
    /// must not claim its pre-sweep history was empty); warm records
    /// idle past the horizon are dropped outright.
    pub fn sweep_idle(&self, idle: Duration) -> usize {
        let cutoff = Instant::now() - idle;
        let mut g = self.inner.lock().unwrap();
        let before = g.slices.len();
        let dead: Vec<Key> = g
            .slices
            .iter()
            .filter(|(_, e)| e.last_used < cutoff)
            .map(|(k, _)| *k)
            .collect();
        for k in dead {
            if let Some(e) = g.slices.remove(&k) {
                g.total_bytes -= e.slice.decoded_bytes;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(w) = g.warm.get_mut(&k.0) {
                w.clean = false;
            }
        }
        g.warm.retain(|_, w| w.touched >= cutoff);
        before - g.slices.len()
    }
}

fn clip_runs(runs: &[AbsRun], from: u64, upto: u64) -> Vec<AbsRun> {
    let mut out = Vec::new();
    for r in runs {
        let start = r.start.max(from);
        let end = (r.start + r.count as u64).min(upto);
        if start >= end {
            continue;
        }
        out.push(AbsRun {
            start,
            count: (end - start) as u32,
            matching_bytes: r.matching_bytes,
            gap_bytes_before: r.gap_bytes_before,
        });
    }
    out
}

/// One contiguous postings scan from `start_bucket` toward
/// `target_offset`, bounded by the cold-load window (spec §7.2).
/// Returns (runs, encoded bytes read, provable-to offset, corrupt).
async fn load_runs(
    cache: &Arc<PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    kh: RoutingKeyHash,
    start_bucket: u64,
    target_offset: u64,
) -> anyhow::Result<(Vec<AbsRun>, u64, u64, bool)> {
    cache.index_loads.fetch_add(1, Ordering::Relaxed);
    let end_bucket = (target_offset.div_ceil(BUCKET_OFFSETS))
        .min(start_bucket + LOAD_MAX_BUCKETS)
        .max(start_bucket + 1);
    let lo = crate::postings::postings_key(route, inc, &kh, start_bucket, 0);
    let hi = crate::postings::postings_key(route, inc, &kh, end_bucket, 0);
    let mut runs: Vec<AbsRun> = Vec::new();
    let mut encoded = 0u64;
    let mut last_full_bucket = start_bucket;
    let mut iter = part
        .scan_with_options(lo..hi, &crate::history::postings_scan_opts_pub())
        .await?;
    while let Some(kv) = iter.next().await? {
        encoded += kv.value.len() as u64;
        let bucket = u64::from_be_bytes(
            kv.key[kv.key.len() - 16..kv.key.len() - 8]
                .try_into()
                .expect("postings bucket"),
        );
        let first = u64::from_be_bytes(
            kv.key[kv.key.len() - 8..]
                .try_into()
                .expect("postings first"),
        );
        match crate::postings::decode_page_abs(first, &kv.value) {
            Some(abs) => crate::postings::append_page_runs(&mut runs, abs),
            None => {
                cache.index_bytes_read.fetch_add(encoded, Ordering::Relaxed);
                return Ok((Vec::new(), encoded, 0, true));
            }
        }
        last_full_bucket = bucket;
        if encoded >= LOAD_MAX_ENCODED_BYTES {
            break;
        }
    }
    cache.index_bytes_read.fetch_add(encoded, Ordering::Relaxed);
    // Coverage proof: every bucket scanned to completion is covered to
    // its end (absent pages there = provably no matches); a byte-capped
    // load is covered through its last decoded run's end.
    let provable_to = if encoded >= LOAD_MAX_ENCODED_BYTES {
        runs.last()
            .map(|r| r.start + r.count as u64)
            .unwrap_or((last_full_bucket + 1) * BUCKET_OFFSETS)
    } else {
        end_bucket * BUCKET_OFFSETS
    };
    Ok((runs, encoded, provable_to, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(start: u64, count: u32) -> AbsRun {
        AbsRun {
            start,
            count,
            matching_bytes: count as u64 * 100,
            gap_bytes_before: 0,
        }
    }

    async fn mem_db(prefix: &str) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        Arc::new(Db::builder(prefix, store).build().await.unwrap())
    }

    fn ids(n: u8) -> (RouteHash, SegmentHash, RoutingKeyHash) {
        (
            RouteHash([n; 16]),
            SegmentHash([n.wrapping_add(1); 16]),
            RoutingKeyHash([n.wrapping_add(2); 16]),
        )
    }

    async fn runs_of(
        c: &Arc<PostingsCache>,
        part: &Arc<Db>,
        n: u8,
        from: u64,
        upto: u64,
    ) -> Vec<AbsRun> {
        let (route, inc, kh) = ids(n);
        match c
            .runs_for(part, route, inc, kh, from, upto, upto)
            .await
            .unwrap()
        {
            CacheRuns::Runs { runs, provable_to } => {
                assert!(provable_to >= upto, "honest coverage to the request");
                runs
            }
            CacheRuns::Corrupt => panic!("unexpected corruption"),
        }
    }

    /// Write-through warming: a chunk installed from offset 0 serves a
    /// from-0 read entirely from the cache — no index round trip.
    #[tokio::test]
    async fn warm_install_serves_from_zero_without_index_load() {
        let part = mem_db("wt/a").await;
        let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
        let (_, inc, kh) = ids(1);
        cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
        let got = runs_of(&cache, &part, 1, 0, 100).await;
        assert_eq!(got, vec![run(5, 3)]);
        assert_eq!(
            cache.index_loads.load(Ordering::Relaxed),
            0,
            "no store load"
        );
        assert_eq!(cache.hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.warm_installs.load(Ordering::Relaxed), 1);
    }

    /// Contiguous chunks extend the same slice; the stitched slice
    /// serves the union without touching the store.
    #[tokio::test]
    async fn warm_extension_stitches_chunks() {
        let part = mem_db("wt/b").await;
        let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
        let (_, inc, kh) = ids(2);
        cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
        cache.install_chunk(inc, 100, 200, vec![(kh.0, vec![run(150, 2)])]);
        assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
        let got = runs_of(&cache, &part, 2, 0, 200).await;
        // The chunk seam is a stitched boundary: its gap is UNKNOWN by
        // design (the planner refuses to coalesce across it), exactly as
        // if the two pages had been loaded from the store.
        let seam = AbsRun {
            gap_bytes_before: crate::postings::GAP_UNKNOWN,
            ..run(150, 2)
        };
        assert_eq!(got, vec![run(5, 3), seam]);
        assert_eq!(cache.index_loads.load(Ordering::Relaxed), 0);
    }

    /// A gap in the chunk sequence (restart / ownership move) resets the
    /// claim window: a key first seen AFTER the gap must not pretend its
    /// earlier history is empty — a from-0 read consults the store.
    #[tokio::test]
    async fn noncontiguous_chunk_resets_absence_claim() {
        let part = mem_db("wt/c").await;
        let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
        let (_, inc, kh) = ids(3);
        cache.install_chunk(inc, 0, 100, vec![]);
        cache.install_chunk(inc, 150, 220, vec![(kh.0, vec![run(160, 1)])]);
        // From the chunk itself: warm hit.
        let got = runs_of(&cache, &part, 3, 150, 220).await;
        assert_eq!(got, vec![run(160, 1)]);
        assert_eq!(cache.index_loads.load(Ordering::Relaxed), 0);
        // From 0: below covered_from — must go to the store.
        let _ = runs_of(&cache, &part, 3, 0, 220).await;
        assert!(
            cache.index_loads.load(Ordering::Relaxed) >= 1,
            "read below the claim window must consult the index"
        );
    }

    /// A key absent from intermediate chunks stays warm: the clean
    /// contiguous warm window proves the hole match-free, so the
    /// extension bridges it (seam marked GAP_UNKNOWN like any stitched
    /// page boundary).
    #[tokio::test]
    async fn warm_extension_bridges_matchfree_hole() {
        let part = mem_db("wt/e").await;
        let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
        let (_, inc, kh) = ids(5);
        cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 3)])]);
        cache.install_chunk(inc, 100, 200, vec![]); // key absent
        cache.install_chunk(inc, 200, 300, vec![(kh.0, vec![run(250, 2)])]);
        assert_eq!(cache.warm_extends.load(Ordering::Relaxed), 1);
        let got = runs_of(&cache, &part, 5, 0, 300).await;
        let seam = AbsRun {
            gap_bytes_before: crate::postings::GAP_UNKNOWN,
            ..run(250, 2)
        };
        assert_eq!(got, vec![run(5, 3), seam]);
        assert_eq!(
            cache.index_loads.load(Ordering::Relaxed),
            0,
            "bridged, no load"
        );

        // A DIRTY window must NOT bridge: poison via a sweep, then a
        // later chunk cannot extend across the unproven middle.
        cache.sweep_idle(Duration::ZERO);
        cache.install_chunk(inc, 300, 400, vec![(kh.0, vec![run(350, 1)])]);
        let loads0 = cache.index_loads.load(Ordering::Relaxed);
        let _ = runs_of(&cache, &part, 5, 0, 400).await;
        assert!(
            cache.index_loads.load(Ordering::Relaxed) > loads0,
            "post-sweep reads must consult the store"
        );
    }

    /// Review finding 7's scale shape, cache-level and suite-sized (the
    /// field campaign runs the full 1M x 32-engine version): a large
    /// cold key population written through 32 segments must not blow
    /// the ONE process budget; a small active read set stays >= 90%
    /// warm after each key's first read; inactive keys do not hold
    /// long-lived entries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn million_key_shape_holds_process_budget() {
        let part = mem_db("wt/scale").await;
        let budget = 2 * 1024 * 1024usize;
        let cache = PostingsCache::new(budget);
        // 32 segments x 4,000 keys/segment x 8 chunks: 128k distinct
        // keys pushed through write-through installs.
        let per_seg_keys = 4_000u64;
        for seg in 0..32u8 {
            let inc = SegmentHash([seg.wrapping_add(50); 16]);
            for chunk in 0..8u64 {
                let base = chunk * 1_000;
                let per_key: Vec<([u8; 16], Vec<AbsRun>)> = (0..per_seg_keys / 8)
                    .map(|i| {
                        let key_id = chunk * (per_seg_keys / 8) + i;
                        let mut kh = [seg; 16];
                        kh[..8].copy_from_slice(&key_id.to_le_bytes());
                        (kh, vec![run(base + i, 2)])
                    })
                    .collect();
                cache.install_chunk(inc, base, base + 1_000, per_key);
            }
        }
        let (bytes, entries) = {
            let g = cache.inner.lock().unwrap();
            (g.total_bytes, g.slices.len())
        };
        assert!(
            bytes <= budget,
            "process budget must hold: {bytes} > {budget}"
        );
        assert!(
            entries < 128_000 / 4,
            "cold keys must not all hold entries (entries={entries})"
        );

        // Active read set: 100 keys, 20 reads each. After each key's
        // FIRST read, everything must be a hit.
        let inc = SegmentHash([50; 16]);
        let route = RouteHash([0; 16]);
        let mut first_reads = 0u64;
        for key_id in 0..100u64 {
            let mut kh = [0u8; 16];
            kh[..8].copy_from_slice(&key_id.to_le_bytes());
            first_reads += 1;
            for _ in 0..20 {
                let _ = cache
                    .runs_for(&part, route, inc, RoutingKeyHash(kh), 0, 8_000, 8_000)
                    .await
                    .unwrap();
            }
        }
        let hits = cache.hits.load(Ordering::Relaxed);
        let total_reads = 100 * 20;
        let warm_reads = total_reads - first_reads; // first read may load
        assert!(
            hits >= warm_reads * 9 / 10,
            "active-set warm hit rate >= 90%: hits={hits} warm={warm_reads}"
        );
    }

    /// Evicting any entry of a segment poisons its absence proof: a key
    /// evicted and later reinstalled fresh must not claim from 0.
    #[tokio::test]
    async fn eviction_poisons_fresh_claims() {
        let part = mem_db("wt/d").await;
        let cache = PostingsCache::new(1); // clamps to the 1 MiB floor
        let (_, inc, kh) = ids(4);
        let (_, _, other) = ids(9);
        // Write-admission stops FRESH installs at half budget, so the
        // over-budget pressure comes from an EXTEND (extends always
        // apply to existing entries).
        cache.install_chunk(inc, 0, 100, vec![(kh.0, vec![run(5, 1)])]);
        cache.install_chunk(inc, 100, 200, vec![(other.0, vec![run(150, 1)])]);
        let fat: Vec<AbsRun> = (0..40_000u64).map(|i| run(200 + i * 2, 1)).collect();
        cache.install_chunk(inc, 200, 200_000, vec![(kh.0, fat)]);
        assert!(
            cache.evictions.load(Ordering::Relaxed) >= 1,
            "budget must evict"
        );
        // The evicted key reinstalls fresh in the next contiguous chunk:
        // it must claim only the chunk, so a from-0 read hits the store.
        cache.install_chunk(
            inc,
            200_000,
            200_100,
            vec![(other.0, vec![run(200_050, 1)])],
        );
        let loads_before = cache.index_loads.load(Ordering::Relaxed);
        let _ = runs_of(&cache, &part, 9, 0, 200_100).await;
        assert!(
            cache.index_loads.load(Ordering::Relaxed) > loads_before,
            "poisoned segment must not serve absence from the warm claim"
        );
    }
}

#[cfg(test)]
mod straddle_tests {
    use super::*;
    use crate::crypto::SegmentHash;

    /// Round-13 CODE-RED unit red: an install run STRADDLING the
    /// slice's extension cut must contribute its [cut, end) tail —
    /// the old filter dropped the whole run and the slice then proved
    /// a match-free hole over durable records (11 lost in field leg
    /// A1v2).
    #[test]
    fn straddling_install_run_is_split_not_dropped() {
        let cache = Arc::new(PostingsCache::new(1 << 20));
        let inc = SegmentHash([9u8; 16]);
        let kh = crate::postings::rk_hash("");
        // Seed a slice covering [0, 59).
        cache.install_chunk(
            inc,
            0,
            59,
            vec![(
                kh.0,
                vec![AbsRun {
                    start: 0,
                    count: 59,
                    matching_bytes: 59 * 100,
                    gap_bytes_before: 0,
                }],
            )],
        );
        // Extend with a chunk whose run STRADDLES the cut: [50, 90).
        cache.install_chunk(
            inc,
            59,
            90,
            vec![(
                kh.0,
                vec![AbsRun {
                    start: 50,
                    count: 40,
                    matching_bytes: 40 * 100,
                    gap_bytes_before: 0,
                }],
            )],
        );
        let covered: Vec<(u64, u64)> = cache
            .runs_for_test(inc, kh)
            .iter()
            .map(|r| (r.start, r.start + r.count as u64))
            .collect();
        let holds = |q: u64| covered.iter().any(|(a, b)| q >= *a && q < *b);
        for q in 0..90 {
            assert!(
                holds(q),
                "offset {q} lost by the straddle drop: {covered:?}"
            );
        }
    }
}
