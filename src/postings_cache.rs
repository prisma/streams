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

/// Default decoded-byte budget per shard engine (spec §7.1).
pub const POSTINGS_CACHE_BYTES: usize = 16 * 1024 * 1024;
/// Idle eviction horizon (spec §7.1).
pub const POSTINGS_CACHE_IDLE: Duration = Duration::from_secs(600);
/// Cold-load forward window (spec §7.2).
pub const LOAD_MAX_BUCKETS: u64 = 64;
pub const LOAD_MAX_ENCODED_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct PostingsSlice {
    pub first_bucket: u64,
    pub last_bucket_exclusive: u64,
    /// The index provably covers [first_bucket*B, indexed_to_offset):
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

struct Inner {
    slices: HashMap<Key, Entry>,
    total_bytes: usize,
    inflight: HashMap<Key, tokio::sync::watch::Receiver<bool>>,
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
        })
    }

    /// Runs covering [from, upto) for one (segment, key), through the
    /// cache. `absorbed` is the caller's durable boundary — the ceiling
    /// of what the index can prove and the prefetch target.
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
            Hit(Arc<PostingsSlice>),
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
                let covered = g.slices.get_mut(&key).and_then(|e| {
                    if e.slice.first_bucket <= want_bucket && upto <= e.slice.indexed_to_offset {
                        e.last_used = Instant::now();
                        Some(Decision::Hit(e.slice.clone()))
                    } else if e.slice.first_bucket > want_bucket {
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
                Decision::Hit(s) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    self.maybe_prefetch(part, route, inc, kh, &s, upto, absorbed);
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
                    g.slices
                        .get(&key)
                        .map(|e| {
                            e.slice.first_bucket <= want_bucket && upto <= e.slice.indexed_to_offset
                        })
                        .unwrap_or(false),
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
            if let Ok((new_runs, _enc, provable_to, corrupt)) = res {
                if !corrupt {
                    let (runs, first_bucket, decoded) = match &existing {
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
                            let bytes = merged.len() * std::mem::size_of::<AbsRun>();
                            (merged, s.first_bucket, bytes)
                        }
                        _ => {
                            let bytes = new_runs.len() * std::mem::size_of::<AbsRun>();
                            (new_runs, start_bucket, bytes)
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
                            }
                            None => break,
                        }
                    }
                }
            }
            drop(g);
            cache.prefetch_completed.fetch_add(0, Ordering::Relaxed);
            let _ = tx.send(true);
        });
    }

    /// Best-effort forward prefetch once 75% of the slice is consumed
    /// (spec §7.6). Single-flight via the same in-flight map; never
    /// blocks or delays the current response.
    fn maybe_prefetch(
        self: &Arc<Self>,
        part: &Arc<Db>,
        route: RouteHash,
        inc: SegmentHash,
        kh: RoutingKeyHash,
        slice: &Arc<PostingsSlice>,
        upto: u64,
        absorbed: u64,
    ) {
        if slice.indexed_to_offset >= absorbed {
            return; // nothing further exists yet
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
        );
    }

    /// Idle sweep (engine flush ticker): drop slices unused for the
    /// idle horizon.
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
        }
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
