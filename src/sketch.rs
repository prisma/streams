//! Bounded, in-memory distribution sketches for the unified scaler
//! (spec §5.1): per-segment load EWMAs, a 64-bin key-space histogram,
//! a SpaceSaving heavy-hitter table (detects the one unsplittable hot
//! key), and a small HLL for distinct-key counts.

/// Exponentially weighted moving average over wall-clock seconds.
#[derive(Debug, Clone, Copy)]
pub struct Ewma {
    pub rate: f64,
    last_ms: i64,
    window_secs: f64,
}

impl Ewma {
    pub fn new(window_secs: f64) -> Ewma {
        Ewma {
            rate: 0.0,
            last_ms: 0,
            window_secs: window_secs.max(1.0),
        }
    }

    pub fn add(&mut self, now_ms: i64, amount: f64) {
        if self.last_ms == 0 {
            // First feed: clamp to 1 so t=0 cannot alias the
            // "never fed" sentinel.
            self.last_ms = now_ms.max(1);
        }
        let dt = ((now_ms - self.last_ms).max(0) as f64) / 1000.0;
        if dt > 0.0 {
            let decay = (-dt / self.window_secs).exp();
            self.rate *= decay;
            self.last_ms = now_ms;
        }
        // Treat the amount as an impulse contributing amount/window to
        // the per-second rate (standard event-driven EWMA).
        self.rate += amount / self.window_secs;
    }

    /// Rate decayed to `now` without adding anything.
    pub fn value(&self, now_ms: i64) -> f64 {
        if self.last_ms == 0 {
            return 0.0;
        }
        let dt = ((now_ms - self.last_ms).max(0) as f64) / 1000.0;
        self.rate * (-dt / self.window_secs).exp()
    }
}

/// SpaceSaving with 8 slots: tracks up to 8 heavy hitters with
/// overestimate-bounded counts — enough to answer "does ONE key
/// dominate this segment" (spec §5.2's unsplittable-hot-key check).
#[derive(Debug, Default, Clone)]
pub struct SpaceSaving8 {
    slots: Vec<([u8; 16], u64, u64)>, // (key hash, count, error)
    pub total: u64,
}

impl SpaceSaving8 {
    /// Merge two windows into a fresh table (register the guaranteed
    /// counts of both; totals add). Used for the (current, previous)
    /// generational view — dominance decisions must see recent load,
    /// not all history (review finding 8).
    pub fn merged(&self, other: &SpaceSaving8) -> SpaceSaving8 {
        let mut out = SpaceSaving8::default();
        for src in [self, other] {
            for (k, c, e) in &src.slots {
                out.add(*k, c.saturating_sub(*e));
            }
        }
        out.total = self.total + other.total;
        out
    }

    pub fn add(&mut self, key: [u8; 16], amount: u64) {
        self.total += amount;
        if let Some(s) = self.slots.iter_mut().find(|s| s.0 == key) {
            s.1 += amount;
            return;
        }
        if self.slots.len() < 8 {
            self.slots.push((key, amount, 0));
            return;
        }
        // Replace the minimum, inheriting its count as error bound.
        let min = self.slots.iter_mut().min_by_key(|s| s.1).expect("8 slots");
        *min = (key, min.1 + amount, min.1);
    }

    /// The heaviest key and its guaranteed-minimum share of the total.
    pub fn top_share(&self) -> Option<([u8; 16], f64)> {
        if self.total == 0 {
            return None;
        }
        self.slots
            .iter()
            .max_by_key(|s| s.1)
            .map(|s| (s.0, (s.1.saturating_sub(s.2)) as f64 / self.total as f64))
    }

    /// Distinct keys with load above `frac` of the total (guaranteed
    /// counts) — "at least two keys with meaningful load".
    pub fn keys_above(&self, frac: f64) -> usize {
        if self.total == 0 {
            return 0;
        }
        let floor = (self.total as f64 * frac) as u64;
        self.slots
            .iter()
            .filter(|s| s.1.saturating_sub(s.2) > floor)
            .count()
    }
}

/// 64-register HyperLogLog: distinct routing keys, ±~16% — plenty for
/// "is this segment one key or many".
#[derive(Debug, Clone)]
pub struct Hll64 {
    regs: [u8; 64],
}

impl Default for Hll64 {
    fn default() -> Self {
        Hll64 { regs: [0; 64] }
    }
}

impl Hll64 {
    /// Register-max merge of two windows.
    pub fn merged(&self, other: &Hll64) -> Hll64 {
        let mut out = Hll64::default();
        for i in 0..64 {
            out.regs[i] = self.regs[i].max(other.regs[i]);
        }
        out
    }

    pub fn add(&mut self, key_hash: &[u8; 16]) {
        let h = u64::from_le_bytes(key_hash[8..16].try_into().expect("hll half"));
        let idx = (h & 0x3f) as usize;
        let rest = h >> 6;
        let rank = (rest.trailing_zeros() + 1).min(58) as u8;
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }

    pub fn estimate(&self) -> f64 {
        let m = 64.0f64;
        let sum: f64 = self.regs.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = 0.709 * m * m / sum;
        if raw <= 2.5 * m {
            let zeros = self.regs.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                return m * (m / zeros as f64).ln();
            }
        }
        raw
    }
}

/// Per-segment routing-key distribution (spec §5.1).
pub struct KeyDistribution {
    /// Load by key-point subrange of THIS segment's [lo, hi): 64 equal
    /// bins, EWMA'd so the split point reflects RECENT load.
    pub bins: Vec<Ewma>,
    /// Heavy hitters and distinct counts rotate on the rate window
    /// (review finding 8: cumulative-forever tables let hours-old
    /// dominance steer present split decisions). Queries merge the
    /// (current, previous) generations, so the memory horizon is one
    /// to two windows.
    pub top_keys: SpaceSaving8,
    prev_top_keys: SpaceSaving8,
    pub distinct: Hll64,
    prev_distinct: Hll64,
    window_ms: i64,
    window_started_ms: i64,
    pub bytes: Ewma,
    pub reqs: Ewma,
    pub recs: Ewma,
    pub lo: u64,
    pub hi: u64,
}

impl KeyDistribution {
    pub fn new(lo: u64, hi: u64, window_secs: f64) -> KeyDistribution {
        KeyDistribution {
            bins: vec![Ewma::new(window_secs); 64],
            top_keys: SpaceSaving8::default(),
            prev_top_keys: SpaceSaving8::default(),
            distinct: Hll64::default(),
            prev_distinct: Hll64::default(),
            window_ms: (window_secs.max(1.0) * 1000.0) as i64,
            window_started_ms: 0,
            bytes: Ewma::new(window_secs),
            reqs: Ewma::new(window_secs),
            recs: Ewma::new(window_secs),
            lo,
            hi,
        }
    }

    /// Rotate the generational sketches when the rate window elapses.
    fn maybe_rotate(&mut self, now_ms: i64) {
        if self.window_started_ms == 0 {
            self.window_started_ms = now_ms.max(1);
            return;
        }
        if now_ms - self.window_started_ms >= self.window_ms {
            self.prev_top_keys = std::mem::take(&mut self.top_keys);
            self.prev_distinct = std::mem::take(&mut self.distinct);
            self.window_started_ms = now_ms;
        }
    }

    /// Heavy-hitter view over the current + previous window.
    pub fn top_keys_windowed(&self) -> SpaceSaving8 {
        self.top_keys.merged(&self.prev_top_keys)
    }

    /// Distinct estimate over the current + previous window.
    pub fn distinct_windowed(&self) -> f64 {
        self.distinct.merged(&self.prev_distinct).estimate()
    }

    pub fn bin_of(&self, point: u64) -> usize {
        let span = self.hi.saturating_sub(self.lo).max(1);
        let rel = point.saturating_sub(self.lo).min(span - 1);
        ((rel as u128 * 64u128) / span as u128) as usize
    }

    pub fn note(&mut self, now_ms: i64, point: u64, key_hash: [u8; 16], bytes: u64, records: u64) {
        self.maybe_rotate(now_ms);
        let b = self.bin_of(point);
        self.bins[b].add(now_ms, bytes as f64);
        self.top_keys.add(key_hash, bytes.max(1));
        self.distinct.add(&key_hash);
        self.bytes.add(now_ms, bytes as f64);
        self.reqs.add(now_ms, 1.0);
        self.recs.add(now_ms, records as f64);
    }

    /// Load-weighted median split point (spec §5.2): the key-point at
    /// the bin boundary where cumulative recent load crosses half —
    /// never the segment's own bounds. Returns (split_point,
    /// left_fraction) or None when no interior boundary divides load.
    pub fn weighted_median(&self, now_ms: i64) -> Option<(u64, f64)> {
        let loads: Vec<f64> = self.bins.iter().map(|e| e.value(now_ms)).collect();
        let total: f64 = loads.iter().sum();
        if total <= 0.0 {
            return None;
        }
        let mut acc = 0.0;
        for (i, l) in loads.iter().enumerate() {
            acc += l;
            if acc >= total / 2.0 {
                if i + 1 >= 64 {
                    return None; // all load in the last bin
                }
                let span = self.hi.saturating_sub(self.lo).max(1) as u128;
                let point = self.lo + ((span * (i as u128 + 1)) / 64) as u64;
                if point <= self.lo || point >= self.hi {
                    return None;
                }
                return Some((point, acc / total));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_decays_and_accumulates() {
        let mut e = Ewma::new(120.0);
        e.add(1_000, 1200.0);
        let v0 = e.value(1_000);
        assert!(v0 > 9.0 && v0 < 11.0, "impulse ≈ amount/window: {v0}");
        let v = e.value(121_000);
        assert!(
            v < v0 * 0.5 && v > v0 * 0.2,
            "one window decays to ~1/e: {v}"
        );
    }

    #[test]
    fn spacesaving_finds_the_dominant_key() {
        let mut s = SpaceSaving8::default();
        let hot = [1u8; 16];
        for i in 0..1000u64 {
            s.add(hot, 10);
            let mut other = [0u8; 16];
            other[..8].copy_from_slice(&i.to_le_bytes());
            s.add(other, 1);
        }
        let (k, share) = s.top_share().unwrap();
        assert_eq!(k, hot);
        assert!(share > 0.8, "dominant share underestimated: {share}");
        assert_eq!(s.keys_above(0.5), 1);
    }

    #[test]
    fn hll_estimates_within_reason() {
        let mut h = Hll64::default();
        for i in 0..5000u64 {
            let mut k = [0u8; 16];
            k[..8].copy_from_slice(&i.to_le_bytes());
            k[8..16].copy_from_slice(&crate::crypto::stream_hash(&format!("k{i}"))[..8]);
            h.add(&k);
        }
        let est = h.estimate();
        assert!(
            est > 2500.0 && est < 10000.0,
            "5000 distinct estimated as {est}"
        );
        let mut one = Hll64::default();
        let k = crate::crypto::stream_hash("only");
        for _ in 0..1000 {
            one.add(&k);
        }
        assert!(one.estimate() < 3.0, "single key must estimate ~1");
    }

    /// Review finding 8: hours-old dominance must not steer present
    /// decisions — the heavy-hitter and distinct views rotate with the
    /// rate window (memory horizon: one to two windows).
    #[test]
    fn stale_dominance_decays_across_windows() {
        let win = 10.0; // seconds
        let mut d = KeyDistribution::new(0, u64::MAX, win);
        let a = crate::crypto::stream_hash("old-dominant");
        let b = crate::crypto::stream_hash("new-traffic");
        for _ in 0..1000 {
            d.note(1_000, 10, a, 1_000, 1);
        }
        let top0 = d.top_keys_windowed();
        assert_eq!(top0.top_share().unwrap().0, a);
        assert!(top0.top_share().unwrap().1 > 0.9);

        // Two full windows later, only B traffic: A must be out of BOTH
        // generations.
        for t in [12_000i64, 23_000] {
            for _ in 0..50 {
                d.note(t, 20, b, 100, 1);
            }
        }
        let top = d.top_keys_windowed();
        let (k, share) = top.top_share().unwrap();
        assert_eq!(k, b, "stale dominant key must age out");
        assert!(share > 0.9, "recent traffic owns the window: {share}");

        // Distinct view rotates too: thousands of old keys must not pin
        // the estimate high forever.
        let mut d2 = KeyDistribution::new(0, u64::MAX, win);
        for i in 0..5_000u64 {
            d2.note(
                1_000,
                i,
                crate::crypto::stream_hash(&format!("k{i}")),
                10,
                1,
            );
        }
        assert!(d2.distinct_windowed() > 100.0);
        for t in [12_000i64, 23_000] {
            for _ in 0..20 {
                d2.note(t, 5, crate::crypto::stream_hash("solo"), 10, 1);
            }
        }
        assert!(
            d2.distinct_windowed() < 8.0,
            "distinct estimate must follow the window: {}",
            d2.distinct_windowed()
        );
    }

    #[test]
    fn weighted_median_tracks_load_not_midpoint() {
        let mut d = KeyDistribution::new(0, 6400, 120.0);
        // All load in the last quarter of the keyspace.
        for p in [5000u64, 5200, 5400, 5600, 5800] {
            d.note(1000, p, crate::crypto::stream_hash(&p.to_string()), 1000, 1);
        }
        let (split, left) = d.weighted_median(1000).unwrap();
        assert!(
            split > 3200,
            "median must sit inside the loaded region, not the midpoint: {split}"
        );
        assert!(left >= 0.5);
        // One-bin load: no useful interior boundary … unless it IS interior.
        let mut one = KeyDistribution::new(0, 6400, 120.0);
        for _ in 0..10 {
            one.note(1000, 6399, crate::crypto::stream_hash("z"), 100, 1);
        }
        assert!(
            one.weighted_median(1000).is_none(),
            "all-last-bin load cannot split"
        );
    }
}
