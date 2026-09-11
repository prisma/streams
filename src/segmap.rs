//! Stream segment map (SCALING.md §1-2): a CAS-versioned total partition
//! of the routing-key hash space [0,1) — represented in u64 fixed point —
//! into segments, each pinned to one shard. Splits seal a parent and open
//! children over its subranges; merges seal two adjacent parents into one
//! child. No data ever moves: transitions only redirect FUTURE writes.
//!
//! Persistence belongs to the tenant-qualified stream descriptor in `registry`;
//! this module owns the segment-map value and its transition rules.

use serde::{Deserialize, Serialize};

pub(crate) const KEYSPACE_END: u64 = u64::MAX; // ranges are [lo, hi) over u64

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct SegmentDesc {
    pub seg_id: u32,
    /// Inclusive lower bound of the hashed-key range.
    pub lo: u64,
    /// Exclusive upper bound (KEYSPACE_END means "to the top", inclusive).
    pub hi: u64,
    /// Shard log prefix this segment's appends land on.
    /// LEGACY placement field (child-stream scaler); v3 uses route_hash.
    pub shard_prefix: String,
    /// ROUTING-V3 §5.5: persisted shard-routing hash for this segment —
    /// controls placement AND the history-v2 route-first keyspace
    /// prefix. Zeros = the parent stream's default route.
    #[serde(default)]
    pub route_hash: [u8; 16],
    pub created_ms: i64,
    /// Parent segment ids (1 for a split child, 2 for a merge child).
    #[serde(default)]
    pub predecessors: Vec<u32>,
    /// Live successors of THIS segment once sealed (spec Stage 3
    /// §4.2: persisted explicitly, not derived by range intersection).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub successors: Vec<u32>,
    /// Set when sealed: no further appends; readers drain to
    /// `sealed_next_offset` then follow successors.
    #[serde(default)]
    pub sealed_ms: Option<i64>,
    /// The segment-local next offset frozen at seal time (readers prove
    /// drain completion against this).
    #[serde(default)]
    pub sealed_next_offset: Option<u64>,
}

impl SegmentDesc {
    pub(crate) fn contains(&self, k: u64) -> bool {
        k >= self.lo && (k < self.hi || (self.hi == KEYSPACE_END && k == KEYSPACE_END))
    }
    pub(crate) fn is_live(&self) -> bool {
        self.sealed_ms.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct SegmentMap {
    /// Monotonic map version; CAS target.
    pub version: u64,
    /// Next seg_id to allocate.
    pub next_seg_id: u32,
    /// All segments ever (sealed ones retained for lineage until their
    /// shard data is fully absorbed + GC'd, then pruned).
    pub segments: Vec<SegmentDesc>,
    /// ROUTING-V3 two-phase transition intent: CAS'd into the map
    /// BEFORE the parents seal, so a crash between seal and successor
    /// publication is resumable and idempotent (spec §5.3) — the split
    /// point is persisted here, the frozen offsets are re-read from the
    /// sealed identities' tails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<PendingTransition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct PendingTransition {
    /// "split" | "merge"
    pub kind: String,
    /// Parent segment ids (1 for split, 2 for merge).
    pub segs: Vec<u32>,
    /// Split point (splits only).
    #[serde(default)]
    pub split_at: u64,
    pub started_ms: i64,
    /// Seal-claim generation allocated for this transition's parent
    /// closes (from the descriptor's monotonic counter). Transitions
    /// and seal claims are mutually exclusive, so the allocation can
    /// never undercut a live claim — it exists so a parent close is
    /// always above any fence left behind by an aborted takeover.
    #[serde(default)]
    pub seal_gen: u64,
}

#[derive(Debug, PartialEq)]
pub(crate) enum MapError {
    NotFound(u32),
    AlreadySealed(u32),
    NotAdjacent(u32, u32),
    NotSealed(u32),
    SingleSegmentStream,
    InvalidSplitPoint,
    IdExhausted,
}

impl SegmentMap {
    /// A fresh single-segment map covering the whole keyspace.
    pub(crate) fn initial(shard_prefix: &str, now_ms: i64) -> SegmentMap {
        SegmentMap {
            version: 1,
            next_seg_id: 1,
            pending: None,
            segments: vec![SegmentDesc {
                seg_id: 0,
                lo: 0,
                hi: KEYSPACE_END,
                shard_prefix: shard_prefix.to_string(),
                route_hash: [0u8; 16],
                created_ms: now_ms,
                predecessors: vec![],
                successors: Vec::new(),
                sealed_ms: None,
                sealed_next_offset: None,
            }],
        }
    }

    pub(crate) fn live(&self) -> impl Iterator<Item = &SegmentDesc> {
        self.segments.iter().filter(|s| s.is_live())
    }

    /// The live segment owning hashed key `k`. The partition invariant
    /// guarantees exactly one.
    pub fn route(&self, k: u64) -> Option<&SegmentDesc> {
        self.live().find(|s| s.contains(k))
    }

    pub(crate) fn get(&self, seg_id: u32) -> Option<&SegmentDesc> {
        self.segments.iter().find(|s| s.seg_id == seg_id)
    }

    /// Successors of a sealed segment: the PERSISTED list (written
    /// atomically with the seal at Phase B — spec Stage 3 §4.2), with
    /// range-intersection derivation only as a defensive fallback.
    pub fn successors(&self, seg_id: u32) -> Vec<&SegmentDesc> {
        let Some(sealed) = self.get(seg_id) else {
            return vec![];
        };
        if !sealed.successors.is_empty() {
            return sealed
                .successors
                .iter()
                .filter_map(|id| self.get(*id))
                .collect();
        }
        self.live()
            .filter(|s| s.lo < sealed.hi && sealed.lo < s.hi)
            .collect()
    }

    /// Validate persisted topology, including sealed predecessor coverage and
    /// partially closed transitions. Requiring every segment to be live would
    /// reject legitimate recovery states; terminal leaves must cover keyspace.
    pub fn validate(&self) -> Result<(), String> {
        use std::collections::HashSet;
        if self.segments.is_empty() {
            return Err("explicit map is empty".into());
        }
        let mut ids = HashSet::new();
        for segment in &self.segments {
            if !ids.insert(segment.seg_id) {
                return Err(format!("duplicate segment {}", segment.seg_id));
            }
            if segment.seg_id >= self.next_seg_id {
                return Err(format!("segment {} exceeds allocator", segment.seg_id));
            }
            if segment.lo >= segment.hi {
                return Err(format!(
                    "segment {} has empty/reversed range",
                    segment.seg_id
                ));
            }
            if segment.sealed_ms.is_some() != segment.sealed_next_offset.is_some() {
                return Err(format!(
                    "segment {} has incomplete seal metadata",
                    segment.seg_id
                ));
            }
            if segment.is_live() && !segment.successors.is_empty() {
                return Err(format!("live segment {} has successors", segment.seg_id));
            }
        }
        for segment in &self.segments {
            for (references, successor) in
                [(&segment.predecessors, false), (&segment.successors, true)]
            {
                let mut seen = HashSet::new();
                for id in references {
                    if !seen.insert(*id) || *id == segment.seg_id {
                        return Err(format!(
                            "segment {} has duplicate/self reference",
                            segment.seg_id
                        ));
                    }
                    let Some(other) = self.get(*id) else {
                        // Absorbed predecessors may have been pruned. Their
                        // allocated IDs remain historical references; a missing
                        // successor would instead lose future routing authority.
                        if !successor && *id < segment.seg_id {
                            continue;
                        }
                        return Err(format!(
                            "segment {} references missing segment {id}",
                            segment.seg_id
                        ));
                    };
                    if (successor && *id <= segment.seg_id) || (!successor && *id >= segment.seg_id)
                    {
                        return Err(format!(
                            "segment {} has cyclic/reversed lineage",
                            segment.seg_id
                        ));
                    }
                    if other.lo >= segment.hi || segment.lo >= other.hi {
                        return Err("lineage ranges do not overlap".into());
                    }
                    if successor && !other.predecessors.contains(&segment.seg_id) {
                        return Err("successor does not reference parent".into());
                    }
                    if !successor && other.is_live() {
                        return Err("predecessor remains live".into());
                    }
                }
            }
        }
        let mut leaves: Vec<_> = self
            .segments
            .iter()
            .filter(|segment| segment.successors.is_empty())
            .map(|segment| (segment.lo, segment.hi))
            .collect();
        leaves.sort_unstable();
        if leaves.first().map(|range| range.0) != Some(0)
            || leaves.last().map(|range| range.1) != Some(KEYSPACE_END)
            || !leaves.windows(2).all(|ranges| ranges[0].1 == ranges[1].0)
        {
            return Err("terminal segments do not exactly cover keyspace".into());
        }
        if let Some(pending) = &self.pending {
            let required = match pending.kind.as_str() {
                "split" => 1,
                "merge" => 2,
                _ => return Err("unknown transition kind".into()),
            };
            if pending.segs.len() != required {
                return Err("transition parent count is invalid".into());
            }
            if pending.segs.iter().collect::<HashSet<_>>().len() != required {
                return Err("transition repeats a parent".into());
            }
            for id in &pending.segs {
                if self.get(*id).is_none() {
                    return Err("transition parent is missing".into());
                }
            }
            if pending.kind == "split" {
                let parent = self.get(pending.segs[0]).unwrap();
                if pending.split_at <= parent.lo || pending.split_at >= parent.hi {
                    return Err("split point is outside parent".into());
                }
            }
            if pending.kind == "merge" {
                let a = self.get(pending.segs[0]).unwrap();
                let b = self.get(pending.segs[1]).unwrap();
                if a.hi != b.lo && b.hi != a.lo {
                    return Err("merge parents are not adjacent".into());
                }
            }
        }
        Ok(())
    }

    /// Partition invariant: live segments exactly tile [0, KEYSPACE_END).
    pub(crate) fn check_partition(&self) -> bool {
        let mut ranges: Vec<(u64, u64)> = self.live().map(|s| (s.lo, s.hi)).collect();
        ranges.sort_unstable();
        if ranges.is_empty() {
            return false;
        }
        if ranges[0].0 != 0 || ranges.last().unwrap().1 != KEYSPACE_END {
            return false;
        }
        ranges.windows(2).all(|w| w[0].1 == w[1].0)
    }

    /// Split `seg_id` at `split_at` (exclusive upper of the low child).
    /// Seals the parent (sealed_next_offset recorded by the caller once
    /// the shard-side Sealed op commits; passed here for atomicity of the
    /// map transition) and opens two children on PERSISTED, independent
    /// shard routes — a split adds physical capacity, not just lineage
    /// (review blocker 1). The caller picks the routes: the low child
    /// conventionally inherits the parent's route (its predecessor data
    /// is already local), the high child moves.
    pub fn split(
        &mut self,
        seg_id: u32,
        split_at: u64,
        sealed_next_offset: u64,
        low_route: [u8; 16],
        high_route: [u8; 16],
        now_ms: i64,
    ) -> Result<(u32, u32), MapError> {
        let a = self.next_seg_id;
        let b = a.checked_add(1).ok_or(MapError::IdExhausted)?;
        let next = b.checked_add(1).ok_or(MapError::IdExhausted)?;
        let parent = self
            .segments
            .iter_mut()
            .find(|s| s.seg_id == seg_id)
            .ok_or(MapError::NotFound(seg_id))?;
        if !parent.is_live() {
            return Err(MapError::AlreadySealed(seg_id));
        }
        let (lo, hi) = (parent.lo, parent.hi);
        if split_at <= lo || split_at >= hi {
            return Err(MapError::InvalidSplitPoint);
        }
        parent.sealed_ms = Some(now_ms);
        parent.sealed_next_offset = Some(sealed_next_offset);
        parent.successors = vec![a, b];
        self.next_seg_id = next;
        self.segments.push(SegmentDesc {
            seg_id: a,
            lo,
            hi: split_at,
            shard_prefix: String::new(),
            route_hash: low_route,
            created_ms: now_ms,
            predecessors: vec![seg_id],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        self.segments.push(SegmentDesc {
            seg_id: b,
            lo: split_at,
            hi,
            shard_prefix: String::new(),
            route_hash: high_route,
            created_ms: now_ms,
            predecessors: vec![seg_id],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        self.version += 1;
        debug_assert!(self.check_partition());
        Ok((a, b))
    }

    /// Merge two ADJACENT live segments into one child on a PERSISTED
    /// route (same discipline as split — review blocker 1).
    pub fn merge(
        &mut self,
        a_id: u32,
        b_id: u32,
        a_sealed_next: u64,
        b_sealed_next: u64,
        child_route: [u8; 16],
        now_ms: i64,
    ) -> Result<u32, MapError> {
        let (a_lo, a_hi) = {
            let a = self.get(a_id).ok_or(MapError::NotFound(a_id))?;
            if !a.is_live() {
                return Err(MapError::AlreadySealed(a_id));
            }
            (a.lo, a.hi)
        };
        let (b_lo, b_hi) = {
            let b = self.get(b_id).ok_or(MapError::NotFound(b_id))?;
            if !b.is_live() {
                return Err(MapError::AlreadySealed(b_id));
            }
            (b.lo, b.hi)
        };
        let (lo, hi) = if a_hi == b_lo {
            (a_lo, b_hi)
        } else if b_hi == a_lo {
            (b_lo, a_hi)
        } else {
            return Err(MapError::NotAdjacent(a_id, b_id));
        };
        let c = self.next_seg_id;
        for (id, next) in [(a_id, a_sealed_next), (b_id, b_sealed_next)] {
            let s = self.segments.iter_mut().find(|s| s.seg_id == id).unwrap();
            s.sealed_ms = Some(now_ms);
            s.sealed_next_offset = Some(next);
            s.successors = vec![c];
        }
        self.next_seg_id += 1;
        self.segments.push(SegmentDesc {
            seg_id: c,
            lo,
            hi,
            shard_prefix: String::new(),
            route_hash: child_route,
            created_ms: now_ms,
            predecessors: vec![a_id, b_id],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        self.version += 1;
        debug_assert!(self.check_partition());
        Ok(c)
    }

    /// Drop sealed segments whose shard data is fully drained (caller
    /// verifies absorption/GC) AND which no live segment lists as a
    /// predecessor-of-predecessor chain readers might still walk. We keep
    /// it simple: prune only sealed segments none of whose range-successors
    /// are themselves sealed (lineage depth 1 retained).
    pub fn prune(&mut self, drained: &[u32]) {
        self.segments
            .retain(|s| s.is_live() || !drained.contains(&s.seg_id));
        self.version += 1;
    }
}

/// Hash a routing key into the segment keyspace: first 8 bytes of the
/// stream_hash SHA-256 construction over the routing key — the same
/// construction touch_keys uses, so the whole codebase derives 64-bit
/// ids one way.
pub fn key_point(routing_key: &str) -> u64 {
    let h = crate::crypto::stream_hash(routing_key);
    u64::from_be_bytes(h[..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_routes_everything() {
        let m = SegmentMap::initial("root", 1);
        assert!(m.check_partition());
        assert_eq!(m.route(0).unwrap().seg_id, 0);
        assert_eq!(m.route(u64::MAX / 2).unwrap().seg_id, 0);
        assert_eq!(m.route(KEYSPACE_END).unwrap().seg_id, 0);
    }

    #[test]
    fn split_then_route_then_successors() {
        let mut m = SegmentMap::initial("root", 1);
        let mid = KEYSPACE_END / 2;
        let (a, b) = m.split(0, mid, 4242, [1u8; 16], [2u8; 16], 2).unwrap();
        assert!(m.check_partition());
        assert_eq!(m.route(mid - 1).unwrap().seg_id, a);
        assert_eq!(m.route(mid).unwrap().seg_id, b);
        let parent = m.get(0).unwrap();
        assert_eq!(parent.sealed_next_offset, Some(4242));
        let succ: Vec<u32> = m.successors(0).iter().map(|s| s.seg_id).collect();
        assert_eq!(succ.len(), 2);
        assert!(succ.contains(&a) && succ.contains(&b));
        // double split of sealed parent rejected
        assert_eq!(
            m.split(0, mid / 2, 0, [3u8; 16], [4u8; 16], 3).unwrap_err(),
            MapError::AlreadySealed(0)
        );
    }

    #[test]
    fn recursive_splits_keep_partition() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, _b) = m
            .split(0, KEYSPACE_END / 2, 0, [1u8; 16], [2u8; 16], 2)
            .unwrap();
        let (c, d) = m
            .split(a, KEYSPACE_END / 4, 0, [3u8; 16], [4u8; 16], 3)
            .unwrap();
        assert!(m.check_partition());
        assert_eq!(m.live().count(), 3);
        assert_eq!(m.route(1).unwrap().seg_id, c);
        assert_eq!(m.route(KEYSPACE_END / 4).unwrap().seg_id, d);
    }

    #[test]
    fn merge_adjacent_only() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, b) = m
            .split(0, KEYSPACE_END / 2, 0, [1u8; 16], [2u8; 16], 2)
            .unwrap();
        let (c, d) = m
            .split(a, KEYSPACE_END / 4, 0, [3u8; 16], [4u8; 16], 3)
            .unwrap();
        // c=[0,q) d=[q,mid) b=[mid,end) — c+b not adjacent
        assert_eq!(
            m.merge(c, b, 0, 0, [9u8; 16], 4).unwrap_err(),
            MapError::NotAdjacent(c, b)
        );
        let e = m.merge(d, b, 1, 2, [9u8; 16], 5).unwrap();
        assert!(m.check_partition());
        assert_eq!(m.live().count(), 2);
        assert_eq!(m.route(KEYSPACE_END / 2).unwrap().seg_id, e);
        let succ_d: Vec<u32> = m.successors(d).iter().map(|s| s.seg_id).collect();
        assert_eq!(succ_d, vec![e]);
    }

    #[test]
    fn serde_round_trip() {
        let mut m = SegmentMap::initial("root", 1);
        m.split(0, KEYSPACE_END / 2, 7, [1u8; 16], [2u8; 16], 2)
            .unwrap();
        let j = serde_json::to_string(&m).unwrap();
        let back: SegmentMap = serde_json::from_str(&j).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn key_point_is_stable_and_spread() {
        let a = key_point("user-1");
        let b = key_point("user-2");
        assert_ne!(a, b);
        assert_eq!(a, key_point("user-1"));
    }
}
