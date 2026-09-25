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

impl SegmentDesc {
    /// Each edge must preserve allocation order, overlap and seal authority.
    fn validate_lineage(&self, map: &SegmentMap) -> Result<(), TopologyError> {
        let parents = self.predecessors.iter().map(|id| (id, false));
        let children = self.successors.iter().map(|id| (id, true));
        // At most two parents and two children: a scan, not a hashed set.
        let mut seen = Vec::new();
        for (id, successor) in parents.chain(children) {
            if seen.contains(&(successor, *id)) || *id == self.seg_id {
                return Err(TopologyError::RepeatedReference(self.seg_id));
            }
            seen.push((successor, *id));
            // Allocation order: a successor is newer and a predecessor older.
            let reversed = if successor {
                *id <= self.seg_id
            } else {
                *id >= self.seg_id
            };
            // Missing historical predecessors may already have been absorbed.
            // A missing successor would lose future routing authority.
            let other = match map.get(*id) {
                Some(other) => other,
                None if !successor && !reversed => continue,
                None => return Err(TopologyError::MissingReference(self.seg_id, *id)),
            };
            if reversed {
                return Err(TopologyError::ReversedLineage(self.seg_id));
            }
            if other.lo >= self.hi || self.lo >= other.hi {
                return Err(TopologyError::DisjointLineage);
            }
            if successor && !other.predecessors.contains(&self.seg_id) {
                return Err(TopologyError::MissingBacklink);
            }
            if !successor && other.is_live() {
                return Err(TopologyError::LivePredecessor);
            }
        }
        Ok(())
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

impl PendingTransition {
    fn validate(&self, map: &SegmentMap) -> Result<(), TopologyError> {
        let required = match self.kind.as_str() {
            "split" => 1,
            "merge" => 2,
            _ => return Err(TopologyError::UnknownTransition),
        };
        if self.segs.len() != required {
            return Err(TopologyError::TransitionParentCount);
        }
        if matches!(self.segs.as_slice(), [a, b] if a == b) {
            return Err(TopologyError::RepeatedTransitionParent);
        }
        let parents = self
            .segs
            .iter()
            .map(|id| map.get(*id).ok_or(TopologyError::MissingTransitionParent))
            .collect::<Result<Vec<_>, _>>()?;
        match parents.as_slice() {
            [parent] if self.split_at <= parent.lo || self.split_at >= parent.hi => {
                Err(TopologyError::SplitOutsideParent)
            }
            [a, b] if a.hi != b.lo && b.hi != a.lo => Err(TopologyError::MergeNotAdjacent),
            _ => Ok(()),
        }
    }
}

/// Whether the `[lo, hi)` ranges tile the key space: sorted, the first starts
/// at 0, each next one starts where the previous ends, and the last ends at
/// `KEYSPACE_END` (so, by `SegmentDesc::contains`, holds it too). With every
/// range nonempty, each key then lies in exactly one (KANI-028).
fn tiles_keyspace(ranges: &mut [(u64, u64)]) -> bool {
    ranges.sort_unstable();
    ranges.first().map(|range| range.0) == Some(0)
        && ranges.last().map(|range| range.1) == Some(KEYSPACE_END)
        && ranges.windows(2).all(|pair| pair[0].1 == pair[1].0)
}

/// Why a persisted topology is refused. Typed rather than formatted where it
/// is found, so validation allocates nothing and the KANI-028 proof need
/// not model formatting; `Display` gives the registry's message.
#[derive(Debug, PartialEq)]
pub(crate) enum TopologyError {
    Empty,
    DuplicateSegment(u32),
    BeyondAllocator(u32),
    EmptyRange(u32),
    IncompleteSeal(u32),
    LiveWithSuccessors(u32),
    RepeatedReference(u32),
    /// The segment and the id it references.
    MissingReference(u32, u32),
    ReversedLineage(u32),
    DisjointLineage,
    MissingBacklink,
    LivePredecessor,
    Coverage,
    UnknownTransition,
    TransitionParentCount,
    RepeatedTransitionParent,
    MissingTransitionParent,
    SplitOutsideParent,
    MergeNotAdjacent,
}

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("explicit map is empty"),
            Self::DuplicateSegment(id) => write!(f, "duplicate segment {id}"),
            Self::BeyondAllocator(id) => write!(f, "segment {id} exceeds allocator"),
            Self::EmptyRange(id) => write!(f, "segment {id} has empty/reversed range"),
            Self::IncompleteSeal(id) => write!(f, "segment {id} has incomplete seal metadata"),
            Self::LiveWithSuccessors(id) => write!(f, "live segment {id} has successors"),
            Self::RepeatedReference(id) => write!(f, "segment {id} has duplicate/self reference"),
            Self::MissingReference(id, other) => {
                write!(f, "segment {id} references missing segment {other}")
            }
            Self::ReversedLineage(id) => write!(f, "segment {id} has cyclic/reversed lineage"),
            Self::DisjointLineage => f.write_str("lineage ranges do not overlap"),
            Self::MissingBacklink => f.write_str("successor does not reference parent"),
            Self::LivePredecessor => f.write_str("predecessor remains live"),
            Self::Coverage => f.write_str("terminal segments do not exactly cover keyspace"),
            Self::UnknownTransition => f.write_str("unknown transition kind"),
            Self::TransitionParentCount => f.write_str("transition parent count is invalid"),
            Self::RepeatedTransitionParent => f.write_str("transition repeats a parent"),
            Self::MissingTransitionParent => f.write_str("transition parent is missing"),
            Self::SplitOutsideParent => f.write_str("split point is outside parent"),
            Self::MergeNotAdjacent => f.write_str("merge parents are not adjacent"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum MapError {
    NotFound(u32),
    AlreadySealed(u32),
    NotAdjacent(u32, u32),
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

    /// The segment hashed key `k` routes to: the live segment containing it
    /// (a validated map has at most one), or, with no live cover
    /// (mid-transition: a seal published before its successors, or a scaler
    /// that died between the two), the NEWEST sealed cover — the deepest
    /// lineage point, the one whose successor the refresh will reveal —
    /// never a long-superseded ancestor. A validated map's terminal segments
    /// cover every key, `KEYSPACE_END` included, so it always answers
    /// (KANI-028).
    pub(crate) fn route(&self, k: u64) -> Option<&SegmentDesc> {
        self.live().find(|s| s.contains(k)).or_else(|| {
            self.segments
                .iter()
                .filter(|s| s.contains(k))
                .max_by_key(|s| (s.created_ms, s.seg_id))
        })
    }

    pub(crate) fn get(&self, seg_id: u32) -> Option<&SegmentDesc> {
        self.segments.iter().find(|s| s.seg_id == seg_id)
    }

    /// Validate persisted topology, including sealed predecessor coverage and
    /// partially closed transitions. Requiring every segment to be live would
    /// reject legitimate recovery states; terminal leaves must cover keyspace.
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if self.segments.is_empty() {
            return Err(TopologyError::Empty);
        }
        // A linear scan, like each `get` the lineage check makes: a map holds
        // tens of segments, and a hashed set seeds from the OS random source,
        // which the KANI-028 proof cannot model.
        for (i, segment) in self.segments.iter().enumerate() {
            if self
                .segments
                .iter()
                .take(i)
                .any(|earlier| earlier.seg_id == segment.seg_id)
            {
                return Err(TopologyError::DuplicateSegment(segment.seg_id));
            }
            if segment.seg_id >= self.next_seg_id {
                return Err(TopologyError::BeyondAllocator(segment.seg_id));
            }
            if segment.lo >= segment.hi {
                return Err(TopologyError::EmptyRange(segment.seg_id));
            }
            if segment.sealed_ms.is_some() != segment.sealed_next_offset.is_some() {
                return Err(TopologyError::IncompleteSeal(segment.seg_id));
            }
            if segment.is_live() && !segment.successors.is_empty() {
                return Err(TopologyError::LiveWithSuccessors(segment.seg_id));
            }
        }
        for segment in &self.segments {
            segment.validate_lineage(self)?;
        }
        let mut leaves: Vec<_> = self
            .segments
            .iter()
            .filter(|segment| segment.successors.is_empty())
            .map(|segment| (segment.lo, segment.hi))
            .collect();
        if !tiles_keyspace(&mut leaves) {
            return Err(TopologyError::Coverage);
        }
        if let Some(pending) = &self.pending {
            pending.validate(self)?;
        }
        Ok(())
    }

    /// Partition invariant: live segments exactly tile [0, KEYSPACE_END).
    pub(crate) fn check_partition(&self) -> bool {
        let mut ranges: Vec<(u64, u64)> = self.live().map(|s| (s.lo, s.hi)).collect();
        tiles_keyspace(&mut ranges)
    }

    /// Split `seg_id` at `split_at` (exclusive upper of the low child).
    /// Seals the parent (sealed_next_offset recorded by the caller once
    /// the shard-side Sealed op commits; passed here for atomicity of the
    /// map transition) and opens two children on PERSISTED, independent
    /// shard routes — a split adds physical capacity, not just lineage
    /// (review blocker 1). The caller picks the routes: the low child
    /// conventionally inherits the parent's route (its predecessor data
    /// is already local), the high child moves.
    #[expect(
        clippy::too_many_arguments,
        reason = "SegmentMap::split; a split names the segment, the boundary, the new segments' ids, epochs and owners and the transition version separately as the rebalancer decided them; a request struct would exist for this single call site"
    )]
    pub(crate) fn split(
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
    #[expect(
        clippy::too_many_arguments,
        reason = "SegmentMap::merge; phase B supplies both parents, each parent's frozen next offset, the child's route and the clock as separate facts it proved; a request struct would exist for this single call site"
    )]
    #[expect(
        clippy::unwrap_used,
        reason = "SegmentMap::merge; both parents were found live and adjacent and the child id allocated before either seals, so the lookup finds them; a fallible find would add a branch no validated merge reaches"
    )]
    pub(crate) fn merge(
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
        // Allocate before either parent seals: a refused merge leaves the
        // map exactly as phase B read it, so its intent can stay pending.
        let next_seg_id = c.checked_add(1).ok_or(MapError::IdExhausted)?;
        for (id, next) in [(a_id, a_sealed_next), (b_id, b_sealed_next)] {
            let s = self.segments.iter_mut().find(|s| s.seg_id == id).unwrap();
            s.sealed_ms = Some(now_ms);
            s.sealed_next_offset = Some(next);
            s.successors = vec![c];
        }
        self.next_seg_id = next_seg_id;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_transition_validation_preserves_shape_and_boundary_errors() {
        let mut map = SegmentMap::initial("root", 1);
        let mid = KEYSPACE_END / 2;
        let (a, b) = map.split(0, mid, 7, [1; 16], [2; 16], 2).unwrap();
        let (c, d) = map.split(a, mid / 2, 8, [1; 16], [3; 16], 3).unwrap();
        let cases = [
            ("unknown", vec![], 0, Some("unknown transition kind")),
            (
                "split",
                vec![],
                0,
                Some("transition parent count is invalid"),
            ),
            (
                "split",
                vec![c, d],
                0,
                Some("transition parent count is invalid"),
            ),
            (
                "merge",
                vec![c],
                0,
                Some("transition parent count is invalid"),
            ),
            ("merge", vec![c, c], 0, Some("transition repeats a parent")),
            ("split", vec![999], 0, Some("transition parent is missing")),
            (
                "merge",
                vec![c, 999],
                0,
                Some("transition parent is missing"),
            ),
            ("split", vec![c], 0, Some("split point is outside parent")),
            (
                "split",
                vec![c],
                mid / 2,
                Some("split point is outside parent"),
            ),
            (
                "merge",
                vec![c, b],
                0,
                Some("merge parents are not adjacent"),
            ),
            ("split", vec![c], mid / 4, None),
            ("merge", vec![d, b], 0, None),
            ("merge", vec![b, d], 0, None),
        ];
        for (kind, segs, split_at, error) in cases {
            map.pending = Some(PendingTransition {
                kind: kind.into(),
                segs,
                split_at,
                started_ms: 4,
                seal_gen: 1,
            });
            let refused = map.validate().err().map(|error| error.to_string());
            assert_eq!(refused.as_deref(), error, "{:?}", map.pending);
        }
    }

    #[test]
    fn lineage_preserves_direction_and_historical_predecessor_rules() {
        let mut map = SegmentMap::initial("root", 1);
        let (a, b) = map
            .split(0, KEYSPACE_END / 2, 7, [1; 16], [2; 16], 2)
            .unwrap();
        assert!(map.validate().is_ok());
        let mut historical = map.clone();
        historical.segments.retain(|segment| segment.seg_id != 0);
        assert!(
            historical.validate().is_ok(),
            "absorbed predecessors may be absent"
        );

        let mut missing_successor = map.clone();
        missing_successor
            .segments
            .retain(|segment| segment.seg_id != a);
        assert_eq!(
            missing_successor.validate().unwrap_err().to_string(),
            format!("segment 0 references missing segment {a}")
        );

        let mut repeated = map.clone();
        repeated.segments[0].successors.push(a);
        assert_eq!(
            repeated.validate().unwrap_err().to_string(),
            "segment 0 has duplicate/self reference"
        );

        let mut missing_backlink = map.clone();
        missing_backlink.segments[1].predecessors.clear();
        assert_eq!(
            missing_backlink.validate().unwrap_err().to_string(),
            "successor does not reference parent"
        );

        let mut disjoint = map.clone();
        disjoint.segments[2].predecessors.push(a);
        assert_eq!(
            disjoint.validate().unwrap_err().to_string(),
            "lineage ranges do not overlap"
        );

        let mut reverse = map;
        reverse.segments[1].predecessors.push(b);
        assert_eq!(
            reverse.validate().unwrap_err().to_string(),
            format!("segment {a} has cyclic/reversed lineage")
        );
    }

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
        assert_eq!(m.route(KEYSPACE_END).unwrap().seg_id, b);
        let parent = m.get(0).unwrap();
        assert_eq!(parent.sealed_next_offset, Some(4242));
        let succ = &parent.successors;
        assert_eq!(succ.len(), 2);
        assert!(succ.contains(&a) && succ.contains(&b));
        // double split of sealed parent rejected
        assert_eq!(
            m.split(0, mid / 2, 0, [3u8; 16], [4u8; 16], 3).unwrap_err(),
            MapError::AlreadySealed(0)
        );
        assert_eq!(
            m.version, 2,
            "one split bumps the CAS version once; a refused one never"
        );
    }

    /// A child sealed before its own successors are published leaves its
    /// keys without a live cover: they route to that child, the newest
    /// sealed cover, never to the parent it replaced.
    #[test]
    fn a_key_without_a_live_cover_routes_to_its_newest_sealed_cover() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, b) = m
            .split(0, KEYSPACE_END / 2, 0, [1u8; 16], [2u8; 16], 2)
            .unwrap();
        let low = m.segments.iter_mut().find(|s| s.seg_id == a).unwrap();
        low.sealed_ms = Some(3);
        low.sealed_next_offset = Some(0);
        assert!(m.validate().is_ok() && !m.check_partition());
        assert_eq!(m.route(1).unwrap().seg_id, a);
        assert_eq!(m.route(KEYSPACE_END).unwrap().seg_id, b);
    }

    /// A split must leave both children a non-empty range.
    #[test]
    fn a_split_point_on_the_parents_bounds_is_refused() {
        let mut m = SegmentMap::initial("root", 1);
        for bound in [0, KEYSPACE_END] {
            assert_eq!(
                m.split(0, bound, 0, [1u8; 16], [2u8; 16], 2),
                Err(MapError::InvalidSplitPoint)
            );
        }
        assert_eq!(
            m,
            SegmentMap::initial("root", 1),
            "a refused split changes nothing"
        );
    }

    /// Both invariants anchor at key 0 and at KEYSPACE_END: a map whose
    /// only segment starts above 0, or ends below the end, routes nothing
    /// there and must be refused. Each bound is checked on its own.
    #[test]
    fn a_keyspace_that_starts_late_or_ends_early_is_neither_covered_nor_partitioned() {
        let mut late = SegmentMap::initial("root", 1);
        late.segments[0].lo = 1;
        let mut early = SegmentMap::initial("root", 1);
        early.segments[0].hi = KEYSPACE_END - 1;
        for map in [late, early] {
            assert!(!map.check_partition());
            assert_eq!(map.validate().unwrap_err(), TopologyError::Coverage);
        }
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
        assert_eq!(m.get(d).unwrap().successors, vec![e]);
        assert_eq!(
            m.version, 4,
            "two splits and one merge; the refused merge bumps nothing"
        );
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
    fn an_exhausted_allocator_refuses_before_any_parent_seals() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, b) = m
            .split(0, KEYSPACE_END / 2, 0, [1u8; 16], [2u8; 16], 2)
            .unwrap();
        m.next_seg_id = u32::MAX;
        let spent = m.clone();
        assert_eq!(
            m.merge(a, b, 1, 2, [9u8; 16], 3),
            Err(MapError::IdExhausted),
            "a spent allocator refuses the merge"
        );
        assert_eq!(m, spent, "a refused merge seals neither parent");
        assert_eq!(
            m.split(a, KEYSPACE_END / 4, 0, [3u8; 16], [4u8; 16], 3),
            Err(MapError::IdExhausted),
            "the same allocator refuses a split"
        );
        assert_eq!(m, spent, "a refused split opens no child");
    }
}

#[cfg(kani)]
mod proofs;
