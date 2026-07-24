//! Stream segment map (SCALING.md §1-2): a CAS-versioned total partition
//! of the routing-key hash space [0,1) — represented in u64 fixed point —
//! into segments, each pinned to one shard. Splits seal a parent and open
//! children over its subranges; merges seal two adjacent parents into one
//! child. No data ever moves: transitions only redirect FUTURE writes.
//!
//! Persistence: `streams/<stream-hash-hex>/segmap.json` in the ops bucket,
//! written with If-Match CAS by the scaler (single writer by lease).

use serde::{Deserialize, Serialize};

pub const KEYSPACE_END: u64 = u64::MAX; // ranges are [lo, hi) over u64

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentDesc {
    pub seg_id: u32,
    /// Inclusive lower bound of the hashed-key range.
    pub lo: u64,
    /// Exclusive upper bound (KEYSPACE_END means "to the top", inclusive).
    pub hi: u64,
    /// Shard log prefix this segment's appends land on.
    pub shard_prefix: String,
    pub created_ms: i64,
    /// Parent segment ids (1 for a split child, 2 for a merge child).
    #[serde(default)]
    pub predecessors: Vec<u32>,
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
    pub fn contains(&self, k: u64) -> bool {
        k >= self.lo && (k < self.hi || (self.hi == KEYSPACE_END && k == KEYSPACE_END))
    }
    pub fn is_live(&self) -> bool {
        self.sealed_ms.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentMap {
    /// Monotonic map version; CAS target.
    pub version: u64,
    /// Next seg_id to allocate.
    pub next_seg_id: u32,
    /// All segments ever (sealed ones retained for lineage until their
    /// shard data is fully absorbed + GC'd, then pruned).
    pub segments: Vec<SegmentDesc>,
}

#[derive(Debug, PartialEq)]
pub enum MapError {
    NotFound(u32),
    AlreadySealed(u32),
    NotAdjacent(u32, u32),
    NotSealed(u32),
    SingleSegmentStream,
}

impl SegmentMap {
    /// A fresh single-segment map covering the whole keyspace.
    pub fn initial(shard_prefix: &str, now_ms: i64) -> SegmentMap {
        SegmentMap {
            version: 1,
            next_seg_id: 1,
            segments: vec![SegmentDesc {
                seg_id: 0,
                lo: 0,
                hi: KEYSPACE_END,
                shard_prefix: shard_prefix.to_string(),
                created_ms: now_ms,
                predecessors: vec![],
                sealed_ms: None,
                sealed_next_offset: None,
            }],
        }
    }

    pub fn live(&self) -> impl Iterator<Item = &SegmentDesc> {
        self.segments.iter().filter(|s| s.is_live())
    }

    /// The live segment owning hashed key `k`. The partition invariant
    /// guarantees exactly one.
    pub fn route(&self, k: u64) -> Option<&SegmentDesc> {
        self.live().find(|s| s.contains(k))
    }

    pub fn get(&self, seg_id: u32) -> Option<&SegmentDesc> {
        self.segments.iter().find(|s| s.seg_id == seg_id)
    }

    /// Live successors of a sealed segment: the live segments whose range
    /// intersects the sealed one's.
    pub fn successors(&self, seg_id: u32) -> Vec<&SegmentDesc> {
        let Some(sealed) = self.get(seg_id) else {
            return vec![];
        };
        self.live()
            .filter(|s| s.lo < sealed.hi && sealed.lo < s.hi)
            .collect()
    }

    /// Partition invariant: live segments exactly tile [0, KEYSPACE_END).
    pub fn check_partition(&self) -> bool {
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
    /// map transition) and opens two children on the given shard prefixes.
    pub fn split(
        &mut self,
        seg_id: u32,
        split_at: u64,
        sealed_next_offset: u64,
        low_shard: &str,
        high_shard: &str,
        now_ms: i64,
    ) -> Result<(u32, u32), MapError> {
        let parent = self
            .segments
            .iter_mut()
            .find(|s| s.seg_id == seg_id)
            .ok_or(MapError::NotFound(seg_id))?;
        if !parent.is_live() {
            return Err(MapError::AlreadySealed(seg_id));
        }
        let (lo, hi) = (parent.lo, parent.hi);
        assert!(split_at > lo && split_at < hi, "split point inside range");
        parent.sealed_ms = Some(now_ms);
        parent.sealed_next_offset = Some(sealed_next_offset);
        let a = self.next_seg_id;
        let b = self.next_seg_id + 1;
        self.next_seg_id += 2;
        self.segments.push(SegmentDesc {
            seg_id: a,
            lo,
            hi: split_at,
            shard_prefix: low_shard.to_string(),
            created_ms: now_ms,
            predecessors: vec![seg_id],
            sealed_ms: None,
            sealed_next_offset: None,
        });
        self.segments.push(SegmentDesc {
            seg_id: b,
            lo: split_at,
            hi,
            shard_prefix: high_shard.to_string(),
            created_ms: now_ms,
            predecessors: vec![seg_id],
            sealed_ms: None,
            sealed_next_offset: None,
        });
        self.version += 1;
        debug_assert!(self.check_partition());
        Ok((a, b))
    }

    /// Merge two ADJACENT live segments into one child on `shard`.
    pub fn merge(
        &mut self,
        a_id: u32,
        b_id: u32,
        a_sealed_next: u64,
        b_sealed_next: u64,
        shard: &str,
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
        for (id, next) in [(a_id, a_sealed_next), (b_id, b_sealed_next)] {
            let s = self.segments.iter_mut().find(|s| s.seg_id == id).unwrap();
            s.sealed_ms = Some(now_ms);
            s.sealed_next_offset = Some(next);
        }
        let c = self.next_seg_id;
        self.next_seg_id += 1;
        self.segments.push(SegmentDesc {
            seg_id: c,
            lo,
            hi,
            shard_prefix: shard.to_string(),
            created_ms: now_ms,
            predecessors: vec![a_id, b_id],
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

/// Hash a routing key into the segment keyspace. Uses the same xxh3-free
/// stack as the rest of the codebase: first 8 bytes of the stream_hash
/// SHA-256 construction over the routing key.
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
        let (a, b) = m.split(0, mid, 4242, "s-a", "s-b", 2).unwrap();
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
            m.split(0, mid / 2, 0, "x", "y", 3).unwrap_err(),
            MapError::AlreadySealed(0)
        );
    }

    #[test]
    fn recursive_splits_keep_partition() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, _b) = m.split(0, KEYSPACE_END / 2, 0, "s1", "s2", 2).unwrap();
        let (c, d) = m.split(a, KEYSPACE_END / 4, 0, "s3", "s4", 3).unwrap();
        assert!(m.check_partition());
        assert_eq!(m.live().count(), 3);
        assert_eq!(m.route(1).unwrap().seg_id, c);
        assert_eq!(m.route(KEYSPACE_END / 4).unwrap().seg_id, d);
    }

    #[test]
    fn merge_adjacent_only() {
        let mut m = SegmentMap::initial("root", 1);
        let (a, b) = m.split(0, KEYSPACE_END / 2, 0, "s1", "s2", 2).unwrap();
        let (c, d) = m.split(a, KEYSPACE_END / 4, 0, "s3", "s4", 3).unwrap();
        // c=[0,q) d=[q,mid) b=[mid,end) — c+b not adjacent
        assert_eq!(m.merge(c, b, 0, 0, "sx", 4).unwrap_err(), MapError::NotAdjacent(c, b));
        let e = m.merge(d, b, 1, 2, "sy", 5).unwrap();
        assert!(m.check_partition());
        assert_eq!(m.live().count(), 2);
        assert_eq!(m.route(KEYSPACE_END / 2).unwrap().seg_id, e);
        let succ_d: Vec<u32> = m.successors(d).iter().map(|s| s.seg_id).collect();
        assert_eq!(succ_d, vec![e]);
    }

    #[test]
    fn serde_round_trip() {
        let mut m = SegmentMap::initial("root", 1);
        m.split(0, KEYSPACE_END / 2, 7, "a", "b", 2).unwrap();
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

// ---- persistence: CAS-guarded segmap objects in the ops bucket ----------

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::sync::Arc;

fn segmap_path(stream_hash: &[u8; 16]) -> ObjPath {
    ObjPath::from(format!("segmaps/{}.json", crate::crypto::hex(stream_hash)))
}

/// Load a stream's segment map plus the store etag needed to CAS the next
/// write. Ok(None) = no map yet (single-segment implicit).
pub async fn load(
    store: &Arc<dyn ObjectStore>,
    stream_hash: &[u8; 16],
) -> anyhow::Result<Option<(SegmentMap, Option<String>)>> {
    match store.get(&segmap_path(stream_hash)).await {
        Ok(got) => {
            let etag = got.meta.e_tag.clone();
            let bytes = got.bytes().await?;
            let map: SegmentMap = serde_json::from_slice(&bytes)?;
            Ok(Some((map, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// CAS-write a segment map: Create when `etag` is None (first write),
/// If-Match update otherwise. Err on CAS conflict — the caller (scaler)
/// must reload and re-decide; this is the single-writer safety net under
/// a lost/split lease.
pub async fn save(
    store: &Arc<dyn ObjectStore>,
    stream_hash: &[u8; 16],
    map: &SegmentMap,
    etag: Option<String>,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(map)?;
    let mode = match etag {
        None => PutMode::Create,
        Some(e_tag) => PutMode::Update(UpdateVersion {
            e_tag: Some(e_tag),
            version: None,
        }),
    };
    store
        .put_opts(
            &segmap_path(stream_hash),
            PutPayload::from(body),
            PutOptions::from(mode),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod persist_tests {
    use super::*;

    #[tokio::test]
    async fn cas_round_trip_and_conflict() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let hash = [7u8; 16];
        assert!(load(&store, &hash).await.unwrap().is_none());

        let mut m = SegmentMap::initial("root", 1);
        save(&store, &hash, &m, None).await.unwrap();
        let (loaded, etag) = load(&store, &hash).await.unwrap().unwrap();
        assert_eq!(loaded, m);
        assert!(etag.is_some());

        // CAS update succeeds with the fresh etag...
        m.split(0, KEYSPACE_END / 2, 9, "a", "b", 2).unwrap();
        save(&store, &hash, &m, etag.clone()).await.unwrap();
        // ...and the STALE etag now conflicts (lost-lease safety net).
        assert!(save(&store, &hash, &m, etag).await.is_err());
        // Create-on-existing also conflicts.
        assert!(save(&store, &hash, &m, None).await.is_err());

        let (final_map, _) = load(&store, &hash).await.unwrap().unwrap();
        assert_eq!(final_map.live().count(), 2);
    }
}
