//! A serving descriptor's identity, derived once when the descriptor is
//! validated: its project-qualified reference, its route hash and its
//! storage (segment 0) identity. A `StreamDesc` is immutable, so routing an
//! operation reads them instead of re-validating the name and re-hashing it
//! at every step of every request.
use super::{PersistedDescriptor, SegRoute, StreamDesc};
use crate::crypto::RouteHash;
use crate::segmap::SegmentDesc;
use crate::tenant::TenantStreamRef;

#[derive(Clone, Debug)]
pub(super) struct Identity {
    sref: TenantStreamRef,
    route: RouteHash,
    storage: [u8; 16],
}

impl Identity {
    pub(super) fn of(persisted: &PersistedDescriptor) -> Self {
        let sref = persisted.sref();
        Identity {
            route: RouteHash::for_stream(&sref),
            storage: persisted.storage_hash(),
            sref,
        }
    }

    /// Engine identity of a dynamic-map segment (ROUTING-V3 §2).
    /// Segment 0 is ALWAYS the storage identity: that equality is what
    /// makes every pre-v3 total-order stream already-migrated, with its
    /// whole history as segment 0 and zero data movement.
    fn dynamic_segment(&self, desc: &PersistedDescriptor, seg_id: u32) -> [u8; 16] {
        if seg_id == 0 {
            return self.storage;
        }
        crate::crypto::SegmentHash::for_segment(&self.sref, &desc.stream_epoch, seg_id).0
    }

    /// The physical shard route of one segment: its persisted route_hash
    /// when assigned (split children get real, independent routes — review
    /// blocker 1: a split must add capacity, not just lineage), the
    /// shard-prefix hash for prefix-pinned segments, and the parent stream
    /// route for the implicit/seg-0 case.
    fn segment_route(&self, seg: &SegmentDesc) -> [u8; 16] {
        if seg.route_hash != [0u8; 16] {
            seg.route_hash
        } else if seg.shard_prefix.is_empty() {
            self.route.0
        } else {
            crate::crypto::stream_hash(&seg.shard_prefix)
        }
    }

    /// Unknown explicit segments have no routing authority. The parent
    /// route belongs only to the absent-map implicit segment zero.
    fn segment_route_by_id(&self, desc: &PersistedDescriptor, seg_id: u32) -> Option<[u8; 16]> {
        match &desc.segments {
            Some(map) => map.get(seg_id).map(|segment| self.segment_route(segment)),
            None if seg_id == 0 => Some(self.route.0),
            None => None,
        }
    }

    /// THE unified routing resolution (ROUTING-V3 §1-2): routing key →
    /// the segment that owns it right now. Handles every layout:
    ///
    /// - descriptor-resident dynamic map (`segments: Some`) — the v3
    ///   model; selects the live segment containing the key point;
    /// - everything else — the implicit single-segment map: segment 0,
    ///   the storage identity, the parent's shard route. This arm IS the
    ///   old total-order behavior, unchanged to the byte.
    ///
    /// Legacy `scaling` descriptors are routed by their child-stream
    /// machinery upstream of this call until PR4 folds them in; this
    /// function never sees their parent appends.
    fn resolve(&self, desc: &PersistedDescriptor, routing_key: &str) -> SegRoute {
        let key_hash = crate::crypto::RoutingKeyHash::of(routing_key);
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&key_hash.0[..8]);
        let point = u64::from_be_bytes(prefix);
        if let Some(map) = &desc.segments {
            // The live cover, or for a sealed leaf the newest sealed one
            // (SegmentMap::route), so the caller's refresh path can heal.
            if let Some(seg) = map.route(point) {
                return SegRoute {
                    seg_id: seg.seg_id,
                    identity: self.dynamic_segment(desc, seg.seg_id),
                    shard_route: self.segment_route(seg),
                    sealed: !seg.is_live(),
                    point,
                    key_hash,
                    lo: seg.lo,
                    hi: seg.hi,
                };
            }
            unreachable!("validated explicit topology covers every routing point");
        }
        SegRoute {
            seg_id: 0,
            identity: self.storage,
            shard_route: self.route.0,
            sealed: false,
            point,
            key_hash,
            lo: 0,
            hi: crate::segmap::KEYSPACE_END,
        }
    }
}

impl StreamDesc {
    /// The project-qualified identity of this stream
    /// (`PersistedDescriptor::sref`, validated when this descriptor was).
    pub(crate) fn sref(&self) -> TenantStreamRef {
        self.identity.sref.clone()
    }

    /// This stream's route hash (`RouteHash::for_stream`).
    pub(crate) fn route_hash(&self) -> RouteHash {
        self.identity.route
    }

    /// Storage identity (`PersistedDescriptor::storage_hash`): segment 0's
    /// engine identity.
    pub(crate) fn storage_hash(&self) -> [u8; 16] {
        self.identity.storage
    }

    /// Engine identity of a dynamic-map segment (`Identity::dynamic_segment`).
    pub(crate) fn dynamic_segment_identity(&self, seg_id: u32) -> [u8; 16] {
        self.identity.dynamic_segment(&self.persisted, seg_id)
    }

    /// The physical shard route of one segment (`Identity::segment_route`).
    pub(crate) fn segment_route(&self, seg: &SegmentDesc) -> [u8; 16] {
        self.identity.segment_route(seg)
    }

    /// The shard route of segment `seg_id`, if this descriptor routes it.
    pub(crate) fn segment_route_by_id(&self, seg_id: u32) -> Option<[u8; 16]> {
        self.identity.segment_route_by_id(&self.persisted, seg_id)
    }

    /// The segment that owns `routing_key` right now (`Identity::resolve`).
    pub(crate) fn resolve_segment(&self, routing_key: &str) -> SegRoute {
        self.identity.resolve(&self.persisted, routing_key)
    }
}

/// Resolution over a descriptor under construction, as the registry tests
/// build and edit them; serving code resolves through a `StreamDesc`.
#[cfg(test)]
impl PersistedDescriptor {
    pub(crate) fn dynamic_segment_identity(&self, seg_id: u32) -> [u8; 16] {
        Identity::of(self).dynamic_segment(self, seg_id)
    }

    pub(crate) fn resolve_segment(&self, routing_key: &str) -> SegRoute {
        Identity::of(self).resolve(self, routing_key)
    }
}
