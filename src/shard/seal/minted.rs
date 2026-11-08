#![cfg(test)]
//! Stream hashes for the seal's tests of shard ownership, minted the way the
//! registry mints a row's hash, never planted: `StreamDesc::storage_hash`
//! (`keyspace::entity_hash` of `RouteHash::for_stream` and
//! `SegmentHash::for_stream`). A range is the set of entity hashes whose
//! segment route the registry places on the shard (S6-DESIGN rev 6 §1), so a
//! test of a range picks its streams by their route and keys them by their
//! entity hash, as the engine does (ROUTE-HASH-DECISION-2026-11-08 §7).

use crate::keyspace::range::Range;
use crate::registry::StreamDesc;

/// A stream of the test project named `name`, as the registry validates it.
fn descriptor(name: &str) -> StreamDesc {
    crate::registry::PersistedDescriptor {
        seal_gen_counter: 0,
        project_id: crate::tenant::ProjectId::new("proj-seal-range").expect("a project id"),
        name: name.into(),
        stream_epoch: "0123456789abcdef0123456789abcdef".into(),
        key_fingerprint: "fp".into(),
        created_ms: 1,
        expires_at_ms: None,
        deleted: false,
        content_type: "application/octet-stream".into(),
        ttl_secs: None,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
    .try_into()
    .expect("a valid descriptor")
}

/// The entity hash of the first stream `<tag>-<i>` whose route the registry
/// places inside `prefix`'s range (`inside`) or outside it, checked to fall
/// on the same side: the route decides, the engine keys by the entity hash.
pub(crate) fn stream_routed(prefix: &str, inside: bool, tag: &str) -> [u8; 16] {
    let range = Range::of(prefix);
    (0..256)
        .map(|i| descriptor(&format!("{tag}-{i}")))
        .find(|d| range.holds_route(&d.route_hash().0) == inside)
        .map(|d| {
            let hash = d.storage_hash();
            assert_eq!(
                range.contains(crate::keyspace::EntityHash::from_stored(hash)),
                inside,
                "{tag}: its route and its entity hash fall on different sides of {prefix:?}"
            );
            hash
        })
        .expect("a stream routed to that side within 256 names")
}
