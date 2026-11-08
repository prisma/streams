//! The shard log's keyspace (layout 8; S3a-DESIGN rev 3, scaling package 4a):
//! every per-entity key starts with the entity's hash, then a family byte, so
//! a shard's rows (a bit prefix of the hash) are one contiguous key range.
//!
//! - `hash16 ‖ 'S'`: stream state — tails, producers and sequences, seal
//!   fences, pending rows (unsealed pages), consumer-group rows and billing
//!   rows;
//! - `hash16 ‖ 'Q'`: every native-queue row (state, metadata, the indexes,
//!   dedup, message pages, the queue stream's tail);
//! - `FF×16 ‖ tag`: the sentinel, the shard's own rows (the seal directory,
//!   refcounts, garbage, purge heads, the usage outbox). The hash `FF×16`
//!   names no entity, so the sentinel sorts after every entity's rows and
//!   each of its tags is one prefix scan.
//!
//! One compaction tree holds every family; the queue collector compacts the
//! whole log.

use std::sync::Arc;

#[cfg(kani)]
mod proofs;
pub(crate) mod range;

/// The stream-state family byte, after the stream's hash.
pub(crate) const STREAM: u8 = b'S';
/// The queue family byte, after the queue's hash.
pub(crate) const QUEUE_TAG: u8 = b'Q';
/// The usage month finals' family byte, after the stream's hash: its own
/// family, so a purge's `hash ‖ 'S'` scan never reaches them.
pub(crate) const FINALS: u8 = b'V';
/// The sentinel: sixteen 0xFF bytes, the hash no entity has. Its rows (the
/// seal directory and refcount rows, the usage outbox) sort after every
/// entity's rows, so each of its tags is one range scan.
pub(crate) const SENTINEL: [u8; 16] = [0xFF; 16];
/// The tail row's tag after `hash ‖ 'S'`.
const TAIL_TAG: u8 = b't';

/// The route bytes an entity hash leads with, so the deepest shard prefix is `ROUTE_PREFIX_BITS`:
/// a row's leading bits are then its segment route's, whatever the shard (S3a-DESIGN §1.1).
pub(crate) const ROUTE_BYTES: usize = 3;
/// The deepest shard prefix, in bits; `range::Range::parse` refuses a longer one.
pub(crate) const ROUTE_PREFIX_BITS: usize = 8 * ROUTE_BYTES;

/// A row's entity hash (S3a-DESIGN §1.1, ROUTE-HASH-DECISION-2026-11-08): the leading
/// `ROUTE_BYTES` of its segment's route (`RouteHash`), then the rest of its engine identity
/// (`SegmentHash`). Every per-entity key starts with one. Only `entity_hash` mints one, so a row's
/// key and the shard the registry routes it to never part.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct EntityHash([u8; 16]);

impl EntityHash {
    /// An entity hash read back from a row, a directory entry or a run-index footer: bytes this
    /// shard minted through `entity_hash`. Only stored entity hashes come through here.
    pub(crate) fn from_stored(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The hash's bytes, as keys and the engine take them.
    pub(crate) fn bytes(self) -> [u8; 16] {
        self.0
    }

    /// Whether this is the sentinel's hash, which no entity may have: every place that mints a
    /// route or an identity refuses one (a fresh epoch, the next salt, a declined topology change).
    pub(crate) fn is_sentinel(self) -> bool {
        self.0 == SENTINEL
    }
}

/// The entity hash of the segment whose route is `route` and whose engine identity is `identity`.
pub(crate) fn entity_hash(
    route: &crate::crypto::RouteHash,
    identity: &crate::crypto::SegmentHash,
) -> EntityHash {
    let mut hash = identity.0;
    hash[..ROUTE_BYTES].copy_from_slice(&route.0[..ROUTE_BYTES]);
    EntityHash(hash)
}

/// The sentinel's tags (S3a-DESIGN rev 3 §2.4): every shard-wide row is `SENTINEL ‖ tag ‖ suffix`, and
/// each tag is read by its own prefix. A tag is defined here as its first writer lands; the full set,
/// reserved tags included, is `tests::SENTINEL_TAGS`.
pub(crate) mod tag {
    /// The seal directory: one row per segment object of the current fold period.
    pub(crate) const DIRECTORY: u8 = b'K';
    /// A segment object's refcount.
    pub(crate) const REFCOUNT: u8 = b'R';
    /// A run-index object's pointer.
    pub(crate) const POINTER: u8 = b'I';
    /// A consolidation's superseded chunks.
    pub(crate) const ORPHAN: u8 = b'O';
    /// An own object to delete after its grace.
    pub(crate) const GARBAGE: u8 = b'G';
    /// A dead stream's purge head.
    pub(crate) const HEAD: u8 = b'H';
    /// A background scan's cursor (the orphan sweep's, per object kind).
    pub(crate) const CURSOR: u8 = b'L';
    /// The queue roster: one row per queue with pending work, by deadline.
    pub(crate) const ROSTER: u8 = b'Q';
    /// The usage debt index: a stream that holds at least one month final.
    pub(crate) const USAGE_DEBT: u8 = b'U';
    /// The shard log's identity: its layout and its home (`shard::check_identity`).
    pub(crate) const IDENTITY: u8 = b'Y';
}

/// The home of the shard at `prefix` (S3a-DESIGN §3.1): a static topology's shards are its
/// `2^n` prefixes of `n` bits in numeric order, so a shard's home is its prefix read as a number,
/// and the root's is 0. `None` for a prefix that is no shard's (a label, or past the route bits).
pub(crate) fn home_of(prefix: &str) -> Option<u32> {
    range::Range::parse(prefix)?;
    if prefix.is_empty() {
        return Some(0);
    }
    u32::from_str_radix(prefix, 2).ok()
}

/// A stream-state row key: the stream's hash and the `'S'` family byte,
/// with room for `extra` more bytes.
pub(crate) fn stream_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + extra);
    k.extend_from_slice(hash);
    k.push(STREAM);
    k
}

/// A usage month final's key: the stream's hash and the `'V'` family byte,
/// with room for `extra` more bytes.
pub(crate) fn finals_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + extra);
    k.extend_from_slice(hash);
    k.push(FINALS);
    k
}

/// The stream whose tail row `key` is (`hash ‖ 'S' ‖ 't'`); `None` for any
/// other key, the sentinel's included.
pub(crate) fn tail_hash(key: &[u8]) -> Option<[u8; 16]> {
    let (hash, rest) = key.split_first_chunk::<16>()?;
    (rest == [STREAM, TAIL_TAG] && *hash != SENTINEL).then_some(*hash)
}

/// A queue row key: the queue's hash and the `'Q'` family byte, with room
/// for `extra` more bytes.
pub(crate) fn queue_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + extra);
    k.extend_from_slice(hash);
    k.push(QUEUE_TAG);
    k
}

/// A shard log's database builder at `path`: the pending rows' merge
/// operator (the builder hands it to its compactor too) and the log's
/// metrics recorder (`spill::metrics`) attached, nothing else set.
pub(crate) fn db_builder<P: Into<object_store::path::Path>>(
    path: P,
    store: Arc<dyn object_store::ObjectStore>,
) -> slatedb::DbBuilder<object_store::path::Path> {
    let path: object_store::path::Path = path.into();
    let metrics = crate::spill::metrics::for_log(path.as_ref());
    slatedb::Db::builder(path, store)
        .with_merge_operator(crate::spill::pending::merge_operator())
        .with_metrics_recorder(metrics.recorder())
}

#[cfg(test)]
#[path = "keyspace_projection_tests.rs"]
mod projection_tests;

#[cfg(test)]
mod tests {
    use super::{QUEUE_TAG, SENTINEL, STREAM, queue_row, stream_row, tail_hash};

    /// Every sentinel tag of layout 8 (S3a-DESIGN rev 3 §2.4), the reserved ones included: the seal's
    /// and the gc's (`tag`), and those whose writers land later in 4a, in S3b, S4 and S6. The usage
    /// month finals' `'V'` is not one: they live in their stream's own range (`finals_row`).
    pub(crate) const SENTINEL_TAGS: [u8; 18] = [
        super::tag::DIRECTORY,
        super::tag::REFCOUNT,
        super::tag::POINTER,
        super::tag::ORPHAN,
        super::tag::GARBAGE,
        super::tag::HEAD,
        super::tag::CURSOR,
        super::tag::ROSTER,
        super::tag::USAGE_DEBT,
        b'Y', // the log's layout and home (4a commit 4)
        b'W', // unsealed streams (4a commit 7)
        b'M', // usage awaiting the ledger (4a commit 7)
        b'D', // queue debt index (reserved for #150)
        b'Z', // queue trim hints (S3b)
        b'N', // queue-state gauges (S3b)
        b'E', // lineage pending release (S6)
        b'C', // lineage-sweep cursor (S6)
        b'T', // retired parent (S4)
    ];

    /// An entity hash leads with its route's first `ROUTE_PREFIX_BITS`, so every shard prefix the
    /// registry routes on holds an entity exactly when it holds its route (S3a-DESIGN §1.1), and a
    /// deeper prefix is no shard's.
    /// The route's first `len` bits, or, when `flip`, a prefix the route is not in: its last bit flipped.
    fn route_prefix(bits: &str, len: usize, flip: bool) -> String {
        let mut prefix = bits[..len].to_string();
        if len > 0 && flip {
            let last = if prefix.ends_with('0') { "1" } else { "0" };
            prefix.replace_range(len - 1.., last);
        }
        prefix
    }

    #[test]
    fn an_entity_hash_shares_its_routes_first_24_bits() {
        use rand::{RngCore, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(24);
        for _ in 0..1_000 {
            let (mut route, mut identity) = ([0u8; 16], [0u8; 16]);
            rng.fill_bytes(&mut route);
            rng.fill_bytes(&mut identity);
            let entity = super::entity_hash(
                &crate::crypto::RouteHash(route),
                &crate::crypto::SegmentHash(identity),
            )
            .bytes();
            assert_eq!(entity[..super::ROUTE_BYTES], route[..super::ROUTE_BYTES]);
            assert_eq!(entity[super::ROUTE_BYTES..], identity[super::ROUTE_BYTES..]);
            let bits: String = route.iter().map(|b| format!("{b:08b}")).collect();
            for len in 0..=super::ROUTE_PREFIX_BITS {
                let prefix = route_prefix(&bits, len, rng.next_u32() % 2 == 0);
                let range = super::range::Range::parse(&prefix).unwrap();
                assert_eq!(
                    range.contains(super::EntityHash::from_stored(entity)),
                    range.holds_route(&route),
                    "prefix {prefix}"
                );
            }
        }
        let deeper = "0".repeat(super::ROUTE_PREFIX_BITS + 1);
        assert_eq!(super::range::Range::parse(&deeper), None);
    }

    #[test]
    fn the_sentinel_tags_are_distinct() {
        let mut tags = SENTINEL_TAGS.to_vec();
        tags.sort_unstable();
        tags.dedup();
        assert_eq!(tags.len(), SENTINEL_TAGS.len());
    }

    /// Every sentinel row's key, byte for byte: `FF×16 ‖ tag ‖ suffix`.
    #[test]
    fn every_sentinel_key_is_pinned() {
        use crate::spill::{directory, gc, object::ObjectId, runindex};
        let id = ObjectId { epoch: 3, seq: 9 };
        let object = "00000000000000030000000000000009";
        let ff = "ffffffffffffffffffffffffffffffff";
        let hex = |k: Vec<u8>| crate::crypto::hex(&k);
        let pinned = [
            (hex(directory::directory_key(id)), format!("{ff}4b{object}")),
            (hex(gc::refcount_key(id)), format!("{ff}52{object}")),
            (hex(runindex::pointer_key(id)), format!("{ff}49{object}")),
            (hex(runindex::orphan_key(id)), format!("{ff}4f{object}")),
            (hex(gc::garbage_key(id)), format!("{ff}47{object}")),
            (
                hex(gc::head_key(&[0x11; 16])),
                format!("{ff}48{}", "11".repeat(16)),
            ),
            (
                hex(gc::sweep_key(gc::ObjectKind::Segment)),
                format!("{ff}4c00"),
            ),
            (
                hex(gc::sweep_key(gc::ObjectKind::RunIndex)),
                format!("{ff}4c01"),
            ),
        ];
        for (key, want) in pinned {
            assert_eq!(key, want);
        }
    }

    #[test]
    fn stream_and_queue_keys_carry_their_family_tags() {
        assert_eq!(
            stream_row(&[7; 16], 1)[..],
            [&[7; 16][..], &[STREAM]].concat()[..]
        );
        assert_eq!(
            queue_row(&[7; 16], 1)[..],
            [&[7; 16][..], &[QUEUE_TAG]].concat()[..]
        );
        assert_eq!(QUEUE_TAG, b'Q');
        assert_eq!(
            stream_row(&[0xFF; 16], 0)[..16],
            SENTINEL[..],
            "the sentinel is the last hash"
        );
        assert_eq!(crate::queue::config_key(&[7; 16], "c")[16], STREAM);
        assert_eq!(
            crate::queue::state_prefix(&[7; 16], b'c', "c")[..17],
            stream_row(&[7; 16], 0)[..],
            "queue.rs builds its keys inline (a by-path include) in this order"
        );
    }

    #[test]
    fn the_tail_filter_takes_only_tails() {
        let tail = crate::shard::tail_key(&[7; 16]);
        assert_eq!(tail_hash(&tail), Some([7; 16]));
        let mut sentinel_tail = SENTINEL.to_vec();
        sentinel_tail.extend_from_slice(&[STREAM, b't']);
        let others = [
            crate::spill::pending::pending_key(&[7; 16]),
            crate::queue::config_key(&[7; 16], "t"),
            sentinel_tail,
            [&[7; 16][..], b"t"].concat(),
            [&tail[..], b"x"].concat(),
            tail[1..].to_vec(),
        ];
        for key in others {
            assert_eq!(tail_hash(&key), None, "{key:02x?}");
        }
    }
}
