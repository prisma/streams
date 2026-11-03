//! The shard log's keyspace (layout 8; S3a-DESIGN rev 3, scaling package 4a):
//! every per-entity key starts with the entity's hash, then a family byte, so
//! a shard's rows (a bit prefix of the hash) are one contiguous key range.
//!
//! - `hash16 ‖ 'S'`: stream state — tails, producers and sequences, seal
//!   fences, pending rows (unsealed pages), consumer-group rows and billing
//!   rows;
//! - `'Q' ‖ gen u32 ‖ hash16` (layout 7's order until the queue keys move):
//!   every native-queue row;
//! - `FF×16 ‖ tag`: the sentinel, the shard's own rows (the seal directory,
//!   refcounts, garbage, purge heads, the usage outbox). The hash `FF×16`
//!   names no entity, so the sentinel sorts after every entity's rows and
//!   each of its tags is one prefix scan.
//!
//! One compaction tree holds every family; the queue collector compacts the
//! whole log.

use std::sync::Arc;

/// The stream-state family byte, after the stream's hash.
pub(crate) const STREAM: u8 = b'S';
/// The queue family's tag.
pub(crate) const QUEUE_TAG: u8 = b'Q';
/// The queue family prefix of generation 0: the tag and the generation,
/// big-endian.
pub(crate) const QUEUE: [u8; QUEUE_PREFIX_LEN] = [QUEUE_TAG, 0, 0, 0, 0];
/// The width of a queue family prefix.
pub(crate) const QUEUE_PREFIX_LEN: usize = 5;
/// The sentinel: sixteen 0xFF bytes, the hash no entity has. Its rows (the
/// seal directory and refcount rows, the usage outbox) sort after every
/// entity's rows, so each of its tags is one range scan.
pub(crate) const SENTINEL: [u8; 16] = [0xFF; 16];
/// The tail row's tag after `hash ‖ 'S'`.
const TAIL_TAG: u8 = b't';

/// A stream-state row key: the stream's hash and the `'S'` family byte,
/// with room for `extra` more bytes.
pub(crate) fn stream_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + extra);
    k.extend_from_slice(hash);
    k.push(STREAM);
    k
}

/// The stream whose tail row `key` is (`hash ‖ 'S' ‖ 't'`); `None` for any
/// other key, the sentinel's included.
pub(crate) fn tail_hash(key: &[u8]) -> Option<[u8; 16]> {
    let (hash, rest) = key.split_first_chunk::<16>()?;
    (rest == [STREAM, TAIL_TAG] && *hash != SENTINEL).then_some(*hash)
}

/// A queue row key: the generation-0 queue prefix and the queue's hash,
/// with room for `extra` more bytes.
pub(crate) fn queue_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(QUEUE_PREFIX_LEN + 16 + extra);
    k.extend_from_slice(&QUEUE);
    k.extend_from_slice(hash);
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
    use super::{QUEUE, QUEUE_PREFIX_LEN, SENTINEL, STREAM, queue_row, stream_row, tail_hash};

    #[test]
    fn stream_and_queue_keys_carry_their_family_tags() {
        assert_eq!(
            stream_row(&[7; 16], 1)[..],
            [&[7; 16][..], &[STREAM]].concat()[..]
        );
        assert_eq!(
            queue_row(&[7; 16], 1)[..],
            [&QUEUE[..], &[7; 16]].concat()[..]
        );
        assert_eq!(QUEUE, [b'Q', 0, 0, 0, 0]);
        assert_eq!(QUEUE_PREFIX_LEN, QUEUE.len());
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
