//! The shard log's keyspace (layout 7; DESIGN-6A §1, §3, the lead's ruling of
//! 2026-10-30 under the owner's delegation): every shard-log key starts with
//! a family tag, in ONE compaction tree.
//!
//! - `'S'` (one byte): stream state — tails, producers and sequences,
//!   seal fences, pending rows (unsealed pages), consumer-group rows,
//!   billing rows, and the seal directory and refcount rows under the
//!   sentinel;
//! - `'Q' ‖ gen u32` (five bytes, generation 0): every native-queue row —
//!   state, meta, availability, delivery, lease, retry, failure and dedup
//!   rows, the roster, the queue's message pages and the queue stream's
//!   tail.
//!
//! Layout 6 kept one tree per tag (a SlateDB segment extractor, RFC 0024);
//! the queue tree then paid an L0, its merges and their manifest writes on
//! every memtable flush, above today's reference (QUEUE-TREE-DECISION). The
//! tags stay, so every key layout and codec is layout 6's; the shard log
//! has no extractor, and the queue collector compacts the whole log.

use std::sync::Arc;

/// The stream-state family's tag (and its whole prefix).
pub(crate) const STREAM: u8 = b'S';
/// The queue family's tag.
pub(crate) const QUEUE_TAG: u8 = b'Q';
/// The queue family prefix of generation 0: the tag and the generation,
/// big-endian.
pub(crate) const QUEUE: [u8; QUEUE_PREFIX_LEN] = [QUEUE_TAG, 0, 0, 0, 0];
/// The width of a queue family prefix.
pub(crate) const QUEUE_PREFIX_LEN: usize = 5;
/// The stream family's sentinel: `'S'` and sixteen 0xFF bytes. Its rows
/// (the seal directory and refcount rows, the usage outbox) sort after
/// every stream's rows, so each family is one range scan.
pub(crate) const SENTINEL: [u8; 17] = {
    let mut k = [0xFF; 17];
    k[0] = STREAM;
    k
};

/// A stream-state row key: the `'S'` tag and the stream's hash, with room
/// for `extra` more bytes.
pub(crate) fn stream_row(hash: &[u8; 16], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + extra);
    k.push(STREAM);
    k.extend_from_slice(hash);
    k
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
mod tests {
    use super::{QUEUE, QUEUE_PREFIX_LEN, SENTINEL, STREAM, queue_row, stream_row};

    #[test]
    fn stream_and_queue_keys_carry_their_family_tags() {
        assert_eq!(
            stream_row(&[7; 16], 1)[..],
            [&[STREAM][..], &[7; 16]].concat()[..]
        );
        assert_eq!(
            queue_row(&[7; 16], 1)[..],
            [&QUEUE[..], &[7; 16]].concat()[..]
        );
        assert_eq!(QUEUE, [b'Q', 0, 0, 0, 0]);
        assert_eq!(QUEUE_PREFIX_LEN, QUEUE.len());
        assert_eq!(
            stream_row(&[0xFF; 16], 0),
            SENTINEL.to_vec(),
            "the sentinel is the last hash"
        );
        assert_eq!(crate::queue::config_key(&[7; 16], "c")[0], STREAM);
    }
}
