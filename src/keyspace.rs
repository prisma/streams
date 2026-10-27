//! Layout 6's segmented shard-log keyspace (DESIGN-6A §1, §3).
//!
//! Every shard-log key starts with a segment tag, and SlateDB (RFC 0024)
//! keeps one compaction tree per segment:
//!
//! - `'S'` (one byte): stream state — tails, producers and sequences,
//!   seal fences, stream pages, consumer-group rows, billing rows, the
//!   dirty and maintenance rows under their sentinel;
//! - `'Q' ‖ gen u32` (five bytes, generation 0 only in phase 1): every
//!   native-queue row — state, meta, availability, delivery, lease, retry,
//!   failure and dedup rows, the roster, the queue's message pages and the
//!   queue stream's tail.
//!
//! The extractor is attached when the shard log is created and never
//! changes (its name is persisted in the manifest). A key that starts with
//! neither tag, or a `'Q'` key shorter than its five-byte prefix, has no
//! segment and SlateDB refuses its write.

use std::sync::Arc;

use slatedb::{PrefixExtractor, PrefixTarget};

/// The stream-state segment's tag (and its whole prefix).
pub(crate) const STREAM: u8 = b'S';
/// The queue segment's tag.
pub(crate) const QUEUE_TAG: u8 = b'Q';
/// The queue segment prefix of generation 0: the tag and the generation,
/// big-endian.
pub(crate) const QUEUE: [u8; QUEUE_PREFIX_LEN] = [QUEUE_TAG, 0, 0, 0, 0];
/// The width of a queue segment prefix.
pub(crate) const QUEUE_PREFIX_LEN: usize = 5;
/// The stream segment's sentinel: `'S'` and sixteen 0xFF bytes. Its rows
/// (the dirty-stream index, the usage outbox, the shard's maintenance row)
/// sort after every stream's rows, so each family is one range scan.
pub(crate) const SENTINEL: [u8; 17] = {
    let mut k = [0xFF; 17];
    k[0] = STREAM;
    k
};
/// Where a sentinel row keyed by a stream hash holds the hash: after the
/// sentinel and the row's tag.
pub(crate) const SENTINEL_HASH_AT: usize = SENTINEL.len() + 1;

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

/// The shard log's segment extractor (one name, fixed at creation).
pub(crate) struct ShardLogSegments;

/// The persisted extractor name; a shard log created with another name (or
/// none) refuses to open with this one.
const EXTRACTOR_NAME: &str = "prisma-streams/shard-log-segments/v1";

/// The segment prefix width of `bytes`, or `None` when it has none.
fn segment_len(bytes: &[u8]) -> Option<usize> {
    match bytes.first() {
        Some(&STREAM) => Some(1),
        Some(&QUEUE_TAG) if bytes.len() >= QUEUE_PREFIX_LEN => Some(QUEUE_PREFIX_LEN),
        _ => None,
    }
}

impl PrefixExtractor for ShardLogSegments {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            PrefixTarget::Point(key) => segment_len(key),
            PrefixTarget::Prefix(prefix) => segment_len(prefix),
        }
    }
}

/// The extractor every shard log is built with.
pub(crate) fn extractor() -> Arc<dyn PrefixExtractor> {
    Arc::new(ShardLogSegments)
}

/// A shard log's database builder at `path`: the segment extractor
/// attached, nothing else set.
pub(crate) fn db_builder<P: Into<object_store::path::Path>>(
    path: P,
    store: Arc<dyn object_store::ObjectStore>,
) -> slatedb::DbBuilder<P> {
    slatedb::Db::builder(path, store).with_segment_extractor(extractor())
}

#[cfg(test)]
mod tests {
    use super::{
        PrefixExtractor, PrefixTarget, QUEUE, STREAM, ShardLogSegments, queue_row, stream_row,
    };
    use bytes::Bytes;

    fn point(key: &[u8]) -> Option<usize> {
        ShardLogSegments.prefix_len(&PrefixTarget::Point(Bytes::copy_from_slice(key)))
    }

    #[test]
    fn stream_and_queue_keys_map_to_their_segments() {
        assert_eq!(point(&stream_row(&[7; 16], 1)), Some(1));
        assert_eq!(point(&queue_row(&[7; 16], 1)), Some(5));
        assert_eq!(point(b"S"), Some(1));
        assert_eq!(point(b"Q\0\0\0"), None);
        assert_eq!(point(b"x"), None);
        assert_eq!(point(b""), None);
        let prefix = ShardLogSegments.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(&QUEUE)));
        assert_eq!(prefix, Some(QUEUE.len()));
        assert_eq!(crate::queue::config_key(&[7; 16], "c")[0], STREAM);
    }
}
