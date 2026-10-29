//! Shard-log row keys and the tail row's codec: every stream-state key
//! starts with the `'S'` segment tag, a native queue's tail with the queue
//! segment's prefix (`crate::keyspace`, layout 6).

use crate::billing::stored_meta::UsageCounters;

pub(crate) fn tail_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::stream_row(hash, 1);
    k.push(b't');
    k
}

/// A native queue's tail row: the stream tail's layout under the queue
/// segment (layout 6A), so a queue-only shard's appends touch no stream
/// row but the billing ones.
pub(crate) fn queue_tail_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::queue_row(hash, 1);
    k.push(b't');
    k
}

/// Tail value v4 (layout 6):
/// `[ver u8=4][next u64][last_ts i64][logical u64][absorbed u64][flags u8][seq_len u16][seq][route16][unabsorbed_bytes u64][usage counters?]`
///
/// `flags` bit 0 is closed; no other bit is defined. `absorbed` is the
/// sealed boundary: every page below it is in a segment object, every page
/// at or above it in the stream's pending row. `route` is the
/// shard-routing hash; `unabsorbed_bytes` the exact stored page bytes in
/// `[absorbed, next)` (the pending row's size). A billed segment's usage
/// counters (`UsageCounters`, f3) may follow, whole; nothing else may.
/// `seq` only copies the lane's Stream-Seq (its `s` row owns it): a copy past the
/// u16 length is not written, since a wrapped length wedges every later load.
pub(super) fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let (seq_len, seq) = u16::try_from(seq.len()).map_or((0, &[][..]), |len| (len, seq));
    let mut v = Vec::with_capacity(TAIL_MIN_LEN + seq.len());
    v.push(TAIL_VERSION);
    v.extend_from_slice(&t.next.to_le_bytes());
    v.extend_from_slice(&t.ts.to_le_bytes());
    v.extend_from_slice(&t.logical.to_le_bytes());
    v.extend_from_slice(&t.absorbed.to_le_bytes());
    v.push(u8::from(t.closed));
    v.extend_from_slice(&seq_len.to_le_bytes());
    v.extend_from_slice(seq);
    v.extend_from_slice(&t.route);
    v.extend_from_slice(&t.unabsorbed_bytes.to_le_bytes());
    if let Some(usage) = &t.usage {
        usage.encode_into(&mut v);
    }
    v
}

const TAIL_VERSION: u8 = 4;
/// A tail with an empty seq and no usage counters.
const TAIL_MIN_LEN: usize = 60;

pub(super) fn decode_tail(v: &[u8]) -> Option<TailFields> {
    if v.len() < TAIL_MIN_LEN || v.first() != Some(&TAIL_VERSION) {
        return None;
    }
    let le8 = |at: usize| -> Option<[u8; 8]> { v.get(at..at + 8)?.try_into().ok() };
    let next = u64::from_le_bytes(le8(1)?);
    let ts = i64::from_le_bytes(le8(9)?);
    let logical = u64::from_le_bytes(le8(17)?);
    let absorbed = u64::from_le_bytes(le8(25)?);
    let flags = *v.get(33)?;
    if flags & !1 != 0 {
        return None;
    }
    let seq_len = usize::from(u16::from_le_bytes(v.get(34..36)?.try_into().ok()?));
    let seq = if seq_len == 0 {
        None
    } else {
        Some(String::from_utf8(v.get(36..36 + seq_len)?.to_vec()).ok()?)
    };
    let route_at = 36 + seq_len;
    let route: [u8; 16] = v.get(route_at..route_at + 16)?.try_into().ok()?;
    let unabsorbed_bytes = u64::from_le_bytes(le8(route_at + 16)?);
    let usage = match v.get(route_at + 24..) {
        None | Some([]) => None,
        Some(counters) => Some(UsageCounters::decode(counters)?),
    };
    Some(TailFields {
        next,
        ts,
        logical,
        absorbed,
        seq,
        closed: flags & 1 != 0,
        route,
        unabsorbed_bytes,
        usage,
        billing: super::layout::StaticBilling::Unread,
    })
}

/// Existing malformed bytes must never initialize a fresh segment.
pub(super) fn stored_tail(raw: &[u8]) -> Result<TailFields, slatedb::Error> {
    let tail = decode_tail(raw)
        .ok_or_else(|| slatedb::Error::data("invalid persisted tail encoding".into()))?;
    // Decoding owns byte compatibility; every serving/recovery reader also
    // validates the state before it can authorize an offset or a repair.
    if tail.absorbed > tail.next {
        return Err(slatedb::Error::data("inconsistent persisted tail".into()));
    }
    Ok(tail)
}

/// Fixed-width metadata is exactly eight bytes; short and trailing bytes
/// indicate corruption. Absence is handled separately by the repository.
pub(crate) fn decode_cursor(raw: &[u8]) -> Result<u64, slatedb::Error> {
    let bytes: [u8; 8] = raw
        .try_into()
        .map_err(|_| slatedb::Error::data("invalid persisted cursor length".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

/// Per-routing-key Stream-Seq row (ROUTING-V3 §3.6): seq is scoped to
/// the KEY, not the segment — a segment carries many keys' lanes.
pub(crate) fn seq_key(hash: &[u8; 16], key_hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::stream_row(hash, 17);
    k.push(b's');
    k.extend_from_slice(key_hash);
    k
}

/// A segment's durable seal fence (tag `G`, u64 LE): the highest takeover generation fenced.
pub(crate) fn seal_fence_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = crate::keyspace::stream_row(hash, 1);
    k.push(b'G');
    k
}

pub(crate) fn producer_key(hash: &[u8; 16], key_hash: &[u8; 16], producer_id: &str) -> Vec<u8> {
    // <segment identity> 'q' <routing-key hash> <producer id> — producer
    // sessions are scoped per ROUTING KEY (review finding 5): one
    // producer id keeps independent sequence lanes for different keys,
    // and the scope does not change across a split (each key's lane
    // follows its key through the predecessor chain).
    let mut k = crate::keyspace::stream_row(hash, 17 + producer_id.len());
    k.push(b'q');
    k.extend_from_slice(key_hash);
    k.extend_from_slice(producer_id.as_bytes());
    k
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TailFields {
    pub next: u64,
    pub ts: i64,
    pub logical: u64,
    /// The sealed boundary (layout 6): every page below it is in a
    /// segment object, every page at or above it unsealed.
    pub absorbed: u64,
    pub seq: Option<String>,
    pub closed: bool,
    /// Shard-routing hash (stream_hash(name)); zeros for streams last
    /// written by callers without a name identity.
    pub route: [u8; 16],
    /// Exact stored page bytes in [absorbed, next), maintained by the
    /// committer (appends add page lengths; a seal subtracts the bytes it
    /// sealed): the stream's share of the shard's unsealed ledger.
    pub unabsorbed_bytes: u64,
    /// A billed segment's cumulative usage counters (layout 6, f3): they
    /// ride the tail every billed group writes, and its static billing row
    /// is written only when its identity, month or retention flag changes
    /// (`crate::billing::stored_meta`). `None` for an unbilled stream.
    pub usage: Option<UsageCounters>,
    /// The segment's static billing meta as this tail's group left it
    /// (layout 6, f3), with zero counters. Not part of the stored tail: its
    /// row is `billing_meta_key`; the applied and durable tails carry it so
    /// a group stages on the applied one and the usage drain meters the
    /// durable one.
    pub billing: super::layout::StaticBilling,
}

impl TailFields {
    /// Whether this tail's stored row differs from `base`'s: the fields a
    /// group writes the tail for, the usage counters included (layout 6, f3).
    pub(super) fn stored_differs(&self, base: &TailFields) -> bool {
        self.next != base.next
            || self.absorbed != base.absorbed
            || self.unabsorbed_bytes != base.unabsorbed_bytes
            || self.seq != base.seq
            || self.closed != base.closed
            || self.usage != base.usage
    }
}
