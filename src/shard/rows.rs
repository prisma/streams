//! Shard-log row keys and the tail row's codec: every stream-state key
//! starts with the `'S'` segment tag, a native queue's tail with the queue
//! segment's prefix (`crate::keyspace`, layout 6).

use super::TailFields;

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

/// Tail value v3:
/// `[ver u8=3][next u64][last_ts i64][logical u64][absorbed u64][trimmed u64][flags u8][seq_len u16][seq][route16?][trim_safe_to u64?][unabsorbed_bytes u64?]`
///
/// `flags` is a bitmask: bit0 = closed, bit1 = history v2 (the stream's
/// absorbed range lives in the shared per-shard partition, not a
/// per-stream DB). The optional trailing route16 (the shard-routing
/// hash), trim_safe_to and unabsorbed_bytes are backward-compatible
/// extensions: v3 decoders read exactly `seq_len` seq bytes and ignore
/// trailing bytes. `trim_safe_to` is the highest offset physical
/// trimming may reach (the absorbed boundary before the latest advance:
/// a snapshot at most one advance stale keeps its range; see
/// `TailFields::trim_safe_to`); `unabsorbed_bytes` is the
/// exact stored frame bytes in [absorbed, next), maintained by the
/// committer so restart rediscovery sizes pending work truthfully
/// instead of estimating (a single 32 MiB record used to estimate as
/// 1 KiB and never re-absorb under the default policy). Downgrade
/// caveat: a pre-bitmask binary reads flags with `== 1`, so it would
/// see a closed+v2 stream (flags=3) as open — acceptable for
/// forward-only deployments, noted here because it is not zero.
/// `seq` only copies the lane's Stream-Seq (its `s` row owns it): a copy past the
/// u16 length is not written, since a wrapped length wedges every later load.
pub(super) fn encode_tail(t: &TailFields) -> Vec<u8> {
    let seq = t.seq.as_deref().unwrap_or("").as_bytes();
    let (seq_len, seq) = u16::try_from(seq.len()).map_or((0, &[][..]), |len| (len, seq));
    let mut v = Vec::with_capacity(76 + seq.len());
    v.push(3);
    v.extend_from_slice(&t.next.to_le_bytes());
    v.extend_from_slice(&t.ts.to_le_bytes());
    v.extend_from_slice(&t.logical.to_le_bytes());
    v.extend_from_slice(&t.absorbed.to_le_bytes());
    v.extend_from_slice(&t.trimmed.to_le_bytes());
    let mut flags = 0u8;
    if t.closed {
        flags |= 1;
    }
    if t.history_v2 {
        flags |= 2;
    }
    v.push(flags);
    v.extend_from_slice(&seq_len.to_le_bytes());
    v.extend_from_slice(seq);
    v.extend_from_slice(&t.route);
    v.extend_from_slice(&t.trim_safe_to.to_le_bytes());
    v.extend_from_slice(&t.unabsorbed_bytes.to_le_bytes());
    v
}

pub(super) fn decode_tail(v: &[u8]) -> Option<TailFields> {
    if v.len() < 43 || (v[0] != 2 && v[0] != 3) {
        return None;
    }
    let v3 = v[0] == 3;
    let next = u64::from_le_bytes(v[1..9].try_into().ok()?);
    let ts = i64::from_le_bytes(v[9..17].try_into().ok()?);
    let logical = u64::from_le_bytes(v[17..25].try_into().ok()?);
    let absorbed = u64::from_le_bytes(v[25..33].try_into().ok()?);
    let trimmed = u64::from_le_bytes(v[33..41].try_into().ok()?);
    let (flags, seq_at) = if v3 { (v[41], 42usize) } else { (0u8, 41usize) };
    let seq_len = u16::from_le_bytes(v.get(seq_at..seq_at + 2)?.try_into().ok()?) as usize;
    let seq = if seq_len == 0 {
        None
    } else {
        Some(String::from_utf8(v.get(seq_at + 2..seq_at + 2 + seq_len)?.to_vec()).ok()?)
    };
    let route_at = seq_at + 2 + seq_len;
    // Historical tails end after seq, route, or trim_safe_to. A partial
    // known extension is corruption. Once the complete known suffix is
    // present, preserve the layout's forward-compatible trailing bytes.
    let extension_len = v.len().checked_sub(route_at)?;
    if (!matches!(extension_len, 0 | 16 | 24) && extension_len < 32) || flags & !3 != 0 {
        return None;
    }
    let route: [u8; 16] = v
        .get(route_at..route_at + 16)
        .and_then(|r| r.try_into().ok())
        .unwrap_or([0u8; 16]);
    let le8 = |at: usize| -> u64 {
        v.get(at..at + 8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    };
    let trim_safe_to = le8(route_at + 16);
    let unabsorbed_bytes = le8(route_at + 24);
    Some(TailFields {
        next,
        ts,
        logical,
        absorbed,
        trimmed,
        seq,
        closed: flags & 1 != 0,
        history_v2: flags & 2 != 0,
        route,
        trim_safe_to,
        unabsorbed_bytes,
    })
}

/// Existing malformed bytes must never initialize a fresh segment.
pub(super) fn stored_tail(raw: &[u8]) -> Result<TailFields, slatedb::Error> {
    let tail = decode_tail(raw)
        .ok_or_else(|| slatedb::Error::data("invalid persisted tail encoding".into()))?;
    // Decoding owns byte compatibility; every serving/recovery reader also
    // validates the state before it can authorize an offset or a repair.
    if tail.trimmed > tail.absorbed
        || tail.absorbed > tail.next
        || tail.trim_safe_to > tail.absorbed
    {
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
