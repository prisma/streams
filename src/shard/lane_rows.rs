#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Stored dedupe state of one routing-key lane: its Stream-Seq row and its
//! producer rows. A missing row means the lane never committed. Bytes that do
//! not decode are corruption and never read as missing, because a missing row
//! reopens the lane and accepts a duplicate or a regressed sequence as new.

/// One producer's committed state: epoch, seq, commit offset and the
/// request hash of its last accepted request.
pub(super) type ProducerRow = (u64, u64, u64, [u8; 16]);

/// Producer rows hold epoch and seq, then the commit offset (24 bytes) and
/// the request hash (40 bytes). Rows committed before a field existed stay
/// readable: `u64::MAX` marks an unknown offset and a zero hash matches any
/// request. Every other width is corruption.
pub(super) fn decode_producer_row(raw: &[u8]) -> Result<ProducerRow, slatedb::Error> {
    let invalid = || slatedb::Error::data("invalid persisted producer row width".into());
    let (epoch, rest) = raw.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (seq, rest) = rest.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (offset, hash) = match rest.split_first_chunk::<8>() {
        Some((offset, hash)) => (u64::from_le_bytes(*offset), hash),
        None => (u64::MAX, rest),
    };
    let hash: [u8; 16] = match hash {
        [] => [0; 16],
        hash => hash.try_into().map_err(|_| invalid())?,
    };
    Ok((
        u64::from_le_bytes(*epoch),
        u64::from_le_bytes(*seq),
        offset,
        hash,
    ))
}

/// New commits write only the full width; the decoder owns the older ones.
pub(super) fn encode_producer_row(row: ProducerRow) -> Vec<u8> {
    let (epoch, seq, offset, hash) = row;
    let mut raw = Vec::with_capacity(40);
    for field in [epoch, seq, offset] {
        raw.extend_from_slice(&field.to_le_bytes());
    }
    raw.extend_from_slice(&hash);
    raw
}

/// Stream-Seq rows hold the header text of the lane's last accepted
/// sequence. Bytes that are not UTF-8 are corruption, never an unset
/// sequence that would accept any value.
pub(super) fn decode_seq_row(raw: &[u8]) -> Result<String, slatedb::Error> {
    String::from_utf8(raw.to_vec())
        .map_err(|_| slatedb::Error::data("invalid persisted Stream-Seq row encoding".into()))
}
