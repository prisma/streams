//! Queue row keys (docs/QUEUES-DESIGN.md §5; layout 8, S3a-DESIGN §2.3):
//! every row of a queue is `hash ‖ 'Q' ‖ family ‖ suffix`, big-endian so key
//! order is numeric order, and a queue's rows are one range beside its
//! hash's other rows. A message is one page of the queue's stream (S3b part
//! 2): its pending row while unsealed, a segment's chunk once sealed, so no
//! family holds pages; the queue stream's tail is `shard::queue_tail_key`.
//! The shard-wide roster is under the sentinel (`roster`).

/// The queue's state row: configuration, clock, cursor, frontier,
/// counters, debt.
pub(crate) const STATE: u8 = b's';
/// A scheduled message's `availableAt`, by offset: how a cancel or a read
/// by id finds its scheduled entry. Only scheduled messages have one.
pub(crate) const SCHEDULED_AT: u8 = b'm';
/// The scheduled index `A_s`: (availableAt, offset) of every unresolved
/// message sent with a future `deliverAt`. Immediate messages have no
/// entry: the lane's frontier walks their pages (S3b-DESIGN §4).
pub(crate) const AVAIL: u8 = b'a';
/// A range row (`range::RangeRow`), by `offset >> range::SHIFT`.
pub(crate) const RANGE: u8 = b'x';
/// Per-message delivery row of the default group.
pub(crate) const DELIVERY: u8 = b'd';
/// Lease index `L`: (leaseDeadline, offset).
pub(crate) const LEASES: u8 = b'l';
/// Retry index `R`: (availableAt, offset).
pub(crate) const RETRIES: u8 = b'r';
/// Failure index `F`: (failedAt, offset).
pub(crate) const FAILED: u8 = b'f';
/// Failure-expiry index `E`: (failureExpiresAt, offset).
pub(crate) const FAIL_EXPIRY: u8 = b'e';
/// Dedup entry: tag → offset and expiry.
pub(crate) const DEDUP: u8 = b'u';
/// Dedup expiry index: (expiresAt, tag).
pub(crate) const DEDUP_EXPIRY: u8 = b'v';

/// Where a queue row's family byte sits: after the queue's hash and the
/// `'Q'` family byte.
pub(crate) const FAMILY_AT: usize = 17;

/// The shard-wide queue roster (§6.8): one row per queue with pending work,
/// `SENTINEL ‖ 'Q' ‖ deadline i64 BE ‖ hash`, empty, so one prefix scan reads
/// the queues in the order they fall due. A new owner finds them by it; the
/// state row records the deadline (`roster_at`), so a move of the row needs
/// no read of it.
pub(crate) fn roster_prefix() -> Vec<u8> {
    let mut k = crate::keyspace::SENTINEL.to_vec();
    k.push(crate::keyspace::tag::ROSTER);
    k
}

pub(crate) fn family(hash: &[u8; 16], family: u8) -> Vec<u8> {
    let mut k = crate::keyspace::queue_row(hash, 17);
    k.push(family);
    k
}

pub(crate) fn state(hash: &[u8; 16]) -> Vec<u8> {
    family(hash, STATE)
}

pub(crate) fn by_offset(hash: &[u8; 16], fam: u8, offset: u64) -> Vec<u8> {
    let mut k = family(hash, fam);
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

/// A time-ordered index key. Times are non-negative milliseconds; a
/// negative time sorts as zero.
pub(crate) fn by_time(hash: &[u8; 16], fam: u8, at: i64, offset: u64) -> Vec<u8> {
    let mut k = family(hash, fam);
    k.extend_from_slice(&u64::try_from(at).unwrap_or(0).to_be_bytes());
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

/// A container's 16-byte key in the shard's queue gauges (S3b-DESIGN §8.3):
/// the first half of the SHA-256 of its id.
pub(crate) fn container_key(container_id: &str) -> [u8; 16] {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(container_id.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// The range row holding `offset`.
pub(crate) fn range(hash: &[u8; 16], offset: u64) -> Vec<u8> {
    by_offset(hash, RANGE, super::range::index_of(offset))
}

pub(crate) fn dedup(hash: &[u8; 16], tag: &[u8; 16]) -> Vec<u8> {
    let mut k = family(hash, DEDUP);
    k.extend_from_slice(tag);
    k
}

pub(crate) fn dedup_expiry(hash: &[u8; 16], at: i64, tag: &[u8; 16]) -> Vec<u8> {
    let mut k = family(hash, DEDUP_EXPIRY);
    k.extend_from_slice(&u64::try_from(at).unwrap_or(0).to_be_bytes());
    k.extend_from_slice(tag);
    k
}

/// The roster row of the queue `hash` due at `at` (a negative time sorts
/// as zero).
pub(crate) fn roster(at: i64, hash: &[u8; 16]) -> Vec<u8> {
    let mut k = roster_prefix();
    k.extend_from_slice(&u64::try_from(at).unwrap_or(0).to_be_bytes());
    k.extend_from_slice(hash);
    k
}

/// The (deadline, queue) a roster row's key names.
pub(crate) fn decode_roster(key: &[u8]) -> Option<(i64, [u8; 16])> {
    let rest = key.strip_prefix(&roster_prefix()[..])?;
    let (at, hash) = rest.split_first_chunk::<8>()?;
    Some((
        i64::try_from(u64::from_be_bytes(*at)).ok()?,
        hash.try_into().ok()?,
    ))
}

/// Tests: the deadline of the queue `hash`'s roster row, if it has one (a
/// test knows its hash, not its deadline).
#[cfg(test)]
pub(crate) async fn rostered(db: &slatedb::Db, hash: &[u8; 16]) -> Option<i64> {
    roster_rows(db).await.get(hash).copied()
}

/// Tests: every roster row, by queue: its deadline.
#[cfg(test)]
pub(crate) async fn roster_rows(db: &slatedb::Db) -> std::collections::BTreeMap<[u8; 16], i64> {
    let mut out = std::collections::BTreeMap::new();
    let Ok(mut rows) = db.scan_prefix(roster_prefix(), ..).await else {
        return out;
    };
    while let Ok(Some(row)) = rows.next().await {
        if let Some((at, hash)) = decode_roster(&row.key) {
            out.insert(hash, at);
        }
    }
    out
}

/// (time, offset) of a time-ordered index key under `prefix`.
pub(crate) fn decode_time_key(prefix_len: usize, key: &[u8]) -> Option<(i64, u64)> {
    let rest = key.get(prefix_len..)?;
    if rest.len() != 16 {
        return None;
    }
    let at = u64::from_be_bytes(rest[..8].try_into().ok()?);
    let off = u64::from_be_bytes(rest[8..].try_into().ok()?);
    Some((i64::try_from(at).ok()?, off))
}

/// The offset of a by-offset key under `prefix`.
pub(crate) fn decode_offset_key(prefix_len: usize, key: &[u8]) -> Option<u64> {
    let rest = key.get(prefix_len..)?;
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

/// The lane a message lives on (SCALING-DESIGN §6.2): ids, lease receipts
/// and dedup entries carry it, so a hot queue's later lanes need no new
/// format. Only lane 0 exists; an id naming another lane resolves to
/// nothing.
pub(crate) const LANE: u8 = 0;

/// Public message ids: `msg_` + base32 of (incarnation tag, lane, offset).
/// The incarnation tag is the low 32 bits of the queue's entity hash, all
/// identity bits (its leading bytes are route bits, the same for every
/// incarnation of a name), so an id from an earlier incarnation of the same
/// name does not resolve (§4).
pub(crate) fn message_id(hash: &[u8; 16], offset: u64) -> String {
    id_string("msg_", &base32::<13, 21>(&message_raw(hash, offset)))
}

pub(crate) fn parse_message_id(hash: &[u8; 16], id: &str) -> Option<u64> {
    message_offset(hash, &unbase32::<13>(id.strip_prefix("msg_")?.as_bytes())?)
}

/// A message id's bytes: incarnation tag, lane, offset.
fn message_raw(hash: &[u8; 16], offset: u64) -> [u8; 13] {
    let mut raw = [0u8; 13];
    raw[..4].copy_from_slice(&hash[12..]);
    raw[4] = LANE;
    raw[5..].copy_from_slice(&offset.to_be_bytes());
    raw
}

/// The offset a message id's bytes name under `hash`: none for another
/// incarnation or another lane.
fn message_offset(hash: &[u8; 16], raw: &[u8; 13]) -> Option<u64> {
    if raw[..4] != hash[12..] || raw[4] != LANE {
        return None;
    }
    Some(u64::from_be_bytes(raw[5..].try_into().ok()?))
}

/// Lease tokens: `lt_` + base32 of (incarnation tag, lane, offset, lease
/// gen).
pub(crate) fn lease_token(hash: &[u8; 16], token: super::Token) -> String {
    id_string("lt_", &base32::<17, 28>(&lease_raw(hash, token)))
}

pub(crate) fn parse_lease_token(hash: &[u8; 16], s: &str) -> Option<super::Token> {
    lease_of(hash, &unbase32::<17>(s.strip_prefix("lt_")?.as_bytes())?)
}

/// A lease token's bytes: incarnation tag, lane, offset, lease gen.
fn lease_raw(hash: &[u8; 16], token: super::Token) -> [u8; 17] {
    let mut raw = [0u8; 17];
    raw[..4].copy_from_slice(&hash[12..]);
    raw[4] = LANE;
    raw[5..13].copy_from_slice(&token.offset.to_be_bytes());
    raw[13..].copy_from_slice(&token.lease_gen.to_be_bytes());
    raw
}

/// The lease a token's bytes name under `hash`: none for another
/// incarnation or another lane.
fn lease_of(hash: &[u8; 16], raw: &[u8; 17]) -> Option<super::Token> {
    if raw[..4] != hash[12..] || raw[4] != LANE {
        return None;
    }
    Some(super::Token {
        offset: u64::from_be_bytes(raw[5..13].try_into().ok()?),
        lease_gen: u32::from_be_bytes(raw[13..].try_into().ok()?),
    })
}

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Each byte's value in `ALPHABET`, or `NOT_BASE32`: one table read per
/// character instead of a search of the alphabet.
const DECODE: [u8; 256] = {
    let mut table = [NOT_BASE32; 256];
    let mut value: u8 = 0;
    while (value as usize) < ALPHABET.len() {
        table[ALPHABET[value as usize] as usize] = value;
        value += 1;
    }
    table
};
const NOT_BASE32: u8 = 0xFF;

/// A queue's backoff jitter seed (`StateRow::jitter`): the entity hash's last eight bytes, all
/// identity bits, so queues whose route bytes agree still jitter apart; odd, as xorshift needs.
pub(crate) fn jitter_seed(hash: &[u8; 16]) -> u64 {
    let mut seed = [0u8; 8];
    seed.copy_from_slice(&hash[8..]);
    u64::from_le_bytes(seed) | 1
}

/// `prefix` followed by `chars` (base32 characters, all ASCII).
fn id_string(prefix: &str, chars: &[u8]) -> String {
    let mut out = String::with_capacity(prefix.len() + chars.len());
    out.push_str(prefix);
    out.extend(chars.iter().map(|&c| char::from(c)));
    out
}

/// The base32 characters of `N` bytes: `M` of them, five bits each, the
/// last padded with zero bits. Fixed sizes and no heap, so KANI-111 checks
/// the codec itself (over a `String` it ran out of time and memory).
fn base32<const N: usize, const M: usize>(raw: &[u8; N]) -> [u8; M] {
    const { assert!(M == (N * 8).div_ceil(5)) };
    let mut out = [0u8; M];
    let (mut acc, mut bits, mut at) = (0u32, 0u32, 0usize);
    for &b in raw {
        acc = (acc << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out[at] = ALPHABET[((acc >> bits) & 31) as usize];
            at += 1;
        }
    }
    if bits > 0 {
        out[at] = ALPHABET[((acc << (5 - bits)) & 31) as usize];
    }
    out
}

/// The `N` bytes base32 characters `s` decode to: `s` must be all base32
/// and give exactly `N` whole bytes (`5 * len / 8 == N`); bits beyond the
/// last whole byte are ignored, as they always were.
fn unbase32<const N: usize>(s: &[u8]) -> Option<[u8; N]> {
    if s.len() * 5 / 8 != N {
        return None;
    }
    let mut out = [0u8; N];
    let (mut acc, mut bits, mut at) = (0u32, 0u32, 0usize);
    for &c in s {
        let v = DECODE[usize::from(c)];
        if v == NOT_BASE32 {
            return None;
        }
        acc = (acc << 5) | u32::from(v);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out[at] = ((acc >> bits) & 0xff) as u8;
            at += 1;
        }
    }
    Some(out)
}

#[cfg(kani)]
mod proofs;
