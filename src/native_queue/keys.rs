//! Queue row keys (docs/QUEUES-DESIGN.md §5): every row of a queue lives
//! under `hash ‖ 'Q' ‖ family`, big-endian so key order is numeric order.
//! The message pages themselves keep the stream's page key
//! (`crypto_page::shard_page_key`), one message per page.

pub(crate) const TAG: u8 = b'Q';
/// The queue's state row: configuration, clock, cursor, counters, debt.
pub(crate) const STATE: u8 = b's';
/// Per-message metadata row.
pub(crate) const META: u8 = b'm';
/// The availability index `A`: (availableAt, offset).
pub(crate) const AVAIL: u8 = b'a';
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

/// The shard-wide queue roster: one row per queue with pending work,
/// keyed under the dirty sentinel so a new owner finds it by one prefix
/// scan (§6.8). Value: the next deadline, i64 big-endian.
pub(crate) const ROSTER_PREFIX: [u8; 18] = {
    let mut k = [0xFFu8; 18];
    k[16] = TAG;
    k[17] = b'R';
    k
};

pub(crate) fn family(hash: &[u8; 16], family: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(34);
    k.extend_from_slice(hash);
    k.push(TAG);
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

pub(crate) fn roster(hash: &[u8; 16]) -> Vec<u8> {
    let mut k = ROSTER_PREFIX.to_vec();
    k.extend_from_slice(hash);
    k
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

/// Public message ids: `msg_` + base32 of (incarnation tag, offset). The
/// incarnation tag is the low 32 bits of the stream's storage hash, so an
/// id from an earlier incarnation of the same name does not resolve (§4).
pub(crate) fn message_id(hash: &[u8; 16], offset: u64) -> String {
    let mut raw = [0u8; 12];
    raw[..4].copy_from_slice(&hash[..4]);
    raw[4..].copy_from_slice(&offset.to_be_bytes());
    format!("msg_{}", base32(&raw))
}

pub(crate) fn parse_message_id(hash: &[u8; 16], id: &str) -> Option<u64> {
    let raw = unbase32(id.strip_prefix("msg_")?)?;
    if raw.len() != 12 || raw[..4] != hash[..4] {
        return None;
    }
    Some(u64::from_be_bytes(raw[4..].try_into().ok()?))
}

/// Lease tokens: `lt_` + base32 of (incarnation tag, offset, lease gen).
pub(crate) fn lease_token(hash: &[u8; 16], token: super::Token) -> String {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&hash[..4]);
    raw[4..12].copy_from_slice(&token.offset.to_be_bytes());
    raw[12..].copy_from_slice(&token.lease_gen.to_be_bytes());
    format!("lt_{}", base32(&raw))
}

pub(crate) fn parse_lease_token(hash: &[u8; 16], s: &str) -> Option<super::Token> {
    let raw = unbase32(s.strip_prefix("lt_")?)?;
    if raw.len() != 16 || raw[..4] != hash[..4] {
        return None;
    }
    Some(super::Token {
        offset: u64::from_be_bytes(raw[4..12].try_into().ok()?),
        lease_gen: u32::from_be_bytes(raw[12..].try_into().ok()?),
    })
}

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

fn base32(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 8 / 5 + 1);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &b in raw {
        acc = (acc << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((acc >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((acc << (5 - bits)) & 31) as usize] as char);
    }
    out
}

fn unbase32(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = u32::try_from(ALPHABET.iter().position(|&a| a == c)?).ok()?;
        acc = (acc << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    Some(out)
}
