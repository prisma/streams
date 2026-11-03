//! Queue row keys (docs/QUEUES-DESIGN.md §5; layout 8, S3a-DESIGN §2.3):
//! every row of a queue is `hash ‖ 'Q' ‖ family ‖ suffix`, big-endian so key
//! order is numeric order, and a queue's rows are one range beside its
//! hash's other rows. The message pages are the `'p'` family (`page`), one
//! message per page, and the queue stream's tail is `shard::queue_tail_key`.
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
/// A message's sealed page, by its last offset (`page`): one message per
/// page, so the page's last offset is the message's.
pub(crate) const PAGE: u8 = b'p';

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

/// The message page of the queue `hash` whose last offset is `last`.
pub(crate) fn page(hash: &[u8; 16], last: u64) -> Vec<u8> {
    by_offset(hash, PAGE, last)
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
    let mut raw = [0u8; 13];
    raw[..4].copy_from_slice(&hash[12..]);
    raw[4] = LANE;
    raw[5..].copy_from_slice(&offset.to_be_bytes());
    format!("msg_{}", base32(&raw))
}

pub(crate) fn parse_message_id(hash: &[u8; 16], id: &str) -> Option<u64> {
    let raw = unbase32(id.strip_prefix("msg_")?)?;
    if raw.len() != 13 || raw[..4] != hash[12..] || raw[4] != LANE {
        return None;
    }
    Some(u64::from_be_bytes(raw[5..].try_into().ok()?))
}

/// Lease tokens: `lt_` + base32 of (incarnation tag, lane, offset, lease
/// gen).
pub(crate) fn lease_token(hash: &[u8; 16], token: super::Token) -> String {
    let mut raw = [0u8; 17];
    raw[..4].copy_from_slice(&hash[12..]);
    raw[4] = LANE;
    raw[5..13].copy_from_slice(&token.offset.to_be_bytes());
    raw[13..].copy_from_slice(&token.lease_gen.to_be_bytes());
    format!("lt_{}", base32(&raw))
}

pub(crate) fn parse_lease_token(hash: &[u8; 16], s: &str) -> Option<super::Token> {
    let raw = unbase32(s.strip_prefix("lt_")?)?;
    if raw.len() != 17 || raw[..4] != hash[12..] || raw[4] != LANE {
        return None;
    }
    Some(super::Token {
        offset: u64::from_be_bytes(raw[5..13].try_into().ok()?),
        lease_gen: u32::from_be_bytes(raw[13..].try_into().ok()?),
    })
}

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// A queue's backoff jitter seed (`StateRow::jitter`): the entity hash's last eight bytes, all
/// identity bits, so queues whose route bytes agree still jitter apart; odd, as xorshift needs.
pub(crate) fn jitter_seed(hash: &[u8; 16]) -> u64 {
    let mut seed = [0u8; 8];
    seed.copy_from_slice(&hash[8..]);
    u64::from_le_bytes(seed) | 1
}

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
