//! Row codecs (docs/QUEUES-DESIGN.md §5.1-§5.3, §8.2). Every row is
//! little-endian with fixed-width fields, so a message's rows stay small
//! and the state row (one per queue, rewritten by every group that changes
//! the queue) costs no JSON.
//!
//! The state row, in order: version u32 (`STATE_ROW_VERSION`, the only one
//! decoded); cfg: retention_ms, failure_retention_ms i64, max_queued_bytes,
//! max_queued_messages u64, dedup_window_ms i64, max_dedup_entries u64,
//! group.max_attempts u32, backoff.initial_ms, backoff.max_ms i64,
//! backoff.multiplier u32, group.lease_ms i64, group.max_in_flight,
//! group.max_batch u32; clock, cursor_at i64, cursor_off u64; a flags byte
//! (bit 0 cursor_started, 1 paused, 2 deleting, 3 roster_at present,
//! 4 wakeup_url present, others zero); queued_messages, queued_bytes,
//! leased, retrying, failed, dedup_entries, gap_count u64; the 11 counters
//! u64 in struct order; debt_bytes u64, debt_oldest_at i64,
//! debt_generation u64; wakeup_url (u32 length + UTF-8) when present;
//! wakeup_name (u32 length + UTF-8); wakeup_version u32;
//! last_useful_claim_at, last_notified_at, notify_backoff_ms i64; jitter
//! u64; floors avail, leases, retries, fail_expiry, dedup_expiry,
//! retry_cycle i64; roster_at i64 when present.
use serde::{Deserialize, Serialize};

use super::{Backoff, GroupConfig, QueueConfig};

/// The state row's layout version: the first four bytes of every state
/// row, and the only one `StateRow::decode` accepts.
pub(crate) const STATE_ROW_VERSION: u32 = 3;
/// The state row's length without its variable parts (the wake-up URL and
/// name bytes, the URL's length, `roster_at`).
const STATE_ROW_FIXED: usize = 373;
const FLAG_CURSOR_STARTED: u8 = 1;
const FLAG_PAUSED: u8 = 1 << 1;
const FLAG_DELETING: u8 = 1 << 2;
const FLAG_ROSTER: u8 = 1 << 3;
const FLAG_WAKEUP_URL: u8 = 1 << 4;
const FLAGS_KNOWN: u8 = (1 << 5) - 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Counters {
    pub sent: u64,
    pub delivered: u64,
    pub acked: u64,
    pub retried: u64,
    pub failed: u64,
    pub expired: u64,
    pub discarded: u64,
    pub cancelled: u64,
    pub redriven: u64,
    pub stale: u64,
    pub duplicates: u64,
}

impl Counters {
    fn put(&self, v: &mut Vec<u8>) {
        v.extend_from_slice(
            [
                self.sent,
                self.delivered,
                self.acked,
                self.retried,
                self.failed,
                self.expired,
                self.discarded,
                self.cancelled,
                self.redriven,
                self.stale,
                self.duplicates,
            ]
            .map(u64::to_le_bytes)
            .as_flattened(),
        );
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, String> {
        Ok(Self {
            sent: r.u64()?,
            delivered: r.u64()?,
            acked: r.u64()?,
            retried: r.u64()?,
            failed: r.u64()?,
            expired: r.u64()?,
            discarded: r.u64()?,
            cancelled: r.u64()?,
            redriven: r.u64()?,
            stale: r.u64()?,
            duplicates: r.u64()?,
        })
    }
}

/// The queue's state row (§5.3, §5.5, §8.2): one per queue, written by
/// every batch that changes queue state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StateRow {
    pub cfg: QueueConfig,
    /// The queue clock (§5.5): monotone, written with every state change.
    pub clock: i64,
    /// The default group's cursor `C` over the availability index.
    pub cursor_at: i64,
    pub cursor_off: u64,
    /// Whether any message was ever taken past `C` (a fresh cursor is
    /// before everything).
    pub cursor_started: bool,
    pub paused: bool,
    pub deleting: bool,
    /// Live undone messages and their record bytes (the admission budget,
    /// §6.6): sent and not yet done or failed.
    pub queued_messages: u64,
    pub queued_bytes: u64,
    pub leased: u64,
    pub retrying: u64,
    pub failed: u64,
    pub dedup_entries: u64,
    pub gap_count: u64,
    pub counters: Counters,
    /// Deletion debt (§8.2 stage 2): bytes deleted and not yet collected.
    pub debt_bytes: u64,
    pub debt_oldest_at: i64,
    pub debt_generation: u64,
    /// Wake-up state (§9.2).
    pub wakeup_url: Option<String>,
    /// The queue's name, for notification bodies.
    pub wakeup_name: String,
    pub wakeup_version: u32,
    pub last_useful_claim_at: i64,
    pub last_notified_at: i64,
    pub notify_backoff_ms: i64,
    /// The rng state for backoff jitter (deterministic per queue).
    pub jitter: u64,
    /// Where each time-ordered index's live entries begin (scans start
    /// here, past the dead keys before it).
    pub floors: Floors,
    /// The roster value this row's batch last wrote (`None`: off the
    /// roster).
    pub roster_at: Option<i64>,
}

/// Lower bounds on the time of each time-ordered index's first live entry
/// (`i64::MAX`: no live entry). Invariant F: every live entry of a family
/// has time >= its floor (for `avail`, every live entry past the cursor).
/// Every put lowers a floor (`NativeStage::put`); a scan may raise it to
/// the first live entry it found, written in the batch whose deletes
/// justified it; deletes never touch it, as a low floor is only slower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Floors {
    pub avail: i64,
    pub leases: i64,
    pub retries: i64,
    pub fail_expiry: i64,
    pub dedup_expiry: i64,
    /// The earliest cycle deadline among retry entries: the retry step of
    /// the sweep has work only there (§6.3).
    pub retry_cycle: i64,
}

impl Default for Floors {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl Floors {
    /// A new queue's floors: every index is empty.
    pub(crate) const EMPTY: Self = Self {
        avail: i64::MAX,
        leases: i64::MAX,
        retries: i64::MAX,
        fail_expiry: i64::MAX,
        dedup_expiry: i64::MAX,
        retry_cycle: i64::MAX,
    };

    /// The floor of a time-ordered family, if it has one.
    pub(crate) fn of(&mut self, fam: u8) -> Option<&mut i64> {
        use super::keys;
        match fam {
            keys::AVAIL => Some(&mut self.avail),
            keys::LEASES => Some(&mut self.leases),
            keys::RETRIES => Some(&mut self.retries),
            keys::FAIL_EXPIRY => Some(&mut self.fail_expiry),
            keys::DEDUP_EXPIRY => Some(&mut self.dedup_expiry),
            _ => None,
        }
    }

    fn put(&self, v: &mut Vec<u8>) {
        v.extend_from_slice(
            [
                self.avail,
                self.leases,
                self.retries,
                self.fail_expiry,
                self.dedup_expiry,
                self.retry_cycle,
            ]
            .map(i64::to_le_bytes)
            .as_flattened(),
        );
    }
    fn read(r: &mut Reader<'_>) -> Result<Self, String> {
        Ok(Self {
            avail: r.i64()?,
            leases: r.i64()?,
            retries: r.i64()?,
            fail_expiry: r.i64()?,
            dedup_expiry: r.i64()?,
            retry_cycle: r.i64()?,
        })
    }
}

impl StateRow {
    /// The row in the module's layout, `STATE_ROW_VERSION` first.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let url = self.wakeup_url.as_deref();
        let mut v = Vec::with_capacity(
            STATE_ROW_FIXED
                + url.map_or(0, |u| 4 + u.len())
                + self.wakeup_name.len()
                + self.roster_at.map_or(0, |_| 8),
        );
        v.extend_from_slice(&STATE_ROW_VERSION.to_le_bytes());
        put_config(&mut v, &self.cfg);
        v.extend_from_slice(
            [
                self.clock.to_le_bytes(),
                self.cursor_at.to_le_bytes(),
                self.cursor_off.to_le_bytes(),
            ]
            .as_flattened(),
        );
        v.push(self.flags());
        v.extend_from_slice(
            [
                self.queued_messages,
                self.queued_bytes,
                self.leased,
                self.retrying,
                self.failed,
                self.dedup_entries,
                self.gap_count,
            ]
            .map(u64::to_le_bytes)
            .as_flattened(),
        );
        self.counters.put(&mut v);
        v.extend_from_slice(
            [
                self.debt_bytes.to_le_bytes(),
                self.debt_oldest_at.to_le_bytes(),
                self.debt_generation.to_le_bytes(),
            ]
            .as_flattened(),
        );
        if let Some(url) = url {
            put_str(&mut v, url);
        }
        put_str(&mut v, &self.wakeup_name);
        v.extend_from_slice(&self.wakeup_version.to_le_bytes());
        v.extend_from_slice(
            [
                self.last_useful_claim_at.to_le_bytes(),
                self.last_notified_at.to_le_bytes(),
                self.notify_backoff_ms.to_le_bytes(),
                self.jitter.to_le_bytes(),
            ]
            .as_flattened(),
        );
        self.floors.put(&mut v);
        if let Some(at) = self.roster_at {
            v.extend_from_slice(&at.to_le_bytes());
        }
        v
    }

    fn flags(&self) -> u8 {
        let mut flags = 0;
        for (set, bit) in [
            (self.cursor_started, FLAG_CURSOR_STARTED),
            (self.paused, FLAG_PAUSED),
            (self.deleting, FLAG_DELETING),
            (self.roster_at.is_some(), FLAG_ROSTER),
            (self.wakeup_url.is_some(), FLAG_WAKEUP_URL),
        ] {
            if set {
                flags |= bit;
            }
        }
        flags
    }

    /// A row of `STATE_ROW_VERSION`, exactly as long as its layout.
    pub(crate) fn decode(raw: &[u8]) -> Result<Self, String> {
        let mut r = Reader::new(raw);
        let version = r.u32()?;
        if version != STATE_ROW_VERSION {
            return Err(format!("queue state row: version {version}"));
        }
        let cfg = read_config(&mut r)?;
        let clock = r.i64()?;
        let cursor_at = r.i64()?;
        let cursor_off = r.u64()?;
        let flags = r.u8()?;
        if flags & !FLAGS_KNOWN != 0 {
            return Err(format!("queue state row: flags {flags:#04x}"));
        }
        let queued_messages = r.u64()?;
        let queued_bytes = r.u64()?;
        let leased = r.u64()?;
        let retrying = r.u64()?;
        let failed = r.u64()?;
        let dedup_entries = r.u64()?;
        let gap_count = r.u64()?;
        let counters = Counters::read(&mut r)?;
        let debt_bytes = r.u64()?;
        let debt_oldest_at = r.i64()?;
        let debt_generation = r.u64()?;
        let wakeup_url = if flags & FLAG_WAKEUP_URL != 0 {
            Some(r.string()?)
        } else {
            None
        };
        let wakeup_name = r.string()?;
        let wakeup_version = r.u32()?;
        let last_useful_claim_at = r.i64()?;
        let last_notified_at = r.i64()?;
        let notify_backoff_ms = r.i64()?;
        let jitter = r.u64()?;
        let floors = Floors::read(&mut r)?;
        let roster_at = if flags & FLAG_ROSTER != 0 {
            Some(r.i64()?)
        } else {
            None
        };
        r.end()?;
        Ok(Self {
            cfg,
            clock,
            cursor_at,
            cursor_off,
            cursor_started: flags & FLAG_CURSOR_STARTED != 0,
            paused: flags & FLAG_PAUSED != 0,
            deleting: flags & FLAG_DELETING != 0,
            queued_messages,
            queued_bytes,
            leased,
            retrying,
            failed,
            dedup_entries,
            gap_count,
            counters,
            debt_bytes,
            debt_oldest_at,
            debt_generation,
            wakeup_url,
            wakeup_name,
            wakeup_version,
            last_useful_claim_at,
            last_notified_at,
            notify_backoff_ms,
            jitter,
            floors,
            roster_at,
        })
    }
    /// Next jitter sample in millionths (xorshift; the state row carries
    /// its state).
    pub(crate) fn next_jitter(&mut self) -> u64 {
        let mut x = self.jitter | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.jitter = x;
        x % super::JITTER_SCALE
    }
}

/// The queue configuration's 88 bytes of the state row.
fn put_config(v: &mut Vec<u8>, c: &QueueConfig) {
    let g = &c.group;
    v.extend_from_slice(
        [
            c.retention_ms.to_le_bytes(),
            c.failure_retention_ms.to_le_bytes(),
            c.max_queued_bytes.to_le_bytes(),
            c.max_queued_messages.to_le_bytes(),
            c.dedup_window_ms.to_le_bytes(),
            c.max_dedup_entries.to_le_bytes(),
        ]
        .as_flattened(),
    );
    v.extend_from_slice(&g.max_attempts.to_le_bytes());
    v.extend_from_slice(
        [
            g.backoff.initial_ms.to_le_bytes(),
            g.backoff.max_ms.to_le_bytes(),
        ]
        .as_flattened(),
    );
    v.extend_from_slice(&g.backoff.multiplier.to_le_bytes());
    v.extend_from_slice(&g.lease_ms.to_le_bytes());
    v.extend_from_slice(
        [g.max_in_flight, g.max_batch]
            .map(u32::to_le_bytes)
            .as_flattened(),
    );
}

fn read_config(r: &mut Reader<'_>) -> Result<QueueConfig, String> {
    let retention_ms = r.i64()?;
    let failure_retention_ms = r.i64()?;
    let max_queued_bytes = r.u64()?;
    let max_queued_messages = r.u64()?;
    let dedup_window_ms = r.i64()?;
    let max_dedup_entries = r.u64()?;
    let max_attempts = r.u32()?;
    let initial_ms = r.i64()?;
    let max_ms = r.i64()?;
    let multiplier = r.u32()?;
    let lease_ms = r.i64()?;
    let max_in_flight = r.u32()?;
    let max_batch = r.u32()?;
    Ok(QueueConfig {
        retention_ms,
        failure_retention_ms,
        max_queued_bytes,
        max_queued_messages,
        dedup_window_ms,
        max_dedup_entries,
        group: GroupConfig {
            max_attempts,
            backoff: Backoff {
                initial_ms,
                max_ms,
                multiplier,
            },
            lease_ms,
            max_in_flight,
            max_batch,
        },
    })
}

/// A u32 length and the string's bytes (a wake-up URL or queue name is far
/// below 4 GiB: the request that set it was bounded).
fn put_str(v: &mut Vec<u8>, s: &str) {
    v.extend_from_slice(&u32::try_from(s.len()).unwrap_or(u32::MAX).to_le_bytes());
    v.extend_from_slice(s.as_bytes());
}

/// An availability entry's value (§5.2): the message's metadata as sent
/// (everything a claim of an untaken message needs) and, when it is at
/// most `INLINE_PAGE_MAX` long, its sealed page.
pub(crate) fn encode_avail(meta: &MetaRow, page: &[u8]) -> Vec<u8> {
    let meta = meta.encode();
    let mut v = Vec::with_capacity(1 + meta.len() + page.len());
    v.push(u8::try_from(meta.len()).unwrap_or(u8::MAX));
    v.extend_from_slice(&meta);
    v.extend_from_slice(page);
    v
}

/// The metadata and the inline page (empty when not inline) of an
/// availability entry's value.
pub(crate) fn decode_avail(raw: &[u8]) -> Result<(MetaRow, &[u8]), String> {
    let (&len, rest) = raw.split_first().ok_or("empty availability entry")?;
    let at = usize::from(len).min(rest.len());
    let (meta, page) = rest.split_at(at);
    Ok((MetaRow::decode(meta)?, page))
}

/// The per-message metadata row (§5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetaRow {
    pub enqueued_at: i64,
    pub available_at: i64,
    pub retention_end: i64,
    pub hard_expires_at: i64,
    pub bytes: u32,
    /// Open deliveries (leased, retrying, failed) of the message.
    pub open: u16,
    pub cancelled: bool,
    /// Whether the default group has taken it (its cursor passed it).
    pub taken: bool,
    pub dedup_tag: Option<[u8; 16]>,
}

impl MetaRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(54);
        v.extend_from_slice(&self.enqueued_at.to_le_bytes());
        v.extend_from_slice(&self.available_at.to_le_bytes());
        v.extend_from_slice(&self.retention_end.to_le_bytes());
        v.extend_from_slice(&self.hard_expires_at.to_le_bytes());
        v.extend_from_slice(&self.bytes.to_le_bytes());
        v.extend_from_slice(&self.open.to_le_bytes());
        let flags = u8::from(self.cancelled)
            | (u8::from(self.taken) << 1)
            | (u8::from(self.dedup_tag.is_some()) << 2);
        v.push(flags);
        if let Some(tag) = &self.dedup_tag {
            v.extend_from_slice(tag);
        }
        v
    }
    pub(crate) fn decode(raw: &[u8]) -> Result<Self, String> {
        let r = Reader::new(raw);
        let mut r = r;
        let enqueued_at = r.i64()?;
        let available_at = r.i64()?;
        let retention_end = r.i64()?;
        let hard_expires_at = r.i64()?;
        let bytes = r.u32()?;
        let open = r.u16()?;
        let flags = r.u8()?;
        let dedup_tag = if flags & 4 != 0 { Some(r.tag()?) } else { None };
        r.end()?;
        Ok(Self {
            enqueued_at,
            available_at,
            retention_end,
            hard_expires_at,
            bytes,
            open,
            cancelled: flags & 1 != 0,
            taken: flags & 2 != 0,
            dedup_tag,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DState {
    Leased = 1,
    Retrying = 2,
    Failed = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cause {
    Explicit = 1,
    Exhausted = 2,
    Retention = 3,
    LeaseExpired = 4,
}

impl Cause {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Cause::Explicit => "explicit",
            Cause::Exhausted => "exhausted",
            Cause::Retention => "retention",
            Cause::LeaseExpired => "lease_expired",
        }
    }
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Cause::Explicit,
            2 => Cause::Exhausted,
            3 => Cause::Retention,
            4 => Cause::LeaseExpired,
            _ => return None,
        })
    }
}

/// The default group's delivery row for one message (§5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryRow {
    pub state: DState,
    pub cycle: u32,
    pub cycle_deadline: i64,
    pub attempts: u32,
    pub releases: u8,
    pub lease_gen: u32,
    pub lease_deadline: i64,
    /// When it is due again (retrying), or 0.
    pub due_at: i64,
    pub first_delivered_at: i64,
    pub failed_at: i64,
    pub failure_expires_at: i64,
    pub cause: Option<Cause>,
    pub code: Option<String>,
}

impl DeliveryRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(64);
        v.push(self.state as u8);
        v.extend_from_slice(&self.cycle.to_le_bytes());
        v.extend_from_slice(&self.cycle_deadline.to_le_bytes());
        v.extend_from_slice(&self.attempts.to_le_bytes());
        v.push(self.releases);
        v.extend_from_slice(&self.lease_gen.to_le_bytes());
        v.extend_from_slice(&self.lease_deadline.to_le_bytes());
        v.extend_from_slice(&self.due_at.to_le_bytes());
        v.extend_from_slice(&self.first_delivered_at.to_le_bytes());
        v.extend_from_slice(&self.failed_at.to_le_bytes());
        v.extend_from_slice(&self.failure_expires_at.to_le_bytes());
        v.push(self.cause.map_or(0, |c| c as u8));
        let code = self.code.as_deref().unwrap_or("").as_bytes();
        let code = code.get(..64).unwrap_or(code);
        v.push(u8::try_from(code.len()).unwrap_or(64));
        v.extend_from_slice(code);
        v
    }
    pub(crate) fn decode(raw: &[u8]) -> Result<Self, String> {
        let mut r = Reader::new(raw);
        let state = match r.u8()? {
            1 => DState::Leased,
            2 => DState::Retrying,
            3 => DState::Failed,
            other => return Err(format!("queue delivery row: state {other}")),
        };
        let cycle = r.u32()?;
        let cycle_deadline = r.i64()?;
        let attempts = r.u32()?;
        let releases = r.u8()?;
        let lease_gen = r.u32()?;
        let lease_deadline = r.i64()?;
        let due_at = r.i64()?;
        let first_delivered_at = r.i64()?;
        let failed_at = r.i64()?;
        let failure_expires_at = r.i64()?;
        let cause = Cause::from_u8(r.u8()?);
        let len = r.u8()? as usize;
        let code = r.bytes(len)?;
        r.end()?;
        Ok(Self {
            state,
            cycle,
            cycle_deadline,
            attempts,
            releases,
            lease_gen,
            lease_deadline,
            due_at,
            first_delivered_at,
            failed_at,
            failure_expires_at,
            cause,
            code: if code.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(code).into_owned())
            },
        })
    }
}

/// A dedup entry: the original offset and when the entry expires (§8.4).
pub(crate) fn encode_dedup(offset: u64, expires_at: i64) -> Vec<u8> {
    let mut v = offset.to_le_bytes().to_vec();
    v.extend_from_slice(&expires_at.to_le_bytes());
    v
}

pub(crate) fn decode_dedup(raw: &[u8]) -> Result<(u64, i64), String> {
    let mut r = Reader::new(raw);
    let off = r.u64()?;
    let at = r.i64()?;
    r.end()?;
    Ok((off, at))
}

struct Reader<'a> {
    raw: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, at: 0 }
    }
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).ok_or("queue row: length overflow")?;
        let out = self.raw.get(self.at..end).ok_or("queue row: truncated")?;
        self.at = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().map_err(|_| "queue row")?,
        ))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().map_err(|_| "queue row")?,
        ))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().map_err(|_| "queue row")?,
        ))
    }
    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_le_bytes(
            self.bytes(8)?.try_into().map_err(|_| "queue row")?,
        ))
    }
    /// A u32 length and that many bytes of UTF-8.
    fn string(&mut self) -> Result<String, String> {
        let len = usize::try_from(self.u32()?).map_err(|_| "queue row: length")?;
        String::from_utf8(self.bytes(len)?.to_vec()).map_err(|_| "queue row: not UTF-8".to_owned())
    }
    fn tag(&mut self) -> Result<[u8; 16], String> {
        self.bytes(16)?
            .try_into()
            .map_err(|_| "queue row: tag".to_owned())
    }
    fn end(&self) -> Result<(), String> {
        if self.at == self.raw.len() {
            Ok(())
        } else {
            Err("queue row: trailing bytes".to_owned())
        }
    }
}
