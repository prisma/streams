//! Row codecs (docs/QUEUES-DESIGN.md §5.1-§5.3, §8.2). The state row is
//! JSON (one per queue, read once per group); the per-message rows are
//! fixed-width little-endian so a message's rows stay small.
use serde::{Deserialize, Serialize};

use super::QueueConfig;

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

/// The state row layout this build writes and the only one it reads.
pub(crate) const STATE_ROW_VERSION: u32 = 2;

/// The queue's state row (§5.3, §5.5, §8.2): one per queue, written by
/// every batch that changes queue state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StateRow {
    pub v: u32,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
}

impl StateRow {
    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    /// A stored state row of this build's layout; any other version is
    /// refused (one layout, no legacy decoders).
    pub(crate) fn decode(raw: &[u8]) -> Result<Self, String> {
        let row: Self = serde_json::from_slice(raw).map_err(|e| format!("queue state row: {e}"))?;
        if row.v != STATE_ROW_VERSION {
            return Err(format!(
                "queue state row: version {} is not {STATE_ROW_VERSION}",
                row.v
            ));
        }
        Ok(row)
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
