//! The descriptor cache's retained tier: what an expired or evicted hot
//! slot leaves behind so the next read of the stream revalidates with a
//! conditional GET (a 304 is unbilled) instead of reading the body again.
//!
//! An entry is an ordinary descriptor compacted to its serving fields and
//! the ETag it was served under, keyed by the stream's route hash (its
//! project and name). It never serves a read: a read that finds one hands
//! its ETag and rebuilt descriptor to the store as `If-None-Match`, exactly
//! as an expired hot slot does, and only a 304 makes the rebuilt descriptor
//! the answer. A descriptor that is not ordinary (initializing, forked or
//! forking, segmented, sealing, with watches, another layout) or that was
//! served without an ETag is not retained; it pays the GET as before.
//! The rebuilt descriptor is validated again (`StreamDesc::try_from`), so a
//! wrong compaction fails closed and the read goes unconditional.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::registry::{CachedDesc, LAYOUT_VERSION, PersistedDescriptor, StreamDesc};
use crate::tenant::TenantStreamRef;

/// The retained tier's byte budget until it is configurable: 64 MiB.
pub(super) const RETAIN_BYTES: usize = 64 << 20;

/// A short token: 16 bytes of lowercase hex (the epoch, the key
/// fingerprint), a quoted one (an S3/Tigris MD5 ETag), or the text as it
/// came.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Hex([u8; 16]),
    QuotedHex([u8; 16]),
    Raw(Box<str>),
}

impl Token {
    fn of(text: &str) -> Self {
        if let Some(bytes) = hex16(text) {
            Token::Hex(bytes)
        } else if let Some(bytes) = text
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .and_then(hex16)
        {
            Token::QuotedHex(bytes)
        } else {
            Token::Raw(text.into())
        }
    }

    fn text(&self) -> String {
        match self {
            Token::Hex(b) => crate::crypto::hex(b),
            Token::QuotedHex(b) => format!("\"{}\"", crate::crypto::hex(b)),
            Token::Raw(t) => t.to_string(),
        }
    }

    fn heap(&self) -> usize {
        match self {
            Token::Raw(t) => t.len().next_multiple_of(8),
            Token::Hex(_) | Token::QuotedHex(_) => 0,
        }
    }
}

/// 16 bytes from exactly 32 lowercase hex digits (the form `crypto::hex`
/// writes, so the text round-trips byte for byte).
fn hex16(text: &str) -> Option<[u8; 16]> {
    let raw = text.as_bytes();
    if raw.len() != 32 || !raw.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; 16];
    for (byte, pair) in out.iter_mut().zip(raw.as_chunks::<2>().0) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}

/// The watch-signature verifier: 32 bytes of standard base64 (what create
/// writes), or the text as it came.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Verifier {
    Key([u8; 32]),
    Raw(Box<str>),
}

impl Verifier {
    fn of(text: &str) -> Self {
        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;
        match engine.decode(text) {
            Ok(bytes) if bytes.len() == 32 && engine.encode(&bytes) == text => {
                let mut key = [0u8; 32];
                key.copy_from_slice(&bytes);
                Verifier::Key(key)
            }
            _ => Verifier::Raw(text.into()),
        }
    }

    fn text(&self) -> String {
        use base64::Engine;
        match self {
            Verifier::Key(k) => base64::engine::general_purpose::STANDARD.encode(k),
            Verifier::Raw(t) => t.to_string(),
        }
    }

    fn heap(&self) -> usize {
        match self {
            Verifier::Raw(t) => t.len().next_multiple_of(8),
            Verifier::Key(_) => 0,
        }
    }
}

const DELETED: u8 = 1;
const SOFT_DELETED: u8 = 2;
const SEALED: u8 = 4;
const PARENT_REF_PENDING: u8 = 8;

/// One retained descriptor: every field an ordinary descriptor serves,
/// compacted, and the ETag it was served under. The name and project are
/// the request's (`TenantStreamRef`), so they are not held.
struct Entry {
    etag: Token,
    epoch: [u8; 16],
    key_fingerprint: Token,
    watch_sig_key: Option<Verifier>,
    account_id: Option<Arc<str>>,
    content_type: Arc<str>,
    seal_op: Option<Box<str>>,
    created_ms: i64,
    expires_at_ms: Option<i64>,
    logical_close_ms: Option<i64>,
    ttl_secs: Option<u64>,
    seal_gen_counter: u64,
    flags: u8,
    /// The tier's keep counter when this entry was kept, for the LRU.
    touched: u64,
}

impl Entry {
    /// The compact form of an ordinary descriptor served under `etag`,
    /// or `None` for one this tier does not hold.
    fn of(d: &PersistedDescriptor, etag: &str, interned: &mut HashSet<Arc<str>>) -> Option<Self> {
        let ordinary = d.init.is_none()
            && d.forked_from.is_none()
            && d.fork_children.is_empty()
            && d.segments.is_none()
            && d.sealing.is_none()
            && d.watch_definitions.is_empty()
            && d.layout_version == LAYOUT_VERSION;
        if !ordinary {
            return None;
        }
        let epoch = hex16(&d.stream_epoch)?;
        let mut intern = |s: &str| -> Arc<str> {
            if let Some(held) = interned.get(s) {
                return held.clone();
            }
            let held: Arc<str> = s.into();
            interned.insert(held.clone());
            held
        };
        // Distinct bits, so their sum is their union.
        let flags = [
            (d.deleted, DELETED),
            (d.soft_deleted, SOFT_DELETED),
            (d.sealed, SEALED),
            (d.parent_ref_pending, PARENT_REF_PENDING),
        ]
        .into_iter()
        .filter(|&(on, _)| on)
        .map(|(_, bit)| bit)
        .sum();
        Some(Entry {
            etag: Token::of(etag),
            epoch,
            key_fingerprint: Token::of(&d.key_fingerprint),
            watch_sig_key: d.watch_sig_key.as_deref().map(Verifier::of),
            account_id: d.account_id.as_deref().map(&mut intern),
            content_type: intern(&d.content_type),
            seal_op: d.seal_op.as_deref().map(Into::into),
            created_ms: d.created_ms,
            expires_at_ms: d.expires_at_ms,
            logical_close_ms: d.logical_close_ms,
            ttl_secs: d.ttl_secs,
            seal_gen_counter: d.seal_gen_counter,
            flags,
            touched: 0,
        })
    }

    /// The descriptor this entry was made from, for the stream `sref`.
    fn rebuild(&self, sref: &TenantStreamRef) -> PersistedDescriptor {
        PersistedDescriptor {
            name: sref.name().as_str().to_string(),
            account_id: self.account_id.as_deref().map(str::to_string),
            project_id: sref.project_id().clone(),
            stream_epoch: crate::crypto::hex(&self.epoch),
            key_fingerprint: self.key_fingerprint.text(),
            created_ms: self.created_ms,
            expires_at_ms: self.expires_at_ms,
            deleted: self.flags & DELETED != 0,
            soft_deleted: self.flags & SOFT_DELETED != 0,
            logical_close_ms: self.logical_close_ms,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            content_type: self.content_type.to_string(),
            ttl_secs: self.ttl_secs,
            segments: None,
            sealed: self.flags & SEALED != 0,
            seal_gen_counter: self.seal_gen_counter,
            sealing: None,
            seal_op: self.seal_op.as_deref().map(str::to_string),
            watch_definitions: Vec::new(),
            parent_ref_pending: self.flags & PARENT_REF_PENDING != 0,
            watch_sig_key: self.watch_sig_key.as_ref().map(Verifier::text),
            layout_version: LAYOUT_VERSION,
        }
    }

    /// What the entry is charged against the budget: itself, its key and
    /// one control byte of the table, and the heap of its raw strings
    /// (interned project-wide strings are charged once, to the table).
    fn bytes(&self) -> usize {
        std::mem::size_of::<Entry>()
            + std::mem::size_of::<[u8; 16]>()
            + 1
            + self.etag.heap()
            + self.key_fingerprint.heap()
            + self.watch_sig_key.as_ref().map_or(0, Verifier::heap)
            + self
                .seal_op
                .as_ref()
                .map_or(0, |s| s.len().next_multiple_of(8))
    }
}

/// The retained descriptors, by route hash, within a byte budget.
pub(super) struct Retained {
    by_route: HashMap<[u8; 16], Entry>,
    /// Account ids and content types, held once however many entries name
    /// them.
    interned: HashSet<Arc<str>>,
    bytes: usize,
    budget: usize,
    /// Keeps so far: each kept entry's `touched`.
    kept: u64,
}

/// The key of `sref` in the tier.
fn route(sref: &TenantStreamRef) -> [u8; 16] {
    crate::crypto::RouteHash::for_stream(sref).0
}

impl Retained {
    pub(super) fn new(budget: usize) -> Self {
        Retained {
            by_route: HashMap::new(),
            interned: HashSet::new(),
            bytes: 0,
            budget,
            kept: 0,
        }
    }

    /// Keep what a hot slot held as it leaves the hot tier: an ordinary
    /// descriptor served with an ETag. Anything else leaves nothing.
    pub(super) fn keep(&mut self, sref: &TenantStreamRef, held: &CachedDesc) {
        let (Some(desc), Some(etag)) = (held.desc.as_ref(), held.etag.as_deref()) else {
            return;
        };
        let Some(mut entry) = Entry::of(desc, etag, &mut self.interned) else {
            return;
        };
        self.kept += 1;
        entry.touched = self.kept;
        self.bytes += entry.bytes();
        if let Some(old) = self.by_route.insert(route(sref), entry) {
            self.bytes -= old.bytes();
        }
        if self.bytes > self.budget {
            self.shed();
        }
    }

    /// The ETag and rebuilt descriptor retained for `sref`, taken out of
    /// the tier (the read that takes it fills a hot slot); `None` when
    /// nothing is retained or the rebuilt descriptor does not validate.
    pub(super) fn take(&mut self, sref: &TenantStreamRef) -> Option<(String, StreamDesc)> {
        let entry = self.by_route.remove(&route(sref))?;
        self.bytes -= entry.bytes();
        let desc = StreamDesc::try_from(entry.rebuild(sref)).ok()?;
        Some((entry.etag.text(), desc))
    }

    /// Hold at most `budget` bytes from now on, shedding the least recently
    /// kept entries while over it. Each shed takes at least one entry, so
    /// as many sheds as entries empty the tier: the loop is bounded.
    pub(super) fn set_budget(&mut self, budget: usize) {
        self.budget = budget;
        for _ in 0..self.by_route.len() {
            if self.bytes <= self.budget {
                break;
            }
            self.shed();
        }
    }

    /// Drop what is retained for `sref` (a write, or a hot slot taking the
    /// stream over).
    pub(super) fn forget(&mut self, sref: &TenantStreamRef) {
        if let Some(entry) = self.by_route.remove(&route(sref)) {
            self.bytes -= entry.bytes();
        }
    }

    /// Over budget: the least recently kept eighth of the entries goes, in
    /// one pass (keep counters are distinct, so exactly that many), so the
    /// cost per keep stays constant.
    fn shed(&mut self) {
        let mut ages: Vec<u64> = self.by_route.values().map(|e| e.touched).collect();
        let n = (ages.len() / 8).max(1);
        let (_, cutoff, _) = ages.select_nth_unstable(n - 1);
        let cutoff = *cutoff;
        let mut freed = 0;
        self.by_route.retain(|_, e| {
            let gone = e.touched <= cutoff;
            if gone {
                freed += e.bytes();
            }
            !gone
        });
        self.bytes -= freed;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_route.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
pub(super) fn entry_size() -> usize {
    std::mem::size_of::<Entry>()
}

/// What an entry made from `d` and `etag` is charged.
#[cfg(test)]
pub(super) fn charged(d: &PersistedDescriptor, etag: &str) -> usize {
    Entry::of(d, etag, &mut HashSet::new()).map_or(0, |e| e.bytes())
}

#[cfg(test)]
pub(super) fn round_trip(
    d: &PersistedDescriptor,
    etag: &str,
    sref: &TenantStreamRef,
) -> Option<(PersistedDescriptor, String)> {
    let mut interned = HashSet::new();
    Entry::of(d, etag, &mut interned).map(|e| (e.rebuild(sref), e.etag.text()))
}
