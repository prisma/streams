//! The engine's layout 6 state (DESIGN-6A): what the shard log's
//! segmented layout keeps in memory beside the store. Every engine-level
//! state layout 6 adds lives here, in `LayoutState` (the owner's decision,
//! by the lead under delegation, 2026-10-27), so `ShardEngine::start`
//! builds it with one field and never changes for it again.
//!
//! - **The usage dirty set** (f3): every billed segment whose usage
//!   counters hold a version the `_usage` ledger has not acknowledged, with
//!   that version. The committer is the only writer of the counters, so the
//!   set is exact while the engine lives: a billed group marks its segment
//!   when it is applied, and an acknowledgement through the segment's
//!   version clears it. It replaces the durable usage-dirty rows and their
//!   scan. The sealer's open rebuilds it in its one scan of the shard log's
//!   tails, which marks every billed segment: the drain then emits each
//!   once more, and the rollup ignores a snapshot at or below the version
//!   it holds, so nothing is billed twice.
//! - **The unsealed set**: every stream whose handle holds unsealed pages.
//! - **The sealer's state** (`shard::seal`): its directory, triggers and
//!   counters.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Mutex;

/// A stream's static billing meta as its handle holds it (layout 6, f3):
/// not yet read, or read (or written) with zero counters, `None` for a
/// segment never billed.
#[derive(Clone, Debug, Default)]
pub(crate) enum StaticBilling {
    #[default]
    Unread,
    Read(Option<crate::billing::SegmentBillingMetaV1>),
}

impl StaticBilling {
    /// The meta, `None` when unread or never billed.
    pub(crate) fn into_meta(self) -> Option<crate::billing::SegmentBillingMetaV1> {
        match self {
            Self::Unread => None,
            Self::Read(meta) => meta,
        }
    }
}

/// One page of usage-dirty segments, (hash, version), and whether more
/// follow.
pub(crate) type UsagePage = (Vec<([u8; 16], u64)>, bool);

/// The engine's layout 6 state.
#[derive(Default)]
pub(crate) struct LayoutState {
    usage: UsageDirty,
    /// Every stream whose handle holds unsealed pages, held by its handle:
    /// the sealer's work list, and what keeps those handles from eviction
    /// (an evictable handle is referenced by the engine's map alone).
    unsealed: Mutex<BTreeMap<[u8; 16], std::sync::Arc<super::StreamHandle>>>,
    /// The sealer's state (`shard::seal`).
    pub(crate) seal: super::seal::SealState,
}

/// The usage dirty set.
#[derive(Default)]
struct UsageDirty {
    set: Mutex<BTreeMap<[u8; 16], u64>>,
}

impl LayoutState {
    /// Hold `handle`'s stream for the sealer (it holds unsealed pages).
    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::hold_unsealed; a poisoned unsealed set may hold a half-applied hold or release; recovering it could let a handle holding unsealed pages be evicted, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    pub(crate) fn hold_unsealed(&self, handle: &std::sync::Arc<super::StreamHandle>) {
        self.unsealed
            .lock()
            .unwrap()
            .entry(handle.hash)
            .or_insert_with(|| handle.clone());
    }

    /// The streams holding unsealed pages.
    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::unsealed; a poisoned unsealed set may hold a half-applied hold or release; recovering it could let a handle holding unsealed pages be evicted, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    pub(crate) fn unsealed(&self) -> Vec<std::sync::Arc<super::StreamHandle>> {
        self.unsealed.lock().unwrap().values().cloned().collect()
    }

    /// Release `hash` once its handle holds no unsealed page.
    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::release_sealed; a poisoned unsealed set or stream state may hold a half-applied release; recovering it could let a handle holding unsealed pages be evicted, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    pub(crate) fn release_sealed(&self, hash: &[u8; 16]) {
        let mut set = self.unsealed.lock().unwrap();
        if set
            .get(hash)
            .is_some_and(|h| h.state.lock().unwrap().pending.is_empty())
        {
            set.remove(hash);
        }
    }

    /// Mark `hash` dirty at `version` (a billed group was applied).
    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::mark_usage; a poisoned dirty set may hold a half-applied mark or clear; recovering it could drop a version the ledger never acknowledged, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    pub(crate) fn mark_usage(&self, hash: [u8; 16], version: u64) {
        let mut set = self.usage.set.lock().unwrap();
        let entry = set.entry(hash).or_insert(version);
        *entry = (*entry).max(version);
    }

    /// Clear `hash` when the ledger acknowledged `through` and the
    /// segment's counters hold no later version.
    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::clear_usage; a poisoned dirty set may hold a half-applied mark or clear; recovering it could drop a version the ledger never acknowledged, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    pub(crate) fn clear_usage(&self, hash: [u8; 16], current: Option<u64>) {
        let mut set = self.usage.set.lock().unwrap();
        if current.is_none_or(|v| set.get(&hash).is_none_or(|marked| *marked <= v)) {
            set.remove(&hash);
        }
    }

    /// Whether any segment's usage awaits the ledger.
    pub(crate) fn usage_dirty(&self) -> bool {
        !self.usage_page(None, 1).0.is_empty()
    }

    #[expect(
        clippy::unwrap_used,
        reason = "LayoutState::usage_page; a poisoned dirty set may hold a half-applied mark or clear; recovering it could drop a version the ledger never acknowledged, whereas panicking stops its caller instead of acting on a half-applied set: the committer's or the sealer's exit retires the engine and the next open rebuilds the set from the tails, and a failed drain round retries"
    )]
    /// One page of the dirty set after `after`, at most `limit` entries,
    /// and whether more follow (complete once the sealer's open rebuilt
    /// it, `ShardEngine::seal_ready`).
    pub(crate) fn usage_page(&self, after: Option<[u8; 16]>, limit: usize) -> UsagePage {
        let set = self.usage.set.lock().unwrap();
        let from = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = set.range((from, Bound::Unbounded)).map(|(h, v)| (*h, *v));
        let page: Vec<_> = rows.by_ref().take(limit).collect();
        (page, rows.next().is_some())
    }
}
