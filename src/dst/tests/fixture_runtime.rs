//! Fixture runtime.

use std::sync::Arc;

// ---- seal-gap read semantics (review blocker: a topology transition
// may delay a reader, but it must NEVER look like permanent closure) --

/// A logical PROCESS incarnation of the deterministic rig (PR 4.1.1).
/// Every simulated process — a restart over the same store, a second
/// fleet instance, a token-refresh peer — carries its OWN incarnation:
/// same base seed + same incarnation reproduces every migrated
/// identity exactly; same base seed + a new incarnation is
/// deterministic but DISTINCT (boot id, first stream epoch, first
/// touch-journal epoch). Explicit by construction, never a
/// process-global counter — that would make identity depend on test
/// scheduling and parallelism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RigIncarnation(pub(super) u64);

/// The suite's base seed and wall-clock start.
pub(super) const RIG_SEED: u64 = 0x5eed_0000_0000_0001;
pub(super) const RIG_START_MS: i64 = 1_700_000_000_000;

/// One process's runtime capabilities: the domain-separated entropy
/// streams ("runtime-identity", "stream-epoch", "touch-journal") and
/// the manual clock the test drives.
pub(super) struct RigRuntime {
    pub(super) caps: crate::runtime::RuntimeCaps,
    pub(super) clock: crate::runtime::ManualClock,
    pub(super) touch_entropy: Arc<dyn crate::runtime::Entropy>,
}

/// Fold the incarnation into the base seed (splitmix64 finalizer over
/// `base ^ incarnation·φ`): a pure function of (base, incarnation)
/// whose distinct incarnations land on unrelated seeds.
pub(super) fn derive_rig_seed(base: u64, incarnation: RigIncarnation) -> u64 {
    let mut z = base ^ incarnation.0.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub(super) fn rig_runtime(base_seed: u64, incarnation: RigIncarnation) -> RigRuntime {
    use crate::runtime::{ManualClock, RuntimeCaps, SeededEntropy};
    let seed = derive_rig_seed(base_seed, incarnation);
    let clock = ManualClock::at(RIG_START_MS);
    let caps = RuntimeCaps::with_sources(
        Arc::new(clock.clone()),
        &SeededEntropy::domain(seed, "runtime-identity"),
        Arc::new(SeededEntropy::domain(seed, "stream-epoch")),
        "dst-instance",
    );
    RigRuntime {
        caps,
        clock,
        touch_entropy: Arc::new(SeededEntropy::domain(seed, "touch-journal")),
    }
}

impl RigRuntime {
    /// The first (and, for single-process tests, only) incarnation.
    pub(super) fn first() -> Self {
        Self::incarnation(0)
    }
    /// An explicit incarnation under the suite's base seed — restart
    /// and multi-instance tests name each simulated process this way.
    pub(super) fn incarnation(n: u64) -> Self {
        rig_runtime(RIG_SEED, RigIncarnation(n))
    }
}
