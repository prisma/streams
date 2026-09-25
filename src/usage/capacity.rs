//! The permanent per-stream capacity refusal (external review §5).
//!
//! A request larger than a FRESH token bucket can never be admitted, so every
//! append surface refuses it 413 `payload_too_large`, before any side effect,
//! and the refusal names the limit it crossed: the raw message carries the
//! numbers, the product error adds them as `details`.

/// A request no fresh per-stream bucket admits: the dimension that refused
/// it, that bucket's capacity in whole units (the largest request it admits)
/// and the size requested, measured exactly as the append core measures it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapacityRefusal {
    pub(crate) dimension: &'static str,
    pub(crate) capacity: u64,
    pub(crate) requested: u64,
}

impl CapacityRefusal {
    /// `tokens` is the bucket's `LIMIT_*_PER_SEC × LIMIT_BURST_SECS`. The
    /// refusal exists only for an integer request above it, and for an
    /// integer `n`, `n > tokens` exactly when `n > ⌊tokens⌋`, so the whole-unit
    /// capacity reported agrees with the verdict.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "CapacityRefusal::new; boot validation (config::admission_limits) proved an enabled bucket finite and at least one token, and a refusal exists only below a u64 request, so the cast is the exact whole-unit floor; a checked conversion would only restate the boot proof"
    )]
    pub(crate) fn new(dimension: &'static str, tokens: f64, requested: u64) -> Self {
        Self {
            dimension,
            capacity: tokens as u64,
            requested,
        }
    }
}

impl std::fmt::Display for CapacityRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            dimension,
            capacity,
            requested,
        } = self;
        write!(
            f,
            "request of {requested} {dimension} exceeds the per-stream ingest capacity of {capacity} {dimension}"
        )
    }
}
