//! Kani proofs for the open indexes' triggers: KANI-107 (S3a-DESIGN §4.1, §7). The committer stages
//! a stream's `'W'` row from the group's base and staged tails alone, and clears its `'M'` row only on
//! an acknowledgement that covers the durable usage version. Over every base and staged tail (and
//! every acknowledgement), the rows keep their invariants:
//! - W: the row exists exactly when the durable tail holds unsealed pages (`absorbed < next`);
//! - M: when the row is absent, the ledger has acknowledged the durable version.
use super::{IndexOp, dirty_cleared, unsealed_op};

#[kani::proof]
fn kani_107_the_unsealed_row_follows_the_staged_tail() {
    let base: (u64, u64) = (kani::any(), kani::any());
    let staged: (u64, u64) = (kani::any(), kani::any());
    // Invariant W before the group: the row exists exactly when the base tail is unsealed.
    let before = base.0 < base.1;
    let after = match unsealed_op(base, staged) {
        Some(IndexOp::Put) => true,
        Some(IndexOp::Delete) => false,
        None => before,
    };
    assert!(
        after == (staged.0 < staged.1),
        "the unsealed row follows the staged tail"
    );
}

#[kani::proof]
fn kani_107_the_dirty_row_clears_only_through_the_durable_version() {
    let through: u64 = kani::any();
    let durable: Option<u64> = if kani::any() { Some(kani::any()) } else { None };
    if dirty_cleared(through, durable) {
        assert!(
            durable.is_none_or(|version| version <= through),
            "a cleared dirty row was acknowledged through the durable version"
        );
    }
    if let Some(version) = durable {
        assert!(
            dirty_cleared(version, durable),
            "an acknowledgement of the durable version clears the row"
        );
    }
}
