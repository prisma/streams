# KANI-001 / KANI-003 seed: high segment ordinals collided with low ones

**Classification:** production defect (source inspection, confirmed on the real code).
**Pre-fix revision:** `fb18840d1ee12cc054e57ff74b37396d3c0bcf41` (`src/offsets.rs`).
**Fixed by:** the commit "Offset tokens carry the whole segment ordinal, and a read
position cannot overflow".

## Minimized input

`encode_ep(epoch, Offset(Some(0)))` for `epoch = 0` and `epoch = 1 << 30`.

The pre-fix encoder built the 128-bit tuple `(epoch << 96) | (rawSeq << 32)` in a
`u128` and then shifted it left by two for the 130-bit token. The shift discarded
the top two epoch bits. The parser also accumulated 26 five-bit digits in a
`u128`, so it discarded the same two bits of any token. Every ordinal `e` at or
above `1 << 30` therefore wrote the token of `e mod 2^30`, and parsing that
token returned the low ordinal. Segment ordinals are allocated as the full `u32`
(`SegMap::split` uses `checked_add` up to `u32::MAX`), so this was a valid,
if distant, input.

## Replay against the pre-fix code

Replayed by appending this test to the pre-fix `src/offsets.rs` in a detached
worktree and running
`cargo test --locked --lib kani_001_seed_replay -- --nocapture`:

```rust
#[test]
fn high_epoch_collides_with_epoch_zero() {
    let high = encode_ep(1 << 30, Offset(Some(0)));
    let zero = encode_ep(0, Offset(Some(0)));
    assert_ne!(high, zero, "distinct epochs must not share a token");
}
```

Observed output (Rust 1.98.1, debug):

```text
encode_ep(1 << 30, Some(0)) = 0000000000000000000G000000
encode_ep(0, Some(0))       = 0000000000000000000G000000
parse_ep(high)              = Ok((0, Offset(Some(0))))
assertion `left != right` failed: distinct epochs must not share a token
```

A per-key read cursor or SSE `streamNextOffset` issued for segment `1 << 30`
would therefore have resumed in segment 0.

## Permanent regressions

- `golden_tests::cursors::golden_layout4_raw_high_epoch_offset_tokens` pins the
  corrected tokens for ordinals `1 << 30`, `1 << 31` and `u32::MAX`. The expected
  strings were computed independently, as base32 of the 130-bit value. It also
  asserts that ordinal `1 << 30` no longer shares a token with ordinal 0, and
  that the raw codec refuses a high leading digit instead of aliasing it.
- `offsets::tests::epoch_round_trip` checks ordering across the `1 << 30` and
  `u32::MAX` boundaries.
- The Kani harnesses `offsets::proofs::kani_001_every_epoch_and_position_round_trips`
  and `offsets::proofs::kani_003_tokens_are_injective_and_ordered` cover the full
  `u32 × u64` domain.

Compatibility: every ordinal below `1 << 30` keeps its previous token byte for
byte. A random 100,000-case comparison against the pre-fix arithmetic agreed,
and the existing golden tokens are unchanged. Tokens with a leading digit at or
above `8` were never emitted before. They now decode to the high ordinal they
denote instead of aliasing a low one.
