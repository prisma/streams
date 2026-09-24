# KANI-002 seeds: the last position and malformed tokens

**Classification:** production defects (source inspection, confirmed on the real code),
plus one tracked domain decision.
**Pre-fix revision:** `fb18840d1ee12cc054e57ff74b37396d3c0bcf41` (`src/offsets.rs`).
**Fixed by:** the commit "Offset tokens carry the whole segment ordinal, and a read
position cannot overflow".

## 1. `Offset(Some(u64::MAX))` overflowed its successor

The pre-fix `Offset(pub Option<u64>)` stored "after entry n" and computed
`n + 1` in `encode` and `scan_from`. `Some(u64::MAX)` was representable through
the public field. Replaying `Offset(Some(u64::MAX)).encode()` and `.scan_from()`
on the pre-fix code in a debug build gave:

```text
panicked at src/offsets.rs:28:24: attempt to add with overflow
panicked at src/offsets.rs:21:24: attempt to add with overflow
```

The release profile has no overflow checks, so the same values wrapped to 0:
the token for START, and a scan from index 0 (a replay from the beginning).
Production call sites built positions with `checked_sub`, or with `next - 1`
behind a `next == 0` test, so no shipped path produced `Some(u64::MAX)`. The
type did not prevent it, though.

**Fix:** `Offset` now stores the scan index itself (`next`, the token's rawSeq)
in a private field. `Offset::before(next)` is the only constructor, and every
`u64` is a valid position, so no successor arithmetic remains. The five
production construction sites now call `Offset::before`.

## 2. A 26-byte token with a multi-byte character parsed

The pre-fix parser checked `input.len()` (bytes) and then iterated `chars()`. It
decoded each char with `ch.to_ascii_uppercase() as u8`, which truncates a
non-ASCII scalar to its low byte. Replaying `Offset::parse` on 24 `'0'`
characters followed by `U+0141` (26 bytes, 25 chars) on the pre-fix code gave
`Ok(Offset(None))`: the malformed token was accepted as START.

**Fix:** the parser walks the 26 bytes. A non-ASCII byte is never a digit.
Regression: `offsets::tests::a_multibyte_char_is_not_a_digit`.

## 3. Resolved: scan index `u64::MAX` no longer doubles as "now"

`ReadCommand::position_in` used to map `ReadStart::Now` to scan index
`u64::MAX`, and `read_request.rs` replaced any start of `u64::MAX` with the
current tail. A position token whose rawSeq is `2^64 - 1` was therefore served
as "now". The server issues such a token only after a stream has held
`2^64 - 1` records, so only a crafted token reached it; its effect was
live-tail semantics instead of `CursorBeyondTail` or an empty read.

Fixed by "A read position of u64::MAX is a position, and now has its own
representation": the planner carries `ScanStart { Now, At(u64) }`, the peer
relay sends "now" as the literal `now` (as it always did on the wire), and
every numeric scan index follows the ordinary past-the-tail rule. Regressions:
`dst::dst_tests::read_application::scan_index_u64_max_is_a_position_not_now_locally_and_relayed`
and `dst::dst_tests::read_application::adapters_never_serve_a_u64_max_token_as_the_live_tail`.
During a mixed-version rollout, an owner that has not been upgraded still
treats a forwarded `2^64 - 1` position as its tail until every owner runs the
fix.
