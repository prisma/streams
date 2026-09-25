# Item 88, step 1: one offset codec (behaviour-preserving)

Tree: `slate` at `33fbd10e`. `origin/slate` reached `33fbd10e` while this plan was being written, so the merge base and the push `before` are both `33fbd10e`.
Scope: only step 1. Step 2 (a strict decoder, which changes the wire) goes to Søren as a decision (section 9).

Summary: `src/offsets.rs` ends up with one encoder, `encode(epoch: u32, next: u64) -> String`, and one decoder, `parse(&str) -> Result<(u32, u64), OffsetError>`. A five-line wrapper, `parse_scalar`, owns the epoch-0 rule that used to be the only difference between the two copies. `Offset(Option<u64>)`, `Offset::START`, `scan_from`, `encode_ep` and `parse_ep` are deleted. All five encode sites and three decode sites pass `next` directly. Token bytes and client-visible error text do not change, and the golden literals stay as they are.

---

## 1. Problem (checked against `33fbd10e`)

### 1a. Two copies of the encoder and two copies of the lax decoder (`src/offsets.rs`, 153 lines)

- The encoder's digit loop appears twice. It is at `Offset::encode` (lines 25-39) and again at `encode_ep` (lines 83-97):
  ```rust
  let padded = n << 2; // 128 -> 130 bits
  let mut out = String::with_capacity(26);
  for i in 0..26 {
      let shift = 5 * (25 - i);
      let idx = ((padded >> shift) & 31) as usize;
      out.push(ALPHABET[idx] as char);
  }
  ```
- The decoder also appears twice, at `Offset::parse` (lines 41-64) and at `parse_ep` (lines 100-121). The two copies share the same `"-1"` check, length check, char loop and shifts. They differ only here:
  ```rust
  if epoch != 0 {
      return Err(format!("unsupported offset epoch: {epoch}"));
  }
  ```
  `Offset::parse` has this check. `parse_ep` accepts any epoch.

### 1b. `Offset(Option<u64>)` adds a round trip at every site

The type is `pub(crate) struct Offset(pub Option<u64>);` where `None` means START. Both the encoder and the decoder immediately undo it: `Some(n) => n + 1` on the way out (lines 21, 28, 86) and `Ok(Offset(Some(raw_seq - 1)))` on the way in (lines 62, 118). No production site uses the `Option`. Every site converts `next` into it and straight back out:

| # | Site (HEAD line) | Code today | What it computes |
|---|---|---|---|
| E1 | `src/http.rs:32` `append_position` | `crate::offsets::encode_ep(seg, Offset(next.checked_sub(1)))` | token(seg, next) |
| E2 | `src/http.rs:2324-2331` `tail_token` | `if next == 0 { Offset::START } else { Offset(Some(next - 1)) }.encode()` | token(0, next) |
| E3 | `src/http/read.rs:343-350` `raw_position` | `let offset = Offset(position.after.checked_sub(1)); if segmented { encode_ep(position.segment, offset) } else { offset.encode() }` | token(segmented ? segment : 0, after) |
| E4 | `src/sse/wire.rs:70-77` `sse_control_ep` | `encode_ep(seg_id, if next == 0 { Offset::START } else { Offset(Some(next - 1)) })` | token(seg_id, next) |
| E5 | `src/application/read_remote.rs:392` `remote_read_page` | `crate::offsets::encode_ep(segment, crate::offsets::Offset(from.checked_sub(1)))` | token(segment, from) |
| D1 | `src/http/read.rs:115-120` `raw_start` (segmented) | `parse_ep(raw)` then `after: offset.scan_from()` | (epoch, next) |
| D2 | `src/http/read.rs:122-126` `raw_start` (unsplit) | `Offset::parse(raw)` then `after: offset.scan_from()` | next; epoch ≠ 0 becomes InvalidCursor |
| D3 | `src/http.rs:2299` `parse_fork_offset` | `Offset::parse(tok).map(\|o\| o.scan_from())` | next; the error String goes to the wire |
| I  | `src/http.rs:127` | `use crate::offsets::Offset;` (also reaches `http/read.rs` through its `use super::*`) | import |

Tests that use the old API:
- `src/golden_tests.rs:726` (`use crate::offsets::{Offset, encode_ep};`) and lines 785-811 (`golden_layout4_raw_offset_tokens`, `golden_layout4_raw_epoch_offset_token`).
- `src/offsets.rs:127-152` (`round_trip`, `epoch_round_trip`).
- DST test bodies:
  - `src/dst/tests/read_application.rs:83-85` (`r06_empty_filtered_page_has_one_position_across_application_and_protocols`) and `:417` (`the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope`).
  - `src/dst/tests/sse_delivery.rs:579` (`raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window`).
  - `src/dst/tests/livefeed_basics.rs:351, 541, 625` (`livefeed_raw_surface_uses_the_raw_vocabulary`, `livefeed_exact_framing_mixed_surfaces_share_one_lane`, `livefeed_exact_framing_mixed_surfaces_product_first`).
  - `src/dst/tests/livefeed_swap.rs:124, 692, 848` (`livefeed_raw_disconnects_without_terminal_on_split`, `livefeed_raw_late_attach_after_swap_gets_no_lineage_scalars`, `livefeed_raw_swap_between_peek_and_attach_is_refused`).
- The DST helper `sse_delivery.rs:382-389` (`raw_next_tok`).

`git grep -n -E '(^|[^A-Za-z_:])Offset(\(|::)|offsets::Offset' -- src` on HEAD returns exactly these sites plus `offsets.rs` itself. `GetRange::Offset` and `RecordCorruption::Offset` are unrelated types and the pattern excludes them. No other Rust copy of the codec exists: `offsets` is a private module in `lib.rs:42`, the `src/bin/*` binaries cannot see it, and neither `fuzz/` nor `tools/` includes it by path. There are two Python copies in bench tooling (`bench/soak/reconcile.py:32-36`, `bench/fleet/drain-account.py:21`); they are out of scope (section 8).

Latent problem caused by the type: `Offset(Some(u64::MAX)).encode()` and `.scan_from()` compute `n + 1`, which panics in debug builds and wraps to START in release. No path can reach it, because every constructor is `x.checked_sub(1)` or `raw_seq - 1`. The type allows it anyway, and removing the type removes it.

The reviewer's line reference `src/http.rs:2473-2480` is out of date. The real sites are E1, E2, D3 and I above. The `offsets.rs:41-122` and `golden_tests.rs:785-811` references are still correct.

### 1c. The decoder is lax (traced through `Offset::parse` / `parse_ep`)

These are real behaviours today. Step 1 keeps them, and a test in C1 pins them:

- **Byte length versus char count, plus `as u8` truncation.** `input.len() != 26` counts bytes, but the loop runs over `input.chars()`. `decode_char` then takes `_ => ch.to_ascii_uppercase() as u8`, which keeps only the low byte of a non-ASCII char. Take `"00000000000000000\u{131}0000000"`: 17 zeros, the 2-byte char U+0131, then 7 zeros. That is 26 bytes but 25 chars. U+0131 becomes `0x31`, which is `'1'`, value 1, at shift 35. So n = 2^35, n>>2 = 2^33, and raw_seq = 2. The result is `Ok(Offset(Some(1)))`, which is the position of the canonical token `"00000000000000000010000000"`.
- **The top 2 of 130 bits are dropped.** `n = (n << 5) | v` is computed in `u128`, so the first char's top two bits are shifted out. `"G0000000000000000000000000"` gives n = 16 << 125 = 2^129, which becomes 0, so the result is `Ok(Offset::START)`.
- **Pad bits are ignored.** `let n = n >> 2;` throws them away. `"0000000000000000000G000003"` decodes to `Ok(Offset(Some(0)))`, the same as the canonical `"...G000000"`.
- **in_block is ignored.** Only bits 32..96 are read. `"0000000000000000000G000010"` gives n = 2^34 + 2^5, and after >>2 that is 2^32 + 8. The in_block value of 8 is dropped, so the result is `Ok(Offset(Some(0)))`.

### 1d. New finding: the encoder keeps only 30 epoch bits

In `encode_ep`, `n = (epoch << 96) | ...` fills bits up to 127, and then `padded = n << 2` shifts epoch bits 30 and 31 out of the `u128`. So `encode_ep(1 << 30, Offset::START)` equals `"00000000000000000000000000"`, which is the START token of segment 0. Segment ordinals are allocated from 1 by `segmap.rs:303-305` and `:391-394`, with `IdExhausted` only at `u32::MAX`. The type therefore permits ordinals ≥ 2^30. In practice they cannot be reached (it would take about 5×10^8 splits of one stream). Step 1 keeps this behaviour and documents it. How to close it is decision 9B.

---

## 2. Contract decision

The new API in `src/offsets.rs`:

```rust
pub(crate) enum OffsetError { Length(usize), Char(char), Epoch(u32) }   // Debug, Clone, Copy, PartialEq, Eq + Display
pub(crate) fn encode(epoch: u32, next: u64) -> String
pub(crate) fn parse(input: &str) -> Result<(u32, u64), OffsetError>    // "-1" -> (0, 0); never returns Epoch
pub(crate) fn parse_scalar(input: &str) -> Result<u64, OffsetError>    // (0, next) -> next; (e, _) -> Epoch(e)
```

- `next` is the token's rawSeq: the first record index a read at that token returns. It is the value every call site already holds (`next`, `after`, `from`), so the `checked_sub(1)` / `n + 1` pair disappears.
- The tuple order `(epoch, next)` has the types `(u32, u64)`, and both sides of every call site are typed too. Writing `encode(next, seg)` does not compile, and destructuring `let (segment, after) = parse(raw)?` into `ReadPosition { segment: u32, after: u64 }` does not compile if the two are swapped.
- `parse_scalar` is not a second decoder. It is the epoch-0 rule that used to be the only difference between the two copies, and it now lives in one place. Both production users need it: unsplit reads (D2) and fork offsets (D3). If each caller checked the epoch itself, the rule and its error text would be written twice again. The reviewer's "one parse -> (u32, u64)" is buildable, but on its own it moves that duplication into the callers. That is why this plan adds `parse_scalar`.
- `OffsetError`'s `Display` produces exactly the old strings: `invalid offset length: {len}`, `invalid base32 char: {ch}`, `unsupported offset epoch: {epoch}`. They are checked in the same order as before (length, then first bad char, then epoch). This matters because D3 puts the text on the wire: `err_resp(StatusCode::BAD_REQUEST, "invalid_fork_offset", &e)` at `src/http.rs:2443-2446`. Using a typed error replaces a `String` verdict, and `Display` is the only place where the wire words are built.

**No wire change.** Every site maps one-to-one: token(seg, next), token(0, next), raw_position's `segmented ? segment : 0`, and D1/D2/D3 with their error mapping unchanged. `encode` keeps the same arithmetic as before (`((epoch as u128) << 96) | ((next as u128) << 32)`, `<< 2`, the same digit loop), so token bytes are identical. `parse` is the old loop, copied verbatim, so it accepts exactly the same language. The status codes and error codes are unchanged (`invalid_offset` / `invalid_fork_offset`, both 400), and so are the body messages. No metrics, `/v1/debug` JSON or product-surface contract is involved: product cursors are a separate signed codec in `product_cursor.rs`.

---

## 3. Pinning tests and compile-level proofs (step 1 is a refactor, so there are no red tests)

### 3a. C1 pins (test-only): green on HEAD, carried byte-identical or with the same values into C2

**P-HTTP.** `src/http/tests.rs`, new test `raw_position_tokens_and_fork_refusals_are_exact`. It is reached through the existing `use super::*;` at line 64, and its test path is `http::tests::…`, which the `http` owner's `http::` filter selects.
```rust
/// The raw surface's position tokens and fork-offset refusals, pinned as
/// bytes: clients store the tokens and read the refusal words, so no codec
/// refactor may move a byte of either.
#[test]
fn raw_position_tokens_and_fork_refusals_are_exact() {
    assert_eq!(tail_token(0), "00000000000000000000000000");
    assert_eq!(tail_token(42), "000000000000000000N0000000");
    assert_eq!(append_position(3, 6, true), "000000R0000000000030000000");
    assert_eq!(append_position(3, 42, false), "000000000000000000N0000000");
    assert_eq!(parse_fork_offset("-1"), Ok(0));
    assert_eq!(parse_fork_offset("000000000000000000N0000000"), Ok(42));
    assert_eq!(parse_fork_offset("0000000000000000_000000000000002a"), Ok(42));
    assert_eq!(
        parse_fork_offset("000000R0000000000030000000"),
        Err("unsupported offset epoch: 3".to_string())
    );
    assert_eq!(parse_fork_offset("0"), Err("invalid offset length: 1".to_string()));
    assert_eq!(
        parse_fork_offset("0000000000000000000000000U"),
        Err("invalid base32 char: U".to_string())
    );
}
```
Why this passes on HEAD:
- `tail_token(0)`: START, raw 0, all `'0'`.
- `tail_token(42)`: `Offset(Some(41))`, raw 42, padded = 42<<34. At i=18 (shift 35) the digit is 42>>1 = 21, which is `'N'`. At i=17 the digit is 42>>6 = 0, and at i=19 it is (42<<4)&31 = 0.
- `append_position(3,6,true)`: `encode_ep(3, Offset(Some(5)))`, raw 6. Bits 98-99 give 3<<3 = 24 (`'R'`) at i=6. Bits 35-36 give 3 at i=18. That matches the existing golden literal at `golden_tests.rs:808`.
- `append_position(3,42,false)` is `tail_token(42)`.
- `parse_fork_offset("-1")`: START, and `scan_from` gives 0. The `'N'` token decodes to `Some(41)`, and `scan_from` gives 42.
- The hex branch computes 0.saturating_add(0x2a) = 42.
- The `'R'` token has epoch 3, so the result is `Err("unsupported offset epoch: 3")`.
- `"0"` fails the length check: `"invalid offset length: 1"`.
- For the `'U'` token, the length is 26, and `decode_char('U')` is None because U is not in the alphabet, so the result is `"invalid base32 char: U"`.

**P-SSE.** New inline module at the end of `src/sse/wire.rs`. It is placed there so that C1 counts as a trailing `#[cfg(test)]` root item and the planner can classify it as production-unchanged. Its path is `sse::wire::tests::…`, which the `sse_wire` owner's `sse::` filter selects. It has no glob import.
```rust
#[cfg(test)]
mod tests {
    /// Segment 0's control names the scalar token and a successor segment
    /// names its ordinal: raw transcripts are pinned byte for byte.
    #[test]
    fn raw_control_names_next_in_its_segment() {
        assert_eq!(
            super::sse_control_ep(0, 42, None, true, true),
            "event: control\ndata:{\"streamNextOffset\":\"000000000000000000N0000000\",\"upToDate\":true,\"streamClosed\":true}\n\n"
        );
        assert_eq!(
            super::sse_control_ep(3, 6, None, false, true),
            "event: control\ndata:{\"streamNextOffset\":\"000000R0000000000030000000\",\"streamClosed\":true}\n\n"
        );
    }
}
```
`closed = true` leaves out the time-dependent `streamCursor` (the `if !closed` branch at `wire.rs:79`). The field order follows `wire.rs:78-91`.

**P-LAX.** `src/offsets.rs` tests, new test `non_canonical_tokens_keep_their_lax_reading`. C1 writes it against the old API:
```rust
/// Today's decoder reads four kinds of non-canonical token as a position
/// instead of refusing it: a first digit above '7' (its top bits fall off
/// u128), a two-byte char (the gate counts bytes, the loop counts chars and
/// `as u8` keeps the low byte), nonzero pad bits and nonzero in_block bits.
/// Refusing them is a pending wire decision (review item 88 step 2); until
/// it is made, no refactor may move them.
#[test]
fn non_canonical_tokens_keep_their_lax_reading() {
    assert_eq!(Offset::parse("G0000000000000000000000000"), Ok(Offset::START));
    assert_eq!(Offset::parse("00000000000000000\u{131}0000000"), Ok(Offset(Some(1))));
    assert_eq!(Offset::parse("0000000000000000000G000003"), Ok(Offset(Some(0))));
    assert_eq!(Offset::parse("0000000000000000000G000010"), Ok(Offset(Some(0))));
}
```
C2 rewrites it with the same positions: `parse(..) == Ok((0, 0))`, `Ok((0, 2))`, `Ok((0, 1))`, `Ok((0, 1))`. The traces are in 1c. The test catches the refactor mistake most likely to slip through: switching to `bytes()` would make the U+0131 case return `Err(Char(..))`, and indexing by byte would turn the 25-char case into a length error.

### 3b. Existing tests that pin behaviour through C2 (same literals, new call form)

- `golden_tests::cursors::golden_layout4_raw_offset_tokens`: `encode(0,0)`, `encode(0,1)`, `encode(0,42)` and `encode(0,u64::MAX)` against the unchanged literals `"00000000000000000000000000"`, `"0000000000000000000G000000"`, `"000000000000000000N0000000"` and `"0000007ZZZZZZZZZZZZG000000"`. It also asserts `parse("-1") == Ok((0,0))`, `parse("…G000000") == Ok((0,1))` and `parse("0000007ZZZZZZZZZZZZG000000") == Ok((0,u64::MAX))`.
- `golden_tests::cursors::golden_layout4_raw_epoch_offset_token`: `encode(3,6) == "000000R0000000000030000000"` and `parse(..) == Ok((3,6))`.
- DST tests that exercise E3, E5 and D1/D2 end to end:
  - `dst_tests::read_application::r06_…`: raw GET `stream-next-offset` parses to `Ok((0, out.next.after))`.
  - `dst_tests::read_application::the_page_route_…`: the relay offset is `encode(0, 100)`.
  - `dst_tests::read_application` line 387 and `dst_tests::read_page_limits` lines 85/109: `remote_read_page(…, 0, from)` puts the E5 token on the peer wire and reads it back through D2.
  - `dst_tests::reads_raw::*`: these follow `stream-next-offset`.
  - `dst_tests::sse_delivery::*` and `dst_tests::livefeed_*`: raw SSE from START, with controls compared against `raw_next_tok`.

### 3c. Compile-level proofs (C2)

- `Offset`, `Offset::START`, `scan_from`, `encode_ep` and `parse_ep` no longer exist, so a site the migration missed fails `cargo clippy --all-targets` (DST and golden tests are `cfg(test)`). Expected result: `git grep -n -E '(^|[^A-Za-z_:])Offset(\(|::)|offsets::Offset' -- src` prints nothing, and `git grep -n -w -E 'encode_ep|parse_ep' -- src` prints nothing. The `-w` matters because `src/bin/keys.rs` has `parse_epoch`.
- The types `(u32, u64)` make a swapped epoch/next a type error at all eight production sites (see 2).
- The `u64::MAX` overflow hazard (1b) cannot be expressed any more, because no `+ 1` / `- 1` remains.

### 3d. Property (C2): the policy's codec rule, valid-data half

`src/offsets.rs` tests get one `proptest::proptest!` block with 1,024 cases. The name starts with `quality_` so that the release `--lib quality_` codec leg runs it:
```rust
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(1024))]
    /// Every position the codec can carry (epochs below 2^30, see the
    /// module doc) has one 26-char token that decodes back to it, and
    /// tokens sort like positions: per-key readers compare them as strings.
    #[test]
    fn quality_offsets_every_position_has_one_ordered_token(
        a in (0u32..(1 << 30), any::<u64>()),
        b in (0u32..(1 << 30), any::<u64>()),
    ) {
        let (left, right) = (encode(a.0, a.1), encode(b.0, b.1));
        prop_assert_eq!(left.len(), 26);
        prop_assert_eq!(parse(&left), Ok(a));
        prop_assert_eq!(left.cmp(&right), a.cmp(&b));
    }
}
```
It needs `use proptest::prelude::{any, prop_assert_eq};`, an explicit import rather than a glob. The domain has to stop at 2^30 because of 1d. With an arbitrary `u32` epoch the property fails on HEAD's arithmetic and on the new arithmetic alike. The malformed-data half of the policy rule depends on the accepted grammar, and that grammar is exactly what step 2 decides, so it is assigned to step 2 together with the reviewer's second property. In step 1, malformed input is covered by P-LAX (four structural classes) and P-HTTP (the three refusal texts).

---

## 4. Edits file by file, in commit order

Ceilinged files (merge base and push `before` = `33fbd10e`; limit = no growth): `src/http.rs` 3,225 is the only ceilinged file this plan touches. It goes to 3,220 (budget ≤ 3,225). The other ceilinged files are untouched: `product.rs` 4,205, `shard.rs` 3,196, `billing.rs` 2,201, `history.rs` 1,713, `auth.rs` 1,676, `registry.rs` 1,492, `sse/feed.rs` 1,170, `fleet.rs` 1,142. Every other touched file stays below 1,000, and the DST files stay below their ceiling: `read_application.rs` 485→483, `sse_delivery.rs` 774→770, `livefeed_basics.rs` 907→907, `livefeed_swap.rs` 936→936.

### C1: "Raw offset tokens, SSE controls and fork-offset refusals are pinned before the codec folds into one" (test-only)

1. `src/http/tests.rs` (64 → about 87): insert P-HTTP before the trailing `use super::*;`. The file is entirely `#![cfg(test)]`, so it is production-unchanged and needs no owner row. `efe12b2e` already changed this file the same way.
2. `src/sse/wire.rs` (92 → about 109): append P-SSE as the last root item.
3. `src/offsets.rs` (153 → about 167): add P-LAX, in the old API, inside `mod tests`.

Ledgers: none. There are no DST changes, no new glob, no new macro-dsl fact (`assert_eq!` is an expression macro) and no attribute exceptions.

### C2: "One offset codec: encode(epoch, next) and parse -> (epoch, next); the Offset round trip and the second lax decoder go"

1. **`src/offsets.rs`**: rewrite the non-test part as below. `decode_char` is unchanged. The `encode`/`parse` bodies are the old loops copied verbatim, with `raw_seq` renamed to `next` and the `Offset` wrapping removed.
   ```rust
   //! Canonical Durable Streams offset encoding: the one codec every raw
   //! surface (reads, appends, creates, fork offsets, SSE controls, peer
   //! relays) speaks, so a position is the same string wherever it is minted.
   //!
   //! Offsets are 26-char Crockford base32 of a big-endian 128-bit tuple
   //! (epoch u32, rawSeq u64 split hi/lo, in_block u32) padded to 130 bits.
   //! rawSeq is `next`, the first record index a read at the token returns,
   //! so the reserved "-1" and rawSeq 0 both name start-of-stream. The epoch
   //! is the segment ordinal on per-key streams (PER-KEY-ORDERING.md §3) and
   //! 0 elsewhere; the padding shifts its top two bits out of u128, so only
   //! ordinals below 2^30 round-trip (segment ids are allocated from 1).

   const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

   /// Why a token names no position. The Display words are wire text: the
   /// fork-offset refusal hands them to clients verbatim.
   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
   pub(crate) enum OffsetError {
       Length(usize),
       Char(char),
       Epoch(u32),
   }

   impl std::fmt::Display for OffsetError {
       fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
           match self {
               OffsetError::Length(len) => write!(f, "invalid offset length: {len}"),
               OffsetError::Char(ch) => write!(f, "invalid base32 char: {ch}"),
               OffsetError::Epoch(epoch) => write!(f, "unsupported offset epoch: {epoch}"),
           }
       }
   }

   /// The one token for "records from `next` on" in `epoch`; in_block is
   /// always 0, so equal positions are equal strings on every surface.
   pub(crate) fn encode(epoch: u32, next: u64) -> String {
       let n: u128 = ((epoch as u128) << 96) | ((next as u128) << 32);
       let padded = n << 2; // 128 -> 130 bits
       let mut out = String::with_capacity(26);
       for i in 0..26 {
           let shift = 5 * (25 - i);
           let idx = ((padded >> shift) & 31) as usize;
           out.push(ALPHABET[idx] as char);
       }
       out
   }

   /// The one decoder behind every raw surface; segmented reads take the
   /// epoch as the segment. It reads, rather than refuses, bits no encoder
   /// sets (pad, in_block, bits past u128, a non-ASCII char's low byte):
   /// refusing them is a pending wire decision.
   pub(crate) fn parse(input: &str) -> Result<(u32, u64), OffsetError> {
       if input == "-1" {
           return Ok((0, 0));
       }
       if input.len() != 26 {
           return Err(OffsetError::Length(input.len()));
       }
       let mut n: u128 = 0;
       for ch in input.chars() {
           let v = decode_char(ch).ok_or(OffsetError::Char(ch))?;
           n = (n << 5) | v as u128;
       }
       let n = n >> 2; // strip pad bits
       let epoch = (n >> 96) as u32;
       let next = ((n >> 32) & 0xffff_ffff_ffff_ffff) as u64;
       Ok((epoch, next))
   }

   /// Unsplit reads and fork offsets name positions in epoch 0 only: a
   /// segment token there is refused, never re-based onto segment 0.
   pub(crate) fn parse_scalar(input: &str) -> Result<u64, OffsetError> {
       match parse(input)? {
           (0, next) => Ok(next),
           (epoch, _) => Err(OffsetError::Epoch(epoch)),
       }
   }
   ```
   Tests module:
   - Keep `use super::*;`, because the `unresolved-glob` row for `src/offsets.rs` / `crate::tests` in `source-allowances.json:3574-3580` must stay in use.
   - Add `use proptest::prelude::{any, prop_assert_eq};`.
   - `round_trip`: `for next in [0u64, 1, 2, 3, 42, (1 << 33) + 1, u64::MAX]`, asserting `encode(0,next).len()==26`, `parse(&t)==Ok((0,next))` and `parse_scalar(&t)==Ok(next)`. Also `parse("-1")==Ok((0,0))`, `parse_scalar("-1")==Ok(0)` and `encode(0,0)=="0000…0"`. These values are the old `Some(n)` inputs plus 1, plus START.
   - `epoch_round_trip`: `[(0u32,6u64),(3,1),(255,(1<<40)+1)]` round-trips, `parse_scalar(&encode(3,1))==Err(OffsetError::Epoch(3))`, and `encode(1,1) > encode(0,1000)`. The old `encode_ep(0,o)==o.encode()` assertion is deleted: with one encoder it is true by construction.
   - P-LAX in the new form, and the 3d property.
   Clippy note: every `as` cast is kept verbatim, and clippy's shift/mask reduction already accepts them today, so there is no `cast_possible_truncation`. `offsets.rs` has no `#[expect]`, and there is no module-wide lint in step 1.
2. **`src/http.rs`** (3,225 → 3,220):
   - L32 becomes `        crate::offsets::encode(seg, next)`.
   - Delete L127 `use crate::offsets::Offset;` (−1).
   - L2299 becomes `    crate::offsets::parse_scalar(tok).map_err(|e| e.to_string())` (the same idiom as L2294).
   - L2324-2331 becomes the following (−4):
     ```rust
     /// Create and unsplit-append responses answer in the scalar (epoch 0) lane.
     pub(crate) fn tail_token(next: u64) -> String {
         crate::offsets::encode(0, next)
     }
     ```
   Ratcheted scopes: none of `append_position` (30-36), `parse_fork_offset` (2286-2300) or `tail_token` has an attribute, and there is no `#![…]`. The `render_append` `#[expect(clippy::unwrap_used)]` scope (37-67) holds the call site `append_position(out.seg_id, out.next_offset, out.materialized)`, whose text does not change, so none of its fingerprints (call-site, path, ordinary-call) move. No unwrap/expect scope in `http.rs` names `Offset`, so deleting the import changes no alias resolution.
3. **`src/http/read.rs`** (637 → 633):
   - `raw_start` L114-127:
     ```rust
                 if segmented {
                     let (segment, after) =
                         crate::offsets::parse(raw).map_err(|_| ReadFailure::InvalidCursor)?;
                     Ok(ReadStart::Position(ReadPosition { segment, after }))
                 } else {
                     let after =
                         crate::offsets::parse_scalar(raw).map_err(|_| ReadFailure::InvalidCursor)?;
                     Ok(ReadStart::Position(ReadPosition {
                         segment: desc.resolve_segment(selector.unwrap_or("")).seg_id,
                         after,
                     }))
                 }
     ```
   - `raw_position` L343-350:
     ```rust
     /// An unsplit stream answers the scalar (epoch 0) token whatever its
     /// resolved segment id: that surface never shows a segment lane.
     fn raw_position(position: ReadPosition, segmented: bool) -> String {
         let epoch = if segmented { position.segment } else { 0 };
         crate::offsets::encode(epoch, position.after)
     }
     ```
   Ratcheted scopes: neither function has an attribute. The `render_raw_read` expects (L362-377: `too_many_lines`, `cast_possible_truncation`, `unwrap_used`, `needless_pass_by_value`) cover the call sites `raw_position(out.next, out.segmented)` and `raw_position(durable, out.segmented)`, and their text does not change. Nothing grows, and `too_many_lines` still applies because the body is untouched.
4. **`src/sse/wire.rs`** (about 109 → about 102): in `sse_control_ep`, L70-77 become `    let tok = crate::offsets::encode(seg_id, next);`. The doc text at L55-56 is replaced within the same two lines: "the token is the scalar token by construction (one codec, / `offsets::encode(seg_id, next)`), so unsplit raw". Ratcheted scope: `#[expect(clippy::fn_params_excessive_bools, reason = "sse_control_ep; …")]` (L59-62). `scope_lines` goes from 41 to 34 and `syntax_facts` goes down (the paths `Offset::START`, `Offset(..)` and `Some(..)` are gone). `nested_items` stays at 0 and the reason text is unchanged, so this is the same identity and does not grow. The lint is still met because there are still 2 bool params.
5. **`src/application/read_remote.rs`** (483 → 483): L392 becomes `        crate::offsets::encode(segment, from)`. The if/else stays on multiple lines because it exceeds rustfmt's single-line if/else width of 50. There is no attribute on `remote_read_page`, and `crate::offsets` is not a `crate::http`/`crate::product` edge for this hard-owner file.
6. **`src/golden_tests.rs`** (832 → about 822): `use crate::offsets::{encode, parse};`, both golden tests rewritten as in 3b with the literals unchanged, and the doc line at L780 changed to "rawSeq = next (the old offset+1) split hi/lo". The file is `#![cfg(test)]`, not critical and not in the inventory.
7. **DST tests** (the body changes are 9 test hashes; the helper is not inventoried):
   - `read_application.rs:82-87` becomes `assert_eq!(crate::offsets::parse(headers.get("stream-next-offset").unwrap()), Ok((0, out.next.after)));`. This is as strict as before: epoch 0 and next equal to `after`.
   - `read_application.rs:416-417`: the comment becomes `// next == 100, two records: beyond the tail.` and the offset becomes `let offset = crate::offsets::encode(0, 100);`.
   - The `sse_delivery.rs:380-389` helper keeps its name and becomes `crate::offsets::encode(0, next)`, with the doc "(segment 0)". Its 4 callers do not change, so their hashes stay the same.
   - `sse_delivery.rs:579`, `livefeed_basics.rs:351/541/625` and `livefeed_swap.rs:124/692/848` become `crate::offsets::encode(0, 0)`.
   - None of these tests sits under an `#[expect]`. `sse_delivery.rs` 36-43/171-178 and `livefeed_swap.rs` 387-390/497-500 belong to other tests.
8. **Ledgers**: see section 6.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, comparison `33fbd10e`)

Plan receipt:
- `mutation_source_files` = `src/application/read_remote.rs`, `src/http.rs`, `src/http/read.rs`, `src/sse/wire.rs`.
- `src/http/tests.rs` and `src/golden_tests.rs` are `#![cfg(test)]`, so they count as production-unchanged. `src/offsets.rs` and `src/dst/**` are neither critical nor registered.
- `selected_mutation_owners` = `['http_read', 'read_remote', 'sse_wire', 'http']` (the order of the OWNERS table). `unregistered_mutation_source_files` = `[]`.
- Legs: compiler, properties_fuzz (`src/application/read_`), loom (`src/sse`) and miri (`src/http`) are all true.

The function bodies that change, the mutants in-diff selects (FnValue spans cover the whole body; no operator sits on an inserted line or on a line next to a deletion), and the test that kills each one:

| Owner (filters) | Mutant | Outcome / killer (in the owner's filters) |
|---|---|---|
| `http` (`http::` …) | `append_position -> String`: `String::new()` | `http::tests::raw_position_tokens_and_fork_refusals_are_exact`: `left: ""` / `right: "000000R0000000000030000000"` |
| | `append_position`: `"xyzzy".into()` | same test, `left: "xyzzy"` |
| | `parse_fork_offset -> Result<u64, String>`: `Ok(0)` | same test: `parse_fork_offset("000000000000000000N0000000")` gives `left: Ok(0)`, `right: Ok(42)` |
| | `parse_fork_offset`: `Ok(1)` | same test, first assertion: `left: Ok(1)`, `right: Ok(0)` |
| | `tail_token -> String`: `String::new()` | same test: `left: ""`, `right: "00000000000000000000000000"` |
| | `tail_token`: `"xyzzy".into()` | same test |
| `http_read` (`http::read:: dst_tests::reads_raw:: … dst_tests::read_application::`) | `raw_position -> String`: `String::new()` | `dst_tests::read_application::r06_empty_filtered_page_has_one_position_across_application_and_protocols`: `left: Err(Length(0))`, `right: Ok((0, 3))` (the raw GET header comes from `read.rs:426`) |
| | `raw_position`: `"xyzzy".into()` | same test, `left: Err(Length(5))`. Also every `reads_raw` token-following test (bounded loops and status asserts, so they fail fast rather than hang) |
| | `raw_start -> Result<ReadStart, ReadFailure>`: `Ok(Default::default())` | **unviable**: `ReadStart` derives only `Clone, Copy, Debug` (`read_request.rs:10`) |
| `sse_wire` (`sse::`) | `sse_control_ep -> String`: `String::new()` / `"xyzzy".into()` | `sse::wire::tests::raw_control_names_next_in_its_segment` |
| `read_remote` (`application::read …`) | `remote_read_page`: `Ok(Default::default())` | **unviable**: `ReadOutcome` has no derives (`read_request.rs:47`) |

That is 12 listed mutants: 10 viable, all caught, and 2 unviable. None is equivalent. For hangs: no test in any of these filters follows a raw token in an unbounded loop (checked in `reads_raw.rs` 22-40 and 625-645). No `sse::` unit test drives `Surface::RawToken`, so the `sse_control_ep` mutants fail only the new test and nothing parks. The `http` filter's `security_workload::`/`debug_surface_` tests read `offset=now` and never use these tokens.

Owner rows and filters: no change. There are no new files under a critical prefix, and every killer is already inside the owner's filters. `src/offsets.rs` is deliberately **not** registered in this step. Its verbatim decoder has equivalent mutants (`(n << 5) | v` becomes `^`, and `(epoch<<96) | (next<<32)` becomes `^`, because the operands never share bits). Registering it is item 89's job, after step 2 restructures those expressions.

---

## 6. Ledgers (all in C2 unless noted)

- `docs/refactor/test-inventory.json`: run `python3 scripts/test-inventory.py --write`. It changes `function_sha256` for exactly 9 tests: `r06_empty_filtered_page_has_one_position_across_application_and_protocols`, `the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope`, `raw_up_to_date_rides_only_the_last_record_of_a_multi_record_window`, `livefeed_raw_surface_uses_the_raw_vocabulary`, `livefeed_exact_framing_mixed_surfaces_share_one_lane`, `livefeed_exact_framing_mixed_surfaces_product_first`, `livefeed_raw_disconnects_without_terminal_on_split`, `livefeed_raw_late_attach_after_swap_gets_no_lineage_scalars` and `livefeed_raw_swap_between_peek_and_attach_is_refused`. The count stays at 517, with no names, attributes, scenarios or configuration lines changed.
- `docs/quality/owners.json`: add one row next to the `src/quota/bucket.rs` proptest row. Order is not checked; `from_entries` rejects only duplicates.
  ```json
  {"category": "macro-dsl", "count": 1, "owner": "crate::tests::macro(proptest::proptest)", "path": "src/offsets.rs",
   "reason": "Offset codec round-trip and order property; the pinned macro generates 1,024 cases against the production encode/parse; syntax tokens remain inventoried.",
   "syntax": "proptest::proptest"}
  ```
- `docs/quality/source-allowances.json`: no change. The `src/offsets.rs` `unresolved-glob` row stays in use because `use super::*` is kept, and no row is vacated.
- `docs/refactor/review-mechanisms.json`, `review-unit-relocations.json`: none of the 9 tests or the `raw_next_tok` helper is pinned there (checked by grep). No change.
- `scripts/quality/mutation_owners.py`, `docs/refactor/architecture-policy.json`, the scenario map and dispositions, and `src/dst/tests/README.md`: no change (no new files, no renames, no new DST modules).
- `docs/refactor/WIRE-MATRIX.md`: no wire rows change. Prose hygiene in C2: replace the 4 mentions of `encode_ep` / `` `Offset`/`encode_ep` `` (lines 40, 48, 233, 237) with `offsets::encode(segment, next)` / `offsets::encode`/`offsets::parse`, so that `git grep encode_ep` is empty outside the immutable legacy ledgers.
- Immutable, and must not be edited: `docs/quality/legacy-diagnostics*.json`, which still name `crate::Offset…` / `crate::encode_ep` (`policy.json immutable_sha256`). `docs/refactor/test-additions.json`'s r06 hash is a historical record that no gate reads.

---

## 7. Controls (run after the in-tree mutation and gate runs finish)

C1:
1. `cargo fmt --all -- --check` should print nothing and exit 0.
2. `scripts/test-leg.sh target/legs/offset-pins.log --min 3 --exact http::tests::raw_position_tokens_and_fork_refusals_are_exact --exact sse::wire::tests::raw_control_names_next_in_its_segment --exact offsets::tests::non_canonical_tokens_keep_their_lax_reading -- --locked --lib -- --exact http::tests::raw_position_tokens_and_fork_refusals_are_exact sse::wire::tests::raw_control_names_next_in_its_segment offsets::tests::non_canonical_tokens_keep_their_lax_reading` should print `test result: ok. 3 passed; 0 failed` and `TESTS_RAN_OK: target/legs/offset-pins.log: floor 3, exact 3`.

C2:
3. `git grep -n -E '(^|[^A-Za-z_:])Offset(\(|::)|offsets::Offset' -- src` and `git grep -n -w -E 'encode_ep|parse_ep' -- src` should both print nothing.
4. `cargo clippy --locked --workspace --all-targets -- -D warnings` should be clean, including no `unfulfilled_lint_expectations` and no `dead_code` on `OffsetError`.
5. `cargo test --locked --lib offsets::` should give `test result: ok. 4 passed` (round_trip, epoch_round_trip, the lax test and the quality_ property).
6. `cargo test --locked --lib golden_tests::cursors::` should pass. The same test-leg command as in step 2 should still give `3 passed` with the new API.
7. `cargo test --locked --lib -- dst_tests::read_application:: dst_tests::read_page_limits:: dst_tests::reads_raw:: dst_tests::sse_delivery:: dst_tests::livefeed_basics:: dst_tests::livefeed_swap::` should be all ok.
8. `cargo test --locked --release --lib quality_offsets_` should give `1 passed`, with 1,024 cases.
9. `python3 scripts/test-inventory.py --check` should give `test-inventory: OK (517 tests, 0 ignored)`.
10. `scripts/quality.sh` should exit 0. In particular: no `file growth` (http.rs 3,220 ≤ 3,225), no `accepted exception grew`, no `unregistered source occurrence`, and no `obsolete source allowances`.
11. `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out target/quality-plan` should produce a `plan.json` whose `mutation_source_files` is the 4 files in section 5, `selected_mutation_owners` is `["http_read","read_remote","sse_wire","http"]`, and `unregistered_mutation_source_files` is `[]`.
12. `cargo mutants --list --json --in-diff target/quality-plan/pr.diff --file <src> --package streams-slate` for each of the 4 files should list exactly the 12 mutants in section 5. If an extra mutant appears because of how the diff aligned, add a killer or reshape the hunk before pushing.
13. `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=… scripts/quality/mutations.sh` should report `Mutation verification executed 12 selected mutant(s) across 4 registered owner(s).`, with 10 caught, 2 unviable, 0 missed and 0 timeouts.
14. The full `cargo test --locked` should pass, with `tests_ran.py --inventory docs/refactor/test-inventory.json --skipped 1` giving TESTS_RAN_OK.

---

## 8. Out of scope

- Step 2: the strict decoder, `indexing_slicing`/`arithmetic_side_effects` on `offsets.rs`, the malformed-data property, and the alias decision (section 9A). The step-2 red test is P-LAX with each `Ok(..)` flipped to `Err(..)`.
- The 30-bit epoch ceiling fix (9B). Step 1 only documents it in the module doc and limits the property's domain to match.
- Item 89: registering `src/offsets.rs` in `mutation_owners.py` (it has the equivalent `|`/`^` mutants noted in section 5).
- The Python decoders in `bench/soak/reconcile.py` and `bench/fleet/drain-account.py`, which are bench tooling copies.
- Narrowing `tail_token` from `pub(crate)` (it only has callers in `http.rs`), and inlining the DST helper `raw_next_tok` (that would change 4 more test hashes for no gain).

---

## 9. Decisions for Søren (step 1 needs none; these belong to step 2)

**A. Strict decoder: this changes the wire.** Today the four non-canonical classes in 1c are served as a position. A raw read or SSE connect returns records from a position the client never held, and a fork is created at that position. With a strict decoder they get `400 invalid_offset` (raw reads, SSE, internal segment-read) or `400 invalid_fork_offset`, and that second one needs new Display text for the new variants. One sub-decision is Crockford aliases: `O/o→0`, `I/i/L/l→1`, and lowercase, all accepted today, none ever emitted by our encoder.
- Strict-all: accept only the 32 uppercase canonical characters.
- Backward-compatible alternative (recommended): keep accepting aliases and lowercase, because that is Crockford's own decoding rule and a client that case-folded a token keeps working. Refuse only the structural classes (first digit > `'7'`, nonzero pad, nonzero in_block, non-ASCII). No server has ever minted one of those, so no well-behaved client can hold one.
- A log-only window before refusing would need a new counter on `/metrics`, which is itself an edge change. It is not recommended.

**B. The 30-bit epoch ceiling (1d).** `encode` silently drops epoch bits 30-31, so segment 2^30+k's tokens collide with segment k's.
- (a) Recommended: cap segment-id allocation at 2^30 in `segmap` (`IdExhausted`). There is no wire change, and the unrepresentable ordinal can then never be allocated.
- (b) Make `encode` fallible. That affects all five render paths.
- (c) Keep it documented only, which is what step 1 does.

---

## Skeptic corrections (C1..C6)

Checked against `33fbd10e` (origin/slate == HEAD, clean tree). Everything else I checked holds up, and each claim below was re-derived from source:
- The use-site list is complete. `git grep -w Offset`, `encode_ep|parse_ep|scan_from()` and `crate::offsets::` over `src` (DST, `cfg(test)` and golden included) return exactly E1-E5, D1-D3, I, the 9 DST sites, the helper and golden. No `#[path]` includer exists in `fuzz/`, `tools/` or `src/bin`, and no second Crockford copy exists in `src`.
- `wc -l` for every ceilinged file matches. http.rs goes from 3,225 to 3,220 (−1 for the import, −4 for `tail_token`), under a budget of 3,225.
- Every literal was re-simulated: `tail_token(42)`, `(3,6)`, `u64::MAX`, the four P-LAX classes (→ (0,0), (0,2), (0,1), (0,1)), the `'U'` refusal and the hex fork branch. All match.
- `ReadStart` has no Default (`read_request.rs:10`) and `ReadOutcome` has no derive (`read_request.rs:43-47`), so those FnValue mutants are unviable. A prior run confirms it: `target/quality-mutations/read_remote/mutants.out/outcomes.json` lists `remote_read_page Ok(Default::default()) Unviable`.
- For `Result<_, String>` the tool generates only `Ok(0)`/`Ok(1)`, with no `Err` replacement. Prior runs show `-> Result<usize, String>` with exactly those two. The genres seen in this repo's runs are FnValue, BinaryOperator, UnaryOperator, MatchArm, MatchArmGuard and StructField. There is no if-condition genre, so `raw_position`'s `if segmented` adds no mutant.
- The owner order is `http_read, read_remote, sse_wire, http` (mutation_owners.py:88,126,146,152). Every killer sits inside its owner's substring filters.
- The only `#[expect]` scopes near the edits are `render_append` `unwrap_used` (text unchanged), `render_raw_read` ×4 (untouched) and `sse_control_ep` `fn_params_excessive_bools`, which shrinks. There are no crate or impl-wide expects in `offsets.rs`, `http.rs`, `http/read.rs`, `sse/wire.rs`, `read_remote.rs` or the 4 DST files.
- The test-inventory impact is exactly the 9 hashes listed. `raw_next_tok` is not inventoried. `review-mechanisms.json`, `review-unit-relocations.json` and `test-relocations.json` pin none of the 9 tests; test-relocations has names only.
- `legacy-*.json` are immutable (policy.json `immutable_sha256`). `architecture-review-baseline.json` and `verification.json` are historical a7e2070f pins that no gate compares to the working tree, and http.rs has long since drifted from both. Neither `WIRE-MATRIX.md` nor `test-additions.json` is read by any script.
- The owners.json identity `("macro-dsl", "src/offsets.rs", "crate::tests::macro(proptest::proptest)", "proptest::proptest")` has the same shape as the quota/bucket.rs row (owners.json:348-354). Nested `prop_assert_eq!` inside `proptest!` is not inventoried (bucket.rs:110 has no row).

**C1: the property plan does not meet the codec rule, and it doesn't have to wait for step 2.** `docs/RUST-QUALITY.md:153` requires valid **and malformed** data at 1,024 or more cases for every affected codec property. Section 3d postpones the malformed half, arguing that the grammar is step 2's decision. A grammar-neutral malformed property exists today and stays true after any strict decoder, because a strict decoder only shrinks the `Ok` set. The key fact is that `parse` can never return an epoch of 2^30 or more: `n >> 2` leaves 126 bits, so `n >> 96 < 2^30` (offsets.rs:53-54). Every accepted token therefore names a position that `encode` represents canonically. Add a second `quality_` property in the same `proptest!` block (the macro-dsl count stays 1), for example `quality_offsets_any_accepted_token_names_one_canonical_position`. Its input is a string of 0-30 chars drawn from a biased set: canonical digits, `'8'..'Z'` first digits, the aliases `O o I i L l`, lowercase, `'U'`, `'-'` and `'\u{131}'`, and also `"-1"`. It asserts:
- (a) `parse` returns `Err(Length(s.len()))` exactly when `s != "-1" && s.len() != 26`;
- (b) `Ok((e, n))` implies `e < 1 << 30` and `parse(&encode(e, n)) == Ok((e, n))`;
- (c) `parse_scalar(s)` equals `parse(s)` mapped through the epoch-0 rule, including `Err(Epoch(e))`;
- (d) nothing panics.

Add `use proptest::{collection, sample}` or fully qualified paths, with no glob. Control 5 then becomes `5 passed`, and control 8 becomes `cargo test --locked --release --lib quality_offsets_` → `2 passed`. Also say in the module doc that the decode side cannot produce the colliding ordinals, because the 1d collision only happens in encoding.

**C2: `render_append`'s exception scope is wrong in section 4.2.** The `#[expect(clippy::unwrap_used)]` scope is `src/http.rs:37-108`, not 37-67. It holds **two** `append_position` call sites: `:53` (`append_position(out.seg_id, out.next_offset, out.materialized)`) and `:101` (`append_position(seg, next, materialized)` inside `HeaderValue::from_str(..).unwrap()`). The text of both stays unchanged, so the call-site, path and ordinary-call fingerprints still do not move, and the conclusion holds. Cite both anyway so the implementer doesn't "tidy" line 101.

**C3: line counts.** The `sse_delivery.rs` helper (`:380-389`, 10 lines) becomes 5 lines (2 doc, signature, body, `}`), so the file goes from 774 to **769**, not 770. The `read_application.rs:82-87` replacement is written on one line in section 4.7, but it is over 100 columns, so rustfmt splits it into `assert_eq!(\n crate::offsets::parse(..),\n Ok((0, out.next.after))\n );`. That is 4 lines, so the 485 → 483 count stays correct. Write it in that form so `cargo fmt --check` (control 1) stays clean on the first try. Neither change touches a ceiling.

**C4: the hang/TIMEOUT argument needs to cover the tests that actually follow the mutated token.** The driver runs every test in the owner's filters under `--timeout 90` (mutation_owners.py:52). A single slow test is enough to turn a caught mutant into a TIMEOUT. Section 5 cites only reads_raw.rs 22-40 and 625-645. Add these, all bounded:
- `dst_tests::reads_raw::seal_gap_long_poll_wakes_without_closure` (reads_raw.rs:116-151). It parks a long poll on `stream-next-offset`. Under `raw_position -> ""/"xyzzy"`, the poll returns 400 immediately and the test fails at `:147` (`poll woke with 400`). The held split is released when `FailpointGuard` drops during unwind, and the poll has an 8 s bound in any case.
- `fixture_requests.rs:149-181` `drain_no_closure`, at most 64 pages.
- `livefeed_engine_retired` (`http` filter), which uses only the product surface (`?cursor=now`), so tail_token and append_position mutants do not reach it.

**C5: an unsupported claim in the new doc comment.** Section 3d's property doc says "per-key readers compare them as strings". No reader in `src` compares raw tokens as strings. The contract that does exist is "opaque, lexicographically sortable offsets" (`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/00-OVERVIEW.md:71`, with SPEC.md:355 C7 opacity). Reword it along those lines: "the surface contract promises lexicographically sortable offsets". Per the repo's style rule, a doc comment must state a reason that is true.

**C6: the `tail_token` doc is imprecise.** Section 4.2's proposed doc says "Create and unsplit-append responses answer in the scalar (epoch 0) lane." `append_position` chooses `tail_token` when `!materialized`, and `materialized` is `desc.segments.is_some()` (`src/application/append.rs:409`). That is not "unsplit": a stream with a one-segment map takes `encode(seg, next)` (http.rs:32). Reads use a different predicate again, `segments.len() > 1 || pending` (http/read.rs:110-113). Suggested wording: "Create responses and appends to a stream without a segment map answer in the scalar (epoch 0) lane." This is documentation only, with no behaviour change.

**Controls.** All are buildable. `test-leg.sh`/`tests_ran.py` (`--min`, `--exact`), the `verification_plan.py --out` + `pr.diff` path, `cargo mutants --list --json --in-diff … --file … --package streams-slate` (matches `mutation_driver.list_command`), the `Mutation verification executed N selected mutant(s) across 4 registered owner(s).` string (mutation_driver.py:158) and `test-inventory: OK (517 tests, 0 ignored)` (test-inventory.py:239) all match the scripts. Update controls 5 and 8 for C1.

**Ledgers.** None were missed. After C1 the owners.json macro-dsl row still has count 1, because both properties sit in one `proptest!` invocation. If they are split into two invocations, the count must be 2.

**Verdict: ready-with-corrections.** It is behaviour-preserving as traced, and the mutation set is complete: 12 mutants, 10 killable, 2 unviable, no equivalents, since `offsets.rs` is unregistered. C1 is required by policy. C2-C6 are corrections to citations, line counts and doc text.
