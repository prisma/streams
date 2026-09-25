# Plan: producer and Stream-Seq rows that do not decode are corruption (review item 55)

Repository `/Users/sorenschmidt/code/streams`, branch `slate @ 0afa2597` (= `origin/slate`).
I only read files. Nothing in the repo was edited or run. I re-found every line number
below by its content on the current tree. The reviewer's numbers
(`src/shard.rs:2076-2137`, `src/shard/transaction/append.rs:174-180`) are still exact.

**Verdict on the review claim: CORRECT and current, and slightly understated.** Both lane
loaders read a row that exists but does not decode as if it were missing. The
reviewer says the producer loader "falls to `Ok(None)`". It is worse than that. A row
shorter than 16 bytes is skipped, and the loop carries on into the sealed-predecessor
chain. So a corrupt row on the segment's own identity serves an older predecessor's
producer state in its place. Rows of 24..39 bytes silently lose the request hash, and
rows of 41 bytes or more silently ignore their trailing bytes.

**Verdict on the reviewer's Change:** the decoder design (exact width, `split_first_chunk`,
strict UTF-8, delete the `unwrap` expectation, a 1,024-case property) is right and
buildable. The *placement* ("beside `decode_cursor`" in `src/shard.rs`) is not the right
one:
- it puts a new decoder in a file that cannot turn on the decoder-owner lints
  (`indexing_slicing`, `arithmetic_side_effects`, which `docs/RUST-QUALITY.md:25` makes a
  MUST for decoder owners);
- it lands `shard.rs` at about 3,229 of its 3,232 ceiling.

I place the codec in a new sibling decoder owner, `src/shard/lane_rows.rs`, following the
precedent of `src/shard/record.rs`. `shard.rs` shrinks to about 3,197. No verbatim-move
commit is needed: nothing moves between files, the loaders stay in `shard.rs`, and every
ceilinged file shrinks or is untouched.

---

## 1. Problem (verified, with quotes)

### 1.1 `load_producer_chain` accepts minimum widths, then falls through — `src/shard.rs:2090-2137`

```rust
2090    #[expect(
2091        clippy::unwrap_used,
2092        reason = "ShardEngine::load_producer_chain; the stored rows are fixed-width, so every eight-byte field slice converts; a fallible decode would add an error path no stored row reaches"
2093    )]
2094    async fn load_producer_chain(
...
2101        for identity in std::iter::once(own).chain(lineage.iter()) {
2102            match self.db.get(producer_key(identity, key_hash, pid)).await? {
2103                Some(v) if v.len() >= 40 => {
...
2113                Some(v) if v.len() >= 24 => {
...
2121                Some(v) if v.len() >= 16 => {
2122                    // Legacy 16-byte row: the commit offset is UNKNOWN.
...
2133                _ => {}
2134            }
2135        }
2136        Ok(None)
```

| Stored width | Today | Consequence |
|---|---|---|
| 0..15 | `_ => {}`: next identity in the chain, then `Ok(None)` | The lane reopens at producer seq 0 (a duplicate is accepted as new). If a predecessor holds a row, *its older state* is staged as the lane's truth. |
| 16 | legacy `(epoch, seq, u64::MAX, [0;16])` | correct |
| 17..23 | decoded as 16 | torn row accepted |
| 24 | `(epoch, seq, offset, [0;16])` | correct |
| 25..39 | decoded as 24 | torn full row loses its request hash, so `producer_sequence_reused` can no longer fire |
| 40 | full | correct |
| 41+ | decoded as 40 | trailing bytes ignored |

The expectation's reason ("a fallible decode would add an error path no stored row
reaches") is exactly the leniency this item removes.

### 1.2 `load_seq_chain` reads non-UTF-8 as "no sequence" — `src/shard.rs:2076-2088`

```rust
2082        for identity in std::iter::once(own).chain(lineage.iter()) {
2083            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
2084                return Ok(String::from_utf8(v.to_vec()).ok());
2085            }
2086        }
```

A present, non-UTF-8 row returns `Ok(None)` immediately (no fall-through). In
`append.rs:127-137`, `None` means "no current sequence", so *any* `Stream-Seq` is
accepted and the monotonic sequence guarantee is lost for that routing key.

### 1.3 The strict siblings it should match

- `src/shard.rs:154-166` `stored_tail`: "Existing malformed bytes must never initialize a
  fresh segment." It returns `Err(slatedb::Error::data("invalid persisted tail encoding"))`.
  `decode_tail` also rejects a non-UTF-8 seq (`String::from_utf8(..).ok()?`, `:117`).
- `src/shard.rs:169-175` `decode_cursor`: "short and trailing bytes indicate corruption.
  Absence is handled separately by the repository."
- `src/shard.rs:556-560` `decode_dirty_value`: `if !matches!(v.len(), 16 | 24 | 32) { return None; }`,
  with the comment "Preserve complete legacy fields (16/24 bytes) and current v2 (32).
  Partial fields or unknown extensions are corrupt, never an empty marker." Its caller
  turns `None` into an error (`:2223-2224`). This is the exact precedent for keeping
  complete legacy widths and rejecting everything else.

### 1.4 The writer — `src/shard/transaction/append.rs:169-180`

```rust
169            let rhash = pr.request_hash.unwrap_or([0u8; 16]);
170            local.producer.rows.insert(
171                (req.key_hash, pr.id.clone()),
172                (pr.epoch, pr.seq, commit_last, rhash),
173            );
174            let mut v = Vec::with_capacity(40);
175            v.extend_from_slice(&pr.epoch.to_le_bytes());
176            v.extend_from_slice(&pr.seq.to_le_bytes());
177            v.extend_from_slice(&commit_last.to_le_bytes());
178            v.extend_from_slice(&rhash);
179            self.batch
180                .put(producer_key(&hash, &req.key_hash, &pr.id), v);
```

This is the only production writer of producer rows (`git grep producer_key -- src`), and
it always writes 40 bytes. The byte layout therefore has two owners today: this inline
writer and the lenient reader. Stream-Seq rows are written at `append.rs:237-238`
(`seq.clone().into_bytes()`). The value comes from `hdr()` (`src/http.rs:2458-2463`,
`HeaderValue::to_str`), so it is always visible ASCII.

### 1.5 Reachability ("practically unreachable" — agreed)

With only the 40-byte writer, a malformed row needs storage damage or a foreign writer.
Width history (`git log -S`):
- the 16-byte writer ended at `e032edf0` (2026-07-27);
- the 24-byte writer ended at `bf4fe33c` (2026-07-31);
- the current `<hash>'q'<key_hash><pid>` key layout dates from `e6717f9c` (2026-07-30),
  so no 16-byte row was ever written under the current key;
- layout 4 (`c7beda35`, 2026-08-15) refuses every older namespace
  (`src/registry.rs:488-499`, "deploy against a fresh bucket/PATH_PREFIX").

So every row a layout-4 store can hold is 40 bytes (this feeds decision D2).

### 1.6 Doc defect in the edited block

The rustdoc of `load_seq_chain` (`src/shard.rs:2060-2075`) stacks three paragraphs:
1. an orphan "Enumerate the durable dirty-stream index…" paragraph (2060-2064). It is
   left over from a moved function, and `dirty_key`'s own doc (`:202-222`) states the
   same thing more accurately, including trim debt;
2. the producer-chain paragraph (2065-2070), which belongs on `load_producer_chain`;
3. its own paragraph (2071-2075).

Because this item rewrites both loaders, the doc repair is in scope: delete (1), move (2).

---

## 2. Contract decision

### 2.1 Typed decoder contract (new owner `src/shard/lane_rows.rs`)

| Function | Ok | Err (`slatedb::Error::data`) |
|---|---|---|
| `decode_producer_row(&[u8]) -> Result<(u64, u64, u64, [u8; 16]), slatedb::Error>` | exactly 16 bytes → `(epoch, seq, u64::MAX, [0;16])`; 24 → `(epoch, seq, offset, [0;16])`; 40 → `(epoch, seq, offset, hash)` | every other width: `"invalid persisted producer row width"` |
| `encode_producer_row((u64, u64, u64, [u8; 16])) -> Vec<u8>` | the 40-byte form (the single writer) | — |
| `decode_seq_row(&[u8]) -> Result<String, slatedb::Error>` | exactly the UTF-8 text stored (empty allowed; an empty `Stream-Seq` header stores an empty row) | non-UTF-8: `"invalid persisted Stream-Seq row encoding"` |

`u64::MAX` and a zero hash keep their existing meanings in `decide_producer`
(`src/shard/commit_plan.rs:88-143`, unchanged): unknown commit offset, and "matches any
request".

### 2.2 Loader contract (unchanged signatures)

`load_producer_chain` and `load_seq_chain` are decided by the **nearest identity whose row
exists**.
- `Ok(None)` only when no identity in the chain has a row.
- A present row either decodes or is `Err`. It never defers to an older predecessor.

### 2.3 What changes at the edge (only when a lane row is corrupt)

The loader `Err` already maps (unchanged code) through `append.rs:42-45` / `:117-120` to
`AppendErr::Internal`, then through `contract.rs:276` to `FailureClass::Internal`:
- **raw**: `500 internal` (`src/http.rs:27`; `WIRE-MATRIX.md:41` already lists `500 internal`);
- **product**: `500 append_failed`, `retryable: false` (`src/product.rs:2421`;
  `WIRE-MATRIX.md:117`'s "else `append_failed`").

No new status, code or header, so there is no WIRE-MATRIX edit. This is the same refusal a
corrupt tail already produces (`src/shard/transaction/mod.rs:152-156`, `stream_handle`
error becomes `AppendErr::Internal`).

The refusal writes nothing and consumes no offset (the group has no writes;
`finalize.rs:12-18`). The blast radius is exactly the requests that consult the corrupt
row:
- a corrupt producer row refuses appends carrying *that producer id* on *that routing key*;
- a corrupt seq row refuses appends carrying `Stream-Seq` on *that routing key*;
- plain appends to the same stream are unaffected (both loads are gated on
  `req.producer` / `req.seq`, `append.rs:27` / `:103`).

A row already resident in `st.producers` / `st.seqs` is not re-read, so a live owner keeps
serving until restart, handoff or eviction.

**Backward-compatible alternative:** keep reading an undecodable row as missing (today's
behaviour), optionally with a log line or counter. This is not recommended: the result is
silent loss of producer idempotence, meaning a duplicate append commits a second copy
under a fresh offset. It also loses Stream-Seq monotonicity. Neither can be detected by the
client.

### 2.4 Decisions for Søren

- **D1 (edge):** approve the fail-closed refusal in 2.3 (raw `500 internal`, product
  `500 append_failed`, non-retryable, nothing written) for appends whose producer or
  Stream-Seq row does not decode. This also covers "a corrupt own row no longer falls
  through to a predecessor". The alternative is 2.3's "keep reading as missing".
- **D2 (storage compatibility):** which widths does the decoder accept?
  - **Recommended: {16, 24, 40}.** Every row that decodes today at an exact width keeps its
    meaning. It mirrors `decode_dirty_value`'s retained `{16, 24, 32}` and keeps this item
    a pure strictness change.
  - **Alternative: {40} only.** Per 1.5, the 16- and 24-byte arms are unreachable in any
    layout-4 namespace. Choosing this deletes both arms and the legacy doc sentence.
    Deltas: T3 and T5 expect `Some` only at 40, and `decode_producer_row` becomes one
    `<&[u8; 40]>::try_from` plus three `split_first_chunk`s.

---

## 3. Red tests

Red work order (all of this lands in ONE commit together with the fix):
- **(i)** add T1 and T2 only, and run them: runtime RED;
- **(ii)** add T3, T4 and T5: compile RED;
- **(iii)** apply section 4: GREEN.

T1 and T2 compile on today's tree because child modules of `shard` can call the private
`ShardEngine::load_*_chain` and `commit_group`.

### T1 — `shard::storage_decode_tests::r12_undecodable_lane_rows_are_corruption_not_absence` (runtime red)

File: `src/shard/storage_decode_tests.rs` (119 lines, "R12: corrupt storage refuses to
open…", the reviewer's file). Append a test-local oracle and the test. The oracle stays
after the fix as an encoder independent of production, like r03a's hand-built row at
`transaction_tests.rs:215-220`.

```rust
/// Hand-built producer row, independent of the production encoder.
fn stored_producer_row(epoch: u64, seq: u64, offset: u64, hash: [u8; 16]) -> Vec<u8> {
    let mut row = Vec::with_capacity(40);
    for field in [epoch, seq, offset] {
        row.extend_from_slice(&field.to_le_bytes());
    }
    row.extend_from_slice(&hash);
    row
}

/// Review item 55: a lane row that exists but does not decode is corruption.
/// Read as missing it reopened the lane (producer seq 0, unset Stream-Seq) or
/// served an older predecessor's state in its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r12_undecodable_lane_rows_are_corruption_not_absence() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r12-lanes", store.clone()).build().await.unwrap());
    let (own, parent, key) = ([12; 16], [13; 16], [14; 16]);
    let mut wb = WriteBatch::new();
    wb.put(producer_key(&parent, &key, "p"), stored_producer_row(7, 3, 11, [9; 16]));
    wb.put(seq_key(&parent, &key), b"parent-seq");
    db.write(wb).await.unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r12-lanes".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let producer = engine.load_producer_chain(&own, &[parent], &key, "p").await.unwrap();
    assert_eq!(producer, Some((7, 3, 11, [9; 16])), "a missing own row defers to its predecessor");
    let seq = engine.load_seq_chain(&own, &[parent], &key).await.unwrap();
    assert_eq!(seq.as_deref(), Some("parent-seq"));
    assert_eq!(engine.load_producer_chain(&own, &[], &key, "p").await.unwrap(), None);
    assert_eq!(engine.load_seq_chain(&own, &[], &key).await.unwrap(), None);
    for width in [15, 17, 23, 25, 39, 41] {
        let mut wb = WriteBatch::new();
        wb.put(producer_key(&own, &key, "p"), vec![1u8; width]);
        db.write(wb).await.unwrap();
        let loaded = engine.load_producer_chain(&own, &[parent], &key, "p").await;
        assert!(loaded.is_err(), "a {width}-byte producer row must fail closed, got {loaded:?}");
    }
    let mut wb = WriteBatch::new();
    wb.put(seq_key(&own, &key), [0xffu8]);
    db.write(wb).await.unwrap();
    let loaded = engine.load_seq_chain(&own, &[parent], &key).await;
    assert!(loaded.is_err(), "a non-UTF-8 Stream-Seq row must fail closed, got {loaded:?}");
    engine.begin_close();
    engine.await_terminated(std::time::Duration::from_secs(5)).await.unwrap();
}
```

Typed literals (`1u8`, `0xffu8`) are required: `WriteBatch::put` takes `V: AsRef<[u8]>`,
which does not fix the integer type. Width 0 is deliberately absent: an empty value is
T3's job, which avoids depending on SlateDB's empty-value handling. All waits are bounded.

**Expected red on the current tree.** The four positive assertions pass today. Width 15
takes `_ => {}` into the parent row:

```
---- shard::storage_decode_tests::r12_undecodable_lane_rows_are_corruption_not_absence stdout ----
thread 'shard::storage_decode_tests::r12_undecodable_lane_rows_are_corruption_not_absence' panicked at src/shard/storage_decode_tests.rs:<line of that assert!>:9:
a 15-byte producer row must fail closed, got Ok(Some((7, 3, 11, [9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9])))
...
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; <N> filtered out
```

### T2 — `shard::transaction_tests::r12_undecodable_lane_rows_refuse_the_append_and_write_nothing` (runtime red; pins D1)

File: `src/shard/transaction_tests.rs` (433 lines; reuses `Fixture::new/append/rows`).
This is a new function: the review-pinned `r03a_…` body is untouched.

```rust
/// Review item 55: an undecodable lane row refuses the append as internal
/// and writes nothing: no offset is consumed and no ack claims a lane state
/// the store does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r12_undecodable_lane_rows_refuse_the_append_and_write_nothing() {
    let fixture = Fixture::new().await;
    for (label, row, value) in [
        ("a 15-byte producer", producer_key(&HASH, &KEY, "writer"), vec![1u8; 15]),
        ("a non-UTF-8 Stream-Seq", seq_key(&HASH, &KEY), vec![0xffu8]),
    ] {
        let mut seed = WriteBatch::new();
        seed.put(&row, &value);
        fixture.engine.db.write(seed).await.unwrap();
        let before = fixture.rows().await;
        let (append, reply) = fixture.append(HASH, 0, AppendFinish::Open);
        fixture.engine.commit_group(vec![append], &fixture.cfg).await;
        let result = tokio::time::timeout(Duration::from_secs(5), reply).await.unwrap().unwrap();
        assert!(
            matches!(result, Err(AppendErr::Internal(_))),
            "{label} row must refuse the append, got {result:?}"
        );
        assert_eq!(fixture.rows().await, before, "{label} row: a refused append writes no row");
        let mut clear = WriteBatch::new();
        clear.delete(&row);
        fixture.engine.db.write(clear).await.unwrap();
    }
    fixture.journal.close();
    fixture.engine.begin_close();
    fixture.engine.await_terminated(Duration::from_secs(5)).await.unwrap();
}
```

`Fixture::append` carries `billing: None`, so this test can never consume r03a's
prefix-keyed `billing_read_faults()` entry (the `"r03a-mixed"` prefix is shared). Billing
rows are read only for `Append { billing: Some }` and usage/billing ops
(`prepare.rs:60-66`).

**Expected red on the current tree.** The 15-byte row reads as missing. Producer seq 0 is
accepted, the seq lane is empty, `seal_gen 10 >= fence 0`, and the append commits:

```
thread 'shard::transaction_tests::r12_undecodable_lane_rows_refuse_the_append_and_write_nothing' panicked at src/shard/transaction_tests.rs:<line of that assert!>:9:
a 15-byte producer row must refuse the append, got Ok(AppendAck { last_offset: 0, next_offset: 1, closed: false, producer: Some((1, 0)), duplicate: false })
...
test result: FAILED. 0 passed; 1 failed; ...
```

### T3 — `shard::storage_decode_tests::r12_producer_row_requires_exact_supported_width` (the reviewer's name; compile red)

```rust
#[test]
fn r12_producer_row_requires_exact_supported_width() {
    let mut row = stored_producer_row(7, 3, 11, [9; 16]);
    assert_eq!(encode_producer_row((7, 3, 11, [9; 16])), row, "new commits write the full width");
    row.push(0);
    for len in 0..=row.len() {
        let expected = match len {
            16 => Some((7, 3, u64::MAX, [0; 16])),
            24 => Some((7, 3, 11, [0; 16])),
            40 => Some((7, 3, 11, [9; 16])),
            _ => None,
        };
        assert_eq!(decode_producer_row(&row[..len]).ok(), expected, "len={len}");
    }
}
```

### T4 — `shard::storage_decode_tests::r12_stream_seq_row_requires_utf8` (compile red)

```rust
#[test]
fn r12_stream_seq_row_requires_utf8() {
    assert_eq!(decode_seq_row(b"alpha").unwrap(), "alpha");
    assert_eq!(decode_seq_row(b"").unwrap(), "");
    for raw in [&[0xffu8][..], &[b'a', 0xc3][..], &[0xed, 0xa0, 0x80][..]] {
        assert!(decode_seq_row(raw).is_err(), "raw={raw:?}");
    }
}
```

### T5 — 1,024-case properties (decoder-owner requirement; compile red)

These are named `quality_*` so they also run in the quality workflow's "Generated codec
and actual handoff state tests" leg (`rust-quality.yml:48`, `--lib quality_`, the same
convention as `queue.rs:556/571` and `quota/bucket.rs:107/119`).

```rust
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 1024, ..proptest::prelude::ProptestConfig::default() })]

    /// Every committed producer state reads back exactly; arbitrary stored
    /// bytes decode only at a supported width, into the fields a hand-built
    /// row carries there.
    #[test]
    fn quality_producer_rows_decode_only_at_a_supported_width(
        epoch in proptest::num::u64::ANY,
        seq in proptest::num::u64::ANY,
        offset in proptest::num::u64::ANY,
        hash in proptest::array::uniform16(proptest::num::u8::ANY),
        raw in proptest::collection::vec(proptest::num::u8::ANY, 0..=48),
    ) {
        let row = (epoch, seq, offset, hash);
        proptest::prop_assert_eq!(decode_producer_row(&encode_producer_row(row)).ok(), Some(row));
        let field = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        let expected = match raw.len() {
            16 => Some((field(0), field(8), u64::MAX, [0; 16])),
            24 => Some((field(0), field(8), field(16), [0; 16])),
            40 => Some((field(0), field(8), field(16), raw[24..].try_into().unwrap())),
            _ => None,
        };
        proptest::prop_assert_eq!(decode_producer_row(&raw).ok(), expected);
    }

    /// Stream-Seq rows decode exactly the text they hold and nothing else.
    #[test]
    fn quality_stream_seq_rows_decode_exactly_their_utf8(
        text in "\\PC{0,24}",
        raw in proptest::collection::vec(proptest::num::u8::ANY, 0..=16),
    ) {
        proptest::prop_assert_eq!(decode_seq_row(text.as_bytes()).ok(), Some(text));
        proptest::prop_assert_eq!(decode_seq_row(&raw).ok(), std::str::from_utf8(&raw).ok().map(str::to_owned));
    }
}
```

(If the borrow checker rejects `Some(text)` after `text.as_bytes()` inside the macro's
expansion, bind `let expected = Some(text.clone());` first. Do not leave a clone that
`redundant_clone` flags.)

**Expected red for T3–T5 (step ii, before section 4).** The lib test target does not
compile. It shows only `E0425` for the three new names, one per call site as written (2 ×
`encode_producer_row`, 3 × `decode_producer_row`, 5 × `decode_seq_row`):

```
error[E0425]: cannot find function `encode_producer_row` in this scope
error[E0425]: cannot find function `decode_producer_row` in this scope
error[E0425]: cannot find function `decode_seq_row` in this scope
...
error: could not compile `streams-slate` (lib test) due to 10 previous errors
```

(rustc may add a `help:` "similar name" line. No error other than E0425 may appear.)

---

## 4. Edits, file by file, in commit order

**One commit** (tests, fix and ledgers; the red steps in section 3 are local only). There
are **no verbatim-move commits**: no code moves between files. The new codec is new code,
the loaders are rewritten in place, and the ceilinged file shrinks.

Suggested subject, in the branch's style: *"A producer or Stream-Seq row that does not
decode refuses the append; it never reads as a lane that never committed"*, ending with
`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### Ceiling budgets (current `wc -l`; none may grow)

| File | Now | After | Ceiling |
|---|---|---|---|
| `src/shard.rs` | 3,232 | **~3,197** (−37 region, +2 mod/use; rustfmt may move this ±2) | 3,232 |
| `src/http.rs` | 3,371 | untouched | 3,371 |
| `src/product.rs` | 4,205 | untouched | 4,205 |
| `src/billing.rs` | 2,201 | untouched | 2,201 |
| `src/history.rs` | 1,713 | untouched | 1,713 |
| `src/auth.rs` | 1,676 | untouched | 1,676 |
| `src/registry.rs` | 1,509 | untouched | 1,509 |
| `src/sse/feed.rs` | 1,200 | untouched | 1,200 |
| `src/fleet.rs` | 1,143 | untouched | 1,143 |

Other touched files (1,000-line rule):

| File | Now | After |
|---|---|---|
| `src/shard/lane_rows.rs` | new | ~45 |
| `src/shard/transaction/append.rs` | 298 | ~293 |
| `src/shard/storage_decode_tests.rs` | 119 | ~230 |
| `src/shard/transaction_tests.rs` | 433 | ~465 |

### 4.1 `src/shard/lane_rows.rs` (new decoder owner)

```rust
#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
//! Stored dedupe state of one routing-key lane: its Stream-Seq row and its
//! producer rows. A missing row means the lane never committed. Bytes that do
//! not decode are corruption and never read as missing, because a missing row
//! reopens the lane and accepts a duplicate or a regressed sequence as new.

/// Producer rows hold epoch and seq, then the commit offset (24 bytes) and
/// the request hash (40 bytes). Rows committed before a field existed stay
/// readable: `u64::MAX` marks an unknown offset and a zero hash matches any
/// request. Every other width is corruption.
pub(super) fn decode_producer_row(
    raw: &[u8],
) -> Result<(u64, u64, u64, [u8; 16]), slatedb::Error> {
    let invalid = || slatedb::Error::data("invalid persisted producer row width".into());
    let (epoch, rest) = raw.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (seq, rest) = rest.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (offset, hash) = match rest.split_first_chunk::<8>() {
        Some((offset, hash)) => (u64::from_le_bytes(*offset), hash),
        None => (u64::MAX, rest),
    };
    let hash: [u8; 16] = match hash {
        [] => [0; 16],
        hash => hash.try_into().map_err(|_| invalid())?,
    };
    Ok((u64::from_le_bytes(*epoch), u64::from_le_bytes(*seq), offset, hash))
}

/// New commits write only the full width; the decoder owns the older ones.
pub(super) fn encode_producer_row(row: (u64, u64, u64, [u8; 16])) -> Vec<u8> {
    let (epoch, seq, offset, hash) = row;
    let mut raw = Vec::with_capacity(40);
    for field in [epoch, seq, offset] {
        raw.extend_from_slice(&field.to_le_bytes());
    }
    raw.extend_from_slice(&hash);
    raw
}

/// Stream-Seq rows hold the header text of the lane's last accepted
/// sequence. Bytes that are not UTF-8 are corruption, never an unset
/// sequence that would accept any value.
pub(super) fn decode_seq_row(raw: &[u8]) -> Result<String, slatedb::Error> {
    String::from_utf8(raw.to_vec())
        .map_err(|_| slatedb::Error::data("invalid persisted Stream-Seq row encoding".into()))
}
```

Width walk:
- < 8 fails at `epoch`;
- 8..15 fails at `seq`;
- 16 gives `rest=[]`, then `(MAX, [])`, then zero hash;
- 17..23 gives `(MAX, 1..7 bytes)`, and `try_into` fails;
- 24 gives `(offset, [])`, then zero hash;
- 25..39 gives a 1..15-byte hash, which fails;
- 40 is exact;
- 41+ gives a 17+-byte hash, which fails.

No indexing and no arithmetic, so the owner lints pass with no `#[expect]`. No `use`
(no `unresolved-glob`), no macros, no statics, so no `owners.json` row is needed for this
file. Visibility `pub(super)`: `shard` and its descendants only.

### 4.2 `src/shard.rs` (3,232 → ~3,197)

1. **Module list** (`:27-31`): add `mod lane_rows;` after `mod history_partition;` (`:28`).
2. **Imports**: add `use lane_rows::{decode_producer_row, decode_seq_row, encode_producer_row};`
   next to `use commit_plan::{…}` (`:33-36`; rustfmt orders it).
   - `encode_producer_row` is not used in `shard.rs` itself. It reaches
     `transaction/append.rs` through the existing `use super::*` chain, which is the same
     route `decide_producer`/`seal_authorized` take today (`:35`; they are used only in
     `append.rs`), so it compiles warning-free.
   - The decoders reach `storage_decode_tests.rs` through its `use super::*`.
3. **Replace `:2060-2137`** (78 lines: the stacked docs, both loaders and the `#[expect]`)
   with these 41 lines:

```rust
    /// Split-safe Stream-Seq (review blocker 4): a sequence lane lives
    /// per (segment, routing key), but a child segment starts empty —
    /// without consulting its sealed predecessors, a sequence the
    /// PARENT already accepted would be accepted again on the child.
    /// Nearest identity wins, exactly like the producer chain: the nearest
    /// row that exists decides, and one that does not decode is corruption.
    async fn load_seq_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
    ) -> Result<Option<String>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
                return decode_seq_row(&v).map(Some);
            }
        }
        Ok(None)
    }

    /// Producer-state lookup through the routing key's predecessor
    /// chain (ROUTING-V3 §3.6): own identity first, then each sealed
    /// predecessor. A hit on a predecessor means the producer's last
    /// commit landed before a split — the caller stages it locally so
    /// the duplicate check answers with the ORIGINAL offset and no new
    /// offset is consumed. A row that exists but does not decode is
    /// corruption: it never defers to an older predecessor's row.
    async fn load_producer_chain(
        &self,
        own: &[u8; 16],
        lineage: &[[u8; 16]],
        key_hash: &[u8; 16],
        pid: &str,
    ) -> Result<Option<(u64, u64, u64, [u8; 16])>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(producer_key(identity, key_hash, pid)).await? {
                return decode_producer_row(&v).map(Some);
            }
        }
        Ok(None)
    }
```

The orphan dirty-index paragraph is deleted, not moved. `dirty_key`/`DIRTY_SENTINEL`
(`:202-222`) already own that explanation, and more accurately (they also cover trim
debt).

**`#[expect]`-ratcheted functions touched in `shard.rs`:**

| Function | Exception | Effect | Remedy |
|---|---|---|---|
| `ShardEngine::load_producer_chain` | fn-wide `#[expect(clippy::unwrap_used, reason = "ShardEngine::load_producer_chain; …")]` | all six `unwrap()` disappear; the expectation would be unfulfilled (denied) | **delete it** (this is the reviewer's "delete the unwrap expect"). The contract identity disappears; `exception_growth` only compares identities present on both sides, and no `source-allowances.json` / `diagnostic-allowances*.json` row names it (checked). The three `legacy-diagnostics*.json` entries for it (`:13830-13848`) are the immutable adoption inventory: **do not touch**. |
| `ShardEngine::load_seq_chain` | none | — | — |

`impl ShardEngine` (`:1335`) carries no impl-wide exception, and the file has no `#![…]`.
No other exception's scope contains edited lines.

### 4.3 `src/shard/transaction/append.rs` (298 → ~293)

Replace `:169-180` (inside `CommitTransaction::accept_append`) with:

```rust
            let row = (pr.epoch, pr.seq, commit_last, pr.request_hash.unwrap_or([0u8; 16]));
            local.producer.rows.insert((req.key_hash, pr.id.clone()), row);
            self.batch.put(
                producer_key(&hash, &req.key_hash, &pr.id),
                encode_producer_row(row),
            );
```

(rustfmt decides the final breaks. `unwrap_or` is not `unwrap_used`.)

**Ratcheted functions in this file:**

| Function | Exceptions | Edited? |
|---|---|---|
| `append` | `too_many_lines`, `let_underscore_must_use`, `unwrap_used`, `excessive_nesting` | **not edited** (lines 19-155 untouched): scope_lines, syntax_facts and call/path fingerprints unchanged, and it gains no call to the new functions |
| `accept_append` | none | edited; carries no exception |
| `bill_append` | `unwrap_used` | not edited; it only shifts up. Contracts are line-relative and keyed by `qualified` + value, so they are unchanged |

The `impl CommitTransaction<'_>` has no attributes and the file has no inner attributes.

### 4.4 `src/shard/storage_decode_tests.rs` (119 → ~230)

Append `stored_producer_row`, T1, T3, T4 and the T5 `proptest!` block as in section 3.
The file already has `#![cfg(test)]` and the `use super::*` owner row. It stays
production-unchanged for the planner (`#![cfg(test)]` normalizes to empty), so it needs
no mutation-owner row.

### 4.5 `src/shard/transaction_tests.rs` (433 → ~465)

Append T2 at the end. `r03a_mixed_transaction_preserves_every_row_reply_and_publication`
and `Fixture` are untouched, so the `review-mechanisms.json` pin (`:1017-1024`,
`after_sha256 bf37a0aa…`) stays valid.

### 4.6 Ledgers

See section 6. `scripts/quality/mutation_owners.py` and `docs/quality/owners.json` are
edited in the same commit.

---

## 5. Mutation-kill analysis

Tooling: cargo-mutants 27.1.0, `--in-diff`, `--timeout 90`, one owner at a time with that
owner's filters (`mutation_driver.py:40-53`).

Selected owners:
- `shard` (`src/shard.rs`, filter `shard::`);
- `transaction_append` (`src/shard/transaction/append.rs`, `shard::`);
- new `lane_rows` (`src/shard/lane_rows.rs`, `shard::`).

`storage_decode_tests.rs` and `transaction_tests.rs` are production-unchanged, so they are
not mutated. All killing tests named below live under `shard::`, so every owner's filter
runs them.

**Deterministic kills only.** The properties draw a random seed, so every mutant below is
also killed by T1–T4, which are fixed inputs.

| Owner / function | cargo-mutants variants (in diff) | Killed by |
|---|---|---|
| `lane_rows::decode_producer_row` | body → `Ok((c1, c2, c3, [c4; 16]))` for each `c ∈ {0, 1}` (tuple product, about 16) | T3 at `len = 0` (expects `None`, gets `Some`) and at `len = 40` (expects `(7,3,11,[9;16])`) |
| same, match arms | first match has no wildcard: no arm deletion. Second match `[] =>` / `hash =>` is an identifier pattern; if the tool treats it as a catch-all and deletes `[] =>`, 16- and 24-byte rows become `Err` | T3 at `len = 16` and `24` |
| `lane_rows::encode_producer_row` | `vec![]`, `vec![0]`, `vec![1]` | T3's first `assert_eq!` against the hand-built oracle |
| `lane_rows::decode_seq_row` | `Ok(String::new())`, `Ok("xyzzy".into())` | T4 (`"alpha"`); also T1's `[0xff]` → `is_err()` |
| `shard::ShardEngine::load_seq_chain` | `Ok(None)`, `Ok(Some(String::new()))`, `Ok(Some("xyzzy".into()))` | T1 `Some("parent-seq")`; the `Ok(None)` mutant also fails T2's seq case (append accepted) |
| `shard::ShardEngine::load_producer_chain` | `Ok(None)`, `Ok(Some((c1, c2, c3, [c4; 16])))` (about 17) | T1's first assertion `Some((7, 3, 11, [9; 16]))` |
| `transaction_append::CommitTransaction::accept_append` | body → `()` (the only mutant whose span overlaps the changed lines; the operators at `:165-167` are on unchanged lines) | r03a: `reply.try_recv()` is `Closed`, not `Empty`, and `reply.await.unwrap()` hits `RecvError`. Every appending `shard::` test fails fast because the reply sender is dropped. |

- **New guards or predicates:** none. The decoder has no `if`/guard/comparison (the width
  checks are structural `split_first_chunk` and `try_into`), so there are no guard
  true/false or operator mutants. No match guards were added. No equivalent mutants.
- **TIMEOUT exposure:**
  - lane_rows and loader mutants return instantly, so they cannot hang.
  - For `accept_append → ()`, replies are dropped, so a waiter fails instead of hanging.
    The `shard::` waits I inspected are bounded (`durability_frontier_tests.rs:55-70` 10 s,
    `transaction_tests` 5 s). T1 and T2 bound every wait (5 s).
  - Section 7.6 runs the leg before push. If a `timeout.txt` entry appears, bound that
    test's wait; do not restructure production.
- **Owner-row changes:** add `owner('lane_rows', 'src/shard/lane_rows.rs', 'shard::')`
  immediately after `owner('record', …)` (`mutation_owners.py:91`). No filter changes for
  `shard` or `transaction_append`: their `shard::` filter already selects T1–T5.
- **Expected volume:** roughly 42 mutants (~21 lane_rows, ~20 shard, 1 append), each
  running the `shard::` subset. This fits the leg's 240-minute budget.

---

## 6. Ledgers (same commit)

| Ledger | Change |
|---|---|
| `scripts/quality/mutation_owners.py` | +1 row: `owner('lane_rows', 'src/shard/lane_rows.rs', 'shard::'),` (new file under `src/shard` = critical, so it must be registered or `validate_plan` fails "register every changed critical mutation owner") |
| `docs/quality/owners.json` | +1 `macro-dsl` row: `{"category": "macro-dsl", "count": 1, "owner": "crate::macro(proptest::proptest)", "path": "src/shard/storage_decode_tests.rs", "syntax": "proptest::proptest", "reason": "Lane-row codec properties; the pinned macro generates 1,024 full-width producer states and arbitrary stored bytes per property against the production encoder and both decoders; syntax tokens remain inventoried."}`. The owner spelling follows the file-root precedents `src/registry/catalog/tests.rs` and `src/product_cursor/regressions.rs`. |
| `docs/refactor/test-inventory.json` | **no change**: it inventories `src/dst/**` only (`scripts/test-inventory.py:138`). `--check` must pass untouched. |
| `docs/refactor/review-mechanisms.json` | **no change**: the only pinned test in touched files (r03a) keeps its body |
| `docs/refactor/architecture-policy.json` | **no change**: no new file references `crate::http` |
| `docs/refactor/WIRE-MATRIX.md` | **no change**: raw `500 internal` (`:41`) and product "else `append_failed`" (`:117`) already describe the refusal. D1 changes only *when* it happens (corrupt storage). |
| `src/dst/tests/README.md` | **no change**: no DST module |
| `docs/quality/source-allowances.json`, `diagnostic-allowances*.json` | **no change**: nothing becomes stale (no row names the edited items; active diagnostic allowances are empty) |
| `docs/quality/legacy-diagnostics*.json` | **must not change** (immutable adoption inventory) |

---

## 7. Controls

Run from `/Users/sorenschmidt/code/streams` once the tree's current work is idle.

1. **Runtime red** (section 3 step i: T1 and T2 only):
   ```
   cargo test --locked --release --lib shard::storage_decode_tests::r12_undecodable_lane_rows_are_corruption_not_absence -- --exact 2>&1 | tee target/legs/item55-red-t1.log
   cargo test --locked --release --lib shard::transaction_tests::r12_undecodable_lane_rows_refuse_the_append_and_write_nothing -- --exact 2>&1 | tee target/legs/item55-red-t2.log
   ```
   Expect exactly the panics in section 3 and `test result: FAILED. 0 passed; 1 failed` in
   each log.
2. **Compile red** (step ii: T3–T5 added, no fix):
   `cargo test --locked --release --lib --no-run 2>&1 | grep -E '^error'` shows only
   `E0425` for `encode_producer_row` / `decode_producer_row` / `decode_seq_row` and
   `could not compile … due to 10 previous errors`.
3. **Green** (step iii). Each leg proves it ran what it names:
   ```
   scripts/test-leg.sh target/legs/item55-decode.log \
     --exact shard::storage_decode_tests::r12_undecodable_lane_rows_are_corruption_not_absence \
     --exact shard::storage_decode_tests::r12_producer_row_requires_exact_supported_width \
     --exact shard::storage_decode_tests::r12_stream_seq_row_requires_utf8 \
     --exact shard::storage_decode_tests::quality_producer_rows_decode_only_at_a_supported_width \
     --exact shard::storage_decode_tests::quality_stream_seq_rows_decode_exactly_their_utf8 \
     -- --locked --release --lib shard::storage_decode_tests::
   scripts/test-leg.sh target/legs/item55-txn.log \
     --exact shard::transaction_tests::r12_undecodable_lane_rows_refuse_the_append_and_write_nothing \
     --exact shard::transaction_tests::r03a_mixed_transaction_preserves_every_row_reply_and_publication \
     -- --locked --release --lib shard::transaction_tests::
   scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_
   ```
4. **Neighbours** (must stay green):
   - `cargo test --locked --release --lib shard::` (the exact mutation filter; this is also
     the mutation baseline);
   - `cargo test --locked --release --lib dst_tests::producer_` (`producer_protocol`,
     `producer_handoff`: 40-byte rows through handoff and split chains);
   - `cargo test --locked --release --lib dst_tests::durability_fences:: dst_tests::durability_failures::`;
   - `cargo test --locked --release --lib golden_tests::shard_keys::` (key bytes unchanged).
5. **Budgets:**
   `wc -l src/shard.rs src/shard/lane_rows.rs src/shard/transaction/append.rs src/shard/storage_decode_tests.rs src/shard/transaction_tests.rs`
   must show `shard.rs` ≤ 3232 (expect about 3197) and every other file ≤ 1000.
   `git diff --stat origin/slate` must not list `src/http.rs`, `src/product.rs`,
   `src/billing.rs`, `src/history.rs`, `src/auth.rs`, `src/registry.rs`,
   `src/sse/feed.rs` or `src/fleet.rs`.
6. **Planner receipt and mutation leg** (commit made locally; local comparison =
   merge-base with `origin/slate`):
   ```
   cargo build --locked -p streams-quality-syntax
   python3 scripts/quality/verification_plan.py --out target/q55-plan && python3 -c 'import json;p=json.load(open("target/q55-plan/plan.json"));print(p["mutants"],p["mutation_source_files"],p["selected_mutation_owners"],p["unregistered_mutation_source_files"],p["production_unchanged_files"])'
   ```
   Expect:
   `True ['src/shard.rs', 'src/shard/lane_rows.rs', 'src/shard/transaction/append.rs'] ['shard', 'transaction_append', 'lane_rows'] [] ['src/shard/storage_decode_tests.rs', 'src/shard/transaction_tests.rs']`.
   Because `scripts/quality/mutation_owners.py` changed, `compiler`, `properties_fuzz`,
   `loom` and `miri` are all `true`: the tooling rule selects the full invariant battery.
   That is expected for any new owner row.

   Then run `QUALITY_MUTANTS_OUT=target/q55-mutants scripts/quality/mutations.sh`. Each of
   `target/q55-mutants/{shard,transaction_append,lane_rows}/mutants.out/` must have empty
   `missed.txt` and `timeout.txt`. Every listed mutant must appear in `caught.txt` (or
   `unviable.txt`).
7. **Quality entry point:** `bash scripts/quality.sh`. It covers:
   - fmt;
   - clippy `-D warnings`: no `unfulfilled_lint_expectations`, and no
     `indexing_slicing`/`arithmetic_side_effects` in `lane_rows.rs`;
   - rustdoc `-D warnings --document-private-items`;
   - source gate: no `unregistered source occurrence` for the proptest, no stale
     allowances, no `accepted exception grew`;
   - `test-inventory --check`, review evidence, mt-audit, mt-lint.
8. **Whole gate before push**, as this branch requires:
   `OUT=/tmp/gate-item55.txt bash scripts/gate.sh`. After the push, use `gh run view` on
   the `slate` run. Never claim CI green from the local gate alone.

---

## 8. Out of scope / follow-ups

- **Other decoders in `shard.rs`.** Move `decode_tail` / `stored_tail` / `decode_cursor` /
  `decode_dirty_value` into a decoder owner with the owner lints. They index (`v[0]`,
  `v[1..9]`, `v[o..o + 8]`), and `decode_dirty_value` keeps the same "fixed-width, so
  unwrap" expectation (`:552-555`) that this item deletes for producer rows. That is a
  larger move with its own mutation surface.
- **Unreachable legacy widths.** Drop the 16/24-byte producer widths (D2 alternative) if
  Søren prefers to delete compatibility that is unreachable under layout 4.
- **One producer-state type.** `transaction/overlay.rs:3` `ProducerRecord`,
  `shard.rs:872` `ProducerRows` and the literal in `decide_producer`
  (`commit_plan.rs:90`) are the same tuple; one named type owned by `lane_rows` would
  remove the duplication.
- **Tighter Stream-Seq decode.** Require the stored Stream-Seq to be visible ASCII (the
  ingress rule), not just UTF-8. Not needed for fail-closed, and it risks rejecting rows
  written under an older header rule.
- **Planner selection.** Add `src/shard/lane_rows` to `CODEC_PREFIXES`
  (`scripts/quality/verification_plan.py:22-24`) so later diffs to this owner alone select
  the properties/corpus leg. There is no lane-row fuzz target; the saved corpus covers
  postings only.
- **Test-only lenient decode.** The `#[cfg(test)]` `scan_dirty_streams` (`shard.rs:2159-2187`, test-only)
  still skips short dirty values (`kv.value.len() < 16 → continue`). It is test-only; the
  production pager (`:2189-2232`) is strict.
- **Scan sentinel row.** `record_scan_tests.rs:48-51` stores a 19-byte producer row as a
  scan sentinel. It is never loaded, so it is harmless. After this change it would refuse
  an append for producer `"unchanged"` on that key, which nothing issues.

---

## Skeptic corrections (C1..C6)

I checked every claim against the tree at `HEAD = origin/slate = 47799eb3`, reading files only.

**What holds (checked, no change needed):**
- The quoted code at `src/shard.rs:2060-2137` is exact. It is 78 lines, and the arithmetic 3,232 − 78 + 41 + 2 = 3,197 is right.
- `wc -l` of all nine ceilinged files matches the table: shard 3,232, http 3,371, product 4,205, billing 2,201, history 1,713, auth 1,676, registry 1,509, sse/feed 1,200, fleet 1,143.
- The other touched files are `append.rs` 298, `storage_decode_tests.rs` 119 and `transaction_tests.rs` 433.
- The only readers of `'q'`/`'s'` rows are the two loaders (`git grep "b'q'\|b's'"`, `producer_key`, `seq_key`). The only writers are `append.rs:174-180` (40 bytes) and `append.rs:237-238`.
- `load_producer_chain` carries the only function-wide `unwrap_used` expectation that is touched. Deleting it is correct. `exception_growth` (`scripts/quality/source_rules.py:204-216`) skips identities that no longer exist. The rows at `legacy-diagnostics*.json:13830-13848` are pinned in `docs/quality/policy.json` `immutable_sha256`: do not touch them.
- `append`'s four expectations (`append.rs:3-18`) and `bill_append` (`:255-258`) are not in the edit range. Contract identity is `(path, qualified, kind, value)`, which does not depend on line numbers. `impl ShardEngine` (`:1335`) has no impl-wide exception, and `shard.rs` has no `#![…]`.
- Glob route: `transaction/mod.rs:9` `use super::*;` and `append.rs:1` together carry `decide_producer`, which is imported only at `shard.rs:35`. So `encode_producer_row` reaches `append.rs` the same way.
- T1 red trace: width 15 → `_ => {}` → the parent's 40-byte row → `Ok(Some((7, 3, 11, [9; 16])))`. The four positive assertions pass today.
- T2 red trace: `decide_producer` → `Accept((1, 0))`. `seal_authorized(Some(10), false, 0)` is true, so the append commits. `AppendAck` field order (`shard.rs:828-836`) matches the predicted Debug text.
- Green: a refused append leaves `changed` false (`finalize.rs:91-115`), `has_writes()` is false, and nothing is written. `billing: None` means T2 never reaches `load_billing_meta` (`prepare.rs:60-70`). That is the only consumer of `billing_read_faults` besides tests, so T2 cannot take r03a's fault.
- Reads default to `DurabilityLevel::Memory` (slatedb `config.rs:273-282`), so an un-awaited `db.write` is visible to `db.get`. The same pattern is at `billing_read_tests.rs:55` and `transaction_tests.rs:273`.
- Mutation owners and filters (`mutation_owners.py:82,90,91`), `resolve_sources` ordering (`:224-234`), the `production_unchanged` omission (`verification_plan.py:75`) and `test_only_file` (`tools/quality-syntax/src/scan.rs:28`) all match the expected receipt.
- The `owners.json` macro-dsl row is needed even in `#![cfg(test)]` files. Every one of the 15 proptest blocks in `src/` has exactly one row. The owner spelling `crate::macro(proptest::proptest)` matches `src/billing/read_accumulator/tests.rs` and `src/registry/catalog/tests.rs`.
- `test-inventory.py:138` scans only `src/dst`. The only pinned test in the touched files is r03a (`review-mechanisms.json:1017-1024`), and its body is unchanged.
- The mutation list is complete. cargo-mutants arm deletion needs a `Pat::Wild`, and `hash =>` is an ident pattern, so no arm-deletion mutant exists and there are no equivalent mutants.
- Docs: `ROUTING-V3.md:259-266`, `WIRE-MATRIX.md:41` (`500 internal`) and `:117` (else `append_failed`) do not conflict with the change. `MULTITENANCY-MAP.md:204`'s `load_producer_chain:1914` was already stale.

**C1 — The base moved; stage by path.** The plan says `slate @ 0afa2597`. The branch is now `47799eb3`, two commits later:
- `0a8ed40a` changes how `quality.sh` reports clippy;
- `47799eb3` makes the architecture gate refuse obsolete `budget_exceptions`.

Neither commit touches a file in this plan. The new "obsolete budget exception" check does not fire, because `function:src/shard.rs::start` is unchanged. The working tree also has other work's uncommitted `scripts/mt-audit-baseline.txt` and `scripts/multitenancy-audit.sh`, and an untracked `scripts/quality/test_mt_audit.py`. So:
- commit with explicit paths (never `git add -A`/`-u`);
- remember that `verification_plan.py` diffs the working tree against the base (`discover_changes`: `git diff … base --`), so run control 6 on a clean tree.

**C2 — The first placement argument is factually wrong.** §0 says `shard.rs` is "a file that cannot turn on the decoder-owner lints". Item-scoped `#[warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]` on a single function is established precedent at `src/queue.rs:97` and `:254`. `lane_rows.rs` is still the right placement, for two reasons: `shard.rs` would sit 3 lines under its ceiling, and it gives the byte layout one canonical owner. Replace the first bullet with those reasons. No code change.

**C3 — T1 and T2 do not show every behaviour change red.** Both stop at the first failing case, so these are never seen failing at runtime on today's tree:
- the Stream-Seq loader change (non-UTF-8 → `Err`);
- the width-17..41 changes (today they decode as 16/24/40);
- T2's seq case.

T4 is only compile-red, which proves a function is missing, not that the loader misbehaves. Restructure:
- **T1a** `r12_undecodable_producer_rows_are_corruption_not_absence`: collect the widths that do not error, then assert on the list:
  ```rust
  let mut accepted = Vec::new();
  for width in [15, 17, 23, 25, 39, 41] { /* write */ if engine.load_producer_chain(&own, &[parent], &key, "p").await.is_ok() { accepted.push(width); } }
  assert!(accepted.is_empty(), "undecodable producer rows were accepted at widths {accepted:?}");
  ```
  Expected red: `undecodable producer rows were accepted at widths [15, 17, 23, 25, 39, 41]`.
- **T1b** `r12_non_utf8_stream_seq_row_is_corruption_not_absence`: a separate test, own row `[0xff]`, parent `"parent-seq"`. Expected red: `a non-UTF-8 Stream-Seq row must fail closed, got Ok(None)`. It returns `None` without falling through (`shard.rs:2083-2084`).
- **T2 split into two tests**, each with its own `Fixture` (for example, a shared `async fn refused_append(row, value) -> Result<AppendAck, AppendErr>` helper). They cannot share one loop and collect results: in red, case 1 commits producer `(1,0)` and seq `"alpha"`, so case 2 would see a duplicate ack instead of the planted row. Each test's expected red is `…, got Ok(AppendAck { last_offset: 0, next_offset: 1, closed: false, producer: Some((1, 0)), duplicate: false })`. For the seq test this holds because `load_seq_chain` returns `Ok(None)` today and the fresh producer is accepted.
- **Update to match:** the §5 kill table (T1a/T1b replace T1; both are still in `shard::`) and the §7.1/§7.3 `--exact` names.

**C4 — D1's edge half is claimed but not pinned.** "raw `500 internal`, product `500 append_failed`, `retryable: false`" is read from unchanged code. `append_failed` appears only at `src/product.rs:2421`, and no test anywhere asserts it for `AppendErr::Internal`. T2 pins only the shard-level `AppendErr::Internal`. For Søren's D1 approval, either:
- state explicitly that the edge mapping is existing code and no test covers it; or
- add a small edge assertion (for example `contract.rs:276` `Internal` → `FailureClass::Internal` → product translation `("append_failed", …, false)`).

That test would be green-on-arrival (the mapping does not change), so it is not a red requirement.

**C5 — Mutation leg budget (PLAUSIBLE, measure it).** Changing `scripts/quality/mutation_owners.py` sets `tooling`, which selects the corpus, Miri and mutation steps (`rust-quality.yml:84-95`). They all run in the single 240-minute `invariant-tools` job. About 42 mutants will run with `--jobs 1 --profile quality` (`mutation_driver.py:40-49`), each rebuilding the lib tests and running the whole `shard::` filter; `accept_append → ()` alone touches every appending `shard::` test. I checked the spin-waits the plan could not name: `commit_command_tests.rs:70-83`, `durability_frontier_tests.rs:55-70,109`, `task_lifecycle_tests.rs:59-64,209-214,262-267` are all inside 10 s `tokio::time::timeout`, so no TIMEOUT exposure was found. Control 6 should record the local wall time per owner.

**C6 — Optional, not gated.** `docs/review-storage-evidence.md:5` ("R12 — persisted tails and cursors") and `docs/review-resolution.md:20` describe R12's scope. The new `r12_` tests extend it to lane rows, so a one-line addendum keeps the evidence trail honest. No gate reads these files.

**Unbuildable controls:** none. Every control's command, flag and path exists:
- `scripts/test-leg.sh` passes `--exact` through to `tests_ran.py:41`;
- `golden_tests::shard_keys` is at `golden_tests.rs:26`;
- `dst_tests::producer_*` and `durability_fences`/`durability_failures` exist;
- `mutations.sh` → `mutation_driver.py --out` regenerates `plan.json` itself.

The §7.1/§7.3 `--exact` names must follow C3's renames.

**Missed ledgers or ratchets:** none mandatory. Deferring `CODEC_PREFIXES` is acceptable: `nightly.sh corpus` has no lane-row corpus, so selecting it adds nothing, and the `quality_` properties run unconditionally at `rust-quality.yml:48`.

**Verdict: ready-with-corrections.** C3 is required, because red-first is not met for the seq loader or for T2's seq case. C1, C2 and C4 are wording and procedure fixes. C5 means measuring the mutation leg. C6 is optional.
