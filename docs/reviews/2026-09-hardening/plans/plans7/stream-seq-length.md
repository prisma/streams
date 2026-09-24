# Item 53: a long Stream-Seq wraps the tail row's u16; SSE_H1_MAX_BUF is unvalidated

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `7c4f8606` (origin/slate = `9b2f53bc`).
I read everything below on this tree. The review's line numbers are stale; current ones are given.
I ran no cargo commands and no scripts. Every expected output below comes from reading the source.

## Short version

The reviewer found a real bug and described it correctly. It also builds. But the Change it proposes
defends a copy of the Stream-Seq that no code reads. There is a smaller fix, and it removes the
cause instead of fencing it off:

1. **The wrap and the wedge are real.** `encode_tail` writes `seq.len() as u16`, and nothing limits
   the length of the `Stream-Seq` header. A value of 65,536 bytes stores `seq_len = 0` followed by
   65,536 bytes of text. When the row is read back, the decoder takes the first text bytes as
   `route` and `trim_safe_to`. `stored_tail` then refuses the row with `Data error: inconsistent
   persisted tail`. From then on every `stream_handle`, `tail_fields` or `seed_fork_tail` call for
   that stream fails. If the shard ledger ever has to be rebuilt from tails, the whole shard fails
   to open.
2. **The copy that wraps is never read.** Since routing v3 (`e6717f9c`), each Stream-Seq lane is
   stored in its own `s` row (`seq_key`), and `load_seq_chain` reads only that row.
   `TailFields.seq` is assigned in one place (`transaction/append.rs:237`) and read in one place,
   `encode_tail`. No decision anywhere uses it.
3. **Recommended fix (commit B):** stop writing the copy. The tail row keeps its layout and always
   writes `seq_len = 0`. The decoder still steps over the copy that older rows carry, and still
   refuses the row if that copy is not UTF-8. The field `TailFields.seq` is deleted, and with it
   the cast and its `#[expect(clippy::cast_possible_truncation)]`. Nothing changes on the wire, and
   Stream-Seq needs no length limit. The lane row stores the value with no length prefix.
4. **SSE_H1_MAX_BUF (commit A):** hyper's `max_buf_size` asserts `>= 8192`. `h1_builder` runs once,
   at the start of `serve_h1` (`http.rs:1289`), so a value of 4096 does not panic "every connection
   task", as the review says. It panics once on the bootstrap future, after bootstrap has already
   opened engines and spawned loops, and the process exits without serving a request. Fix: a named
   floor `HttpConfig::MIN_H1_MAX_BUF = 8 * 1024`, checked in `validate_topology_and_ceilings`, plus a
   test that pins it to hyper's assert.
5. **Found while verifying, left out of scope:** hyper checks `max_buf_size` only after a head fails
   to parse, and one read can take the buffer past it. The limit is therefore a soft threshold, not
   an exact bound on a request head. The same header-length gap also exists on `Producer-Id`. There,
   a key longer than `u16::MAX` makes slatedb's `WriteBatch::put` panic inside the committer. See §8.

The two commits are independent. Neither needs a verbatim-move commit: no file with a line ceiling
grows. `shard.rs` gets shorter.

---

## 1 Problem (verified, with quotes)

### 1.1 The u16 wrap

`src/shard.rs:75-99`:
```
75  #[expect(
76      clippy::cast_possible_truncation,
77      reason = "encode_tail; the producer sequence is bounded by the u16 width the tail row stores; a checked conversion would only restate the row format"
78  )]
79  fn encode_tail(t: &TailFields) -> Vec<u8> {
80      let seq = t.seq.as_deref().unwrap_or("").as_bytes();
81      let mut v = Vec::with_capacity(76 + seq.len());
 ...
96      v.extend_from_slice(&(seq.len() as u16).to_le_bytes());
97      v.extend_from_slice(seq);
```
The reason text says the sequence "is bounded by the u16 width". That is false: nothing enforces the
bound.

`src/shard.rs:115-121` (decode), `157-168` (`stored_tail`):
```
115     let seq_len = u16::from_le_bytes(v.get(seq_at..seq_at + 2)?.try_into().ok()?) as usize;
116     let seq = if seq_len == 0 {
117         None
118     } else {
119         Some(String::from_utf8(v.get(seq_at + 2..seq_at + 2 + seq_len)?.to_vec()).ok()?)
120     };
121     let route_at = seq_at + 2 + seq_len;
 ...
162     if tail.trimmed > tail.absorbed
163         || tail.absorbed > tail.next
164         || tail.trim_safe_to > tail.absorbed
```
Worked example: a v3 row with a 65,536-byte ASCII Stream-Seq. It stores `seq_len = 65536 as u16 = 0`
at offsets 42..44, then the 65,536 text bytes, then route, `trim_safe_to` and `unabsorbed_bytes`.
The decoder reads `seq_len = 0` and sets `route_at = 44`. The trailing extension is 65,568 bytes
long, which is at least 32, so the width check at `:126` accepts it. `route` becomes `"ssss…"`.
`trim_safe_to = le8(60..68) = 0x7373737373737373` is greater than `absorbed = 0`, so `stored_tail`
returns `slatedb::Error::data("inconsistent persisted tail")`, whose Display is
`Data error: inconsistent persisted tail`. A 70,000-byte value behaves the same way: it stores
`seq_len = 4464`, and the route and extension fields are again read from inside the text.

The row is written with every append that carries `Stream-Seq`, at `src/shard/transaction/append.rs:236-241`:
```
236         if let Some(seq) = &req.seq {
237             local.fields.seq = Some(seq.clone());
238             local.producer.seqs.insert(req.key_hash, seq.clone());
239             self.batch
240                 .put(seq_key(&hash, &req.key_hash), seq.clone().into_bytes());
```
After that, every tail write re-encodes the long copy, including appends without Stream-Seq,
absorption and trim. The corruption therefore persists.

**Where it shows up.** On the next load of a handle: `stream_handle` (`shard.rs:2381-2383`,
`Some(raw) => stored_tail(&raw)?`), `tail_fields` (`:2204-2211`) and `seed_fork_tail` (`:2307-2308`).
Every one of these refuses the stream permanently. `rebuild_maintenance_from_tails` (`:508`, `let mut
tail = stored_tail(&tail_raw)?;`) turns the same error into an engine-open failure for the whole
shard (doc at `:452-454`). That path runs only when the shard's ledger row is missing or legacy.
History `backlog_of` (`history/gather.rs:236`) tolerates the error.

### 1.2 Nothing limits the header, and hyper's buffer limit is soft

- `src/http.rs:2853` passes `sequence: hdr(&headers, "stream-seq"),` through unchanged. `hdr`
  (`:2458-2463`) is only `to_str().ok()`, which enforces visible ASCII and no length.
  `application/append.rs:368` passes the value on (`seq: command.sequence.clone()`). There is no
  other Stream-Seq site; product, system and delivery commands all pass `sequence: None`.
- hyper 1.10.1 (the version in Cargo.lock): `proto/h1/io.rs:200-206` checks `curr_len >= max` only
  after a parse returned "incomplete". `poll_read_from_io` (`:223-228`) calls `reserve(next)` and
  reads into all of the spare capacity. The adaptive strategy (`record`, `:400-402`) doubles
  `next` up to `max`. With `max = 64 KiB` and a fast sender, the fourth read begins at a buffer of
  57,344 bytes and reserves 65,536 more. BytesMut/Vec growth gives a capacity of 122,880. A head of
  up to about 120 KB can therefore parse successfully at our default. hyper limits header *names* to
  64 KB (`role.rs:1551`), but not values. **This comes from reading the source, not from running
  it.** Whether a 70 KB `Stream-Seq` reaches the handler at the default posture depends on how the
  reads are chunked. The fix does not depend on the answer. When `SSE_H1_MAX_BUF` is raised, the
  wrap is reachable in every case.

### 1.3 The wrapping copy has no reader

- `TailFields.seq` (`shard.rs:584`) is assigned only at `transaction/append.rs:237`. The only
  production read is `encode_tail` (`shard.rs:80`). Evidence: `grep -rn '\.seq\b' src` and
  `grep -A12 'TailFields {' | grep seq`. The only other hits are tests: `golden_tests.rs:114,151,216`,
  `storage_decode_tests.rs:12,20` and `transaction_tests.rs:199`.
- The lane is owned by the `s` row. `shard.rs:2067-2079` `load_seq_chain` reads
  `self.db.get(seq_key(identity, key_hash))` along the lineage. `transaction/append.rs:103-135`
  compares against `local.producer.seqs` and `st.seqs`, which are filled from that row. The copy
  in the tail row dates from before routing v3 (`e6717f9c`, "split-safe Stream-Seq"). SPEC.md:122
  and DESIGN.md:64 still describe the tail as holding "last Stream-Seq".

### 1.4 SSE_H1_MAX_BUF

- `config/load.rs:167-169`: `if let Some(v) = env_parse(env, "SSE_H1_MAX_BUF") { self.http.h1_max_buf = v; }`.
  `config/model.rs:187-188` documents the value as "h1 connection buffer ceiling". `validation.rs:783-816`
  (`validate_topology_and_ceilings`) has no h1 check.
- `http/serve.rs:32-37`: `h1_builder` calls `.max_buf_size(http.h1_max_buf)`. hyper
  `server/conn/http1.rs:380-387`:
  `assert!(max >= proto::h1::MINIMUM_MAX_BUFFER_SIZE, "the max_buf_size cannot be smaller than the minimum that h1 specifies.")`,
  with `MINIMUM_MAX_BUFFER_SIZE = INIT_BUFFER_SIZE = 8192` (`io.rs:15,18`). That constant is
  `pub(crate)` in hyper, so we have to name our own.
- `http.rs:1288-1289`: `h1_builder` runs once, at the start of `serve_h1` and before the accept loop.
  Production awaits `serve_h1` directly (`bootstrap.rs:902`) inside `main`'s `block_on`
  (`main.rs:58`). A value below 8192 therefore passes `validate()`. `main.rs:32-37` would have
  exited 1 on a validation failure. Instead, `bootstrap::run` performs every side effect up to line 902, then panics
  once and unwinds out of `main` (exit 101) without serving anything. **The review's "panics every
  connection task" is wrong in detail; its conclusion (the value must be validated) is right.**

### 1.5 The reviewer's Change against this tree

The Change: "Named max Stream-Seq length checked where the AppendCommand is built, ideally a parsed
StreamSeq type, deleting the cast expect; validate h1_max_buf range; encode_tail stays infallible,
no clamp." It builds, but its typed form costs more than the bug is worth:

- Changing `AppendReq.seq` to a proof type forces the comparison at `transaction/append.rs:128`
  (`&& seq <= cur`, where `cur` comes from the `String` map) to change. That line is inside
  `CommitTransaction::append`, which carries four function-wide expectations at `:3-18`:
  `too_many_lines`, `let_underscore_must_use`, `unwrap_used` and `excessive_nesting`. Any new
  method call grows `syntax_facts` under all four. The alternative is `PartialOrd<String>` impls
  written only to leave that line untouched.
- Parsing at `http.rs:2853` adds a call and a path, so +2 `syntax_facts`, under `append_typed`'s
  `too_many_arguments/too_many_lines/excessive_nesting` expectation (`:2734-2739`). `http.rs` is at
  its ceiling of 3,371 of 3,371 lines.
- `prepare_close` (`application/append/close.rs:15-18`, `too_many_lines`) reads
  `command.sequence.clone().unwrap_or_default()` as a `String` at `:48`. Any conversion grows its facts.
- It adds a wire code (`400 invalid_stream_seq`) and still keeps the unread copy.

The untyped version of the review's idea is smaller: a named constant, one guard in
`AppendService::execute_prepared`, and a re-decided reason on the cast that cites it. The codebase
already does this for routing keys: `product.rs:1903` `MAX_ROUTING_KEY_BYTES = 1_024`, cited by the
cast reason at `crypto.rs:540`. It remains the alternative in §2. The recommended fix goes further
and deletes the copy that needs the bound.

---

## 2 Contract decision

**Recommended (D1): the tail row stops keeping a copy of the lane's Stream-Seq.**

- **Wire:** unchanged. A raw `POST /v1/stream/{name}` with `Stream-Seq` returns the same statuses
  and codes as before. There is no new code and no WIRE-MATRIX row. The only visible difference is
  a fixed bug: an append whose Stream-Seq is longer than 65,535 bytes (when one reaches the handler)
  still gets its 204, and now the stream keeps opening afterwards. Until now every later load of the
  stream failed.
- **Stream-Seq length contract (D2):** none. Nothing in storage depends on its length any more. The
  lane row stores the text with no length prefix, and the only limit is the transport's head buffer.
  The alternative, a named protocol ceiling, would be a new wire refusal whose only effect is to
  change a request that works today into a 400. Not recommended.
- **Persisted layout:** rows this binary writes keep the v3 layout with `seq_len = 0`. The decoder
  accepts exactly the rows it accepted before: it still requires the copy in older rows to be
  UTF-8, and it still steps over it to the route. A binary rolled back to one that expects the copy
  reads `seq = None` from new rows. No binary since `e6717f9c` uses that value. Rolling back past
  routing v3 was already unsupported (see the downgrade caveat in the doc comment at `shard.rs:71-74`).
  The encode golden for the full fixture shrinks from 81 to 76 bytes. The old 81 bytes are kept as a
  decode golden.
- **Backward-compatible alternative:** keep the copy and refuse `Stream-Seq` longer than 65,535
  bytes at admission, following the routing-key precedent. That means a new `400 invalid_stream_seq`
  (`FailureClass::Invalid`, added to `AppendCode::as_str`), a guard at the top of
  `AppendService::execute_prepared` (`application/append.rs:144`, which has no expectation) before
  any intent, TTL or commit write, a named `MAX_STREAM_SEQ_BYTES` beside the tail codec, a re-decided
  `encode_tail` cast reason naming it, and a WIRE-MATRIX §1 row. It is a wire change, and the unread
  copy stays.

**SSE_H1_MAX_BUF (D3):**

- Today a value below 8,192 boots, performs bootstrap's side effects, then panics (exit 101).
  After the fix, `ServerConfig::validate()` reports
  `SSE_H1_MAX_BUF=<n> is below hyper's 8192-byte h1 buffer floor` among the collected problems, and
  `main` exits 1 before any side effect. Every value that works today (8,192 and above) is unchanged.
- There is no upper limit. After D1 no invariant needs one, and because hyper's limit is soft (§1.2)
  a ceiling would not bound header values anyway. A ceiling belongs with the Producer-Id follow-up
  (§8, D4) if Søren wants one.

---

## 3 Red tests (all compile against the current tree, before the fix)

Reds for the tail copy go in `src/shard/transaction_tests.rs` (module is `#![cfg(test)]`, 490 lines,
owner row `transaction_tests`, filter `shard::`). They reuse `Fixture`, `Fixture::append`
(`seq: Some("alpha")`, producer `writer`, `seal_gen: Some(10)`), `commit_group`, `rows()`, `HASH` and `KEY`.

**R1 `shard::transaction_tests::r53_a_stream_seq_past_the_tail_rows_u16_leaves_the_stream_openable`**
```rust
/// Review item 53: the tail row kept a copy of the lane's last Stream-Seq
/// behind a u16 length that nothing reads; a longer header wrapped that
/// length and every later load of the stream refused the row for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r53_a_stream_seq_past_the_tail_rows_u16_leaves_the_stream_openable() {
    let fixture = Fixture::new().await;
    let long = "s".repeat(usize::from(u16::MAX) + 1);
    let (mut append, reply) = fixture.append(HASH, 0, AppendFinish::Open);
    let CommitOp::Append(req) = &mut append else { unreachable!("the fixture builds appends") };
    req.seq = Some(long.clone());
    fixture.engine.commit_group(vec![append], &fixture.cfg).await;
    let acked = tokio::time::timeout(Duration::from_secs(5), reply).await.unwrap().unwrap();
    assert!(acked.is_ok(), "the append itself commits, got {acked:?}");
    let stored = fixture.engine.tail_fields(&HASH).await
        .map(|tail| tail.map(|tail| tail.next)).map_err(|e| e.to_string());
    assert_eq!(stored, Ok(Some(1)),
        "a Stream-Seq one byte past the tail row's u16 length must leave the stored tail decodable");
    let lane = fixture.engine.load_seq_chain(&HASH, &[], &KEY).await.unwrap();
    assert_eq!(lane.map(|seq| seq.len()), Some(long.len()), "the lane row holds the whole sequence");
    // journal.close / begin_close / await_terminated(5 s) as in append_over_planted_row
}
```
Expected red on the current tree:
```
assertion `left == right` failed: a Stream-Seq one byte past the tail row's u16 length must leave the stored tail decodable
  left: Err("Data error: inconsistent persisted tail")
 right: Ok(Some(1))
```
The first assertion passes on the current tree, because the commit succeeds. `tail_fields` sends
the error through `?` into `anyhow`, whose `to_string` is the slatedb Display `"{kind}: {msg}"`.

**R3 `shard::transaction_tests::r53_the_tail_row_holds_no_copy_of_the_lanes_stream_seq`**
This test commits the fixture's ordinary `"alpha"` append, waits for the reply (5 s timeout), then:
```rust
let rows = fixture.rows().await;
assert_eq!(rows[&tail_key(&HASH)][42..44].to_vec(), vec![0u8, 0],
    "the tail row must not carry a copy of the lane's Stream-Seq");
assert_eq!(rows[&seq_key(&HASH, &KEY)], b"alpha".to_vec(), "the lane row holds it");
```
Expected red:
```
assertion `left == right` failed: the tail row must not carry a copy of the lane's Stream-Seq
  left: [5, 0]
 right: [0, 0]
```
R1 and R3 can share a small `async fn commit_one(seq: Option<String>) -> (Fixture, Result<AppendAck, AppendErr>)`.

**R1b `dst::dst_tests::producer_protocol::a_stream_seq_past_the_tail_rows_u16_survives_a_reopen`**
(`src/dst/tests/producer_protocol.rs`, 799 lines.) This is the durable, end-to-end version of the
wedge. It uses the existing `LaneSender`, `LaneWrite::Sequence`, `mem`, `skey` and
`open_engine_with_settings`, and follows the same-store reopen pattern of
`history_recovery.rs:350-400`, using a bounded `await_terminated` instead of the sleep. Steps:
open `"dst-longseq"` with `flush_interval 5 ms` and `manifest_poll_interval 50 ms`, then send
`Sequence(Some("s" × 65,536))` on the default key (must be `Ok`). Then `begin_close` and
`await_terminated(10 s)`, reopen on the same store and prefix, and run:
```rust
let opened = engine.stream_handle(hash).await
    .map(|h| h.state.lock().unwrap().durable.next).map_err(|e| e.to_string());
assert_eq!(opened, Ok(1),
    "a Stream-Seq one byte past the tail row's u16 length must not wedge the stream across a reopen");
```
Then resending the same long sequence must match
`Err(AppendErr::SeqConflict { current: Some(ref c) }) if c.len() == 65_536`. Do not print the
65 KB value in the message. Sending `"t"` must return `Ok`. Wrap every send in
`tokio::time::timeout(10 s)`. Expected red:
```
assertion `left == right` failed: a Stream-Seq one byte past the tail row's u16 length must not wedge the stream across a reopen
  left: Err("Data error: inconsistent persisted tail")
 right: Ok(1)
```
Reopening does not scan tails, because the commit writes the shard ledger row
(`r03a_mixed_transaction…` expects `shard_maint_key`). If that row were absent, the red would
instead be the whole-shard escalation from §1.1: a panic in the fixture's
`.expect("load maintenance")` with `Data error: inconsistent persisted tail`.

**R2 `config::validation::validation_tests::validate_boundary_tests::validation_rejects_an_h1_buffer_below_hypers_floor`**
(`src/config/validation_tests.rs`, 714 lines.) Uses the existing `rejects`/`validate_with` helpers:
```rust
/// Review item 53: hyper asserts an h1 read buffer of at least 8 KiB inside
/// the serve loop, after bootstrap has opened engines and spawned loops.
#[test]
fn validation_rejects_an_h1_buffer_below_hypers_floor() {
    rejects(|_| {}, &[("SSE_H1_MAX_BUF", "4096")], "SSE_H1_MAX_BUF");
    rejects(|_| {}, &[("SSE_H1_MAX_BUF", "8191")], "SSE_H1_MAX_BUF");
    validate_with(|_| {}, &[("SSE_H1_MAX_BUF", "8192")]).expect("hyper's floor itself is a valid buffer");
}
```
Expected red:
```
validate() must reject (marker "SSE_H1_MAX_BUF")
```

**Controls (not red):**

- **C1 `http::serve::tests::the_validated_buffer_floor_is_the_one_hyper_asserts`** (`src/http/serve.rs` tests).
  This test can only compile after the fix, because it names the new constant. It checks that
  `std::panic::catch_unwind(move || drop(super::h1_builder(&HttpConfig { h1_max_buf: n, ..HttpConfig::default() })))`
  returns `is_ok()` for `n = HttpConfig::MIN_H1_MAX_BUF` and `is_err()` for `MIN_H1_MAX_BUF - 1`.
  If hyper moves its floor in either direction, the test fails. hyper's assert message appears in
  captured stderr; that is expected.
- **C2 `shard::storage_decode_tests::r53_legacy_tail_rows_with_a_stream_seq_copy_still_decode`**, plus
  a helper `legacy_row(tail, v3, copy)`. The helper takes `encode_tail(tail)` (with `seq_len = 0`),
  writes `u16::try_from(copy.len())` into `[42..44]`, splices the copy in at 44, and for v2 sets
  `row[0] = 2` and removes index 41. The tail is `next 9, absorbed 4, trimmed 2, trim_safe_to 3,
  route [0xA5; 16], unabsorbed_bytes 77, closed, history_v2`. For each case in
  {v3, v2} × {`b"lane"`, 128 × `b's'`}, it asserts that `stored_tail` gives
  `(next, closed, history_v2, route, trim_safe_to, unabsorbed_bytes)` equal to
  `(9, v3, v3, [0xA5; 16], 3, 77)`, and that the same row with its last copy byte set to `0xFF`
  makes `decode_tail` return `None`. C2 passes on both trees.
- **C3 `shard::storage_decode_tests::quality_tail_rows_decode_their_fields_past_any_stream_seq_copy`**.
  This is a property test inside the existing `proptest::proptest!` block (`cases: 1024`), with at
  most five strategy arguments: six `u64` fields, `ts: i64`, `(flags 0..4, v3: bool)`, `route`, and
  `(text "\\PC{0,80}", raw vec<u8> 0..=80)`. For both copies it asserts that
  `decode_tail(&legacy_row(..)).map(fields)` equals `Some(fields(&tail))` exactly when the copy is
  UTF-8. `fields` is the ten-field tuple without `seq`. For v2, the flags are forced to 0. This
  satisfies the codec-change property obligation in RUST-QUALITY.md. The `quality_` prefix means it
  also runs in the `--lib quality_` leg. It passes on both trees.
- **Rewritten, green on both trees:** `r12_supported_tail_versions_and_extensions` builds
  `full = legacy_row(&tail, true, b"lane")` instead of `seq: Some("lane".into())` and drops the
  `decoded.seq` assertion. Its lengths (`base = 48`) and all corruption checks stay unchanged.

---

## 4 Edits, file by file, in commit order

No verbatim-move commit is needed. Line counts now, and after (ceiling = the base line count for
files over 1,000 lines):

| file | now | after | ceiling |
|---|---|---|---|
| src/shard.rs | 3,197 | about 3,190 (−7) | 3,232 against origin/slate; 3,197 once `517f4fa1` is pushed. Fits both |
| src/http.rs / product.rs / billing.rs / history.rs / auth.rs / registry.rs / sse/feed.rs / fleet.rs | 3,371 / 4,205 / 2,201 / 1,713 / 1,676 / 1,501 / 1,200 / 1,143 | untouched | — |
| src/config/validation.rs | 965 | about 975 | 1,000 (stays under) |
| src/config/validation_tests.rs | 714 | about 730 | 1,000 |
| src/config/model.rs | 504 | about 514 | 1,000 |
| src/http/serve.rs | 187 | about 203 | 1,000 |
| src/shard/transaction/append.rs | 300 | 299 | 1,000 |
| src/shard/storage_decode_tests.rs | 292 | about 390 | 1,000 |
| src/shard/transaction_tests.rs | 490 | about 545 | 1,000 |
| src/golden_tests.rs | 832 | about 860 | 1,000 |
| src/dst/tests/producer_protocol.rs (DST) | 799 | about 845 | 1,000 |

### Commit A: `SSE_H1_MAX_BUF below hyper's 8 KiB floor is a configuration error, never a panic after boot`

1. `src/config/model.rs`: add, next to `impl FleetConfig { pub const MAX_MEMBERS … }` (`:457-460`):
   ```rust
   impl HttpConfig {
       /// hyper's `http1::Builder::max_buf_size` asserts at least this (its private
       /// `MINIMUM_MAX_BUFFER_SIZE`) inside `serve_h1`, after bootstrap has opened
       /// engines and spawned loops; validation refuses less before anything boots.
       pub const MIN_H1_MAX_BUF: usize = 8 * 1024;
   }
   ```
   Rewrite the `h1_max_buf` doc (`:187`) to say: at least `MIN_H1_MAX_BUF`; the h1 read-buffer
   threshold that hyper tests only after a head fails to parse, so it limits memory per connection
   and does not bound header values.
2. `src/config/validation.rs` `validate_topology_and_ceilings` (`:783`, no expectation): insert the
   following right after the body-ceiling check, before the fleet early-returns, so the problem is
   always collected:
   ```rust
   if self.http.h1_max_buf < super::HttpConfig::MIN_H1_MAX_BUF {
       f.err(format!(
           "SSE_H1_MAX_BUF={} is below hyper's {}-byte h1 buffer floor",
           self.http.h1_max_buf,
           super::HttpConfig::MIN_H1_MAX_BUF
       ));
   }
   ```
   Extend the doc comment (`:779-782`) to name the h1 floor. The function grows from about 33 to
   about 41 lines.
3. `src/config/validation_tests.rs`: R2.
4. `src/http/serve.rs` `#[cfg(test)] mod tests`: C1. `HttpConfig` is already imported there.
5. Ledger: `python3 scripts/test-inventory.py --write`.

Functions with a ratcheted `#[expect]` that this commit touches: **none**.
`validate_topology_and_ceilings` and the serve test module carry no expectations.

### Commit B: `The tail row no longer keeps a copy of the lane's Stream-Seq, so a longer sequence cannot wedge the stream`

Write R1, R3, R1b and C2/C3 first and record the red output. Then:

1. `src/shard.rs`
   - Doc (`:55-73`): keep the layout line. Add: "`seq` held a copy of the lane's last Stream-Seq
     before the lane got its own `s` row (ROUTING-V3 §3.6). Nothing reads it, and its u16 length
     cannot hold every header, so this encoder writes `seq_len = 0`. Decoders still step over and
     text-validate what older rows carry."
   - Delete `:75-78` (the `cast_possible_truncation` expectation). The cast it covered is gone.
     Keeping it would be an unfulfilled expectation, and those are denied.
   - `encode_tail`: delete `:80`. Change `:81` to `Vec::with_capacity(76)`. Replace `:96-97` with
     `v.extend_from_slice(&0u16.to_le_bytes());` and a trailing comment `// seq_len: no Stream-Seq copy`.
   - `decode_tail`: delete `:116-120`. Leave `:121` (`let route_at = seq_at + 2 + seq_len;`)
     unchanged, and add right after it:
     ```rust
     // Rows written before the lane got its own row carry a copy of its last
     // Stream-Seq here. Nothing reads it; bytes that are not text are
     // corruption, as they always were.
     std::str::from_utf8(v.get(seq_at + 2..route_at)?).ok()?;
     ```
     Remove `seq,` from the `Some(TailFields { … })` literal (`:147`).
   - `TailFields`: delete `pub seq: Option<String>,` (`:584`).
2. `src/shard/transaction/append.rs`: delete `:237` `local.fields.seq = Some(seq.clone());`. It sits
   in `accept_append` (`:156`), which has **no** expectation. The four expectations at `:3-18` are
   scoped to `CommitTransaction::append` (`:19-155`), which is not touched.
3. `src/shard/transaction_tests.rs`: delete `seq: Some("alpha".into()),` (`:199`) from
   `Fixture::assert_rows`. This is a helper. The pinned test `r03a_mixed_transaction…` keeps its
   body and its sha (review-mechanisms `source_adaptations`, R17-B). Add R1 and R3.
4. `src/shard/storage_decode_tests.rs`: add `legacy_row`. Rewrite
   `r12_supported_tail_versions_and_extensions` (`:6-40`). Add C2, and C3 inside the existing
   `proptest!` block (`:256`).
5. `src/golden_tests.rs` `mod tail_codec`: drop `seq:` from `full_tail()` (`:114`). In
   `FULL_V3_HEX`, replace `"0500"` and `"7365712d37"` with `"0000", // seq_len 0: the lane row
   holds the Stream-Seq`. `golden_layout4_tail_v3_full_bytes` now asserts `len() == 76`. Keep the
   old 81-byte hex as `LEGACY_V3_HEX`. Add `golden_layout4_tail_v3_legacy_stream_seq_copy_is_read_past_and_dropped`:
   splice `*b"\x05\x00seq-7"` into `encode(full_tail())[42..44]`, assert the hex equals
   `LEGACY_V3_HEX`, check `assert_full_fields` on the decode, and assert that re-encoding gives
   `FULL_V3_HEX`. Drop the `seq` assertions at `:151` and `:216`. The v2+extensions test still
   proves the copy is stepped over, because it reads the route after it.
6. `src/dst/tests/producer_protocol.rs`: R1b. This is an existing module, so no README entry is needed.
7. `SPEC.md:122`, `DESIGN.md:64`: the tail no longer lists "last Stream-Seq". The per-routing-key
   `s` row holds it (ROUTING-V3 §3.6).
8. Ledgers: see §6.

Functions with an `#[expect]` that this commit touches:

- `encode_tail`: its only expectation (`cast_possible_truncation`) is **deleted**, not re-decided.
  One exception scope fewer.
- Nothing else: `decode_tail`, `stored_tail`, `TailFields`, `accept_append`, `Fixture::assert_rows`,
  the rewritten r12 test and the golden tests carry no expectations. No function with a
  function-wide `unwrap_used`/`expect_used` expectation is touched. The deleted line is outside
  `CommitTransaction::append`'s scope.

Lints to watch: no new `unwrap`/`expect`/`panic` outside `#![cfg(test)]` files. No function over
100 lines, no nesting over 4, no `_ =>` arm on a domain enum, no mt-lint trigger (no
`name: String` parameters, no `.stream_ref(`).

---

## 5 Mutation-kill analysis (cargo-mutants 27.1.0, `--in-diff`)

cargo-mutants' `in_diff.rs:213-257` marks as changed every inserted line **and the lines on both
sides of each deletion**. Any mutant whose span covers one of those lines is in scope.

**Commit A:** `src/config/*` is neither critical nor registered. The `src/http/serve.rs` change sits
entirely under `#[cfg(test)] mod tests`, so the planner puts the file in `production_unchanged_files`.
Expected result: `mutants: false`. R2's 4096/8191/8192 triple would kill `<`→`<=`, `==` and `>`
anyway.

**Commit B:** mutation sources are `src/shard.rs` (owner `shard`, filter `shard::`) and
`src/shard/transaction/append.rs` (owner `transaction_append`, filter `shard::`). The test files are
`#![cfg(test)]` and so production-unchanged. `golden_tests.rs` and the DST file are not critical.
Owner rows and filters do not change. In-scope mutants and the `shard::` tests that kill them:

| # | site (after the fix) | mutant | killed by |
|---|---|---|---|
| 1-3 | `encode_tail` (body touched) | `vec![]`, `vec![0]`, `vec![1]` | `r12_supported_tail_versions_and_extensions` (`legacy_row` indexes `[42..44]` and panics); R1; R3 |
| 4 | `decode_tail` | `None` | r12 (`stored_tail(..).unwrap()`); C2; R1 |
| 5 | `decode_tail` | `Some(Default::default())` | r12 (`decoded.next == 9`); C2 |
| 6-7 | `:115` `seq_at..seq_at + 2` (line before the deletion) | `-`: `get(42..40)` is `None`; `*`: 42-byte slice fails `try_into` | every decode: r12, C2, R1 |
| 8 | `route_at` `seq_at + 2` (line after the deletion) | `-`: route_at 40, then `get(44..40)` is `None` | every decode of a seq-less row: R1, r12_byte_compatibility, C2 |
| 9 | same | `*`: route_at 84, out of range for a 76-byte row | same |
| 10 | `route_at` `… + seq_len` | `-`: same as the original when `seq_len == 0`, so it survives on new rows | **C2** (`"lane"` gives route_at 40, then `None`) and C3 |
| 11 | same | `*`: route_at 0, then `get(44..0)` | every decode |
| 12 | new validation line `seq_at + 2` | `-`: the slice starts 4 bytes early | **C2** (128-byte copy puts `seq_len` low byte 0x80 into the slice, which is not UTF-8); C3 |
| 13 | same | `*`: `get(84..44)` | every seq-less decode |
| 14 | `closed: flags & 1 != 0` (line after the deleted `seq,`) | `!=`→`==` | C2 (v3 expects closed, v2 expects open); C3 |
| 15 | same | `&`→`|` (always true) | C2 v2 row; C3 |
| 16 | same | `&`→`^` (flags 0 gives true) | C2 v2 row; C3 |
| 17 | `accept_append` (lines 236/237 border the deletion) | `()` | `r03a_mixed_transaction_preserves_every_row_reply_and_publication` (no rows written); R3; R1 |

Mutants 10 and 12 are the only ones that new rows cannot kill. C2 is deterministic and exists for
exactly those two, so no equivalent mutant remains. The `history_v2` line and the struct-field
deletion border no operators. Waits: R1 and R3 wrap the reply in `timeout(5 s)` and
`await_terminated(5 s)`. C2 and C3 are pure. R1b is not in any `shard::` run and bounds its waits
anyway. Loom is not needed, because no synchronization changes. The planner's `loom` leg (lifecycle
prefix `src/shard`) re-runs the existing Loom models.

---

## 6 Ledgers

- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write` in each commit.
  Commit A adds R2 and C1. Commit B adds R1, R3, R1b, C2, C3 and the legacy golden, and changes the
  bodies of `r12_supported_tail_versions_and_extensions`, `golden_layout4_tail_v3_full_bytes` and
  `golden_layout4_tail_v2_decode_no_flags_byte`.
- `docs/quality/owners.json`: the `macro-dsl` row for `src/shard/storage_decode_tests.rs`
  (`proptest::proptest`, count stays 1, because C3 goes inside the existing invocation). Update only
  its `reason` to cover the tail row, for example: "Lane-row and tail-row codec properties; the
  pinned macro generates 1,024 cases per property against the production encoders and decoders:
  full-width producer states, arbitrary stored bytes, and tail rows carrying arbitrary Stream-Seq
  copies; syntax tokens remain inventoried." No new statics, globs, effects or `select!`/`json!` sites.
- `scripts/quality/mutation_owners.py`: no change (no new files).
- `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md`, `src/dst/tests/README.md`: no change
  (no new files, no wire change, no new DST module).
- `docs/refactor/review-mechanisms.json`: no change. `r03a_…`'s pinned body is untouched, and no
  mechanism test or support function is edited. Confirm with `python3 scripts/review-evidence.py --check`.
- Source/diagnostic allowances: no ledger entry. The deleted `encode_tail` expectation is an
  in-source reasoned exception. Its `legacy-diagnostics*.json` row is part of the frozen adoption
  baseline and is not edited.
- Docs: SPEC.md:122 and DESIGN.md:64 (commit B), and the `h1_max_buf` doc (commit A).

---

## 7 Controls (exact commands, expected outputs)

Red capture: add the tests to the unfixed tree first.
```
cargo test --locked --lib shard::transaction_tests::r53_ 2>&1 | tail -30
  -> 2 FAILED, with the R1 and R3 messages quoted in §3
cargo test --locked --lib dst::dst_tests::producer_protocol::a_stream_seq_past_the_tail_rows_u16_survives_a_reopen -- --exact
  -> FAILED, with the R1b message
cargo test --locked --lib config::validation::validation_tests::validate_boundary_tests::validation_rejects_an_h1_buffer_below_hypers_floor -- --exact
  -> FAILED: validate() must reject (marker "SSE_H1_MAX_BUF")
cargo test --locked --lib shard::storage_decode_tests::   -> ok (C2, C3 and the rewritten r12 pass before the fix too)
```
After each commit:
```
cargo fmt --all -- --check                                              -> no output
cargo clippy --locked --workspace --all-targets -- -D warnings           -> clean (no unfulfilled_lint_expectations)
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items -> ok
cargo test --locked --lib shard::                                        -> ok (R1, R3, C2, C3 included)
cargo test --locked --lib golden_tests::                                 -> ok
cargo test --locked --lib dst::dst_tests::producer_protocol::            -> ok (R1b, stream_seq_is_scoped_to_the_routing_key)
cargo test --locked --lib dst::dst_tests::durability_failures::a_stream_seq_verdict_is_grounded_in_durable_state -- --exact -> ok
cargo test --locked --lib config::                                       -> ok (R2)
cargo test --locked --lib http::serve::                                  -> ok (C1; hyper's assert text appears in captured stderr)
cargo test --locked --release --lib quality_                             -> ok, count +1 (C3 at 1,024 cases)
python3 scripts/test-inventory.py --check                                -> test-inventory: OK (N tests, …)
scripts/quality.sh                                                       -> QUALITY_OK
```
Plan and mutants. Compare against the tip the push replaces: `origin/slate`, or `7c4f8606` if the
two pending commits are pushed first.
```
cargo build --locked -p streams-quality-syntax
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<pushed tip> python3 scripts/quality/verification_plan.py --out target/quality-plan
  commit B -> selected_mutation_owners ⊇ ["shard", "transaction_append"], unregistered_mutation_source_files [],
              production_unchanged_files ⊇ [src/shard/storage_decode_tests.rs, src/shard/transaction_tests.rs]
  commit A -> mutants false; production_unchanged_files ⊇ [src/http/serve.rs]
git diff <pushed tip> HEAD > target/item53.diff
cargo mutants --list --in-diff target/item53.diff --file src/shard.rs --file src/shard/transaction/append.rs --package streams-slate
  -> the 17 mutants of §5 (the doc-comment hunks add none)
scripts/quality/mutations.sh                                              -> 0 missed, 0 timeouts
```

---

## 8 Out of scope

- **Producer-Id length (recommended follow-up, D4).** `producer_key` is
  16 + 1 + 16 + `id.len()` bytes. `parse_producer` (`application/append/contract.rs:368`) checks
  only that the id is non-empty and does not use the reserved prefix. slatedb's
  `WriteBatch::put` asserts `key.len() <= u16::MAX` (`batch.rs:153-157`, "key size must be <=
  u16::MAX"), so an id of 65,503 bytes or more panics inside the committer at
  `transaction/append.rs:179-182`. It is reachable when `SSE_H1_MAX_BUF` is raised, and possibly at
  the default (§1.2). The fix needs no new wire code: a length check in `parse_producer`, answered
  with the existing `400 invalid_producer`.
- **A ceiling on SSE_H1_MAX_BUF, or an exact head-size bound.** This is a transport-posture
  question. hyper's limit is soft, so bounding header values means checking them by name.
- An unparseable `SSE_H1_MAX_BUF` silently falls back to the default (`env_parse`). This is the
  existing convention for many knobs.
- The reviewer's typed `StreamSeq` and the admission bound. They are the rejected alternative (§1.5, §2).
- `rebuild_maintenance_from_tails` failing the whole shard on one corrupt tail is deliberate
  (`shard.rs:452-454`). After this fix, this bug can no longer produce such a tail.
- Optional control, not added: a real-socket DST that sends a 70 KB `Stream-Seq` over `serve_h1` at
  the default buffer and accepts 204 or 431 as long as the stream stays openable. It is not a
  reliable red, because hyper's acceptance depends on read chunking.

## Decisions for Søren

- **D1:** Delete the tail row's copy of the lane's Stream-Seq. This changes what gets persisted:
  new rows write `seq_len = 0`, the decoder is unchanged in what it accepts, and the full-fixture
  encode golden goes from 81 to 76 bytes while the old bytes are kept as a decode golden. Rollback
  is safe to any binary since `e6717f9c`. The backward-compatible alternative keeps the copy and
  adds a new `400 invalid_stream_seq` for Stream-Seq longer than 65,535 bytes at admission (a wire
  change, WIRE-MATRIX row).
- **D2:** No Stream-Seq length contract; the transport's head buffer is the only limit. The
  alternative is a named protocol ceiling, which would be a new wire refusal. Not recommended.
- **D3:** `SSE_H1_MAX_BUF` below 8,192 becomes a boot-time configuration error (exit 1, named in the
  problem list) instead of a panic after bootstrap's side effects (exit 101). There is no ceiling.
  Every value that works today is unchanged.
- **D4:** Open a follow-up item for the Producer-Id key-size panic in the committer: a length check
  in `parse_producer` using the existing `400 invalid_producer`.

---

## Skeptic corrections (C1..C9)

I checked the plan against the tree at `7c4f8606` (read-only, nothing run). The following hold as
written: the wrap arithmetic (65,536 → `seq_len 0`, `route_at 44`, extension 65,568,
`trim_safe_to = 0x7373…` > `absorbed 0`; 70,000 → 4,464); the Display
`Data error: inconsistent persisted tail` (slatedb `error.rs:545-552`, `"{kind}: {msg}"`, no
source); the fact that `TailFields.seq` has one writer (`transaction/append.rs:237`) and one reader
(`shard.rs:80`), with test literals only at `golden_tests.rs:114,151,216`, `storage_decode_tests.rs:12,20`
and `transaction_tests.rs:199` (every other `seq: None` in `src/` is an `AppendReq`); routing v3
really stopped reading the copy (`git show e6717f9c -- src/shard.rs`, `local.fields.seq` compare
replaced by the `s` row); hyper 1.10.1's assert (`server/conn/http1.rs:380-387`,
`MINIMUM_MAX_BUFFER_SIZE = INIT_BUFFER_SIZE = 8192`); `h1_builder` running once per `serve_h1`
(`http.rs:1289`); `validate_topology_and_ceilings` (`validation.rs:783`) carrying no expectation;
the four expectations at `transaction/append.rs:3-18` being scoped to `CommitTransaction::append`,
with `accept_append` (`:156`) carrying none; the red messages for R1, R3, R1b and R2 (the `rejects`
panic text at `validation_tests.rs:492`); slatedb's `key size must be <= u16::MAX`
(`batch.rs:153-157`), which makes the D4 arithmetic `33 + 65,503 > 65,535` right; and the
cargo-mutants 27.1.0 affected-line rule (`in_diff.rs:204-257`, mutant span ∩ affected lines,
`:117-128`). I found no missed mutant or equivalent mutant: every affected-line operator is killed
by a `shard::` test, and mutants 10 and 12 are killed only by C2's `"lane"` and 128-byte copies,
as the plan says. `review-mechanisms.json` pins only the `r03a` body (`:1018`, not `assert_rows`),
and no mechanism test or support function in the touched files is pinned.

**C1: the ceiling and comparison base are stale.** `origin/slate` is now `7c4f8606`: HEAD has
been pushed, so `git rev-parse origin/slate` equals HEAD. `git show origin/slate:src/shard.rs | wc -l`
gives **3,197**, and that is the only ceiling. "3,232 against origin/slate" was true at
`9b2f53bc` and no longer applies. Every `QUALITY_BEFORE_SHA` and `git diff <pushed tip>` in §7 is
`7c4f8606`. Budget for commit B in `shard.rs`: net ≤ 0. The planned deletions are about 14 lines
(expect 4, `let seq` 1, cast and copy 2→1, decode `seq` block 5, literal `seq,` 1, struct field 1).
The new encoder and decoder doc prose plus the 4-line validation comment must fit within those
14 lines, so keep the doc addition to 4 or 5 lines.

**C2: the test-inventory ledger is described wrong.** `scripts/test-inventory.py:138` scans only
`src/dst/**/*.rs`, and all 502 manifest rows are `src/dst/...`. The precedent `517f4fa1`, which
added shard and proptest tests, did not touch `docs/refactor/test-inventory.json`. Corrected §6:
commit A changes the inventory **not at all** (R2 and C1 are not DST tests). Commit B adds exactly
**one** row, R1b in `src/dst/tests/producer_protocol.rs`. R1, R3, C2, C3, the golden tests and the
rewritten r12 are not inventoried. `--write` stays harmless, but the stated expectation for
`test-inventory.py --check` must match this.

**C3: the golden module's own rule has to be addressed.** `src/golden_tests.rs:10-12` says:
"A failure here means the storage format changed; that is a deliberate act, never something to
'fix' by pasting the new actual value without a layout-version bump." Step 5 of commit B does
exactly that to `FULL_V3_HEX` (81→76) with no version bump. The layout (`ver=3`, the u16 length
field) does not change, only what the encoder writes, so no bump is needed. But D1 must say this
explicitly, and the test edit must not look like a pasted actual. Do it the other way round:
keep `FULL_V3_HEX` byte-for-byte unchanged as the **decode** golden (the legacy test asserts that
it decodes to `assert_full_fields` and re-encodes to the new constant), and add a new
`FULL_V3_ENCODED_HEX` (76 bytes, `"0000"` at the length). That leaves the hand-derived 81-byte
literal untouched. Also update the stale comment in `golden_layout4_tail_v3_minimal_bytes`
(`:169`, "seq=None encodes as seq_len=0"), because the field no longer exists.

**C4: a dangling spec reference, and incomplete doc edits.** The new doc text cites "ROUTING-V3 §3.6".
`docs/ROUTING-V3.md` has no such section: §3 is "Compact postings", and split-safe Stream-Seq is
**§7** (`docs/ROUTING-V3.md:258-266`). The existing "§3.6" citations at `shard.rs:180,638` come from
an older numbering and should not be copied. Cite ROUTING-V3 §7. Separately, `SPEC.md` §3.3
(`:118-125`) and `DESIGN.md:62-66` list no `s` row at all. Dropping "last Stream-Seq" from the `t`
line therefore leaves the lane's owner undocumented. Add the row
`<hash16> s <key_hash16>   Stream-Seq lane (last accepted, per routing key)` in both places.

**C5: the "backward-compatible alternative" is mislabelled.** The plan's alternative adds
`400 invalid_stream_seq`, which is a wire change and so not backward compatible. A strictly
compatible alternative exists. It needs no wire change, and every row shape that exists today is
stored byte-for-byte unchanged, so `FULL_V3_HEX` stays as it is:
`let seq_len = u16::try_from(seq.len()).unwrap_or(0)` (the copy is written only when it fits, and
`seq_len 0` with no copy otherwise). This also deletes the cast `#[expect]` because the conversion
is checked. Its new branch is killed by R1 (65,536 bytes) and by r03a/R3 (a 5-byte copy). Offer it
to Søren as the minimal-diff option (it keeps the unread copy). The plan's deletion remains a sound
recommendation.

**C6: D1 needs its irreversibility facts stated.**
(a) The fix prevents new corruption. It does **not** repair tail rows already written with a
wrapped length. Any such stream stays refused by `stored_tail`, and a missing or legacy ledger
still escalates to an engine-open failure (`shard.rs:508`). State "no repair path" in D1, or add one.
(b) A stream whose last Stream-Seq was written before `e6717f9c` has that value only in the tail
copy. Such lanes have failed open since routing v3, because no reader consults the copy. After
this change the next tail rewrite erases the copy for good. Serving behaviour does not change,
but D1 should say it.
(c) The rollback sentence cites `shard.rs:71-74` as a routing-v3 downgrade caveat. That caveat is
about the flags bitmask. It supports "forward-only deployments" in general, not routing v3
specifically. Reword it.

**C7: D3's exit code is asserted, not verified.** A panic unwinding out of `block_on` drops the
multi-thread `Runtime`, and the drop waits for blocking-pool threads. If bootstrap has started any
blocking work before `bootstrap.rs:902`, the process can stall in teardown instead of exiting 101.
D3 should say "panics after bootstrap's side effects (exit 101 unless runtime teardown blocks)".
The fix is unchanged.

**C8: C1 is buildable, with one note.** `catch_unwind(move || drop(super::h1_builder(..)))`
captures only a `usize`, so the closure is `UnwindSafe`. The precedent is
`ownership.rs:178-195`, which uses the `AssertUnwindSafe` form. `TokioTimer::new()` needs no
runtime. The profile has no `panic = "abort"`. Keep C1 in `http::serve::tests`. Moving it into
`config` tests would make a config file reference `crate::http` (architecture policy). It stays
`production_unchanged` for the `http_serve` owner (`mutation_owners.py:87`), so commit A plans
`mutants: false`, as claimed.

**C9: minor line and citation fixes.** `r12_supported_tail_versions_and_extensions` spans
`storage_decode_tests.rs:5-40`. The `proptest!` block starts at `:256` (correct). §4's row for
`registry.rs` is 1,501 now (the task's 1,509 is a stale ceiling, and the file is untouched). R1b
reopening must use the same `FaultStore` (`store.clone()`), not a fresh `mem()`.

**Controls:** every red and control compiles on the current tree as described, except C1, which
by design compiles only after commit A. None is unbuildable.

**Missed or misdescribed ledgers:** test-inventory scope (C2); the `golden_tests.rs:10-12` layout
rule (C3); the SPEC and DESIGN keyspace `s` row (C4). None of the following needs a change:
`owners.json` (the macro-dsl count stays 1), `mutation_owners.py`, `architecture-policy.json`,
`WIRE-MATRIX.md`, the DST README, `review-mechanisms.json`, `legacy-diagnostics*.json` (a frozen
baseline; the precedent `517f4fa1` deleted an expectation without touching it), and
`verification.json` (its hashes are stale at recorded revision `74e1faae` and not live-checked).

**Verdict: ready-with-corrections.** The diagnosis, the design (delete the unread copy and
validate the h1 floor), the red tests and the mutation analysis all hold. Apply C1 to C4 before
implementing. C5 and C6 change what Søren is asked to approve in D1.
