# Queue settle: `delayMs` / `visibilityMs` reach the lease deadline unbounded

Plan against `slate` @ `668bc80c`, read-only. Nothing was compiled or run; every "expected
failure" is derived from the code and says so. The working tree carries another session's
uncommitted edits (`ci.yml`, `rust-quality.yml`, `docs/RUST-QUALITY.md`, `scripts/gate.sh`,
`quality.sh`, `release-gate.sh`, `src/application/append.rs`, new `scripts/quality/tests_ran.py`,
`scripts/test-leg.sh`); none overlaps a file this plan touches.

## 0. Verdict on the claim

**Confirmed.** Four of the review's pointers are off; the defect is real and production-reachable.

| Review said | First-hand |
| --- | --- |
| `delivery.rs ~:459-476` passes raw `Option<u64>` into `QueueOp` | TRUE. `src/application/consumer/delivery.rs:465` `i.delay_ms.unwrap_or(1_000)` and `:473` `i.visibility_ms.unwrap_or(cfg.visibility_timeout_ms as u64)` go straight into `QueueOp::Settle { retries, extends, .. }` at `:492-499`. |
| `settle.rs ~:87-108` computes `now + x as i64` | TRUE. `src/shard/transaction/queue/settle.rs:89` `deadline_ms: now + delay as i64`; `:106` `deadline_ms: now + vis as i64`. `delay`/`vis` are `u64` (`src/queue.rs:320-321`). |
| the pull clamps at `src/queue.rs ~:273` | WRONG FILE. `queue.rs:273` is only the field `visibility_ms: u64`. The pull's clamp is `delivery.rs:43-46`: `.unwrap_or(cfg.visibility_timeout_ms as u64).clamp(1_000, 12 * 3600 * 1000)`. A second copy of the literal is the config-time clamp `src/application/consumer.rs:465` (`put`). |
| `docs/WIRE-MATRIX.md ~:158` | WRONG PATH, right line: `docs/refactor/WIRE-MATRIX.md:158` (section 2.16). `:152` is the pull row that states "visibility 1 s–12 h". |
| remote/relayed consumer ops may carry the fields | NO. `src/application/consumer_remote.rs` relays only `GET /v1/internal/queue-cursor` (`:15-46`) and `POST /v1/internal/sweep-segment` (`:56-146`). A settle whose segment is foreign returns the ownership error from `state.engine_for(&route)` (`delivery.rs:485-488`); nothing serializes a `QueueOp`. |
| a persisted/serialized `QueueOp` could break | NO. `pub(crate) enum QueueOp` (`queue.rs:266-324`) derives nothing; it only travels `ShardEngine::submit_queue` -> `CommitOp::Queue` in-process. The one durable artefact is the lease row, `encode_lease` (`queue.rs:152-159`): `deadline_ms` i64 LE + count u32 + gen u32 + key hash. Its format does not change. |

What the unbounded value does, by band (now ≈ 1.76e12 ms):

* `(12 h, i64::MAX − now]`, e.g. `9_000_000_000_000_000_000`: no overflow; `deadline_ms ≈ 9e18`
  (285 million years). `receive.rs:68` and `:87` treat `deadline_ms > now` as in flight, so the
  record is never re-leased, never reaches `delivery_count >= max_deliveries` (`receive.rs:91`,
  `settle.rs:83`), never dead-letters, and its routing key stays in `blocked` (per-key FIFO,
  `receive.rs:65-70`). Once the segment seals, `pull` stops the lineage walk on that backlog forever
  (`delivery.rs:202-206`). Only an ack with the token or a consumer delete clears it. **This is the
  production hazard.**
* `(i64::MAX − now, i64::MAX]`: `now + x` overflows. `[profile.release]` (`Cargo.toml:72`) has no
  `overflow-checks`, so the shipped binary and CI's `cargo test --release` (`ci.yml:89`,
  `scripts/gate.sh:16`) wrap to a negative deadline (expired at once). `dev`/`test` and the mutation
  profile `quality` (`inherits = "dev"`, `Cargo.toml:77`) panic inside the committer task.
* `>= 2^63`, e.g. `u64::MAX`: `as i64` is negative (`u64::MAX as i64 == -1`), deadline `now − 1`,
  the record is redelivered at once. `u64 as i64` is `cast_possible_wrap` (pedantic, not enabled:
  `Cargo.toml:151-152` enable only `cast_possible_truncation`/`cast_sign_loss`), so it passes
  `-D warnings` today.

The SDK forwards user numbers verbatim (`sdk/src/index.ts:1248-1261`), so no hand-built client is
needed to hit any band.

## 1. Mechanism (evidence)

```
product.rs:4016   serde_json::from_slice::<SettleInput>          SettleItem { delay_ms, visibility_ms: Option<u64> }  consumer.rs:293-299
delivery.rs:465   .push((o, g, i.delay_ms.unwrap_or(1_000)))                              <- unbounded u64
delivery.rs:473   i.visibility_ms.unwrap_or(cfg.visibility_timeout_ms as u64)             <- unbounded u64
delivery.rs:492   QueueOp::Settle { retries, extends, .. }        queue.rs:320-321  Vec<(u64, u32, u64)>
settle.rs:89      Lease { deadline_ms: now + delay as i64, ..l }  (retry keeps lease_gen, delivery_count)
settle.rs:106     Lease { deadline_ms: now + vis as i64, ..l }    (extend keeps lease_gen, delivery_count)
queue.rs:154      encode_lease: deadline_ms.to_le_bytes()          durable row
receive.rs:68,87  l.deadline_ms > now  => in flight / key blocked
```

The pull is safe only because its single producer clamps (`delivery.rs:43-46`) before
`QueueOp::Receive` (`:125-133`) and `receive.rs:103 now + visibility_ms as i64`.

Every producer of the three window fields (`grep -rn "QueueOp::(Settle|Receive)"`):

| Site | Value |
| --- | --- |
| `delivery.rs:125` Receive | `visibility` (clamped u64) |
| `delivery.rs:492` Settle | client values (the bug) |
| `delivery.rs:724` Settle (dead-letter ack) | `retries: Vec::new(), extends: Vec::new()` |
| `src/shard/queue_codec_tests.rs:45` | `visibility_ms: 10` (literal) |
| `src/dst/tests/consumer_atomicity.rs:457,583,749`, `consumer_saga.rs:503` | `visibility_ms: 30_000` (literal) |
| `src/dst/tests/consumer_atomicity.rs:613-615` | `acks: vec![(off0, gen0)], retries: Vec::new(), extends: Vec::new()` |

Every test producer is an integer literal or an empty `Vec`, so narrowing the fields to `u32`
touches **no test file** (no `function_sha256` churn in `docs/refactor/test-inventory.json`; none
of those tests is pinned in `docs/refactor/review-mechanisms.json`).

No existing DST test sends `extends` over HTTP at all; the only settle window sent anywhere is
`delayMs: 0` (`consumer_dlq.rs:60`). `conformance/conformance.test.mjs` never touches consumers;
`sdk/scripts/consumer-cleanup.test.mjs` uses `delayMs: 5`/`visibilityMs: 100` against a mock fetch.

## 2. Design

### Clamp, not refuse

* The pull clamps silently and the matrix says so (`WIRE-MATRIX.md:152` "clamps: … visibility
  1 s–12 h"); consumer PUT clamps (`:137`). Refusing on settle would be the odd one out.
* Settle is deliberately permissive: "Invalid/foreign tokens counted as `stale`, never errors"
  (`:158`, spec §2.5), and one settle carries acks *and* retries atomically. A 400 for one oversized
  `delayMs` would discard the batch's acks and redeliver work that already succeeded.
* Cloudflare Queues (the profile's model, `docs/history/PROFILES.md:250-262`) caps `delaySeconds`
  at 43 200 = 12 h; the same bound.

Bounds: retry `delayMs` 0 ..= 12 h, default 1 s (unchanged default; zero keeps meaning "release
now", which `consumer_dlq.rs:60` relies on). Extend `visibilityMs` 1 s ..= 12 h, default the
consumer's `visibilityTimeoutMs` (unchanged default) — the same floor a pull has always applied.
**The extend floor is the one wire behaviour change that is not a tightening of absurd values**:
today `extends[].visibilityMs: 0` means "visible now"; after this it is 1 s, and "visible now" is
spelled `retries[{delayMs: 0}]`. Nothing in-repo relies on extend-0. Flagged for Søren; if extend-0
must stay, route extends through `bounded_window_ms(.., 0)` via a fourth entry point and flip one
table row in §3.

### S — the smallest textual change (NOT recommended, and it trips the ratchet)

Keep every type; clamp inline in `delivery.rs` `settle` with the pull's literal:

```rust
.push((o, g, i.delay_ms.unwrap_or(1_000).min(12 * 3600 * 1000)));
i.visibility_ms.unwrap_or(cfg.visibility_timeout_ms as u64).clamp(1_000, 12 * 3600 * 1000),
```

`settle` carries `#[expect(clippy::too_many_lines)]` (`delivery.rs:409-412`), so its
`syntax_facts` is a ceiling (`scripts/quality/source_rules.py:151-158`). Each added method call is
+2 facts (`method-call` + `method-call-site`): +4 → `accepted exception grew without a new
decision: … syntax_facts N -> N+4`. The only sanctioned exits are re-deciding the reason text or
shrinking the function. It also leaves the committer's `now + u64 as i64` non-total by type and a
fourth/fifth copy of the 12 h literal. Rejected.

### O′ — the task's literal direction (`now + i64::from(ms)` in `settle.rs`) — NOT needed and blocked

`CommitTransaction::settle` carries `expect_used` (`settle.rs:7-10`), so every `call-site` and
`path` under it is fingerprinted (`source_rules.py:159-188`, `expect_site:ordinary-call` and
`expect_site:path`). `i64::from(delay)` adds one `call-site` and one `path` fingerprint (0 → 1) and
+1 `syntax_facts` per site: "accepted exception grew" on all three attributes, twice. With `u32`
fields (below) `now + delay as i64` is byte-identical, lossless, and bounded by ~1.76e12 + 4.29e9,
so `i64::from` buys nothing. The committer files stay at 0 bytes changed.

### O — one owner in `src/queue.rs`, fields narrowed to `u32`, committer untouched (RECOMMENDED)

* `src/queue.rs` owns the lease window: `pub(crate) const MAX_LEASE_WINDOW_MS: u32`,
  `visibility_window_ms(Option<u64>, &ConsumerConfig) -> u32` (1 s..=12 h, default configured),
  `retry_delay_ms(Option<u64>) -> u32` (0..=12 h, default 1 s), `configured_visibility_ms(u32) -> u32`
  (the put's clamp), one private `bounded_window_ms(u64, floor) -> u32`.
* `QueueOp::Receive.visibility_ms`, `QueueOp::Settle.retries.2`, `.extends.2` become `u32`. A
  `u32` of milliseconds is < 49.7 days, so `now + x as i64` cannot overflow or wrap: the overflow
  class becomes unrepresentable at the committer, and the 12 h *policy* lives at the single
  application owner.
* `pull`, `settle` (`delivery.rs`) and `put` (`consumer.rs`) call the owner. Every edited line is
  fact-neutral or shrinking (§4 budget), no `#[expect]` changes, no moves.
* `settle.rs`, `receive.rs`, every test producer: untouched.

### B — proof-bearing `LeaseWindow(u32)` newtype (owner-first, deferred)

Private-field newtype with constructors `LeaseWindow::visibility(..)`/`::retry_delay(..)` and the
deadline arithmetic on the type; `QueueOp` and `MessageContext` take it. Nothing could ever hand
the committer an unbounded window. Costs in this repo: touches both `expect_used`-fingerprinted
committer functions (six reason texts to re-decide, or `impl Add<LeaseWindow> for i64` purely to
fit the ratchet); `settle.rs`/`receive.rs` enter the mutation diff under owners
`queue_settle`/`queue_receive` (filter `shard::`, which no DST test matches); all seven test
producers change (five reviewed `function_sha256` changes). Take it only if a second in-crate
producer of window fields ever appears.

**Recommendation: O.** Same guarantee as B for every producer that exists (type-bounded, one
policy owner), zero committer bytes, zero test-producer bytes, net shrink of both ratcheted
application functions.

## 3. Red tests

### 3a. DST — append to `src/dst/tests/consumer_dlq.rs` (383 → ≈545 lines; ceiling 1 000)

Why this file: the consequence of the bug is "poison never dead-letters", and the file already
owns `KEY`, `append_keyed`, `bounded_pull` and the lease-row read (`:85-90`, `:112`). Appending
here needs **no** `#[path]` registration in `src/dst/dst_tests.rs` and therefore no
`by-path-module` row in `docs/quality/owners.json` (every `#[path]` module is inventoried:
`source_rules.py:61-62`, and `crate::consumer_dlq` already has its allowance in
`docs/quality/source-allowances.json`). If a sibling file is preferred, the exact rows are in §5.

Rules the tests obey: bodies built with `format!` — `serde_json::json!` is a `macro-dsl`
occurrence needing an `owners.json` row per function (`source_rules.py:15-19, 53-55`); no
`tokio::spawn`; no glob import; every wait bounded (5 s; a wedge fails, never hangs); no
`#[expect]`; nesting ≤ 2; every fn < 100 lines. The oracle is the durable lease row, read the way
`r07` reads it. `TWELVE_HOURS_MS` is a literal, not the owner's constant, so the tests compile on
the current tree and fail by assertion (red), and the oracle stays independent of the code under
test.

```rust
/// The lease window `:pull` has always kept (`delivery.rs:46`), which a
/// settle must keep too. A literal on purpose: the oracle is independent of
/// `queue::MAX_LEASE_WINDOW_MS`, and these scenarios compile on a tree
/// without it.
const TWELVE_HOURS_MS: i64 = 12 * 3600 * 1000;

/// A JSON `stream` with one record under routing key `k` and consumer
/// `work` (three attempts, no dead letter), leased once by a pull. Returns
/// that lease's token; a retry or an extend keeps its generation, so the
/// same token settles every later window in a scenario.
async fn leased_once(addr: std::net::SocketAddr, stream: &str) -> String {
    let (status, _, body) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{stream}"),
        &KEY,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    append_keyed(addr, stream, "k", br#"{"n":0}"#).await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{stream}/consumers/work"),
        &KEY,
        br#"{"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let first = bounded_pull(addr, stream, b"{}").await;
    let messages = first["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "attempt one leases the record: {first}");
    assert_eq!(messages[0]["attempts"], 1);
    messages[0]["leaseToken"].as_str().unwrap().to_owned()
}

/// One settle of `token` alone under `verb` (`retries` or `extends`) with
/// its window `field` (`delayMs` or `visibilityMs`) at `requested`, or left
/// out. Returns the reply and the wall-clock bracket the committer's `now`
/// fell in. Bounded like `bounded_pull`: a committer that died on the
/// request fails the scenario instead of wedging it.
async fn settle_window(
    addr: std::net::SocketAddr,
    stream: &str,
    token: &str,
    (verb, field, requested): (&str, &str, Option<u64>),
) -> (serde_json::Value, (i64, i64)) {
    let window = requested.map_or_else(String::new, |ms| format!(r#","{field}":{ms}"#));
    let doc = format!(r#"{{"{verb}":[{{"leaseToken":"{token}"{window}}}]}}"#);
    let path = format!("/v1/streams/{stream}/consumers/work:settle");
    let before = crate::shard::now_ms();
    let (status, _, body) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        preq(addr, "POST", &path, &KEY, doc.as_bytes()),
    )
    .await
    .expect("a settle answers within five seconds");
    let after = crate::shard::now_ms();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    (serde_json::from_slice(&body).unwrap(), (before, after))
}

/// The deadline the durable lease row behind `token` holds.
async fn lease_deadline_ms(state: &crate::http::AppState, stream: &str, token: &str) -> i64 {
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(stream))
        .await
        .unwrap()
        .unwrap();
    let stream_key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let lease = crate::product_cursor::LeaseToken::decode(
        token,
        &desc.project_id,
        &stream_key,
        &desc.epoch(),
    )
    .unwrap();
    let segment = desc.resolve_segment("k");
    let engine = state.engine_for(&segment.shard_route).await.unwrap();
    let key = crate::queue::lease_key(
        &segment.identity,
        "work",
        lease.consumer_gen,
        lease.msg.offset,
    );
    let row = engine
        .db
        .get(&key)
        .await
        .unwrap()
        .expect("a held lease keeps its row");
    crate::queue::decode_lease(&row)
        .expect("a lease row decodes")
        .deadline_ms
}

/// The committer stamps `deadline = now + window` at a `now` inside the
/// bracket, so the bound is exact on both sides.
fn assert_held(deadline_ms: i64, (before, after): (i64, i64), held_ms: i64, asked: &str) {
    assert!(
        (before + held_ms..=after + held_ms).contains(&deadline_ms),
        "{asked}: the lease row's deadline is {} ms past the settle, not {held_ms}",
        deadline_ms.saturating_sub(before)
    );
}

/// The longest `delayMs` a `u64` carries is held to twelve hours. Unbounded,
/// the committer's `now + delay as i64` wraps to `now - 1` and the very next
/// pull redelivers the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_asking_for_the_longest_delay_is_not_redelivered_at_once() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-retry-max").await;
    let (reply, bracket) =
        settle_window(addr, "lw-retry-max", &token, ("retries", "delayMs", Some(u64::MAX))).await;
    assert_eq!(reply["retried"], 1, "{reply}");
    let again = bounded_pull(addr, "lw-retry-max", b"{}").await;
    assert_eq!(
        again["messages"].as_array().unwrap().len(),
        0,
        "the longest delay a u64 carries redelivered the record at once: {again}"
    );
    let deadline = lease_deadline_ms(&state, "lw-retry-max", &token).await;
    assert_held(deadline, bracket, TWELVE_HOURS_MS, "delayMs u64::MAX");
    engine_shutdown(&state).await;
}

/// A retry's `delayMs` lands in the lease row as `now + delay`. Unbounded,
/// 9e18 ms never expires: the record is never redelivered, never reaches
/// `maxAttempts`, never dead-letters, and its key stays blocked. A settle
/// holds the delay to the twelve hours a pull already keeps and passes the
/// rest through unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_delay_beyond_the_lease_window_is_held_to_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-retry").await;
    for (requested, held_ms) in [
        (Some(9_000_000_000_000_000_000), TWELVE_HOURS_MS),
        (Some(250), 250),
        (Some(0), 0),
        (None, 1_000),
    ] {
        let (reply, bracket) =
            settle_window(addr, "lw-retry", &token, ("retries", "delayMs", requested)).await;
        assert_eq!(reply["retried"], 1, "{requested:?}: {reply}");
        let deadline = lease_deadline_ms(&state, "lw-retry", &token).await;
        assert_held(deadline, bracket, held_ms, &format!("delayMs {requested:?}"));
    }
    engine_shutdown(&state).await;
}

/// The longest `visibilityMs` a `u64` carries is held to twelve hours on an
/// extend too; unbounded it wraps and the next pull redelivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_extend_asking_for_the_longest_visibility_is_not_redelivered_at_once() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-extend-max").await;
    let (reply, bracket) = settle_window(
        addr,
        "lw-extend-max",
        &token,
        ("extends", "visibilityMs", Some(u64::MAX)),
    )
    .await;
    assert_eq!(reply["extended"], 1, "{reply}");
    let again = bounded_pull(addr, "lw-extend-max", b"{}").await;
    assert_eq!(
        again["messages"].as_array().unwrap().len(),
        0,
        "the longest visibility a u64 carries redelivered the record at once: {again}"
    );
    let deadline = lease_deadline_ms(&state, "lw-extend-max", &token).await;
    assert_held(deadline, bracket, TWELVE_HOURS_MS, "visibilityMs u64::MAX");
    engine_shutdown(&state).await;
}

/// An extend's `visibilityMs` is held to the same one second to twelve hours
/// as a pull's, and defaults to the consumer's configured timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_extended_visibility_is_held_between_one_second_and_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-extend").await;
    for (requested, held_ms) in [
        (Some(9_000_000_000_000_000_000), TWELVE_HOURS_MS),
        (Some(0), 1_000),
        (Some(5_000), 5_000),
        (None, 30_000),
    ] {
        let (reply, bracket) =
            settle_window(addr, "lw-extend", &token, ("extends", "visibilityMs", requested)).await;
        assert_eq!(reply["extended"], 1, "{requested:?}: {reply}");
        let deadline = lease_deadline_ms(&state, "lw-extend", &token).await;
        assert_held(deadline, bracket, held_ms, &format!("visibilityMs {requested:?}"));
    }
    engine_shutdown(&state).await;
}

/// Control: a pull already keeps the window. Its clamp moves onto the shared
/// owner in this change and must keep answering the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pulled_visibility_beyond_the_lease_window_is_held_to_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/lw-pull",
        &KEY,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    append_keyed(addr, "lw-pull", "k", br#"{"n":0}"#).await;
    let (status, _, body) =
        preq(addr, "PUT", "/v1/streams/lw-pull/consumers/work", &KEY, b"{}").await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let before = crate::shard::now_ms();
    let pulled = bounded_pull(addr, "lw-pull", br#"{"visibilityMs":9000000000000000000}"#).await;
    let after = crate::shard::now_ms();
    let token = pulled["messages"][0]["leaseToken"].as_str().unwrap();
    let deadline = lease_deadline_ms(&state, "lw-pull", token).await;
    assert_held(deadline, (before, after), TWELVE_HOURS_MS, "a pull's visibilityMs 9e18");
    engine_shutdown(&state).await;
}
```

Why one lease serves a whole table: a retry and an extend both keep `lease_gen` and
`delivery_count` (`settle.rs:88-92`, `:105-108`), neither checks expiry, and no pull runs between
rows, so the same token settles `1` every row (`maxAttempts` 3 > count 1). `bounded_pull` with
`{}` is `waitMs` 0 (`delivery.rs:47`): one lineage walk, immediate answer, 5 s bound.

**Expected on the current tree** — `cargo test --locked --release --lib dst::dst_tests::consumer_dlq::`
(derived from the code, not executed; `--release` matters: it is CI's and `gate.sh`'s profile, and
it wraps rather than panics):

| Test | Result | First failing assertion, exact shape |
| --- | --- | --- |
| `a_retry_asking_for_the_longest_delay_is_not_redelivered_at_once` | FAIL | ``assertion `left == right` failed: the longest delay a u64 carries redelivered the record at once: {"backlog":1,"messages":[{"attempts":2,…}]}`` / `left: 1` / `right: 0` — `u64::MAX as i64 == -1`, deadline `now − 1`, `receive.rs:87` sees it expired, re-leases with count 2. |
| `a_retry_delay_beyond_the_lease_window_is_held_to_twelve_hours` | FAIL, row 1 | `delayMs Some(9000000000000000000): the lease row's deadline is 9000000000000000xxx ms past the settle, not 43200000` (xxx = the ms between `before` and the committer's `now`; 9e18 + now ≈ 9.0000018e18 < i64::MAX, identical in every profile). |
| `an_extend_asking_for_the_longest_visibility_is_not_redelivered_at_once` | FAIL | same shape as the retry twin, `"extended"` counted 1, pull returns `attempts: 2`, `left: 1` / `right: 0`. |
| `an_extended_visibility_is_held_between_one_second_and_twelve_hours` | FAIL, row 1 | `visibilityMs Some(9000000000000000000): the lease row's deadline is 9000000000000000xxx ms past the settle, not 43200000`. Row 2 alone (`Some(0)`) would fail `… is 0 ms past the settle, not 1000` — the floor decision of §2. Rows 3–4 are green today (pass-through and default controls). |
| `a_pulled_visibility_beyond_the_lease_window_is_held_to_twelve_hours` | PASS | control (`delivery.rs:43-46` already clamps). |

Deliberately excluded inputs: anything in `(i64::MAX − now, i64::MAX]` (e.g. `i64::MAX` itself).
Under `--release` it wraps (a red row, `… is -9223372035… ms past …`), but under the `dev`/`test`
and `quality` profiles it panics *inside the committer task*, which then answers nothing: the
scenario would fail by the 5 s bound or a 500, i.e. not by the assertion that names the defect, and
it would take the shard engine down for the rest of the process. After the fix the band is
unrepresentable at the committer (§4), so the property is proved by type, and the exact 12 h
ceiling (`43_200_001 → 43_200_000`, which over HTTP hides inside the before/after bracket) is
proved by the unit table in 3b.

Not tested end to end: "the poison eventually dead-letters" — the honest wait is 12 h. The lease
row deadline is the direct, bounded oracle for the same property.

### 3b. Unit tests in `src/queue.rs` `mod tests` — the mutation killers

`src/queue.rs` is mutation owner `queue` (`scripts/quality/mutation_owners.py:61`, target
`harness-lib`, filter `queue::`), compiled into `tools/quality-invariants/src/lib.rs:62` by
`#[path]`. Only tests inside this file can kill its mutants; the DST tests above never run there.
These are compile-red (the functions do not exist yet); 3a carries the behavioural red.

Import line becomes:

```rust
    use super::{
        ConsumerConfig, MAX_LEASE_WINDOW_MS, ack_key, configured_visibility_ms, cursor_key,
        decode_state_key, lease_key, retry_delay_ms, state_prefix, visibility_window_ms,
    };
    use proptest::prop_assert_eq;
    use proptest::test_runner::ProptestConfig;
```

```rust
    #[test]
    fn lease_windows_are_held_between_their_floor_and_twelve_hours() {
        assert_eq!(MAX_LEASE_WINDOW_MS, 43_200_000);
        let cfg = ConsumerConfig {
            visibility_timeout_ms: 45_000,
            ..ConsumerConfig::default()
        };
        for (requested, held) in [
            (None, 45_000),
            (Some(0), 1_000),
            (Some(999), 1_000),
            (Some(1_000), 1_000),
            (Some(5_000), 5_000),
            (Some(43_200_000), 43_200_000),
            (Some(43_200_001), 43_200_000),
            // One past u32::MAX: a truncating cast would read this as 0.
            (Some(4_294_967_296), 43_200_000),
            (Some(9_000_000_000_000_000_000), 43_200_000),
            (Some(u64::MAX), 43_200_000),
        ] {
            assert_eq!(visibility_window_ms(requested, &cfg), held, "visibility {requested:?}");
        }
        let unbounded = ConsumerConfig {
            visibility_timeout_ms: 0,
            ..ConsumerConfig::default()
        };
        assert_eq!(visibility_window_ms(None, &unbounded), 1_000, "a configured 0 is floored too");
        for (requested, held) in [
            (None, 1_000),
            (Some(0), 0),
            (Some(1), 1),
            (Some(7), 7),
            (Some(43_200_000), 43_200_000),
            (Some(43_200_001), 43_200_000),
            (Some(4_294_967_296), 43_200_000),
            (Some(u64::MAX), 43_200_000),
        ] {
            assert_eq!(retry_delay_ms(requested), held, "retry {requested:?}");
        }
        for (requested, held) in [
            (0, 1_000),
            (1_000, 1_000),
            (30_000, 30_000),
            (43_200_001, 43_200_000),
            (u32::MAX, 43_200_000),
        ] {
            assert_eq!(configured_visibility_ms(requested), held, "configured {requested}");
        }
    }
```

Mutants cargo-mutants makes here are return-value replacements (`0`, `1`, `u32::MAX`,
`Default::default()`) on the four functions and operator swaps in the const initialiser; each is
killed: every table expects values outside {0, 1, u32::MAX} *and* the retry table expects exactly 0
and 1, and `MAX_LEASE_WINDOW_MS == 43_200_000` pins the product.

Property (policy: `src/queue` is a codec prefix, `verification_plan.py:22`; "at least 1,024 cases
per affected property"). Add **inside the existing** `proptest::proptest! { … }` block (`queue.rs:440`)
so the `macro-dsl` count for `crate::tests::macro(proptest::proptest)` in `docs/quality/owners.json`
stays 1. `quality_` prefix so CI's `cargo test --locked --release --lib quality_` leg
(`rust-quality.yml:41`) runs it. The inner attribute raises the existing roundtrip property to 1 024
cases as well (harmless, ~ms):

```rust
    proptest::proptest! {
        #![proptest_config(ProptestConfig { cases: 1024, .. ProptestConfig::default() })]

        #[test]
        fn quality_lease_windows_follow_the_u64_clamp(
            near in 0_u64..=100_000_000,
            anywhere in proptest::num::u64::ANY,
            configured in proptest::num::u32::ANY,
        ) {
            let cfg = ConsumerConfig { visibility_timeout_ms: configured, ..ConsumerConfig::default() };
            for requested in [near, anywhere] {
                prop_assert_eq!(u64::from(visibility_window_ms(Some(requested), &cfg)), requested.clamp(1_000, 43_200_000));
                prop_assert_eq!(u64::from(retry_delay_ms(Some(requested))), requested.min(43_200_000));
            }
            prop_assert_eq!(u64::from(visibility_window_ms(None, &cfg)), u64::from(configured).clamp(1_000, 43_200_000));
            prop_assert_eq!(configured_visibility_ms(configured), configured.clamp(1_000, 43_200_000));
        }
        // existing quality_queue_state_key_roundtrip unchanged
    }
```

## 4. Code change (option O), per file

No file over 1 000 lines is touched (`product.rs`, `http.rs`, `shard.rs`, `billing.rs`,
`history.rs`, `registry.rs`, `auth.rs`, `fleet.rs`: 0 bytes). No code moves, so no verbatim-move
commit. No new production file, so no `mutation_owners.py` row. `src/queue.rs` is also compiled by
the harness with only std/serde/proptest in reach: the new code references nothing outside the
file (`ConsumerConfig` is local).

### `src/queue.rs` (463 → ≈575 incl. tests; ceiling 1 000; no `#[expect]` added)

Insert after `impl Default for ConsumerConfig` (`:215`):

```rust
/// The lease window. Every span the committer adds to `now_ms()` when it
/// writes a lease row — a pull's or an extend's visibility, a retry's delay,
/// the configured timeout they default to — is bounded here to 12 h. The
/// bound keeps `now + window` total in `shard/transaction/queue` (a `u32` of
/// milliseconds is under 50 days) and keeps every lease expiring: an
/// unbounded wire `u64` wrapped into the past or overflowed the committer,
/// and in between it wrote a lease that never expired, so its record was
/// never redelivered, never reached `maxAttempts`, never dead-lettered, and
/// its routing key stayed blocked.
pub(crate) const MAX_LEASE_WINDOW_MS: u32 = 12 * 3600 * 1000;
/// The floor a pull has always applied to visibility; an extend shares it.
const MIN_VISIBILITY_MS: u32 = 1_000;
/// A retry that names no delay releases its record after one second.
const DEFAULT_RETRY_DELAY_MS: u32 = 1_000;

/// The visibility a pull or an extend asked for, or the consumer's
/// configured timeout when it asked for none: 1 s ..= 12 h.
pub(crate) fn visibility_window_ms(requested: Option<u64>, cfg: &ConsumerConfig) -> u32 {
    let requested = requested.unwrap_or(u64::from(cfg.visibility_timeout_ms));
    bounded_window_ms(requested, MIN_VISIBILITY_MS)
}

/// The delay a retry asked for, or one second when it asked for none:
/// 0 ..= 12 h. Zero releases the record at once.
pub(crate) fn retry_delay_ms(requested: Option<u64>) -> u32 {
    let requested = requested.unwrap_or(u64::from(DEFAULT_RETRY_DELAY_MS));
    bounded_window_ms(requested, 0)
}

/// The timeout a consumer put stores: 1 s ..= 12 h, so the value
/// `visibility_window_ms` defaults to is already inside the window.
pub(crate) fn configured_visibility_ms(requested: u32) -> u32 {
    requested.clamp(MIN_VISIBILITY_MS, MAX_LEASE_WINDOW_MS)
}

/// A wire `u64` into the window: anything past `u32` is past 12 h, so the
/// failed conversion saturates rather than truncates.
fn bounded_window_ms(requested: u64, floor: u32) -> u32 {
    u32::try_from(requested)
        .unwrap_or(u32::MAX)
        .clamp(floor, MAX_LEASE_WINDOW_MS)
}
```

(`unwrap_or` is not `unwrap_used`. Both arms of `try_from` are reachable, so no equivalent
mutant. Rustdoc `-D warnings`: no `<..>`/`[..]` in prose; private items named in plain code.)

`QueueOp` fields (`:273`, `:320-321`):

```rust
        /// From `visibility_window_ms`: 1 s ..= 12 h, so `now + visibility_ms`
        /// is total and the lease always expires.
        visibility_ms: u32,
…
        retries: Vec<(u64, u32, u32)>, // (off, gen, delay_ms from `retry_delay_ms`)
        extends: Vec<(u64, u32, u32)>, // (off, gen, visibility_ms from `visibility_window_ms`)
```

### `src/application/consumer/delivery.rs` (739 → 736)

`pull` (`:14-219`; `#[expect(too_many_lines)]` + `#[expect(excessive_nesting)]`), lines 43-46
(4 lines → 1; 81 columns):

```rust
    let visibility = crate::queue::visibility_window_ms(doc.visibility_ms, &cfg);
```

`:129 visibility_ms: visibility` and `:181 deadline_ms: now + visibility as i64` are unchanged text
(`visibility` is now `u32`; `u32 as i64` is lossless, no enabled cast lint fires).

`settle` (`:409-590`; `#[expect(too_many_lines)]`), line 435:

```rust
    type SegOps = (Vec<(u64, u32)>, Vec<(u64, u32, u32)>, Vec<(u64, u32, u32)>);
```

line 465 (72 columns; the chain stays vertical — it is > `chain_width` 60 either way):

```rust
                .push((o, g, crate::queue::retry_delay_ms(i.delay_ms)));
```

line 473 (73 columns; the tuple stays multi-line — its args are > `fn_call_width` 60 either way):

```rust
                crate::queue::visibility_window_ms(i.visibility_ms, &cfg),
```

`crate::queue` is already a dependency of this file; no `crate::http`/`crate::product`/`axum`
token is introduced, so the `src/application/` hard-owner rule (`source_rules.py:231, 244-247`)
holds. `dlq_and_settle` (`:724-731`) is untouched (`Vec::new()` infers).

### `src/application/consumer.rs` (764 → 764)

`put` (`:446-614`; `#[expect(too_many_lines)]`), line 465 (77 columns):

```rust
        cfg.visibility_timeout_ms = crate::queue::configured_visibility_ms(v);
```

Optional and separable (zero behaviour change, fact-neutral); it is what makes the 12 h literal
exist exactly once. Drop it if the diff should stay inside the settle path.

### Budget for every ceilinged file and ratcheted function

Fact counting per `tools/quality-syntax/src/scan.rs:222-255`: a path expression or type path is 1
fact; a method call is 2 (`method-call` + `method-call-site`); a call with a path callee is 2
(`call-site` + the callee `path`); a field access counts only its base path; literals count 0.

| Function | Exceptions (ratcheted metrics) | scope_lines | syntax_facts | fingerprints |
| --- | --- | --- | --- | --- |
| `delivery::pull` | `too_many_lines`, `excessive_nesting` (scope_lines, nested_items, syntax_facts) | **−3** (`:43-46` → 1 line; body ≈198 → 195, still > 100 so both expectations stay fulfilled — `unfulfilled_lint_expectations` is `deny`) | **−3** (`doc`, `unwrap_or`×2, `cfg`, `u64`, `clamp`×2 = 7 → callee path, call-site, `doc`, `cfg` = 4) | none tracked |
| `delivery::settle` | `too_many_lines` | **0** | **−1** (`:465`: `i`, `unwrap_or`×2 = 3 → call-site, path, `i` = 3; `:473`: `i`, `unwrap_or`×2, `cfg`, `u64` = 5 → call-site, path, `i`, `cfg` = 4; `SegOps` swaps `u64`→`u32` paths 1:1) | none tracked |
| `consumer::put` | `too_many_lines` | **0** | **0** (`cfg`, `v`, `clamp`×2 = 4 → `cfg`, call-site, path, `v` = 4) | none tracked |
| `CommitTransaction::settle` (`settle.rs`) | `too_many_lines`, `expect_used`, `excessive_nesting` | untouched, 0 bytes | 0 | 0 |
| `CommitTransaction::receive` (`receive.rs`) | same three | untouched, 0 bytes | 0 | 0 |
| any file > 1 000 lines | — | none touched | — | — |

File sizes after: `queue.rs` ≈575, `delivery.rs` 736, `consumer.rs` 764, `consumer_dlq.rs` ≈545,
all ≤ 1 000 (`source_rules.py:226-230`: limit is `max(1000, prior)`).

### Untouched on purpose

`src/shard/transaction/queue/*.rs`, `src/shard.rs`, `src/product.rs`, `src/http.rs`,
`src/application/consumer_remote.rs`, `src/dst/dst_tests.rs`, every existing test file.

## 5. Ledgers and docs

| File | Change |
| --- | --- |
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write`, then `--check`. Expect exactly **5 new rows** (`file: src/dst/tests/consumer_dlq.rs`, `scenarios: []`, `mechanisms: []`, attribute `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`) and **no** other `function_sha256` change — one elsewhere means a test producer was edited by mistake. Helpers are not inventoried. Regenerate *after* the concurrent `tests_ran.py` work lands (it raises the suite floor from this file: `tests_ran.py:42-49`). |
| `docs/refactor/WIRE-MATRIX.md:158` (§2.16) | Replace the first paragraph with: `` `product_consumer_settle` (`src/product.rs:6727-6939`). Scope: `consumers.settle`. Body `{acks?, retries?, extends?}` of `{leaseToken, delayMs?, visibilityMs?}` (400 `invalid_body`); clamps, never errors: retry `delayMs` 0–12 h (default 1 s), extend `visibilityMs` 1 s–12 h (default: the consumer's `visibilityTimeoutMs`) — the lease window `:pull` keeps, owned by `src/queue.rs` (`MAX_LEASE_WINDOW_MS`, `retry_delay_ms`, `visibility_window_ms`). Invalid/foreign tokens counted as `stale`, never errors (spec §2.5). `` Leave the stale `product.rs:6727-6939` pointer (the file's pointers predate the split; out of scope). |
| `docs/refactor/WIRE-MATRIX.md:152` (§2.15) | Optional: after "visibility 1 s–12 h" add "(the lease window, `src/queue.rs`)". |
| `docs/quality/owners.json` | **none** with the tests in `consumer_dlq.rs`, the property inside the existing `proptest!` block, bodies via `format!`, no spawn, no glob. If a sibling file `src/dst/tests/consumer_settle_windows.rs` is chosen instead: add `#[path = "tests/consumer_settle_windows.rs"] mod consumer_settle_windows;` to `src/dst/dst_tests.rs` **and** this row: `{"category": "by-path-module", "count": 1, "owner": "crate::consumer_settle_windows", "path": "src/dst/dst_tests.rs", "reason": "Settle lease-window regression module; real HTTP settle requests against the durable lease row; compiled and executed with DST.", "syntax": "path = \"tests/consumer_settle_windows.rs\""}` — otherwise `unregistered source occurrence (1): ('by-path-module', 'src/dst/dst_tests.rs', 'crate::consumer_settle_windows', …)`. Also add the module to the "Consumer delivery and deletion" row of `src/dst/tests/README.md`. |
| `docs/quality/source-allowances.json`, `legacy-source.json` | none (frozen; never regrow). |
| `scripts/quality/mutation_owners.py` | none (`queue` row exists; `delivery.rs`/`consumer.rs` are outside every critical prefix, `verification_plan.py:22-31`). |
| `docs/refactor/review-mechanisms.json` | none (no pinned test or fixture in any touched file). |
| `scripts/clippy-baseline-fingerprints.txt`, `docs/quality/policy.json` | none (no new warning, no new exception). |
| SDK / handover specs | none state a range. Optional follow-up: doc comments on `retry`/`extend` in `sdk/src/index.ts:154-155` ("held to 12 h by the server"); skipped here so the SDK jobs stay out of a server fix. |

Commit: one commit on `slate` (tests + fix + ledgers). A red-only commit on a branch that is
pushed directly would break CI; instead run the red locally first and quote the two red messages
in the body. Suggested subject: `A settle's retry delay and extended visibility are held to the lease window a pull keeps`.

Local order: write 3a → `cargo test --locked --release --lib dst::dst_tests::consumer_dlq::` and
confirm the four red messages above → apply §4 + 3b → `cargo fmt --all -- --check` → the same DST
filter green → `cargo test --locked -p streams-quality-invariants queue::` and
`cargo test --locked --release --lib quality_` → `scripts/test-inventory.py --write` / `--check` →
`scripts/quality.sh` (Python ≥ 3.11: `tomllib`) → CI's own plan
(`scripts/quality/verification_plan.py` against the push's `before` revision: expect
`mutants: true` with owner `queue` only, `properties_fuzz: true`, nothing unregistered) →
`scripts/quality/mutations.sh`. Never claim CI green without `gh run view`.

## 6. What could go wrong

1. **rustfmt reflow vs the ratchet.** The budgets assume the layouts in §4. `pull` has 3 lines of
   slack; `delivery::settle` has none. If rustfmt lays `:465` or `:473` out taller, do not add a
   `let`; shorten instead (`use crate::queue::{retry_delay_ms, visibility_window_ms};` at file top —
   imports sit outside every ratcheted scope and the shorter callee still counts 1 path fact).
2. **Extend floor is a wire behaviour change** (extend 0: "visible now" → 1 s). Everything else is a
   tightening of values no client could have meant. Decision flagged in §2.
3. **Rows already written unbounded.** A lease row with a ~9e18 deadline written by a binary without
   this fix still never expires; this change does not heal it (`receive.rs` would have to bound
   `deadline_ms > now + MAX`, a ratcheted function with three expectations). Remedy is an ack with
   the token or delete/recreate the consumer (the generation fence sweeps the rows). Worth a
   follow-up only if a shared environment was exposed.
4. **Fleet skew.** No wire or persisted format changes: `QueueOp` is in-process, the lease row stays
   `i64` LE, tokens are unchanged. During a rolling deploy a settle answered by an owner still on the
   old binary is applied there unbounded; the bound holds per owner as it upgrades. Rollback is safe
   (new rows are a subset of what old binaries write).
5. **Conformance / contracts.** `conformance/` never touches consumers; the platform e2e and noisy
   campaign scripts send no `delayMs`/`visibilityMs`; the SDK unit test runs against a mock. The
   Durable Streams suite (332/0) does not cover the consumer surface.
6. **Profile split.** DST runs `--release` (wraps); the mutation profile `quality` inherits `dev`
   (overflow panics). The red inputs chosen (`u64::MAX`, 9e18) behave identically in both; after the
   fix no profile can overflow (`i64 + u32`).
7. **Wall clock.** `assert_held` brackets the committer's `SystemTime` between two reads of the same
   clock; a backwards NTP step inside those few ms would fail it. The file already depends on the
   wall clock (`:250`, a 1 200 ms sleep against a 1 000 ms lease).
8. **Harness build.** `src/queue.rs` is also compiled by `tools/quality-invariants` with only
   std/serde/proptest available; the new code must not reach for `crate::shard::now_ms` or anything
   outside the file (it does not). `ProptestConfig` comes from `proptest::test_runner`, already a
   harness dependency.
9. **Inventory floor and concurrent edits.** The new `scripts/quality/tests_ran.py` (untracked in
   the tree today) raises the full-suite floor to the inventory size; the five new DST tests run in
   the main `cargo test --release` leg (no filter excludes `consumer_dlq`). Regenerate the inventory
   after that work lands to avoid a JSON merge conflict.
10. **Future producers.** Under O a new in-crate producer could pass up to `u32::MAX` ms (49.7 days):
    bounded and non-overflowing, but outside the 12 h policy. A second producer is the moment for
    option B.
11. **`put` routing (optional edit).** If `configured_visibility_ms` is dropped, keep `put`'s literal
    exactly as it is: pointing it at the constant instead (`v.clamp(1_000, MAX_LEASE_WINDOW_MS)`) is
    +1 path fact on a ratcheted function.

## Skeptic corrections

Checked read-only against `slate` @ `efe12b2e` (the plan's base `668bc80c` plus `00ff0e7e`
"tests_ran" and `efe12b2e` "idle window", both of which landed while the plan was written).
Verified by reading, not compiling. Verdict: **sound with corrections** — the mechanism, the
producer census, the clamp-not-refuse decision, the option-O fact budgets and the red-test
derivations all hold; three things must change before it is applied, and a few are worth knowing.

### C1 (compile error) — `ProptestConfig` is not in `proptest::test_runner`

§3b and §6.8 say `use proptest::test_runner::ProptestConfig;`. In the pinned proptest `=1.11.0`
the only `ProptestConfig` is the prelude alias: `proptest-1.11.0/src/prelude.rs:25`
`pub use crate::test_runner::Config as ProptestConfig;`; `test_runner/mod.rs:26-32` re-exports
`config::*` (the type is named `Config` there). The plan's import line fails with
`unresolved import proptest::test_runner::ProptestConfig`, in both the service crate and the
harness. Corrected import block for `src/queue.rs` `mod tests` (non-glob, so no
`unresolved-glob` row in `docs/quality/owners.json`; precedent
`src/registry/catalog/tests.rs:10`):

```rust
    use super::{
        ConsumerConfig, MAX_LEASE_WINDOW_MS, ack_key, configured_visibility_ms, cursor_key,
        decode_state_key, lease_key, retry_delay_ms, state_prefix, visibility_window_ms,
    };
    use proptest::prelude::ProptestConfig;
    use proptest::prop_assert_eq;
```

(The alternative with zero new imports is the `src/sketch.rs:300` spelling:
`#![proptest_config(proptest::test_runner::Config { cases: 1024, ..Default::default() })]`.)
The rest of the proptest plan is right: the inner attribute parses as a `macro-attribute` fact
(`tools/quality-syntax/src/scan/macro_attributes.rs:24-33`), which `classify` does not inventory,
and nested `prop_assert_eq!` tokens inside `proptest! { }` are never facts (syn does not parse
macro bodies), so the `crate::tests::macro(proptest::proptest)` count in `owners.json:396-402`
stays 1.

### C2 (gate trap) — `configured_visibility_ms` is not separable from the `put` edit

§4 calls the `consumer.rs:465` change "optional and separable". It is not: if the `put` edit is
dropped, `configured_visibility_ms` has no non-test caller in the service crate. The harness
allows `dead_code` on `mod queue` (`tools/quality-invariants/src/lib.rs:56-63`), the service crate
does not, and `mod tests` is `#[cfg(test)]`, so `cargo clippy --all-targets -- -D warnings` fails
on the lib target with `function configured_visibility_ms is never used`. The two existing
`src/queue.rs` rows in `scripts/clippy-baseline-fingerprints.txt:19,49` are `deleted_rows` and a
stale `parse_token`, not a licence. Either land the `put` edit (recommended; it is fact-neutral,
verified: `cfg`, `v`, `clamp`×2 = 4 → `cfg`, call-site, path, `v` = 4) or drop the function AND
its unit rows AND the last `prop_assert_eq!` of the property together. §6.11 should say this.

### C3 (stale preamble; sequencing) — the concurrent work has landed, and it touched two of this plan's files

§0's "working tree carries another session's uncommitted edits" and §5/§6.9's "regenerate after the
concurrent `tests_ran.py` work lands" are stale: `scripts/quality/tests_ran.py` and
`scripts/test-leg.sh` are committed in `00ff0e7e`, and `efe12b2e` (TTL ceiling) committed edits to
`docs/refactor/WIRE-MATRIX.md` (§1.1, §1.4, §2.2 — same-line replacements, so `:152` and `:158`
still point at §2.15/§2.16) and `docs/refactor/test-inventory.json` (467 → 470 rows, from
`src/dst/tests/lifecycle_creation.rs`). None of the six Rust files this plan edits changed
(`git diff --stat 668bc80c..HEAD`), so every `src/*.rs` line number in the plan is still exact.
Corrections: base the branch on `efe12b2e` or later; expect the inventory `--write` diff to be
exactly **5 added rows on top of 470**, and treat any other row change as the tree carrying
someone else's uncommitted DST edit (a mutation run is using this tree — check `git status`
immediately before `--write`). The `ci.yml`/`gate.sh` line numbers cited in §0 have shifted with
`00ff0e7e`; the facts they cite (`cargo test --release` in CI, `tests_ran --inventory --skipped 1`)
still hold (`ci.yml:89,97`, `gate.sh:23-24`).

### C4 (producer census, informational) — `use crate::queue::*` re-exports the new items into the committer

`src/shard/transaction/queue/mod.rs:2` is `use crate::queue::*;` and `settle.rs:1`/`receive.rs:1`
are `use super::*;`. Every new `pub(crate)` item (`MAX_LEASE_WINDOW_MS`, `visibility_window_ms`,
`retry_delay_ms`, `configured_visibility_ms`) therefore becomes a name inside the two ratcheted
committer functions' modules. No collision today (`grep -rn` for the four names: zero hits in
`src/` and `tools/`), a glob-vs-glob ambiguity only errors on use, and the syntax ratchet counts
facts in the committer *source*, which is unchanged, so no gate fires. Two consequences the plan
should state: keep `bounded_window_ms`, `MIN_VISIBILITY_MS` and `DEFAULT_RETRY_DELAY_MS` private
(the glob would otherwise carry the policy's parts into the committer namespace), and never add a
call to any of the four inside `settle.rs`/`receive.rs` — under `expect_used` every `call-site` and
`path` there is fingerprinted (`source_rules.py:159-188`), so one call is "accepted exception grew"
on three attributes at once (the same reason §O′ is blocked).

### C5 (file identity) — `consumer_dlq.rs:1` module doc no longer describes the file

Appending five lease-window scenarios under `//! Durable DLQ delivery must precede source
settlement, including retry after failure.` leaves a header that names a quarter of the file.
Replace line 1 with (no ledger cost: the inventory hashes function bodies only,
`scripts/test-inventory.py:106-125`):

```rust
//! Dead-letter handoff ordering and the lease window a settle keeps: a durable DLQ
//! append precedes source settlement, and a retry's delay or an extend's visibility
//! never writes a lease that cannot expire.
```

The task's "beside `consumer_dlq.rs`" also reads as a sibling file; the plan's §5 rows for
`src/dst/tests/consumer_settle_windows.rs` are correct if that is preferred (new `by-path-module`
rows go in `docs/quality/owners.json`, never `source-allowances.json`, which is frozen — the plan
has this right; precedent `owners.json:215-225`).

### Verified as stated (so the implementer need not re-derive)

- Signatures and fields: `PullInput.visibility_ms: Option<u64>` (`consumer.rs:274`),
  `SettleItem.{delay_ms,visibility_ms}: Option<u64>` (`:293-299`), `ConfigInput.visibility_timeout_ms:
  Option<u32>` (`:259`), `ConsumerConfig.visibility_timeout_ms: u32` (`queue.rs:182`),
  `QueueOp::Receive.visibility_ms: u64` (`:273`), `Settle.retries/extends: Vec<(u64,u32,u64)>`
  (`:320-321`), `now_ms() -> i64` from `SystemTime` (`shard.rs:1328-1333`; the committer reaches it
  through `transaction/mod.rs:9 use super::*`, so the DST bracket and the lease row read the same
  clock), `ShardEngine.db: Arc<Db>` (`shard.rs:1127`), `AppState::engine_for(&[u8;16]) ->
  Result<Arc<ShardEngine>, Response>` (`http.rs:556`, `.unwrap()` compiles as in r07),
  `http_rig -> (Arc<AppState>, SocketAddr)` (`fixture_http.rs:119-123`; `&state` deref-coerces to
  `&AppState`), `preq` (`fixture_requests.rs:196-202`), `LeaseToken::decode` does not check
  `deadline_ms` (`product_cursor/decode.rs:195-210`), so one token settles every table row.
- Producer census is complete: the only `QueueOp::{Receive,Settle}` constructions are
  `delivery.rs:125,492,724`, `queue_codec_tests.rs:41`, `consumer_atomicity.rs:453,579,610,745`,
  `consumer_saga.rs:499`; every test value is an integer literal or `Vec::new()`, so `u32`
  narrowing touches no test and no `function_sha256`. `consumer_remote.rs` relays only
  `queue-cursor` and `sweep-segment`. `QueueOp` derives nothing and is never serialized; the lease
  row (`encode_lease`, 8+4+4+16 bytes) and the lease token are unchanged, so no fleet-skew or
  persisted-format break.
- No other `deadline_ms` producer exists (`grep -rn deadline_ms src/`): `receive.rs:103`,
  `settle.rs:89,106`, `delivery.rs:181` — all covered by the `u32` fields.
- Fact budgets recomputed from `scan.rs:222-255` (`visit_path` fires for every `syn::Path`,
  including local idents and cast types; a call is call-site + callee path; a method call is 2):
  `pull` 7 → 4 (−3, scope_lines −3, both expectations still fulfilled at 195 body lines);
  `settle` `:465` 3 → 3, `:473` 5 → 4, `SegOps` 1:1; `put` 4 → 4. `u32 as i64` fires no enabled
  lint (`cast_lossless`/`cast_possible_wrap` are pedantic; `Cargo.toml:151-152` enable only
  truncation/sign-loss).
- Line ceilings: `product.rs` 4484, `http.rs` 3373, `shard.rs` 3232 untouched; `queue.rs` 463,
  `delivery.rs` 739, `consumer.rs` 764, `consumer_dlq.rs` 383 all end ≤ 1 000
  (`source_rules.py:226-230`, `architecture-gate.py:122-124`). `architecture-gate`'s 200-line
  function budget: `pull` is 198 today and shrinks to 195 — do not let rustfmt add lines there.
- Hard-owner rule for `src/application/`: no `AppState`/`Response`/`HeaderMap`/`axum` token is
  introduced (`architecture-gate.py:101-108`); `crate::queue::` is not a transport edge.
- Red derivations: `u64::MAX as i64 == -1` → deadline `now − 1` → `receive.rs:65-70,87` treats it
  expired and re-leases (count 2) — tests 1 and 3 fail at the `len == 0` assertion with
  `left: 1 / right: 0`; `9e18 + 1.76e12 < i64::MAX` in every profile → tests 2 and 4 fail at row 1 in
  `assert_held`; test 5 passes today. After option O every row is green by the derivations in §3a.
  Nothing in `(i64::MAX − now, i64::MAX]` is sent, so the dev/quality profiles cannot panic the
  committer.
- Ledgers: `test-inventory` picks up exactly the five `#[tokio::test]` fns (helpers lack the
  attribute, `test-inventory.py:106-109`); test names are unique across `src/dst`; no
  `review-mechanisms.json` pin references `queue.rs`, `delivery.rs`, `consumer.rs` or
  `consumer_dlq.rs`; no `macro-dsl` is added (bodies via `format!`, `serde_json::json!` avoided —
  the existing r07 row is `legacy-source.json`/`source-allowances.json:2720-2725`);
  `verification_plan.py` will select `mutants` with owner `queue` only (`delivery.rs`,
  `consumer.rs`, `src/dst/` are outside every critical prefix and unregistered — which is allowed
  there), `properties_fuzz: true` runs the postings corpus replay only (`nightly.sh:13-14`).
- Wire docs: `docs/WIRE-MATRIX.md` does not exist; `docs/refactor/WIRE-MATRIX.md:158` is the
  settle row and `:152` the pull row; no other doc, SDK type or conformance suite states a range
  for `delayMs`/`visibilityMs`.

### Optional (not gate failures)

- RUST-QUALITY.md:25 "decoder and admission owners MUST additionally enable `indexing_slicing`
  and `arithmetic_side_effects`": `bounded_window_ms` is the wire-admission owner for the window
  and `decode_state_key` (`queue.rs:98`) already carries `#[warn(clippy::indexing_slicing,
  clippy::arithmetic_side_effects)]`. Adding the same `#[warn]` to the four new functions costs
  nothing (they contain no arithmetic or indexing; `warn(` is not an inventoried exception,
  `source_rules.py:52-60`).
- `owners.json:401` reason for `src/queue.rs`'s `proptest!` row reads "Queue key properties";
  reason text is not part of the row identity (`from_entries`, `source_rules.py:82-89`), so it
  cannot fail, but say "Queue key and lease-window properties" for honesty.
- `visibility_window_ms(Option<u64>, &ConsumerConfig)` could take `configured: u32` instead; same
  fact count at both call sites (`cfg` is one path either way), one fewer type in the signature.
  Taste, not a correction.
- Test 4 row 2's stand-alone message is "is *N* ms past the settle, not 1000" where N is the
  settle's latency in ms (typically 0–3), not exactly 0; row 1 fails first anyway.
