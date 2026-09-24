# Stream-TTL -> expires_at_ms overflow: implementation plan

Tree read at `slate` @ 668bc80c (read-only; nothing in the repo was edited, no cargo/git-write run).
Every line number below was read first-hand at that revision.

## 0. Verdict on the review claim

CONFIRMED, with four corrections the implementer must know.

1. The arithmetic is unchecked exactly where the review says: `src/application/creation.rs:209-211`.
2. `u64::MAX` does NOT panic in debug. `u64::MAX as i64 == -1`, `-1 * 1000 == -1000`, `now_ms() + -1000`
   is a legal i64. It is a silent past expiry in EVERY profile. The debug/test panic is only for values
   whose multiply or add overflows, e.g. `9223372036854776` (`* 1000` = 9_223_372_036_854_776_000 >
   i64::MAX by 193; release wraps to -9_223_372_036_854_775_616).
3. The overflow threshold is lower than the two quoted values suggest: the ADD also overflows, so every
   ttl > `(i64::MAX - now_ms)/1000` ~= 9_223_370_246_854_775 s wraps (e.g. `9223372036854775` survives
   the multiply and dies on the add).
4. There is a third entry to `fresh_desc` that no parser guards: fork TTL inheritance
   (`src/application/creation/fork.rs:144-146`, `ttl_secs = src.ttl_secs`) copies a PERSISTED u64.
   This is why the owner arithmetic must be total, not merely hidden behind the parsers.

Also: the wire matrix lives at `docs/refactor/WIRE-MATRIX.md` (there is no `docs/WIRE-MATRIX.md`), and
the commit gate runs the suite in `--release` (`scripts/gate.sh:16`), i.e. WITH wrapping arithmetic, so a
red test must not depend on the debug overflow panic.

## 1. Mechanism (file:line evidence)

| Step | Evidence |
|---|---|
| Raw parser admits any u64 | `src/http.rs:2453-2459` `parse_ttl_strict`: grammar check then `s.parse().ok()`; `"18446744073709551615"` -> `Some(u64::MAX)`. 400 arm already exists at `src/http.rs:2546-2552` (`invalid_ttl`). |
| Product parser admits any u64 | `src/product.rs:217-228` `parse_idle_secs`: `v.checked_mul(mult)?` bounds only the u64 multiply; `"18446744073709551615"` -> `Some(u64::MAX)`, `"106751991167301d"` -> `Some(9_223_372_036_854_806_400)`. 400 arm exists at `src/product.rs:291-301` (`invalid_config`). |
| Unchecked conversion | `src/application/creation.rs:209-211`: `ttl_secs.map(\|t\| now_ms() + (t as i64) * 1000).or(expires_at_ms)`. Three hazards: sign-wrapping `as i64`, `* 1000`, `+`. |
| Release wraps, debug panics | `Cargo.toml:72-73` `[profile.release]` sets only `lto` -> no overflow checks. `[profile.quality]` inherits dev (`Cargo.toml:77-81`) -> panics. No catch-panic layer exists in `src/` (grep: only test uses of `catch_unwind`), so in debug the request task dies and the socket closes with no response. |
| Callers of `fresh_desc` | raw create: `claim.rs:58-65` and `claim.rs:93-100`; product create: `creation/product.rs:88-95` (after the quota reservation at `:60-63`). |
| Dead on arrival | `desc_alive` (`creation.rs:141-145`) is false the moment the descriptor is written; same verdict in `application/lifecycle.rs:15-21`, `application/watch.rs:155-160`, `registry/catalog.rs:159-162`, `application/append.rs:42`, billing tombstone walk `billing.rs:2100`. A bodyless raw PUT and a product PUT still answer 201 (`claim.rs:108-109` -> `raw.rs:106-111`; `creation/product.rs:137-138`). |
| Fork inheritance | `fork.rs:144-146` inherits the source's persisted `ttl_secs`. With a wrapped expiry the child is dead at `anchor.rs:57-72` -> 409 `fork_target_changed` "retry" forever. |
| Renewal | `creation/ttl.rs:53` `(ttl as i64).saturating_mul(1000)`: no panic, but the same sign-wrapping cast (ttl > i64::MAX gives a NEGATIVE window; `:57` then always reads "fresh"). Not a live bug (such streams are already dead) but a second, different spelling of the same conversion. |
| Not an amplifier | billing close uses `expires_at_ms` as `close_ms` (`billing.rs:973`, `:2148`) but `shard/transaction/maintenance.rs:43-47` replaces `close_ms <= 0` with now. `http/read.rs:454` (`expiry - now_ms()`) is only reached for live descriptors. |

Stated limits found in the repo:
- DS conformance (pinned `@durable-streams/server-conformance-tests@0.3.6`, `conformance/node_modules/.../dist/src-wMgS3XWd.js`): every `Stream-TTL` is a literal; the largest is `99999` (`:7522`). 400 is expected only for grammar (`abc`, `-1`, `00060`, `+60`, `60.5`, `1e3`; `:763-783`, `:1491-1531`). HEAD must echo `3600` for a fresh 3600 s stream (`:1611-1622`). No test sends a large TTL, no property generator touches TTL. The protocol states no maximum.
- Product spec `handover/.../07-TYPED-CREATION-DOCUMENT.md:348`: "duration is positive and within service maximum" -- a ceiling is REQUIRED on the product surface; no number is given (`:354-360` lists only watch/config bounds).
- `docs/refactor/WIRE-MATRIX.md:30,32,91`: TTL/idle documented with no bound. No other doc states one. Largest TTL used by any DST test: `2592000` (`product_lifecycle.rs:310`). SDK (`sdk/src/index.ts:58,611`) passes `idle` through as a string, no client validation.

## 2. Design

### A. Smallest correct change (total arithmetic only)
One pure fn in `creation/ttl.rs`; `fresh_desc` and `touch_ttl` call it; parsers untouched; no wire change.
Stops the wrap, but admits `u64::MAX` as "never expires": descriptors persist `ttl_secs = 18446744073709551615`,
product metadata renders `"idle":"18446744073709551615s"` (`product.rs:1513`), HEAD reports a remaining
TTL no JS client can represent, and product spec section 13 stays violated.

### B. Owner-first (the review's direction) -- RECOMMENDED
`creation/ttl.rs` owns three things: the ceiling (`MAX_TTL_SECS`), the admission rule (`admit_ttl`), and the
total conversion (`window_ms` / `expiry_after` / `expiry_from_now`). Both parsers route their accepted value
through `admit_ttl` and fall into their EXISTING 400 arms; `fresh_desc` and `touch_ttl` call the conversion.

Refuse vs saturate, per surface:

| Surface | Decision | Why |
|---|---|---|
| raw `Stream-TTL` (PUT) | REFUSE, existing 400 `invalid_ttl` | Clamping would make `MAX+1` and `MAX+2` compare as the same config in `claim.rs:183` (`d.ttl_secs != ttl_secs`), defeating the conformance-tested "different TTL -> 409"; storing the raw number keeps an unrepresentable policy. Repo convention is loud refusal (`http.rs:2563-2581`). Protocol + suite are silent on a maximum, so 400 is conformant. |
| product `expiry.idle` | REFUSE, existing 400 `invalid_config` | Spec section 13 mandates "within service maximum". |
| fork inheritance (`fork.rs:145`) | SATURATE (owner is total) | Value comes from a persisted descriptor, possibly written by an older binary; the client cannot fix it, so refusing would make a live source unforkable. |
| renewal `touch_ttl` | SATURATE (already does; route through the owner) | Persisted value; no error channel that means anything. Removes the sign-wrapping cast. |
| `fresh_desc` | total, never refuses | Behind the parsers; has no error channel; must be safe for the inheritance path. |
| `Stream-Expires-At` / `expiry.at` | unchanged | chrono-bounded absolute instant, no arithmetic. |

Ceiling: `MAX_TTL_SECS = u32::MAX as u64` = 4_294_967_295 s (~136.1 years). Reasons:
- "Never expires" already has a canonical spelling (omit the TTL), so a number beyond any operational
  horizon carries no policy; refusing it steers clients to the canonical form.
- One machine-natural rule to document ("fits in 32 bits"), and it admits the sentinels clients really send
  for "forever" (`2147483647`, `4294967295`). Everything refused was either already broken (>= ~9.22e15)
  or astronomically meaningless.
- Keeps every derived number exact: `MAX*1000 + now` ~= 4.3e12 ms < 2^53 (exact in the f64 at
  `http/read.rs:454` and in any JS client), inside chrono's range (`product.rs:1515` never renders ""),
  six orders of magnitude from the i64 edge.
- Above everything in use: conformance 99_999, DST 2_592_000.
Alternative if Soren prefers a human-round number: `100 * 365 * 86_400` (3_153_600_000). It is one const,
three test literals and two doc rows to change. This is a wire-contract number: Soren's call.

### C. Proof-bearing `TtlSecs` newtype -- REJECTED
Would touch `CreateCommand`, `ProductCreateConfig`, `Preparation`, `PreparedFork`, `CreatePlan`, the
ratcheted `create_request_hash` and `fresh_desc` signatures, and fork inheritance would still need an
`inherited(u64)` escape hatch for legacy descriptors -- so the type could not prove "<= ceiling", only
"total arithmetic applies", which the total fn already gives for every u64. Pure ceremony.

## 3. Red tests

Red is proven locally against UNMODIFIED production code (tests T1, T2, T4a, T4c compile today), the
failure text is quoted in the commit message, and tests + fix land in ONE commit so every commit on slate
passes the gate. No verbatim-move commit is needed: nothing moves.

Rules honoured: no glob import, no `json!`/`proptest!` (would need `docs/quality/owners.json` rows), no new
`#[expect]`, every test < 100 lines and nesting <= 4, no polling loops (nothing can hang; `hreq`/`preq`
are single-shot `connection: close` requests).

### T1 -- `src/http/tests.rs` (insert before the trailing `use super::*;` at line 39)
```rust
/// Stream-TTL admits the canonical decimal grammar up to the service
/// ceiling (2^32 - 1 seconds) and nothing past it. Anything larger used
/// to be admitted and, from ~9.22e15 seconds up, wrapped the stream's
/// expiry into the past.
#[test]
fn stream_ttl_refuses_windows_past_the_ceiling() {
    // The grammar, unchanged (these also kill every mutant of the fn).
    assert_eq!(parse_ttl_strict("0"), Some(0));
    assert_eq!(parse_ttl_strict("3600"), Some(3600));
    assert_eq!(parse_ttl_strict(""), None);
    assert_eq!(parse_ttl_strict("00060"), None);
    assert_eq!(parse_ttl_strict("+60"), None);
    assert_eq!(parse_ttl_strict("60.5"), None);
    assert_eq!(parse_ttl_strict("1e3"), None);
    // The ceiling is the last admitted window.
    assert_eq!(parse_ttl_strict("4294967295"), Some(4_294_967_295));
    assert_eq!(parse_ttl_strict("4294967296"), None);
    // The review's two values, and one past u64 (always a parse error).
    assert_eq!(parse_ttl_strict("9223372036854776"), None);
    assert_eq!(parse_ttl_strict("18446744073709551615"), None);
    assert_eq!(parse_ttl_strict("18446744073709551616"), None);
}
```
Expected failure on current code (both profiles):
```
assertion `left == right` failed
  left: Some(4294967296)
 right: None
```
This test is also the mutation kill set for the `http` owner (filter `http::`): fn-level mutants
`None`/`Some(0)`/`Some(1)` die on `"3600"`; operator mutants on `http.rs:2455` die on `""`, `"0"`, `"00060"`.

### T2 -- `src/product/tests.rs` (new test after `idle_durations`, which is left untouched)
```rust
/// Stage 7 section 13: an idle duration is positive and within the service
/// maximum (2^32 - 1 seconds), whatever unit spells it.
#[test]
fn idle_durations_stop_at_the_service_maximum() {
    assert_eq!(parse_idle_secs("4294967295"), Some(4_294_967_295));
    assert_eq!(parse_idle_secs("4294967295s"), Some(4_294_967_295));
    assert_eq!(parse_idle_secs("49710d"), Some(49_710 * 86_400));
    assert_eq!(parse_idle_secs("4294967296"), None);
    assert_eq!(parse_idle_secs("49711d"), None);
    assert_eq!(parse_idle_secs("18446744073709551615"), None);
    // Fits u64 after the unit multiply, but not an i64 of milliseconds.
    assert_eq!(parse_idle_secs("106751991167301d"), None);
    // Past u64 in the unit multiply: always refused, still refused.
    assert_eq!(parse_idle_secs("18446744073709551615d"), None);
    // Zero stays refused in every spelling.
    assert_eq!(parse_idle_secs("0"), None);
    assert_eq!(parse_idle_secs("0s"), None);
}
```
Expected failure on current code: `left: Some(4294967296)` / `right: None` (the 4th assertion).

### T3 -- owner pins, inline in `src/application/creation/ttl.rs` (new fns: cannot be red, land with the fix)
```rust
#[cfg(test)]
mod tests {
    use super::{MAX_TTL_SECS, admit_ttl, expiry_after, expiry_from_now, window_ms};

    #[test]
    fn the_ceiling_is_the_documented_number_and_is_inclusive() {
        assert_eq!(MAX_TTL_SECS, 4_294_967_295);
        assert_eq!(admit_ttl(0), Some(0));
        assert_eq!(admit_ttl(MAX_TTL_SECS), Some(MAX_TTL_SECS));
        assert_eq!(admit_ttl(MAX_TTL_SECS + 1), None);
        assert_eq!(admit_ttl(u64::MAX), None);
    }

    #[test]
    fn an_expiry_is_total_over_every_persisted_window() {
        let now = 1_790_000_000_000_i64;
        assert_eq!(expiry_after(now, 0), now);
        assert_eq!(expiry_after(now, 3600), now + 3_600_000);
        assert_eq!(expiry_after(now, MAX_TTL_SECS), now + 4_294_967_295_000);
        // The multiply saturates (was: debug panic, release wrap to the past).
        assert_eq!(window_ms(9_223_372_036_854_776), i64::MAX);
        assert_eq!(expiry_after(now, 9_223_372_036_854_776), i64::MAX);
        // The add saturates on its own: the product fits, the sum does not.
        assert_eq!(window_ms(9_223_372_036_854_775), 9_223_372_036_854_775_000);
        assert_eq!(expiry_after(now, 9_223_372_036_854_775), i64::MAX);
        // The cast no longer sign-wraps (was: -1000 ms, a past expiry in every profile).
        assert_eq!(window_ms(u64::MAX), i64::MAX);
        assert_eq!(expiry_after(now, u64::MAX), i64::MAX);
    }

    #[test]
    fn a_fresh_expiry_opens_its_window_at_the_clock() {
        let before = crate::shard::now_ms();
        let at = expiry_from_now(60);
        let after = crate::shard::now_ms();
        assert!((before + 60_000..=after + 60_000).contains(&at), "{before} {at} {after}");
    }
}
```

### T4 -- DST, `src/dst/tests/lifecycle_creation.rs` (append; existing imports suffice: `engine_shutdown, http_rig, PRISMA_KEY, hreq, preq, mem`)
`u64::MAX` goes FIRST on purpose: on current code it is a clean 201 in both profiles, whereas
`9223372036854776` panics the handler in debug (no response -> `hreq` dies at `expect("header terminator")`,
not an assertion) and answers 201 in release.
```rust
/// An idle window past the service ceiling is refused by BOTH create
/// surfaces through their existing 400 arms, and nothing is written. It
/// used to be admitted: `u64::MAX` seconds became `now - 1000` ms, so the
/// create answered 201 for a stream that was already expired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_window_past_the_ceiling_is_refused_and_creates_nothing() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let over = ["18446744073709551615", "9223372036854776", "4294967296"];
    for (i, huge) in over.iter().enumerate() {
        let raw = format!("ttl-over-raw-{i}");
        let hdrs = [("content-type", "application/json"), ("stream-ttl", *huge)];
        let (st, _, body) = hreq(addr, "PUT", &format!("/v1/stream/{raw}"), &hdrs, b"").await;
        assert_eq!(st, 400, "raw Stream-TTL {huge}: {}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"]["code"], "invalid_ttl");

        let product = format!("ttl-over-product-{i}");
        let doc = format!(r#"{{"format":{{"kind":"json"}},"expiry":{{"idle":"{huge}"}}}}"#);
        let path = format!("/v1/streams/{product}");
        let (st, _, body) = preq(addr, "PUT", &path, &pk, doc.as_bytes()).await;
        assert_eq!(st, 400, "product expiry.idle {huge}: {}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"]["code"], "invalid_config");

        for name in [&raw, &product] {
            let sref = state.deployment.raw_adapter_sref(name);
            assert!(state.registry.get(&sref).await.unwrap().is_none(), "{name} was written");
        }
    }
    engine_shutdown(&state).await;
}
```
Expected failure on current code (debug AND release):
`assertion 'left == right' failed: raw Stream-TTL 18446744073709551615:` / `left: 201` / `right: 400`.

```rust
/// The ceiling itself is a legal window on both surfaces, and HEAD reports
/// it back exactly. (A pin, green before and after: it guards the boundary
/// against an off-by-one in either parser.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_longest_idle_window_is_admitted_and_reported() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let hdrs = [("content-type", "application/json"), ("stream-ttl", "4294967295")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/ttl-max", &hdrs, b"").await;
    assert_eq!(st, 201);
    let (st, h, _) = hreq(addr, "HEAD", "/v1/stream/ttl-max", &[], b"").await;
    assert_eq!(st, 200);
    let remaining: u64 = h["stream-ttl"].parse().unwrap();
    assert!((4_294_967_290..=4_294_967_295).contains(&remaining), "{remaining}");
    let d = state.registry.get(&state.deployment.raw_adapter_sref("ttl-max")).await.unwrap().unwrap();
    let window = d.expires_at_ms.unwrap() - d.created_ms;
    assert!((4_294_967_295_000..4_294_967_296_000).contains(&window), "{window}");

    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let doc = br#"{"format":{"kind":"json"},"expiry":{"idle":"49710d"}}"#;
    let (st, _, body) = preq(addr, "PUT", "/v1/streams/ttl-max-product", &pk, doc).await;
    assert_eq!(st, 201);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["expiry"]["idle"], "4294944000s");
    engine_shutdown(&state).await;
}
```

```rust
/// A descriptor admitted before the ceiling existed can carry any u64, and
/// a fork inherits it without passing a parser. Its expiry must saturate
/// to "never"; it used to wrap into the past, so the child was born dead
/// and the fork answered "retry" forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_inheriting_a_legacy_window_is_born_alive() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let src = [("content-type", "application/json"), ("stream-ttl", "3600")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/ttl-legacy", &src, br#"[{"n":1}]"#).await;
    assert_eq!(st, 201);
    let sref = state.deployment.raw_adapter_sref("ttl-legacy");
    state
        .registry
        .cas_update(&sref, |d| {
            d.ttl_secs = Some(u64::MAX);
            d.expires_at_ms = Some(i64::MAX);
            true
        })
        .await
        .unwrap();
    state.registry.invalidate(&sref);

    let fork = [("content-type", "application/json"), ("stream-forked-from", "ttl-legacy")];
    let (st, _, body) = hreq(addr, "PUT", "/v1/stream/ttl-legacy-child", &fork, b"").await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&body));
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("ttl-legacy-child"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.ttl_secs, Some(u64::MAX), "the window is inherited verbatim");
    assert_eq!(child.expires_at_ms, Some(i64::MAX), "and its expiry saturates");
    let (st, _, _) = hreq(addr, "HEAD", "/v1/stream/ttl-legacy-child", &[], b"").await;
    assert_eq!(st, 200);
    engine_shutdown(&state).await;
}
```
Expected failure on current code (both profiles): `left: 409` / `right: 201`, body
`{"error":{"code":"fork_target_changed",...}}` (predicted from `anchor.rs:57-72`; `-1 * 1000` does not
overflow so debug does not panic here). This is the ONLY test that proves `fresh_desc` calls the total
owner once the parsers mask it -- keep it. Implementer: record the observed status if it differs; any
non-201 is the same red.

## 4. Code change, per file

### `src/application/creation/ttl.rs` (82 lines, no ceiling; -> ~170)
Insert after the `use` block (line 7), before `pub(crate) struct TtlMutation`:
```rust
/// The longest idle window either create surface admits: 2^32 - 1 seconds,
/// about 136 years. "Never expires" is spelled by omitting the TTL, so a
/// larger number carries no policy a stream could act on. Under this bound
/// every derived instant is exact in an f64 and a JavaScript number, inside
/// chrono's calendar, and six orders of magnitude clear of the i64 edge.
const MAX_TTL_SECS: u64 = u32::MAX as u64;

/// The one admission rule `Stream-TTL` and `expiry.idle` share. A window past
/// the ceiling is refused, never clamped: two different requests must not
/// compare as the same configuration.
pub(crate) fn admit_ttl(ttl_secs: u64) -> Option<u64> {
    (ttl_secs <= MAX_TTL_SECS).then_some(ttl_secs)
}

/// An idle window in milliseconds, total over every `u64`. Descriptors written
/// before the ceiling existed, and the forks that inherit them, carry windows no
/// parser vouched for; they saturate to "never" instead of wrapping into the past.
#[warn(clippy::arithmetic_side_effects)]
fn window_ms(ttl_secs: u64) -> i64 {
    i64::try_from(ttl_secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1000)
}

/// The instant a window opened at `now_ms` closes.
#[warn(clippy::arithmetic_side_effects)]
fn expiry_after(now_ms: i64, ttl_secs: u64) -> i64 {
    now_ms.saturating_add(window_ms(ttl_secs))
}

/// A fresh descriptor's expiry: its window opens when creation reads the clock.
pub(super) fn expiry_from_now(ttl_secs: u64) -> i64 {
    expiry_after(crate::shard::now_ms(), ttl_secs)
}
```
`#[warn(clippy::arithmetic_side_effects)]` under `-D warnings` is the compile-time proof that the owner can
never regress to an operator (item-scoped precedent: `src/queue.rs:98`; `warn(` is not an inventoried
exception -- `source_rules.py:58-60` matches only `allow|expect`).

`touch_ttl` (no `#[expect]`, not ratcheted), two in-place replacements:
```rust
-        let window = (ttl as i64).saturating_mul(1000);
+        let window = window_ms(ttl);
...
-            target: now.saturating_add(window),
+            target: expiry_after(now, ttl),
```
Behaviour is identical for every ttl <= i64::MAX; for larger (dead-on-arrival today, so unreachable) the
window becomes i64::MAX instead of negative. `window - window / 4` at `:57` stays: window >= 0 now, so it
cannot overflow. Append the T3 `mod tests` at the end of the file.

### `src/application/creation.rs` (318 -> 316; no ceiling, but `fresh_desc` is RATCHETED)
```rust
-        expires_at_ms: ttl_secs
-            .map(|t| now_ms() + (t as i64) * 1000)
-            .or(expires_at_ms),
+        expires_at_ms: ttl_secs.map(ttl::expiry_from_now).or(expires_at_ms),
```
and line 258: `pub(crate) use ttl::TtlMutation;` -> `pub(crate) use ttl::{TtlMutation, admit_ttl};`

Ratchet budget for `fresh_desc` (`#[expect(clippy::too_many_arguments)]`, `creation.rs:188-229`; metrics
per `scripts/quality/source_rules.py:151-158` are `scope_lines`, `nested_items`, `syntax_facts` only -- no
call fingerprints, those apply to unwrap/expect expectations):
- `scope_lines` 42 -> 40 (38 -> 36 if the parsed item span excludes the attribute; -2 either way). The new chain is 52 chars (<= rustfmt `chain_width` 60) and the line is 76 wide,
  so rustfmt WILL put it on one line (no rustfmt.toml in the repo; defaults apply).
- `syntax_facts` in the touched expression 10 -> 7. Before: paths `ttl_secs`, `now_ms`, `t`, `i64`,
  `expires_at_ms` (5) + call-site `now_ms()` (1) + `map`/`or` as method-call and method-call-site (4).
  After: paths `ttl_secs`, `ttl::expiry_from_now`, `expires_at_ms` (3) + `map`/`or` (4).
- `nested_items` unchanged. Reason text untouched.

TRAP, do not "improve" this: the natural spelling `.map(|t| ttl::expiry_after(now_ms(), t))` is 11 facts
(+1: adds a path AND a call-site, removes only the `i64` path) and fails with
`accepted exception grew without a new decision: ... syntax_facts N -> N+1`. Hoisting `let now = now_ms();`
is also +1 fact and +1 line. The point-free form is the one that shrinks; that is why `expiry_from_now`
(the clock-binding adapter over the pure `expiry_after`) exists.

The pinned test `r05_cancelled_ttl_attempt_releases_only_its_owned_slot` (`creation.rs:270`, sha-pinned in
`docs/refactor/review-mechanisms.json` `source_adaptations[6]`) is not touched; do not reformat or move it.

### `src/http.rs` (3373 -> 3373, budget 0; `parse_ttl_strict` has no `#[expect]`)
```rust
-/// Strict TTL grammar: canonical non-negative decimal only.
+/// Strict TTL grammar: canonical non-negative decimal, at most the `admit_ttl` ceiling.
 fn parse_ttl_strict(s: &str) -> Option<u64> {
     ...
-    s.parse().ok()
+    crate::application::creation::admit_ttl(s.parse().ok()?)
 }
```
The new line is exactly 60 columns and is a CALL, not a chain. Do not write
`s.parse().ok().and_then(crate::application::creation::admit_ttl)`: that chain is 64 chars > `chain_width`
60, rustfmt breaks it over three lines, and the file grows by two. `u64` is inferred from `admit_ttl`'s
parameter. DO NOT touch `create_stream` (`http.rs:2500-2504`, `#[expect(too_many_lines, unwrap_used)]`):
under `unwrap_used` every call expression is fingerprinted with its literal tokens
(`source_rules.py:176-188`), so even rewording `"invalid Stream-TTL"` at `:2549` reads as "accepted
exception grew". The existing arm already produces the right 400; the ceiling is documented in the wire
matrix instead of the message.

### `src/product.rs` (4484 -> 4484, budget 0; `parse_idle_secs` has no `#[expect]`)
```rust
 /// Parse a duration like "30d" / "12h" / "45m" / "30s" / plain seconds
 /// into whole seconds (Stage 7 §7: equivalent spellings normalize to
-/// the same integer).
+/// the same integer); zero and windows past `admit_ttl`'s ceiling refuse.
 fn parse_idle_secs(s: &str) -> Option<u64> {
     ...
-    let v: u64 = num.parse().ok()?;
-    (v > 0).then_some(v.checked_mul(mult)?)
+    let v: u64 = num.parse().ok().filter(|v| *v > 0)?;
+    crate::application::creation::admit_ttl(v.checked_mul(mult)?)
 }
```
Two lines for two lines (chain is 37 chars; call line is 65 wide). DO NOT touch `parse_create_doc`
(`product.rs:232-241`, `#[allow]` + `#[expect(too_many_lines, excessive_nesting)]`, scope/fact ratcheted);
its arm at `:291-301` already answers 400 `invalid_config`.

After `cargo fmt`, assert `wc -l src/http.rs src/product.rs` still prints 3373 and 4484 BEFORE running the gate.

### Tests
The T4 listings are written for reading, not formatted: several chains exceed `chain_width`; run `cargo fmt`
(these files have no line ceiling). `src/http/tests.rs` 39 -> ~62 (T1); `src/product/tests.rs` 316 -> ~335 (T2);
`src/dst/tests/lifecycle_creation.rs` 407 -> ~505 (T4). All far under 1,000.

### Lints checked against the new code
No `unwrap`/`expect`/`panic` outside `#[cfg(test)]` (`unwrap_or` is not `unwrap_used`); no casts that trip
`cast_possible_truncation`/`cast_sign_loss` (`u32::MAX as u64` is lossless widening; the old `t as i64`
is gone); no env read, no spawn; `admit_ttl` returns `None` on a real path (no `unnecessary_wraps`); the
`pub(crate) use` of `admit_ttl` is used by two non-test callers (no unused-import warning -- this is why
`MAX_TTL_SECS` stays private and the transport tests pin the literal `4294967295` instead of importing it);
`src/application/**` gains no `crate::http`/`crate::product`/`axum` path (`source_rules.py:244-247`).

## 5. Ledger / doc rows

| File | Change |
|---|---|
| `docs/refactor/test-inventory.json` | Regenerate: `python3 scripts/test-inventory.py --write`, then `--check`. Adds exactly three entries (the T4 names), all in `src/dst/tests/lifecycle_creation.rs`; no existing hash may change. |
| `docs/refactor/WIRE-MATRIX.md:30` | `Stream-TTL` -> "`Stream-TTL` (canonical decimal seconds, 0..=4294967295)". |
| `docs/refactor/WIRE-MATRIX.md:32` | `invalid_ttl` -> "`invalid_ttl` (grammar, or a window past 4294967295 s)". |
| `docs/refactor/WIRE-MATRIX.md:91` | after `expiry?:{idle}\|{at}` add: "`expiry.idle` is a positive duration of at most 4294967295 s (spec §13 service maximum); zero or larger -> 400 `invalid_config`". |
| `scripts/quality/mutation_owners.py` | NO new row. No new file; `creation.rs`, `creation/ttl.rs`, `product.rs` are neither registered nor under `CRITICAL_PREFIXES` (`verification_plan.py:22-31`). `src/http.rs` IS owner `http` (filter `http::`) -> CI runs mutants on the `parse_ttl_strict` diff; T1 is its kill set. `src/http/tests.rs` is under the `src/http` prefix but has `#![cfg(test)]`, so it classifies as `production_unchanged` (`production_changes.py:66-67`). |
| `docs/quality/owners.json` | NO row: no glob import, no non-expression macro, no effect, no new allow/expect. |
| `docs/quality/source-allowances.json` | No change (exception inventory identical; `fresh_desc` only shrinks). |
| `docs/refactor/review-mechanisms.json` | No change (the pinned creation test is untouched). |
| `CONFORMANCE.md` | Optional History line ONLY after a real run shows 332/0/6. |
| `handover/.../07-TYPED-CREATION-DOCUMENT.md:354-360` | Optional: add `max idle expiry = 4294967295 s` to "Initial bounds" if Soren treats the handover spec as living. |

Deliberately not done: registering `creation/ttl.rs` as a mutation owner. The diff touches two lines of
`touch_ttl`, so its fn-level mutants would be in scope and are only killed by DST rigs
(`dst_tests::runtime_request_work::`), not by `application::creation::` unit tests -- a separate,
reviewable change if wanted.

Suggested commit subject (repo style): `An idle window never wraps a stream's expiry into the past`.

Verification order for the implementer: (1) apply tests only, run T1/T2/T4a/T4c in debug and `--release`,
capture the red text; (2) apply the fix, `cargo fmt`, `wc -l` check; (3) `scripts/quality.sh`;
(4) `python3 scripts/quality/verification_plan.py` against the push `before` revision and run the `http`
mutation scope it selects; (5) `scripts/gate.sh`; (6) conformance suite, expect 332/0/6; (7) confirm CI with
`gh run view`, never assume green.

## 6. What could go wrong

- Conformance: every TTL the pinned suite sends is <= 99_999 and its 400 expectations are grammar-only,
  all still served by the unchanged `:2455` check. HEAD echo (`3600`) uses the same code path. Expect no
  delta, but the raw create path changed, so the suite must actually be run.
- Wire compatibility: TTLs in (4_294_967_295, ~9.22e15] were ADMITTED and worked (far-future expiry,
  e.g. `Number.MAX_SAFE_INTEGER`); they now get 400 -- including an idempotent re-PUT of such an existing
  stream (was 200) and a fork that names one explicitly. The 400 message text does not mention the ceiling
  (both messages live inside ratcheted fns); the matrix is the documentation. SDK needs no change.
- Persisted formats: none change. `ttl_secs` stays u64; golden descriptors (`golden_tests.rs:564,589`) use
  3600. Legacy descriptors above the ceiling stay readable and renewable. The only new persisted value is
  `expires_at_ms = i64::MAX` on a fork inheriting a legacy window: exact in serde_json, compared (never
  added to) everywhere it is read; `TtlMutation::run` declines (`expires < target` is false) and
  `touch_ttl` returns `Ticket::complete()` first, so it causes no renewal churn. A JS reader would round it.
- Fleet skew: during a rolling deploy an old node still answers 201 (and, from ~9.22e15, still writes a
  dead descriptor) where a new node answers 400; clients see it flip until rollout completes. Old nodes
  read everything new nodes write; rollback is safe. Descriptors already wrapped by an old release binary
  stay dead and are recreatable through the dead-incarnation arm (`claim.rs:54-91`).
- Line ceilings: the single biggest practical risk is rustfmt reflowing one of the two replaced lines.
  Both were chosen as call expressions under `fn_call_width`; verify with `wc -l` after fmt.
- Ratchet: anyone "tidying" the `fresh_desc` expression back to a closure trips `syntax_facts` (+1).
- DST timing: `the_longest_idle_window...` tolerates 5 s between PUT and HEAD; the conformance suite
  already relies on < 1 s for the same header.
- Profiles: T4 uses `u64::MAX` first precisely so the red and the green are identical under the gate's
  `--release` suite and under debug mutation runs (`[profile.quality]`).
- Ceiling choice is a product decision; if Soren picks 100 years instead, change the const, the literals
  in T1/T2/T3/T4 (`4294967295`/`4294967296`/`49710d`/`49711d`/`4294944000s`) and the three matrix rows.

## Skeptic corrections

Checked read-only against `slate` @ `5d60dc63` (one commit past the plan's `668bc80c`; that commit touched
only `src/application/append.rs`). Every file:line the plan cites in the files it edits was re-read at this
revision and is still exact. Verdict: the plan is sound and passes every gate trap listed; the items below
are a few precision fixes, one wire decision the plan makes silently, and one policy-taste risk with its
sanctioned alternative. Nothing below blocks implementation as written.

### Verified (no correction needed)
- Signatures: `fresh_desc(service, sref, key, content_type: String, ttl_secs: Option<u64>, expires_at_ms: Option<i64>)`
  (`creation.rs:192-199`); `touch_ttl(self: &Arc<Self>, desc: &StreamDesc)` (`ttl.rs:48`); `parse_ttl_strict(&str) -> Option<u64>`
  (`http.rs:2453`); `parse_idle_secs(&str) -> Option<u64>` (`product.rs:217`); `PersistedDescriptor.ttl_secs: Option<u64>`
  (`registry.rs:124`), `.expires_at_ms: Option<i64>` (`:80`), `.created_ms: i64` (`:78`); `StreamDesc: Deref<Target = PersistedDescriptor>`
  (`:406`). `registry.get -> Result<Option<StreamDesc>, object_store::Error>` (`:1010`), `cas_update(&sref, FnMut(&mut PersistedDescriptor) -> bool) -> anyhow::Result<bool>`
  (`:1404`), `invalidate(&sref)` (`:1463`), `deployment.raw_adapter_sref(&str)` (`deployment.rs:58`). Fixture signatures
  `hreq/preq(addr, method, path, &[(&str,&str)], &[u8]) -> (u16, HashMap<String,String>, Vec<u8>)` (`fixture_requests.rs:6-12, 196-202`),
  `PRISMA_KEY` (`:172`); `lifecycle_creation.rs:3-6` already imports everything T4 uses.
- Ratchet on `fresh_desc`: the only reasoned attribute is `too_many_arguments` (`creation.rs:188-191`), so
  `exception_contracts` (`source_rules.py:151-158`) records `scope_lines`/`nested_items`/`syntax_facts` only; the
  `_fingerprint_sites` branches (`:159-188`) run only for `unwrap_used`/`expect_used`. Counted the facts from
  `tools/quality-syntax/src/scan.rs:222-255` (path, method-call, method-call-site, call-site): the expression is
  10 today (`ttl_secs`, `.map` x2, `now_ms()` call-site + `now_ms` path, `t`, `i64`, `.or` x2, `expires_at_ms`) and
  7 after. The closure spelling is 11, the hoisted-`now` spelling is 13 vs 12 (two `now_ms()` sites become
  `let now = now_ms();` + two `now` paths). Point-free is the only non-growing spelling. Plan is right.
- Line ceilings: `wc -l` gives `http.rs` 3373, `product.rs` 4484; the source gate's limit is
  `min(legacy+adoption, merge-base) = min(3384, 3373)` and `min(4920, 4484)` (`source_gate.py:33-45`, `source_rules.py:207-213`),
  i.e. exactly today's counts. Both edits are 1:1 line replacements; measured widths 60 / 54 / 65 columns, all
  call expressions under `fn_call_width`. `creation.rs` (318) and `ttl.rs` (82) sit under the 1,000 floor.
- No `#[expect]` on `parse_ttl_strict`, `parse_idle_secs`, `touch_ttl`, or `impl CreationService` in `ttl.rs`;
  no file-level `#![allow/expect]` in any touched file (a crate-kind contract would have ratcheted whole-file facts).
- `#[warn(clippy::arithmetic_side_effects)]` on an item: precedent `src/queue.rs:98`; `classify` (`source_rules.py:58-60`)
  only inventories `allow|expect`, and `violations` (`:216-221`) only rejects `warn(` of a lint GROUP or a DENIED lint.
  `allow_attributes_without_reason` does not cover `warn`. No `owners.json` row.
- Inference of `admit_ttl(s.parse().ok()?)`: the crate already relies on the identical shape
  (`i64::from_le_bytes(v[0..8].try_into().ok()?)`, `src/queue.rs:170`; `src/crypto.rs:614`).
- Mutation/inventory ledgers: `mutation_owners.py` registers `src/http.rs` as owner `http` (filter `http::`) and
  nothing under `src/application/creation/`; `CRITICAL_PREFIXES` (`verification_plan.py:22-31`) match `src/http`
  but not `src/application/creation`, `src/product.rs` or `src/dst`. `src/http/tests.rs` has `#![cfg(test)]` (line 2)
  so `normalized_source` returns `''` (`production_changes.py:66-67`) and it is omitted from mutation selection.
  `test-inventory.py` hashes per FUNCTION (`function_hash`, `:71-89`), so `--write` adds three entries and
  changes no existing hash; CI runs `--check` against the committed manifest (`ci.yml:47-48`), no `--compare`
  additions ledger is involved. `review-mechanisms.json` pins only `r05_cancelled_ttl_attempt_releases_only_its_owned_slot`
  by function hash. `architecture-gate.py` edges count only `crate::{http,product}` references (`:33-40`);
  `http.rs`/`product.rs` are `transport_and_composition_files`. `multitenancy-audit.sh` fingerprints
  `registry.<op>("`; every T4 call passes `&sref`/`&state.deployment.raw_adapter_sref(..)`, so no new site.
- Conformance: the pinned suite's `Stream-TTL` literals are exactly {1, 2, 10, 60, 1800, 3600, 7200, 99999} plus the
  six grammar rejects; product spec §13 (`07-TYPED-CREATION-DOCUMENT.md:348`) mandates a service maximum with no
  number. The ceiling decision is well-founded.
- Every consumer of `expires_at_ms` outside creation compares or subtracts, never adds: `append.rs:42,67`,
  `lifecycle.rs:19`, `watch.rs:159`, `registry/catalog.rs:162`, `deletion.rs:163`, `billing.rs:973,2100,2148`,
  `http/read.rs:451-457`. `i64::MAX` on a fork child is safe everywhere, on old and new binaries.
- Red predictions hold. T1 fails at the 10th assertion (`left: Some(4294967296)`), T2 at the 4th, T4a at the very
  first `assert_eq!(st, 400, ..)` with `left: 201` (bodyless raw PUT: `needs_init = false` at `raw.rs:61`, so
  `publish` at `initialization.rs:194` is skipped and nothing re-checks liveness), T4c with `left: 409`.

### C1. T4c: name the exact 409 arm (precision, no code change)
`stamp_fork_reference` (`anchor.rs:198-216`) declines only on `current.deleted`, so on current code the stamp
is `Applied(Installed)` and the 409 comes from the presence check at `anchor.rs:57-72`
(`Ok(Some(c)) if desc_alive(&c) ..` fails because `expires_at_ms = now - 1000`). Body
`{"error":{"code":"fork_target_changed","message":"the fork target changed while it was being created; retry"}}`.
The test's `assert_eq!(st, 201, "{}", body)` prints exactly that. Record it as the observed red in the commit
message. Also: the request deliberately omits `stream-fork-offset`, so `validate_boundary` takes
`base = src_end` (`fork.rs:212-215`); the source has one record, boundary 1. Fine.

### C2. T1/T3 silently cement `Stream-TTL: 0` as a 201-then-dead stream (wire decision, make it explicit)
`parse_ttl_strict("0") -> Some(0)` and `admit_ttl(0) -> Some(0)` are pinned. Today `Stream-TTL: 0` gives
`expires_at_ms = created_ms` and `desc_alive` (`creation.rs:141-145`, `now_ms() < expires`) is false in the
same millisecond: the exact "201 then dead-on-arrival" symptom this item is about, reached without overflow.
The product surface already refuses 0 (`parse_idle_secs`, spec §13 "positive"). The pinned suite never sends
`0` (only `00060`), so either choice is conformant. Two options:
- Keep (plan as written; smallest wire change). Then say so in T1's doc comment and the commit message:
  "`0` stays admitted on the raw surface: the protocol gives it no meaning and the suite never sends it".
  The `"0"` assertion is load-bearing for mutation anyway: it is the only input that kills the `>` -> `>=`
  mutant of `b.len() > 1` at `http.rs:2455` (a one-char input that is not refused by the leading-zero rule).
- Refuse on both surfaces: `admit_ttl` becomes `(1..=MAX_TTL_SECS).contains(&ttl_secs).then_some(ttl_secs)`;
  T1 `("0") -> None`, T3 `admit_ttl(0) -> None` plus `admit_ttl(1) -> Some(1)`; `parse_idle_secs` can then
  drop its own `.filter(|v| *v > 0)` (line stays 1:1: `let v: u64 = num.parse().ok()?;`), T2 unchanged
  (zero still refused, now by the owner); wire matrix row 30 becomes `1..=4294967295`. This is a raw-surface
  wire change with no conformance coverage: Soren's call, same as the ceiling.
Recommendation: keep, and make the pin explicit. Adjacent, out of scope, record as a follow-up: the raw
`Stream-Expires-At` has no future check (`http.rs:2553-2561`) while product `expiry.at` does
(`product.rs:304`), so a past instant is the same dead-on-arrival 201.

### C3. `expiry_from_now` exists to satisfy the ratchet (policy-taste risk, alternative spelled out)
`docs/RUST-QUALITY.md:55`: "never ... introduce a wrapper solely to satisfy a lint". The plan says outright that
`expiry_from_now` exists so `fresh_desc` stays point-free. It is one line, it binds one effect (the clock) over a
pure fn with two real callers, and it also documents that the window opens when creation reads the clock, so it
is defensible; but a reviewer applying line 55 literally may object. The sanctioned alternative from the task
("re-decide the reason text") costs nothing at the gates because a changed reason is a new identity
(`source_rules.py:196-199`; `fresh_desc`'s expect is NOT in `docs/quality/source-allowances.json`, so no
`--prune`): drop `expiry_from_now`, write
`expires_at_ms: ttl_secs.map(|t| ttl::expiry_after(now_ms(), t)).or(expires_at_ms),` (rustfmt will break this
chain: 66 chars > `chain_width` 60, so it becomes three lines, scope_lines 42 -> 42) and re-decide the reason:
`reason = "fresh_desc; a fresh descriptor is built from the resolved name, epoch, policy and fork parts separately as creation decided them, and its expiry is the total owner's arithmetic over the policy; a builder would restate the descriptor's own fields"`.
If the reason is re-decided anyway, prefer the single-clock-read spelling (`let now = now_ms();` used for both
`created_ms` and the expiry, +1 line): the persisted window is then exactly `ttl * 1000` instead of "within
a millisecond", which is the honest contract T4b's `[..295_000, ..296_000)` range is tolerating. Either way is
gate-clean; the plan's choice is the one that does not touch a ratcheted function, which the task prefers.

### C4. `product.rs:226` fallback if rustc asks for an annotation
`let v: u64 = num.parse().ok().filter(|v| *v > 0)?;` should infer (the `let` annotation flows back through
`?`'s `Try::Output` and the closure's `*v > 0` is a deferred `PartialOrd` obligation, not a method call on an
unresolved receiver). If it does not, write `num.parse::<u64>().ok().filter(|v| *v > 0)?` (61 columns, chain
43 chars, still one line, still 1:1). Do not move the annotation into a `let v = ..; let v: u64` pair: +1 line.

### C5. Verification legs the plan does not mention
`src/http.rs` is under `BUFFER_PREFIXES` (`verification_plan.py:27-28`), so `plan.json` will select `miri: true`
in addition to the `http` mutation owner. That is CI's existing Miri leg over compatible unit tests, not new
work, but expect it in the run and do not read it as a selection error. `properties_fuzz` and `loom` stay false
(`src/http` is in neither `CODEC_PREFIXES` nor `LIFECYCLE_PREFIXES`). The `http` mutation scope selects only
mutants whose span overlaps the two changed lines (`:2452`, `:2458`): the function-level `None`/`Some(0)`/
`Some(1)`/`Some(u64::MAX)` replacements (all killed by T1's `"3600"`, `""`, `"0"`), not the operator mutants
at `:2455` the plan lists (those spans do not overlap the diff, so they are not in scope; harmless).

### C6. Two more doc rows
- `docs/refactor/WIRE-MATRIX.md:59` (HEAD row, `Stream-TTL: <remaining secs>`): add "may exceed 4294967295 for a
  descriptor whose window predates the ceiling (legacy or inherited by a fork); the ceiling bounds admission, not
  reporting". Otherwise the row 30 bound reads as a bound on the response header, which it is not
  (`http/read.rs:454` renders `i64::MAX - now` for a fork child from C1's scenario).
- `handover/prisma_streams_surface_spec_prelaunch_hard_cutover/07-TYPED-CREATION-DOCUMENT.md:354-360`: not optional.
  §13 is the contract that mandates "within service maximum" and "Initial bounds" is where every other number
  lives; add `max idle expiry          = 4294967295 s`. This is the contract doc the task's "any contract doc"
  clause points at.

### C7. Small test-text fixes
- T4a comment line 2: "It used to be admitted: `u64::MAX` seconds became `now - 1000` ms" is exact
  (`u64::MAX as i64 == -1`), keep. But the doc should also say why `u64::MAX` is first (the plan explains it
  in prose only): "`u64::MAX` first: it is a clean 201 in every profile, whereas `9223372036854776` panics the
  handler under overflow checks and the client sees a closed socket, not a status".
- T4c: after the fix the child answers HEAD through the fork read path; the test only asserts 200, fine. If the
  implementer wants a tighter pin, `h["stream-ttl"]` will be a 16-digit number (~9.22e15); do not assert it
  against the ceiling (see C6).
- T3 `a_fresh_expiry_opens_its_window_at_the_clock`: bounded by two wall-clock reads; cannot hang. Keep.
- Import spelling under this toolchain's style edition (uppercase before lowercase, as `fixture_requests::{PRISMA_KEY, RIG_KEY_B64, hreq, preq}`
  shows): `pub(crate) use ttl::{TtlMutation, admit_ttl};` and `use super::{MAX_TTL_SECS, admit_ttl, expiry_after, expiry_from_now, window_ms};`
  are already in rustfmt order.

### C8. Commit hygiene
One commit (tests + fix) is right for direct-to-slate work: every commit must pass `scripts/gate.sh`, and a
tests-only commit would be red. Quote the four observed red texts (T1, T2, T4a, T4c) in the message, in both
profiles for T4a as the plan says. Run `python3 scripts/quality/verification_plan.py` with `QUALITY_EVENT_NAME=push`
and `QUALITY_BEFORE_SHA=5d60dc63…` (or whatever `origin/slate` is at push time) BEFORE pushing, and confirm CI
with `gh run view`, never by inference.
