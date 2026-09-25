# F3: an unreadable registry at the transition retry's re-preparation answers a non-retryable 500

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `2ffff86c` (the pushed slate head).
The reproduction ran in a throwaway worktree: `8a2791b0` (the split-boundary harness) was
cherry-picked without a commit, the skeptic's F3 probe and two local probes were added, and one
release test build was made. Nothing was committed or pushed. The probes are saved as
`scratchpad/f3-harness-probes.patch` (against `8a2791b0`) and `scratchpad/f3-red-app.patch`
(against `2ffff86c`). Raw logs: `scratchpad/f3-repro-1.log`, `f3-repro-8.log`, `f3-settled-1.log`,
`f3-settled-8.log`, `f3-red-app.log`, `f3-red-app-12.log`, `f3-controls.log`.

**One-line summary.** The append retry loop in `execute_prepared` refreshes the descriptor twice
per round: once in `closure_is_current` and once when it re-runs `prepare`. When the first refresh
cannot read the registry, the answer is the retryable 503 `segment_transition`. When the second
one cannot, the answer is 500 `Internal`: raw `internal`, product `append_failed` with
`retryable:false`. At that point every attempt of the request has been refused as closed, so
nothing was committed and a retry succeeds. The fix (Commit 1) gives the loop's re-preparation the
same typed answer as its first refresh. First-read answers stay byte-identical. It is a small,
client-visible edge change, so the owner must approve it (D1). A wider follow-up, Commit 2, would
fix the same misclassification at every append's first descriptor read. It is optional and
owner-gated (D2).

---

## 1. Problem (verified on 2ffff86c)

### 1.1 Reproduction (observed)

The probe is `skeptic_registry_blip_at_the_post_closure_retry`, copied verbatim from
`skeptic-probes.patch`. It is appended to `8a2791b0:src/dst/tests/split_boundary_outcomes.rs`.
It holds the split at `ScalerBeforePublish` (parent sealed, publication withheld). It fires
`three_shots` (product `gb` → low child, product `ga` → high child, raw empty key → high child).
Once the appends' resume has parked (`parked() >= 2`), it waits 300 ms, arms
`registry.fail_next_get(name)` and releases.

```
$ cargo test --release --lib skeptic_registry_blip_at_the_post_closure_retry -- --nocapture
running 1 test
SKEPTIC regblip: skreg-a-low -> 500 append_failed ra=- body={"error":{"code":"append_failed","message":"append failed","retryable":false}}
SKEPTIC regblip: skreg-a-high -> 200 - ra=- body={"count":1,"cursor":"EvhzIaOZ...","duplicate":false,"sealed":false}
SKEPTIC regblip: skreg-a-raw -> 200 - ra=- body=
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1302 filtered out; finished in 0.49s
```

The probe was then run 8 more times from the same binary. Each run had exactly one 500: raw 3
times, product 6 times (3 `gb`, 3 `ga`). The raw body names the source:

```
run1: SKEPTIC regblip: skreg-a-raw -> 500 internal ra=- body={"error":{"code":"internal","message":"Generic registry error: injected registry get failure"}}
run3: SKEPTIC regblip: skreg-a-high -> 500 append_failed ra=- body={"error":{"code":"append_failed","message":"append failed","retryable":false}}
```

**Settled through the harness verdict.** A local probe, `f3plan_registry_blip_settled`, runs the
same scenario through `Scenario::finish`, which reads back, retries exactly and reads back again.
It shows that the 500'd append committed nothing and that its retry lands once. It failed 9 of 9
runs, each with exactly one violation. `split published` was true in 8 of 9 runs; in the other,
the appends' request-work resume won Phase B, and the harness still saw the map published.

```
F3PLAN split published=true
SBO | sealed-unpublished+registry-blip | f3plan-a-low Product key="gb" -> seg1 (parent engine 10) | first 500 append_failed ra=- | copies 0 | retry 200 - ra=- | copies 1 | VIOLATION: HTTP 500: {"error":{"code":"append_failed","message":"append failed","retryable":false}}
SBO | sealed-unpublished+registry-blip | f3plan-a-high Product key="ga" -> seg2 (own engine 11) | first 200 - ra=- | copies 1 | retry 200 - ra=- dup | copies 1 | committed
SBO | sealed-unpublished+registry-blip | f3plan-a-raw Raw key="" -> seg2 (own engine 11) | first 200 - ra=- | copies 1 | retry 204 - ra=- dup | copies 1 | committed
split-boundary violations:
sealed-unpublished+registry-blip f3plan-a-low: HTTP 500: {"error":{"code":"append_failed","message":"append failed","retryable":false}}
test result: FAILED. 0 passed; 1 failed; ...
```

Across all 9 runs the violating row always had `copies 0 | retry 200 - (not dup) | copies 1`. Three
were raw `500 internal`; six were product `500 append_failed`.

**Controls, same binary.** `appends_while_the_sealed_parent_awaits_publication`, the same phase
without the fault, gives all three rows `first 200 | copies 1 | retry dup | committed`.
`r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final`, the neighbouring pin where the
fault lands on `closure_is_current`'s read, passes: 503 `segment_transition` with a `retry_after`.

**The proposed red (§3.1), run on the unfixed tree.** It fails 12 of 12 runs at the classification
assertion:

```
assertion `left == right` failed: AppendFailure { class: Internal, code: Internal, message: "Generic registry error: injected registry get failure", retry_after: None, owner: None, conflict: None }
  left: (Internal, Internal, None)
 right: (Unavailable, SegmentTransition, Some(1))
```

### 1.2 Causal chain (file:line at 2ffff86c)

1. **The seal.** `execute_split` → `resume_incarnation` reads the descriptor with
   `st.registry.get` (`src/application/topology.rs:409`). This is before arming. The split then
   seals segment 0 and parks at `pause_scaler_before_publish` (`:435`).
2. **The appends meet the closed parent.** The product handler reads the descriptor
   (`src/product.rs:2035`, cache). `submit_product_append` calls `prepare` (`:2171`, cache). Raw
   calls `append_typed` → `prepare` (`src/http.rs:2503`). `execute_prepared`
   (`src/application/append.rs:144`) → `execute_once` → `submit`. The parent's committer answers
   `AppendErr::Closed`, which becomes `engine_closed_segment()` (`contract.rs:157-167`, with
   `declared:false`), so the loop continues (`append.rs:170`).
3. **The first refresh.** `closure_is_current` (`append.rs:198-239`) invalidates and reads the
   descriptor (`:212-213`). It sees `pending` (`:230`), schedules the topology resume, and waits
   on its ticket (`:231-234`). That resume also reads the registry first (`topology.rs:409`), then
   parks at `:435`. This is the probe's `parked() >= 2`.
4. **The fault is armed and the hold released.** `fail_next_get` is a one-shot, checked before the
   cache (`src/registry/cache.rs:299-304`). Phase B's CAS does not consume it: `mutate_incarnation`
   reads the store directly (`src/registry.rs:1118`). On success it invalidates the cache
   (`topology.rs:543`). There is no `Registry::get` of the name after the park. The ticket
   completes and `closure_is_current` returns `Ok(false)` (`append.rs:235`).
5. **The second refresh.** `append.rs:179` sleeps 10 ms. `:181`,
   `prepared = self.prepare(&command.sref, key).await?`, reads the registry (`append.rs:47`). The
   cache was invalidated by the publication, so this is a real store GET in production. It meets
   the injected error. The `Err(error)` arm (`append.rs:81-87`) builds
   `AppendFailure::new(FailureClass::Internal, AppendCode::Internal, error.to_string())`, and `?`
   returns it from `execute_prepared`.
6. **Rendering.**
   - Raw: `append_failure_status` maps `Internal` → 500 (`src/http.rs:27`), and `render_append`
     writes code `internal` with the store message (`http.rs:66-71`).
   - Product: `render_product_append_error` (`product.rs:2260`) has no case for
     `AppendCode::Internal`, so it falls to the class match: `_ => ("append_failed", "append
     failed", None, false)` (`product.rs:2327`). No `retry-after` is sent.

The fault lands on exactly one of the three shots because they share one coalesced request-work
ticket. The first to re-prepare consumes the one-shot. The other two re-prepare cleanly and land on
the children.

### 1.3 What the neighbouring refreshes do

| Read | Where | Unreadable registry answers |
|---|---|---|
| closure check | `append.rs:212-214` (`unproven`, `:204-211`) | 503 `segment_transition`, `retry_after: 1`; product 503 `temporarily_unavailable`, `retryable:true` |
| sealed-route refresh | `append/route.rs:12-27` (error swallowed; a still-sealed route answers) | 503 `segment_transition`, `retry_after: 1` |
| topology ticket (schedule, wait) | `append.rs:231-234` | same `unproven` 503 |
| **re-preparation** | `append.rs:181` → `:47`, `:81-87` | **500 `internal` / product `append_failed` `retryable:false`** |
| exhaustion of the 4 attempts | `append.rs:183-188` | 503 `segment_transition`, `retry_after: 1` |

The re-preparation is the only step of the transition retry protocol that does not answer
"transition unproven, retry". The doc comment on `closure_is_current` states the rule the
re-preparation breaks: "a refresh that cannot be read proves nothing and answers retryable"
(`append.rs:195-196`).

### 1.4 Every consumer of this classification

The classification site is `prepare`'s `Err` arm, `append.rs:81-87`. `prepare` has four call sites.
Commit 1 changes only the fourth.

| # | Call site | Reached from | How the failure is consumed | Commit 1 | Commit 2 |
|---|---|---|---|---|---|
| 1 | `http.rs:2503` (`append_typed`, first read) | raw `POST /v1/stream/{name}` (`raw_append`, `http.rs:263-273`); the telemetry-append receiver and `billing::system_append::append_local` (`system_append.rs:61-75`; branches only on `NotFound`) | `render_append`: 500 `internal` | unchanged | 503 |
| 2 | `product.rs:2171` (`submit_product_append`, first read, after the handler's own read at `:2035`) | product `records`, `records:batch`, the `:seal` final record (`product_append_sealing`, `:1888`) | `render_product_append_error`: 500 `append_failed`, `retryable:false`. For the seal, `definitively_rejected()` is false (`:1756-1761`), so the disposition is `AmbiguousOrTransient` | unchanged | 503 |
| 3 | `append.rs:139` (`execute`, first read) | consumer DLQ delivery (`consumer/delivery.rs:712`) | `definitively_rejected()` only (not counted as `blocked`), plus a warn log | unchanged | unchanged (class-neutral) |
| 4 | **`append.rs:181` (re-preparation)** | all of the above, only inside the transition retry | as rows 1-3 | **503 `segment_transition`** | same |

`definitively_rejected()` (`contract.rs:307-318`) is false for both `Internal` and `Unavailable`.
The seal final-record disposition and the DLQ's `blocked` count are therefore unchanged by either
commit. Only the rendered wire answers move.

Other `FailureClass::Internal` producers on the append path must keep 500, and neither commit
touches them:
- `append.rs:149-155`, a preparation mismatch (a programming error);
- `AppendErr::Internal` from the committer (`contract.rs:298`): maintenance divergence (see the
  absorber plan) and `db.write` failures (item 47, possibly ambiguous);
- `ResolveError::OpenFailed` → 500 `shard_open` (`contract.rs:239-243`).

A renderer-level change (product `Internal` → `retryable:true`) would mislabel these. That is why
the fix is made at the classification, not in the renderer (§2, option D).

### 1.5 Commit state at the failing point, and the contract the docs set

**The append is certainly uncommitted.** The loop repeats only when an attempt's error is
`engine_closed_segment()` (`append.rs:170`). That is an `AppendErr::Closed` the committer decided
before any write:
- a closed identity is refused at `if local.fields.closed { … Closed; return }`, ahead of
  `accept_append` (`shard/transaction/append.rs:71-95`);
- a producer duplicate would have been answered as a duplicate (success) before that check;
- in the seal-queued group, the appends staged after the close are refused the same way, before
  the write.

At `append.rs:181`, every attempt of this request has been refused as closed, so no attempt wrote
anything. The harness read-back confirms it: `copies 0`, the exact retry answers 200 (not a
duplicate), then `copies 1`. There is one durable side effect a close request can have made
already: an Empty seal intent from `install_intent` (`append/close.rs:183-212`). The retry of the
same close re-enters the same semantic operation id (`close.rs:28-50`) and resumes it, so a retry is
idempotent for closes as well.

**What the documents say.**
- `docs/DST.md` §5 has three outcomes: `Acked`, `Rejected` ("the server decided against it before
  committing anything"), and `Unknown` (no response, or an ambiguous fencing error). F3's append is
  `Rejected`-and-retryable, not `Unknown`.
- `docs/append-transitions.md` step 5: "Timeout remains an explicitly ambiguous failure." 408 is the
  only ambiguous append answer; memory note "ambiguity surface": only fencing and timeout create
  ambiguity.
- Edge record #8 (`docs/reviews/2026-09-hardening/edge-changes.md:186-205`, commits
  `668bc80c`+`5d60dc63`) is the approved contract of this loop: "A refresh read error gives raw 503
  segment_transition with Retry-After: 1 (product 503 temporarily_unavailable, retryable:true,
  Retry-After: 1)". It also says "A descriptor that no longer exists is retried, and the next
  prepare returns the missing/gone answer". The record never mentions the re-preparation's own read
  error. F3 is the one refresh the record's rule missed.
- `docs/refactor/WIRE-MATRIX.md:43` (raw) describes the retry wrapper with "terminal fallback 503
  `segment_transition`". `:119` (product) says "503 `temporarily_unavailable` (retryable) … else
  `append_failed`".
- Edge record #23 and WIRE-MATRIX `:221`, `:225-227` set the cell-wide precedent: a registry fault
  is a retryable 503, "never a gone signal".

**What the SDK does with each answer.** `sdk/src/index.ts:523-526`:

```ts
function retryableRequestError(error: unknown): boolean {
  return error instanceof StreamsTransportError ||
    (error instanceof StreamsError && error.retryable &&
      (error.status === 429 || error.status === 503));
}
```

`req()` retries such errors up to 3 times (`:573-581`). That covers `append`, `appendBytes` and
`appendMany`, with or without a producer tuple (`:690-721`). Two consequences:
1. A 500, even with `retryable:true`, is never retried by the SDK. The fix must produce a 503; 500
   plus `retryable:true` is not enough.
2. A retryable 503 on an append promises that a blind retry is safe. That holds here, because the
   request is certainly uncommitted.

### 1.6 Reachability in production

- The re-preparation's read is a real store GET whenever the transition's publication invalidated
  the cache (`topology.rs:543`, `:616` for merges). That is the case for every append that waited
  out a pending split or merge. One transient registry GET failure there (network, 5xx, throttling
  of the descriptor object) makes that append a non-retryable 500.
- The same answer covers a corrupt descriptor: `fetch_descriptor` reports a decode failure as
  `object_store::Error::Generic` (`registry/cache.rs:340-349`). Commit 1 answers that 503 as well,
  as `closure_is_current` already does for the same read. A corrupt descriptor then answers 503
  inside the loop and 500 `internal` on the client's next first read (§8).
- Not reachable in the capacity test (fault-free store). F3 is not the release-hold 500's cause.
  It is a split-boundary 500 that the hold's criterion ("no split phase answers 500") must still
  exclude.

---

## 2. Contract decision

### 2.1 Options

| | Change | Wire effect | Size and risk | Verdict |
|---|---|---|---|---|
| **A (Commit 1, recommended)** | The loop re-reads the descriptor itself and maps an unreadable registry with the same `unproven` as `closure_is_current`. `prepare`'s decisions move into `authorize(fetched, key)`, shared by both reads | only for the re-preparation's read error inside the transition retry: 500 → 503 `segment_transition`, `Retry-After: 1` | `append.rs` about +15 lines; no exception scope touched; first reads byte-identical | the causal fix for F3; completes edge #8's rule |
| B (Commit 2, owner-gated) | `prepare`'s `Err` arm itself becomes `Unavailable` + `retry(1)`, and the product handler's own read (`product.rs:2057-2065`, 500 `internal` `retryable:true`) becomes 503 | every append's FIRST descriptor read error: raw 500 `internal` → 503; product 500 `append_failed` `retryable:false` (or 500 `internal` `retryable:true`) → 503 `temporarily_unavailable` `retryable:true` | 1 expression in `append.rs`, 2 tokens in `product.rs` (4124 lines, same count); raw code must be chosen (D2) | same defect class, off the split boundary; wider edge change |
| C | Absorb the blip: on a failed re-read, consume the next wait and read again; answer 503 only when the remaining attempts are spent | a one-shot blip becomes success | restructures the loop (`prepared` is moved into `execute_once`); `closure_is_current` would need the same for parity | gold-plating for a release hold; possible later |
| D | Product renderer maps `Internal` to `retryable:true` | 500 stays 500 | 1 line | rejected: mislabels committer `Internal` (divergence, `db.write`, possibly ambiguous) as safe to retry, and the SDK does not retry 500 anyway |

**Why A.** It fixes the rule where it broke. The transition retry has one contract for "the
refresh could not be read": `unproven`, a retryable 503 `segment_transition`. The re-preparation
is the second refresh of the same round, and the request is certainly uncommitted at that point
(§1.5). A keeps every first-read answer, and so every existing wire pin, byte-identical. B is the
principled generalization, but it changes answers on every append path, not only at the split
boundary, and it needs a raw code decision. So it is split out for the owner.

### 2.2 Edge change (Commit 1; D1; record text in §6.3)

| | Before | After |
|---|---|---|
| Condition | an append whose attempt was refused by an engine closure (not a declared one, descriptor not sealed) re-prepares after `closure_is_current` returned false (a stale route, a waited-out pending split or merge, or a descriptor gone), and that re-preparation's registry read errors | same |
| Raw | 500 `{"error":{"code":"internal","message":"<store error>"}}`, no `Retry-After` | 503 `{"error":{"code":"segment_transition","message":"<store error>"}}`, `Retry-After: 1` |
| Product | 500 `{"error":{"code":"append_failed","message":"append failed","retryable":false}}` | 503 `{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}`, `Retry-After: 1` |
| SDK | throws `StreamsError` 500 at once | retried automatically (≤ 3 times, 1 s): the append lands |
| Seal final / DLQ | `AmbiguousOrTransient` / not blocked | unchanged |

Risk: **low**. A non-retryable 500 for an uncommitted request becomes the retryable 503 that the
approved record #8 already gives the neighbouring read. No status, code or header is new to either
surface. The message text (the store error) is the same.

---

## 3. Red tests, pins and non-vacuity controls

### 3.1 Red regression, application level (deterministic; on slate now; no prerequisite)

This goes in `src/dst/tests/append_application.rs` (257 lines at base, about 305 after; DST cap
1,000; no exceptions in the file), after `r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final`.
The exact text was run as a local probe (`scratchpad/f3-red-app.patch`, 12 of 12 red, §1.1). Run
`cargo fmt` over it: one `preq` line exceeds 100 columns.

```rust
/// F3: the re-preparation after a waited-out transition is the retry
/// loop's second refresh. A registry it cannot read proves nothing, as
/// the closure check's cannot: the append, refused as closed by every
/// attempt and so uncommitted, answers the same retryable 503, never a
/// 500, and its retry lands once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal() {
    use crate::application::append::FailureClass;
    use crate::failpoints::Fp::ScalerBeforePublish;
    use super::fixture_livefeed::wait_parked;
    let (state, addr) = http_rig(mem()).await;
    let name = "typed-reprepare";
    let create = br#"{"format":{"kind":"json"}}"#;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    let (status, _, _) = preq(addr, "PUT", "/v1/streams/typed-reprepare", &headers, create).await;
    assert_eq!(status, 201);
    let sref = state.deployment.raw_adapter_sref(name);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let app = state.append_service();
    crate::failpoints::arm_scaler_before_publish(name);
    let held = super::fixture_failpoints::FailpointGuard(name.to_string());
    let split = crate::scaler3::execute_split(&state, &sref, 0, 0x8000_0000_0000_0000);
    let append = async {
        wait_parked(ScalerBeforePublish, name, 1).await;
        app.execute(command(&desc, "reprepare", br#"{"n":1}"#)).await
    };
    let release = async {
        wait_parked(ScalerBeforePublish, name, 2).await;
        state.registry.fail_next_get(name);
        drop(held);
    };
    let (_, answer, ()) = futures_util::future::join3(split, append, release).await;
    let error = answer.unwrap_err();
    assert!(error.message.contains("injected registry get failure"), "{error:?}");
    assert_eq!(
        (error.class, error.code, error.retry_after),
        (FailureClass::Unavailable, AppendCode::SegmentTransition, Some(1)),
        "{error:?}"
    );
    state.registry.invalidate(&sref);
    let published = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(published.segments.as_ref().is_some_and(|m| m.pending.is_none()));
    let landed = app
        .execute(command(&desc, "reprepare", br#"{"n":1}"#))
        .await
        .unwrap();
    assert!(!landed.duplicate, "the refused append committed nothing");
    assert_ne!(landed.seg_id, 0, "the retry lands on a child");
    engine_shutdown(&state).await;
}
```

Why each line is there:
- **`parked >= 1` before the append.** The parent is sealed and the descriptor still pending, so
  the first attempt meets `Closed`.
- **`parked >= 2` before arming.** The append is inside `ticket.wait()` (`append.rs:233`), so its
  closure-check read has already happened. The next `Registry::get` of the name is its
  re-preparation (§1.2 step 4).
- **The message assertion.** It pins the cause: the answer carries the injected read's text before
  and after the fix, because `unproven` keeps the store error as its message.
- **The class/code/retry assertion.** This is the defect. Today: `(Internal, Internal, None)`.
- **No assertion on `execute_split`'s bool.** Either parked resume may win Phase B (observed 1 in 9
  in the harness probe). The publication is read back instead.
- **`!landed.duplicate`.** On this one-prefix rig both children share the parent's engine, so the
  producer lineage (F1) is intact. A duplicate would mean the refused attempt had committed.

### 3.2 Harness pin (only if the harness lands first; D3)

This goes in `src/dst/tests/split_boundary_outcomes.rs` (838 lines at `8a2791b0`; the F2 plan takes
it to about 848 and the F1 plan to about 832; this adds about 25; DST cap 1,000). It is the probe
from §1.1 as a committed scenario, after `appends_while_the_sealed_parent_awaits_publication`:

```rust
/// The sealed parent, publication withheld, and the registry unreadable
/// once when the appends' retry re-prepares after the resume: every append
/// is answered retryably or lands, none 500 (F3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_whose_reprepare_cannot_read_the_registry() {
    let sc = Scenario::start("sbo-regblip", Setup::plain()).await;
    let name = sc.name.clone();
    crate::failpoints::arm_scaler_before_publish(&name);
    let held = FailpointGuard(name.clone());
    let parked = || crate::failpoints::parked(crate::failpoints::Fp::ScalerBeforePublish, &name);
    let shots = three_shots("regblip", "a");
    let fire = async {
        wait_for("the split to park after its seal", || parked() >= 1).await;
        sc.fire(&shots).await
    };
    let release = async {
        wait_for("an append's resume to park", || parked() >= 2).await;
        // Let the other shots reach the coalesced ticket too (the skeptic
        // saw all three queued in the gap); `parked() >= 2` proves only the
        // first one did.
        tokio::time::sleep(Duration::from_millis(300)).await;
        sc.state.registry.fail_next_get(&name);
        drop(held);
    };
    let (_, fired, ()) = futures_util::future::join3(sc.split(), fire, release).await;
    let refused = fired.iter().filter(|(_, a)| a.status == 503).count();
    assert_eq!(refused, 1, "the one-shot fault reached exactly one re-preparation");
    sc.finish("reprepare-blip", fired, &[]).await;
}
```

- **Today** it fails with `refused == 0`, because the blipped shot answered 500 (observed through
  `finish` as `VIOLATION: HTTP 500`, 9 of 9).
- **After Commit 1** the blipped row reads
  `first 503 segment_transition ra=1 | copies 0 | retry 200 | copies 1 | refused, nothing committed`
  (product: `503 temporarily_unavailable ra=1`), and the other two rows are `committed`.
- **The `refused == 1` assertion** closes the skeptic's C3 gap: `Row::verdict` alone would also
  pass "AMBIGUOUS: refused but committed".
- **It is an outcome pin, not the discriminating red.** `parked() >= 2` proves only that the FIRST
  shot's closure check has read and parked. The 300 ms settle, kept from the skeptic's probe, lets
  the other shots reach the ticket as well; it was enough in 9 of 9 runs. If a late shot's closure
  check consumed the fault, that shot would answer the same 503 on the neighbouring path, and the pin
  would still pass. Proving which read failed is §3.1's job: one append, so its closure-check read
  precedes the arming by construction.

### 3.3 Pins that must stay green (unchanged expectations)

- `dst::dst_tests::append_application::` (the five existing r02 tests), in particular:
  - `r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final` (the neighbour's 503);
  - `r02_a_sealed_descriptors_closure_costs_no_refresh` (a sealed descriptor never refreshes; the
    armed fault must stay unconsumed);
  - `r02_a_closure_from_a_replaced_incarnation_is_not_the_new_streams` (409
    `target_incarnation_changed`: `closure_is_current` fences the epoch before the re-preparation).
- `dst::dst_tests::topology_scaling::an_append_routed_by_a_pre_split_descriptor_lands_on_the_child`,
  `…pre_merge_descriptor_lands_on_the_merged_segment`,
  `…pre_seal_descriptor_is_still_refused_as_sealed` (edge #8's pins).
- `dst::dst_tests::billing_operation_counts::an_append_counts_when_its_descriptor_cannot_be_read_again`,
  `raw_append_counts_when_its_descriptor_cannot_be_read_again`: the fault lands after the commit;
  the loop is not entered.
- If landed: the nine `split_boundary_outcomes` phase scenarios, unchanged.

### 3.4 Non-vacuity controls (local edits, reverted, never committed)

- **(a)** Restore `prepared = self.prepare(&command.sref, key).await?` at the loop. §3.1 fails with
  `left: (Internal, Internal, None)`, as recorded in §1.1.
- **(b)** Map the loop's read error with `unproven` but drop `.retry(1)` from it. §3.1 fails on
  `retry_after` (`None` vs `Some(1)`). `r02_a_closure_the_refresh_cannot_confirm…` fails too,
  because `unproven` is shared.
- **(c)** Remove `state.registry.fail_next_get(name)` from §3.1. It fails at `answer.unwrap_err()`
  (the append lands), which shows the fault is what drives the answer.
- **(d)** Arm the fault before `parked >= 1` instead. The first `prepare` consumes it, and §3.1
  fails on the class (`Internal`) through the unchanged first-read path. This shows the test
  discriminates between the two reads.

---

## 4. Edits, file by file

### Commit 1: "The retry's re-preparation answers an unreadable registry as the closure check does: retryable, never a 500"

**`src/application/append.rs`** (423 lines → about 438; no ceiling; the only exception in the file
is `execute_once`'s `too_many_lines`, which this commit does not touch).

1. Hoist the closure `unproven` (`:204-211`) to a module-private function next to `execute_once`.
   Its body is unchanged:
   ```rust
   /// A transition refresh that could not be read proves nothing: the
   /// writer, whose attempts were all refused as closed, is told to retry.
   fn unproven(error: String) -> AppendFailure {
       AppendFailure::new(FailureClass::Unavailable, AppendCode::SegmentTransition, error)
           .retry(1)
   }
   ```
   `closure_is_current` keeps its three call sites (`:214`, `:232`, `:234`) unchanged in text.
2. Split `prepare` (`:42-104`) into the read and the decisions. The first read's mapping is
   byte-identical:
   ```rust
   pub(crate) async fn prepare(&self, sref: &TenantStreamRef, key: AppendKey)
       -> Result<AuthorizedAppend, AppendFailure> {
       let fetched = self.registry.get(sref).await.map_err(|error| {
           AppendFailure::new(FailureClass::Internal, AppendCode::Internal, error.to_string())
       })?;
       self.authorize(fetched, key).await
   }
   /// Everything preparation decides from a descriptor it could read:
   /// liveness, the key, instance admission. What an unreadable registry
   /// means belongs to the caller's point in the protocol.
   async fn authorize(&self, fetched: Option<StreamDesc>, key: AppendKey)
       -> Result<AuthorizedAppend, AppendFailure> { /* :47-80 and :89-103, matching on `fetched` */ }
   ```
   The match arms become `Some(desc) if … init …` → `Creating`, `Some(desc) if desc_alive` →
   `desc`, and `desc` → `Gone`/`NotFound`. The `Err` arm leaves the match.
3. The loop (`:179-181`):
   ```rust
   tokio::time::sleep(wait).await;
   // The round's second refresh: a registry it cannot read proves nothing,
   // and no attempt of this request committed (each was refused closed).
   let fresh = self.registry.get(&command.sref).await;
   let fresh = fresh.map_err(|error| unproven(error.to_string()))?;
   prepared = self.authorize(fresh, AppendKey::Provided(command.key.clone())).await?;
   ```
   Unchanged:
   - the read is still a cache read that `closure_is_current` or the publication primed or
     invalidated, so the same store traffic as today;
   - the gone path (`authorize(None)` → 404/410, edge #8);
   - `Creating`;
   - admission's 429;
   - the epoch fence (`execute_once`, `:252-261`).
4. Rustdoc on `execute_prepared`, one line: every refresh in the loop answers an unreadable registry
   with the retryable 503.

**`src/dst/tests/append_application.rs`**: §3.1.

**`src/dst/tests/split_boundary_outcomes.rs`**: §3.2, only if D3 lands the harness first. Otherwise
it follows in the harness commit's series.

**Documents** (no ceilings), in the same commit:
- `docs/refactor/WIRE-MATRIX.md:43` (raw retry wrapper). Replace the stale `src/http.rs:4761-4794`
  reference with `src/application/append.rs` (`execute_prepared`, `closure_is_current`). Add: "a
  refresh the registry cannot read, at the closure check or the re-preparation, answers 503
  `segment_transition` with `retry-after: 1`; every attempt was refused closed, so nothing
  committed."
- `docs/refactor/WIRE-MATRIX.md:119` needs no change: 503 → `temporarily_unavailable` (retryable)
  already describes the product rendering.
- `docs/append-transitions.md`, after the `execute_prepared` sentence (line 5): "Each round of the
  bounded topology retry refreshes the descriptor twice, the closure check and the re-preparation;
  an unreadable registry at either is the retryable 503 `segment_transition`, never an internal
  failure: the request's attempts were all refused as closed, so it committed nothing."

### Commit 2 (only after D2): "An append's first descriptor read answers an unreadable registry as retryable"

- `append.rs` `prepare`'s `map_err`: `FailureClass::Unavailable`, code per D2, `.retry(1)`.
- `product.rs:2057-2065`: `StatusCode::SERVICE_UNAVAILABLE`, `"temporarily_unavailable"`. Two
  tokens change; the line count stays 4124 against its ceiling. Add `retry-after: 1` only if D2
  wants the header: the `perr` path has no header argument, so the header would add lines, which
  the file's ceiling forbids without restructuring. The recommendation is no header, because the
  SDK defaults to 1 s (`index.ts:574`).
- Red: an application test in `append_application.rs` that arms `fail_next_get` before
  `app.prepare(..)` and asserts `(Unavailable, <code>, Some(1))`. Plus a product HTTP test that arms
  the fault before `POST /records` and asserts 503 `temporarily_unavailable` `retryable:true`. The
  handler's read at `:2035` consumes it.
- Docs: WIRE-MATRIX raw append (line 42: "500 `internal`" loses the registry case) and product
  append (line 117 "503 `creating` from the descriptor read" gains the 503), plus a second edge
  record.

---

## 5. Mutation analysis

Run from `scripts/quality` with Python 3.12:

```
>>> verification_plan.plan(['src/application/append.rs', 'src/dst/tests/append_application.rs',
...                         'src/dst/tests/split_boundary_outcomes.rs'])
{'mutants': False, 'mutation_source_files': [], 'selected_mutation_owners': [],
 'unregistered_mutation_source_files': [], 'miri': False, 'properties_fuzz': False, ...}
>>> verification_plan.plan(['src/product.rs'])   # Commit 2
{'mutants': False, 'selected_mutation_owners': [], 'miri': False}
```

- **No mutation owner is touched.** `src/application/append.rs` has no row in
  `scripts/quality/mutation_owners.py`. It is not under `CRITICAL_PREFIXES` (`verification_plan.py:21-31`
  covers only `src/application/read_`). The fix stays out of `src/registry/cache.rs` (owner
  `registry_cache`, filter `registry::`) and `src/registry.rs`.
- **The mutants that matter are killed by §3.1 anyway:**
  - `unproven` → `Internal` (control a);
  - removing `.retry(1)` (control b);
  - swapping the loop's `map_err` for `?` on a `prepare` call (control a).
- **Optional (D5):** register `src/application/append.rs` with filter
  `dst_tests::append_application::`. Registration requires the whole file's mutants to be killed,
  which is well beyond F3. Not recommended in this plan.

---

## 6. Ledgers and records (same commit as the change unless noted)

1. **`docs/refactor/test-inventory.json`**: `python3 scripts/test-inventory.py --write`, then
   `--check`. There is +1 entry, `r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal`
   (file `src/dst/tests/append_application.rs`, `scenarios: []`). With the harness, +1 more:
   `appends_whose_reprepare_cannot_read_the_registry`. No existing entry changes: the r02 bodies
   are untouched, and comments are not hashed.
2. **`docs/quality/owners.json`**: no change (no new module, spawn or exception).
3. **Edge record** (after D1 approval; D4 places it). Proposed text, numbered after the F2 plan's
   #53:
   > ### #54 <Commit 1 sha> — The transition retry's re-preparation answers an unreadable registry as retryable
   > - **Program item:** release hold (split-boundary outcomes), skeptic finding F3
   > - **Surface:** both
   > - **Endpoint:** Raw POST /v1/stream/{name} (append/close); product POST /v1/streams/{name}/records and records:batch, the final-record append inside :seal; internal AppendService appends (consumer DLQ delivery, system/telemetry appends).
   > - **Condition:** an attempt was refused by an engine closure on an unsealed descriptor, `closure_is_current` found the route stale, a pending split/merge waited out, or the descriptor gone, and the re-preparation's registry read then failed (store error, or an undecodable descriptor).
   > - **Before:** raw 500 {"error":{"code":"internal","message":"<store error>"}}, no Retry-After; product 500 {"error":{"code":"append_failed","message":"append failed","retryable":false}}. The request had committed nothing: every attempt was refused as closed.
   > - **After:** raw 503 {"error":{"code":"segment_transition","message":"<store error>"}} with Retry-After: 1; product 503 {"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}} with Retry-After: 1. This is the answer record #8 gives the closure check's own read. First descriptor reads, the gone path (404/410), `creating` and admission are unchanged.
   > - **Retry semantics:** a permanent 500 becomes a retryable 503; the SDK's automatic retry (429/503 with retryable) now lands the append. Seal final-record disposition and DLQ blocking are unchanged (neither class is a definitive rejection).
   > - **Who is affected:** writers to a collection mid split/merge whose descriptor store read fails once, and writers of a collection whose descriptor became undecodable mid-retry (503 instead of 500 inside the loop only).
   > - **Pinning tests:** src/dst/tests/append_application.rs::r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal; ::r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final; (with the harness) src/dst/tests/split_boundary_outcomes.rs::appends_whose_reprepare_cannot_read_the_registry.
   > - **Risk reason:** low: moves an uncommitted request's answer from non-retryable 500 to the retryable 503 the same loop already gives the neighbouring read; no new status, code or header on either surface.

   Update the index table and the counts (raw/product "both", low: 6 → 7, total 52 → 53, or 54
   after F2).

---

## 7. Controls (exact commands, expected outputs)

Run everything in a worktree at `origin/slate`. The host is loaded: build once, run narrowly.

```
cargo test --locked --release --lib --no-run
BIN=$(ls -t target/release/deps/streams_slate-* | grep -v '\.d$' | head -1)
```

1. **Red first.** Apply only the §3.1 test.
   `$BIN r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal` gives `FAILED` with
   `left: (Internal, Internal, None)` / `right: (Unavailable, SegmentTransition, Some(1))` (§1.1).
2. **Green.** Apply all of Commit 1.
   `$BIN dst::dst_tests::append_application::` gives `test result: ok. 6 passed`. Loop the new test
   12 times; 12 of 12 should pass.
3. **Pins.**
   `$BIN dst::dst_tests::topology_scaling::an_append_routed_by_a_pre_ dst::dst_tests::billing_operation_counts::`
   should all pass.
4. **Harness** (if landed): `$BIN dst::dst_tests::split_boundary_outcomes -- --nocapture | grep -E 'SBO \| reprepare-blip|test result'`
   should give one row `first 503 segment_transition ra=1` (raw) or
   `503 temporarily_unavailable ra=1` (product), `| copies 0 | retry 200 - ra=- | copies 1 | refused, nothing committed`,
   plus two `committed` rows. The phase scenarios are unchanged.
5. **Non-vacuity:** §3.4 (a)-(d), each reverted. Quote the outputs in the commit message.
6. **Quality gate:**
   `PATH=<scratchpad>/pybin:$PATH OUT=/tmp/gate-f3.txt scripts/gate.sh; tail -5 /tmp/gate-f3.txt`
   Expected:
   - no `GATEFAIL-*`;
   - no `file growth:` line (append.rs is not ceilinged; append_application.rs about 305 ≤ 1,000);
   - no `accepted exception grew` line (no exception scope is touched);
   - `cargo clippy --locked --workspace --all-targets -- -D warnings` and the `-D warnings` rustdoc
     build are clean;
   - `test-inventory.py --check` passes.

   Also run the CI plan before pushing:
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<slate before> python3 scripts/quality/verification_plan.py --out target/quality-plan`
   should give `mutants: false`, `selected_mutation_owners: []`.

---

## 8. Out of scope (named, with evidence)

- **First-read misclassification** (Commit 2, D2):
  - Raw: every append's first descriptor read error is 500 `internal` (`http.rs:2503` →
    `append.rs:81-87`).
  - Product: the first read inside the core is 500 `append_failed` `retryable:false`
    (`product.rs:2171`). It is reachable at the split boundary too: concurrent appends'
    `closure_is_current` invalidates the shared cache (`append.rs:212`) between a request's handler
    read (`product.rs:2035`) and its `prepare`.
  - The product handler's own read is 500 `internal` with `retryable:true` (`product.rs:2057-2065`),
    which the SDK does not retry (§1.5).
- **Raw close intent store failure → 409.** `install_intent` maps every `SealError`, including
  `SealError::Storage` from a failed descriptor read in `enter_sealing_cas`
  (`application/lifecycle.rs:184`) and `Resumable` "a split or merge kept the collection busy"
  (`lifecycle.rs:233-235`), to `FailureClass::Conflict`/`AppendCode::Sealed` (`append/close.rs:206`).
  That is a transient failure answered as a permanent 409 `sealed`, the same defect class as F3 on
  the raw close path. It needs its own finding and red.
- **Other product handlers' registry reads** answer 500 `internal` `retryable:true` (for example
  metadata, `product.rs:1526-1534`, and the seal validation read, `:1563`). These are not appends.
  A cell-wide "registry fault is 503" sweep would cover them (edge #23 did so for internal
  receivers).
- **Corrupt versus transient.** `Registry::get` returns one `object_store::Error` for both
  (`registry/cache.rs:340-349`). Typing them apart would let a corrupt descriptor stay a 500
  everywhere. That touches the `registry_cache` mutation owner and belongs with Commit 2 if at all.
- **Option C** (absorb a one-shot blip inside the loop) is an availability improvement, not
  required for correctness.

---

## 9. Decisions for the owner

1. **D1: adopt Commit 1 (edge change, low risk; record §6.3).** The re-preparation's unreadable
   registry answers the loop's `unproven` 503 `segment_transition` / product
   `temporarily_unavailable` with `Retry-After: 1`, instead of 500. It completes edge #8's rule.
   Without approval, nothing in §4 lands.
2. **D2: Commit 2 (first reads).** Pick one:
   - **(a)** do it now, with raw code 503 `internal` (class `Unavailable`, already emitted at
     `append/close.rs:124-129` and `:174-179`, so no new vocabulary);
   - **(b)** do it now, with a new raw append code 503 `temporarily_unavailable` (the raw read
     vocabulary and edge #23's code, but new for raw appends);
   - **(c)** defer it: record the first-read 500 as a known release-note item.

   The recommendation is (a) after the release hold closes. It is not needed to close F3.
3. **D3: harness prerequisite.** Land `8a2791b0` (the siblings' D8) before Commit 1 so §3.2 lands
   with it, or land Commit 1 with §3.1 alone and add §3.2 in the harness's series. §3.1 closes F3 on
   its own.
4. **D4: where the edge record goes.** Use #54 in `edge-changes.md` (after F2's proposed #53), with
   the index and counts updated, or a release-safety record.
5. **D5: mutation registration** of `src/application/append.rs`: decline for now (§5).
6. **D6: raw close intent 409** (§8, second bullet): open it as its own finding.

---

## 10. Status (2026-09-24, implemented locally, not pushed)

Owner-side decisions (made under the owner's standing instruction to adopt the external
reviewer's positions): D1 adopt Commit 1; D3 application-level red only (the split harness is
not landed, so §3.2 is not landed either); D2 not now; D5 do not register append.rs; corrupt
descriptors answering 503 inside the loop are acceptable (same as `closure_is_current`); D6
investigated and fixed as a second commit.

Commits on 7549e28a (worktree branch, not pushed):
- **cf5d6e05** — Commit 1 as §4 describes. Edge record **#54** (low, both, recorded for owner
  ratification; #53 was taken by the usage streamId fix). WIRE-MATRIX §1.2 retry-wrapper line and
  `docs/append-transitions.md` updated. Red at 7549e28a 3/3 `(Internal, Internal, None)`; green
  12/12; controls (a)-(d) behaved as §3.4 predicts.
- **8f53bc25** — D6: `close::install_intent` mapped every `SealError` from
  `begin_sealing_for_close` to 409 `sealed`. `Storage` and `Resumable` now answer
  `(Unavailable, SealIncomplete)` = raw 503 `seal_incomplete`, no Retry-After, the answer
  `close::complete` already gives; `Conflict` and the settled refusals keep 409 `sealed`. Edge
  record **#55** (medium, raw, recorded for ratification). Red test
  `r02_a_close_whose_intent_cannot_read_the_registry_is_retryable_not_sealed` (lapsed owed-final
  claim; `fail_next_get` hits the takeover fence's descriptor read): red at cf5d6e05 6/6
  `(Conflict, Sealed)`, green 12/12. Not pinned: the `Resumable` variants.

### Follow-ups (not done)

- **D2 (Commit 2, first reads), deferred.** An append's FIRST descriptor read still answers an
  unreadable registry as 500: raw `internal` (`http.rs` `append_typed` → `prepare`), product
  `append_failed` `retryable:false` (`submit_product_append` → `prepare`), and the product
  handler's own read 500 `internal` `retryable:true` (which the SDK does not retry). §4 "Commit 2"
  and §9 D2 still describe the change; recommendation (a) after the release hold closes. It needs
  its own edge record.
- **Raw close owed-claim renewal** (`close.rs` `install_intent`, `renew_owed_claim` error arm)
  answers 503 `internal` for a registry failure, while the intent path now answers 503
  `seal_incomplete`. Both are retryable 503s, so no client-visible defect; unify only if a later
  vocabulary pass wants one code.
- **§3.2 harness pin** lands with the split-boundary harness (8a2791b0) if and when it lands.
