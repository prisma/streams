# Plan: delete the sealed-span local reader cache (review rank 16)

Repo: /Users/sorenschmidt/code/streams, branch slate @ 71345c03. Read-only verification; no
file in the repository was edited while producing this plan.

Verdict on the claim: **CORRECT, first-hand.** Both halves hold on the current code:

1. `sealed_span_page` holds a `tokio::sync::Mutex` guard across resolve, `stream_handle` and the
   whole `ReadPlan::execute()` and the `LineageSource` is shared by every session of a feed, so
   catch-up through a sealed span is serialized per feed.
2. The cached `(engine, handle)` pair is cleared only on the not-owned branch; an engine that
   closes and reopens under the SAME owner (storage-fault close, ownership bounce A→B→A with no
   page read while at B, rig retirement) leaves the cache holding a closed engine, every page
   fails with a non-fatal error, and the session's catch-up arm retries it every 100 ms forever.

No test pins the cache. The proposed fix (delete the cache; resolve + `stream_handle` on every
page) is the right one and it is a pure deletion with no new branch, which matters for the
mutation gate (see §2 and §6).

---

## 1. Mechanism, with evidence

### 1.1 The cache and the guard held across the read

- `src/sse/source.rs:180-182` — `type LocalReader = tokio::sync::Mutex<Option<(Arc<ShardEngine>, Arc<StreamHandle>)>>;`
- `src/sse/source.rs:196-201` — `SpanReader::Sealed { route, target, owner_hint, local: LocalReader }`
- `src/sse/source.rs:328-333` — `build` seeds it: `local: tokio::sync::Mutex::new(None)`.
- `src/sse/source.rs:423-467` — the local arm of `sealed_span_page`:
  - `:424` `let mut cached = local.lock().await;` — guard taken
  - `:425-431` `self.state.shards.resolve(route, Adoption::External).await` under the guard
  - `:433-436` `engine.stream_handle(span.identity).await` under the guard
  - `:440` `*cached = Some((engine, handle));` — fill, once
  - `:450-466` `if let Some((engine, handle)) = cached.as_ref() { return ReadPlan::segment(...).execute().await ... }` —
    the full page read runs while `cached` (the guard) is alive; it drops at the `return`.
- `src/sse/source.rs:468-472` — the ONLY eviction: `else { *local.lock().await = None; }`, i.e. when
  `owned_here(...)` is false. Ownership unchanged ⇒ never evicted.

Sharing: `src/sse/session.rs:361` `let csrc = feed.current_source();` and
`src/sse/feed.rs:683-692` (`current_source`/`source_snapshot` clone one `Arc<dyn FeedSourceRead>`).
Every session in catch-up on that feed reads through the same `LineageSource`, hence the same
per-span mutex. A later subscriber's freshly built source is discarded by
`src/sse/feed.rs:708-728` (`install_source` → `AlreadyCurrent` when the span signature is not a
strict extension), so the stale source stays installed for the life of the feed.

### 1.2 Why a closed engine stays cached and why the read fails forever

Same-owner close paths (ownership map untouched, so `owned_here` stays true):

- `src/shard.rs:3141-3151` `acker_loop`: a slatedb `close_reason` (fence OR fatal storage error) →
  `begin_close()`.
- `src/shard/lifecycle.rs:126-135` `RequiredExit::drop`: a required worker exiting → `begin_close()`.
- `src/shard.rs:1876-1934` `begin_close` → `on_close` callback → `src/bootstrap.rs:505-513`
  `notifier.closed(&prefix, incarnation)` → `src/sharddir.rs:716-738` `notify_closed` evicts the
  resident and arms the holdoff. The next `resolve` opens a NEW incarnation (`src/sharddir.rs:492-509`
  waits for the retired incarnation to terminate, then opens).
- Rig: `ShardDirectory::retire(prefix, RetirementReason::Shutdown, |_, _| true)`
  (`src/shard_directory.rs:433-455`) + `clear_holdoff` (`src/shard_directory.rs:205-208`, cfg(test)).
- Bounce: ownership A→B→A. The not-owned branch (`:468-472`) runs only if a sealed page is READ while
  at B; a feed parked at the live tail reads no sealed page, so the cache survives the bounce holding
  the engine that B's fence closed.

What a read of the cached closed engine does (no `is_closed` check anywhere on this path):

- absorbed range → `src/application/read.rs:753-757` `plan.engine.history_partition()` →
  `src/shard.rs:2035-2041` returns `Err(slatedb::Error::closed("engine closed", Clean))` when
  `is_closed()`.
- unabsorbed range → `src/shard/record.rs:268-284` `engine.db.scan_with_options(..)` on a closed Db →
  `Err`.
- Both surface as `Err(String)` → `anyhow!(e)` (`src/sse/source.rs:466`) — NOT a `FatalSpanCutoff`.
- `src/sse/session.rs:447-464`: the `Err(e)` arm downcasts for `FatalSpanCutoff` (`:451`), otherwise
  `FEED_SOURCE_FAILED += 1` and `sleep(100ms)` (`:461-463`), then `while cursor < catchup_bound`
  (`:362`) re-reads the SAME `csrc`. Forever.

One production wrinkle: `src/shard/tail_ring.rs:97-114` `ring_read` has no closed check either, so
with `TAIL_RING_BYTES > 0` a stale handle's in-memory ring can still serve pages that fit the ring.
The stall then shows only for sealed spans larger than the ring (the normal case for a predecessor
with real history). Rigs run with `tail_ring_bytes: 0` (`src/config/cli.rs:563`, `ShardConfig`
default `src/shard.rs:1085`), so the DST below is not masked by the ring.

### 1.3 Cost of resolve + stream_handle per page (both are in-memory fast paths)

- `src/shard_directory.rs:237-260` `resolve`: `prefix_for` (hash→prefix string), `foreign_owner`
  (RwLock read + rendezvous pick, `src/ownership.rs:76-95`), serving-map `read()` + `HashMap::get` +
  `Arc::clone`, and `stamp_external` = 3 relaxed atomics (`src/billing.rs:1871-1879`). No I/O when
  the engine is resident. It also applies "possession yields to the ring" (`:261-264`) — the same
  authority the cache tried to shadow.
- `src/shard.rs:2407-2415` `stream_handle`: std mutex + `HashMap::get` + one atomic store when the
  handle is resident. When the handle was idle-evicted: ONE `db.get(tail_key)` point read (`:2416`),
  exactly what an HTTP page read pays.
- `src/history.rs:561-575` `KeyCache::put`: mutex + insert; refreshing per page is a benefit (the
  entry has a 900 s TTL and a long catch-up used to let it lapse).

The page read itself (history/tail scan) dominates by orders of magnitude; the per-page overhead
is negligible. Note the cached `Arc<StreamHandle>` also pinned the handle against idle eviction
(`src/shard.rs:2447-2459`, strong_count rule) — the "never evicts" half of the finding.

### 1.4 Nothing pins the cache

`grep -rn "LocalReader\|sealed_span_page\|owner_hint" src docs scripts` outside `src/sse/source.rs`
hits only the frozen legacy ledgers (`docs/quality/legacy-source.json:2604` and
`legacy-diagnostics*.json`, an old `allow(clippy::too_many_arguments)` row that already does not
match the current `expect`; leave it, normal PRs must not touch the baseline). `src/sse/source/tests.rs`
tests only `locate_in_spans`, `sig_compatible`, `FatalSpanCutoff`. No `sse::` unit test constructs a
`LineageSource` (it needs a `ReadService`, i.e. a rig).

---

## 2. Design

### 2.1 Smallest correct design (recommended): delete the cache

Remove `LocalReader`, the `local` field, its seed in `build`, the `local` parameter, the
`cached` guard, and the clear branch. The local arm becomes: `resolve(route, Adoption::External)`
→ `stream_handle(span.identity)` → `keys.put` → `ReadPlan::segment(..).execute()` on every page.
`NotOwner` still falls through to the remote path; other resolve errors still bail (retryable).

Why this is also the owner-first answer: the directory already owns "resident engine for this
prefix" and the engine already owns "resident handle for this identity", both with lifecycle-aware
invalidation (`notify_closed`/`retire` remove residents; `evict_idle_handles` ages handles). The
source's pair was a third copy of both with no lifecycle wiring, so it could only ever go stale.
"Ask the owners" == "no cache".

### 2.2 The alternative considered: keep a cache, validate it

Keep `LocalReader` but evict when `engine.is_closed()` (or key it on `EngineIncarnation`) and drop
the guard before the read. Rejected:

- It keeps a mutex and a third copy of lifecycle state, and adds a check-then-use race
  (closed-after-check) that the per-page resolve does not have.
- Mutation gate: the new validity branch `if cached.as_ref().is_some_and(|(e, _)| e.is_closed()) { *cached = None }`
  has a behaviourally EQUIVALENT mutant (condition → `true` = always refill = always correct) that no
  test can kill. With no disposition ledger, that is a guaranteed CI mutation failure.
- Serialization would still need a separate change (drop the guard before `execute`).

Deletion has no new branch, no new comparison, removes a lock and ~17 net lines.

---

## 3. Red test (fails on current code for the stated reason)

File: `src/dst/tests/livefeed_ownership.rs` (645 lines; +~75 → ~720, under the ceiling; not a
critical prefix, not a mutation source). Helpers reused from the file's existing imports:
`http_rig_owner`, `hub_append_lf`, `hub_sse_collect`, `lf_connect`, `lf_record_and_status`,
`split_and_await`, `preq`, `PRISMA_KEY`, `mem`, and the file-local `wait_for_feed_teardown`.
No new imports. Place it after `livefeed_parked_live_session_is_cut_off_by_engine_close`.

```rust
/// Review rank 16 (red): a sealed predecessor's engine that CLOSES and
/// REOPENS under the SAME owner — a storage-fault close, an ownership
/// bounce that comes back, a rig retirement — must not leave the feed's
/// lineage source reading the closed engine forever. The source used to
/// cache the (engine, handle) pair of the last locally served page and
/// cleared it only when ownership moved AWAY; with ownership unchanged
/// every later catch-up page read the closed db, failed, and the session
/// retried it every 100 ms without end. A sealed page now resolves the
/// directory's resident engine on every read, so the reopened engine
/// serves the next page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_reopened_sealed_span_serves_catch_up() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xreopen",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xreopen", r#"{"r":0}"#).await;
    hub_append_lf(addr, "xreopen", r#"{"r":1}"#).await;
    // Seg 0 seals at cap 2; the "" lane continues in the high child,
    // whose route is salted onto a DIFFERENT prefix than the parent
    // (topology.rs split), so the sealed span has an engine of its own.
    split_and_await(&state, "xreopen", 0).await;
    let sref = state.deployment.raw_adapter_sref("xreopen");
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let child_seg = desc.resolve_segment("").seg_id;
    let p_parent = state
        .shards
        .prefix_for(&desc.segment_route_by_id(0).unwrap());
    let p_child = state
        .shards
        .prefix_for(&desc.segment_route_by_id(child_seg).unwrap());
    assert_ne!(p_parent, p_child, "the sealed span must have its own engine");
    hub_append_lf(addr, "xreopen", r#"{"r":2}"#).await;

    // sub1 establishes the feed; its catch-up serves the sealed span
    // LOCALLY (this is where the old code filled its cache).
    let mut sub1 = lf_connect(addr, "xreopen", "?cursor=beginning").await;
    let (a1, eof1) =
        hub_sse_collect(&mut sub1, 15, |t| lf_record_and_status(t, "\"r\":2")).await;
    assert!(
        !eof1 && a1.contains("\"r\":0") && a1.contains("\"r\":2"),
        "sub1 established through the sealed span:\n{a1}"
    );

    // The sealed span's engine closes under the SAME owner and becomes
    // reopenable at once. The close is joined so the old db is CLOSED
    // before the next page (a still-open db could serve it by luck).
    let retired = match state.shards.retire(
        &p_parent,
        crate::shard_directory::RetirementReason::Shutdown,
        |_, _| true,
    ) {
        crate::shard_directory::RetireOutcome::Retired(engine) => engine,
        _ => panic!("the sealed span's engine must have been resident"),
    };
    assert!(retired.is_closed(), "retirement closes the sealed span's engine");
    for _ in 0..500 {
        if retired.termination_complete() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        retired.termination_complete(),
        "the retired engine must close cleanly so the prefix can reopen"
    );
    state.shards.clear_holdoff(&p_parent); // a reopen is allowed at once
    assert!(!state.shards.is_open(&p_parent));

    // sub2 joins the SAME feed (sub1 keeps it alive, so the SAME lineage
    // source serves) and catches up from the beginning: the sealed page
    // must come from the REOPENED engine, not spin on the closed one.
    let mut sub2 = lf_connect(addr, "xreopen", "?cursor=beginning").await;
    let (a2, eof2) =
        hub_sse_collect(&mut sub2, 15, |t| lf_record_and_status(t, "\"r\":2")).await;
    assert!(
        a2.contains("\"r\":0") && a2.contains("\"r\":1") && a2.contains("\"r\":2"),
        "a catch-up through a reopened sealed span must serve, not retry a closed engine forever:\n{a2}"
    );
    assert!(!eof2, "a same-owner reopen is never a cutoff:\n{a2}");
    assert!(
        state.shards.is_open(&p_parent),
        "the page reopened the sealed span's engine"
    );
    drop(sub1);
    drop(sub2);
    wait_for_feed_teardown(&state, 300).await;
}
```

Expected failure on current code (71345c03), at the sub2 assertion after the bounded 15 s collect:

```
thread 'dst::dst_tests::livefeed_ownership::livefeed_reopened_sealed_span_serves_catch_up' panicked at src/dst/tests/livefeed_ownership.rs:NNN:
a catch-up through a reopened sealed span must serve, not retry a closed engine forever:
HTTP/1.1 200 OK
...<response head, then only keep-alive comments / no `event: data`>
```

Why it is red and not flaky: sub2's source is the feed's existing `LineageSource` (reconcile →
`AlreadyCurrent`); `owned_here` is true (none-ring rig); `cached` is `Some(closed engine)`; the
page read fails through `history_partition()` (`engine closed`) or the closed Db scan; the session
retries every 100 ms and never emits `"r":0`. The termination barrier guarantees the old Db is
closed before the first page. Every wait is bounded (15 s collects, 5 s termination poll, 3 s
teardown); the test cannot hang (no mutation TIMEOUT).

Why it is green after the fix: the page calls `resolve` → `get_or_open` (the retiring incarnation
is `Stopped`, holdoff cleared) → a new engine opens on the in-memory store → `stream_handle` loads
seg 0's tail (`durable.next == 2`) → the page serves `r:0,r:1` from history, then the live child
serves `r:2` and the status control.

Controls (run once, not committed): (a) revert only the `sealed_span_page` hunk → the test fails
again with the same message; (b) skip the retirement → the test passes on both revisions
(the split/lineage path itself is already covered by `livefeed_owner_movement_...` phase 3).

A unit test at the `sse::` layer is not practical: `LineageSource::build` needs a `ReadService`
(registry over an object store, directory with a slatedb opener, peer, ownership, key cache,
topology), i.e. the rig `http_rig_build` already is.

---

## 4. Code change, per file

### 4.1 `src/sse/source.rs` (988 lines, under the 1,000 ceiling; net −17 → ~971)

Budget check: this file is NOT ceilinged (988 ≤ 1000) but is 12 lines from the cliff; the change
is net negative. No verbatim-move commit is needed; one commit.

(a) Delete lines 180-183 (the `LocalReader` doc comment, type alias and the blank line after it).

(b) `SpanReader::Sealed` doc, lines 188-195 — line-neutral rewording of the parenthetical:

```rust
    /// A SEALED span, OWNERSHIP-DYNAMIC (round-11.2): every page
    /// resolves the CURRENT effective owner first — local when this
    /// instance owns the shard (the directory's resident engine and
    /// its handle, looked up per page, never cached here), remote
    /// otherwise via the typed one-redirect protocol. `owner_hint`
    /// remembers the last owner that served a page; a successful
    /// redirect updates it. This also covers a predecessor that was
    /// local at build time and later moved away — and one that moved
    /// TO this instance, or that closed and reopened here.
```

(c) Delete line 200 `local: LocalReader,`.

(d) Delete line 332 `local: tokio::sync::Mutex::new(None),` in `build`.

(e) `sealed_span_page` (lines 400-421): keep the doc; RE-DECIDE the reason (the current text says
"caches the reader", which becomes false); the four lints stay because they still fire (see the
line budget below). Parameter `local` removed (7 inputs incl. `self`, still > 5).

```rust
    /// One sealed-span page (round-11.2, ownership-dynamic): local
    /// when this instance owns the shard — through the directory's
    /// resident engine and the engine's resident handle, resolved on
    /// EVERY page so a closed and reopened engine is never read twice
    /// — otherwise the typed remote protocol with at most one verified
    /// redirect. Fatal outcomes ride `FatalSpanCutoff`; retryables
    /// stay anyhow errors (the session's bounded-backoff retry).
    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::excessive_nesting,
        clippy::unwrap_used,
        reason = "LineageSource::sealed_span_page; a sealed span's page resolves the current owner on every read and serves through the directory's resident engine or through one redirect, and a poisoned hint may hold a half-recorded owner that could route the next page to the wrong instance; a request struct, a split, a flattened resolution, a reader cached across pages or a recovered hint would separate the page from the owner resolution it must repeat"
    )]
    async fn sealed_span_page(
        &self,
        span: &LineageSpan,
        route: &[u8; 16],
        target: &crate::application::read_remote::InternalTarget,
        owner_hint: &std::sync::RwLock<Option<String>>,
        local_from: u64,
        budget: usize,
    ) -> anyhow::Result<crate::application::read::ReadPage> {
        use super::feed::SourceCutoff;
        if owned_here(&self.state, route) {
            match self
                .state
                .shards
                .resolve(route, crate::shard_directory::Adoption::External)
                .await
            {
                Ok(engine) => {
                    let handle = engine
                        .stream_handle(span.identity)
                        .await
                        .map_err(|e| anyhow::anyhow!("stream handle: {e}"))?;
                    self.state
                        .keys
                        .put(span.identity, self.key.clone(), self.epoch);
                    return crate::application::read::ReadPlan::segment(
                        &self.key,
                        &self.epoch,
                        &handle,
                        &engine,
                        crate::application::read::ReadRange::bounded(
                            local_from,
                            span.cap.unwrap_or(u64::MAX),
                        ),
                        self.rk_filter.as_deref(),
                        budget,
                        crate::shard::Deliver::Durable,
                    )
                    .execute()
                    .await
                    .map_err(|e| anyhow::anyhow!(e));
                }
                // Ownership raced away between the check and the
                // open: fall through to the remote path below.
                Err(crate::shard_directory::ResolveError::NotOwner { .. }) => {}
                Err(error) => {
                    anyhow::bail!("sealed span engine unavailable: {error:?}")
                }
            }
        }
        // REMOTE: the owner is the hint, or the ring's current answer.
        let owner = {
            ... unchanged from line 474 to the end of the function ...
```

The reason string is three `;`-separated parts, no `;` or `"` inside a part (regex
`"[^";]+;[^";]+;[^";]+"` in `scripts/quality/source_rules.py:258`). Changing the reason changes the
contract identity `(path, qualified, kind, value)` (`source_rules.py:196`), so the ratchet takes a
fresh baseline for this function (`exception_growth`, `:207-208`) — which is REQUIRED here because
`ReadPlan::segment(.., &handle, &engine, ..)` is a new ordinary-call fingerprint (tokens differ from
the old `handle, engine`) and would otherwise be "accepted exception grew". Everything else in the
function only shrinks.

Lint-fulfilment budget for `sealed_span_page` (unfulfilled expectations are DENIED, so each of the
four must still fire):
- `too_many_arguments`: 7 inputs (self + 6) > threshold 5. Fires.
- `too_many_lines`: today 140 code lines (145 physical, comment-only 442, 443, 469, 470, 473).
  The local arm (423-472) goes 50 → 39 physical (46 → 37 code). New ≈ 131 code lines (≈ 119 if
  Clippy counts only inside the body braces). > 100. Fires.
- `excessive_nesting` (threshold 4): the remote `let owner = { match hinted { _ => { ... .ok_or_else(|| { anyhow!(..) })? } } }`
  keeps a depth-5 block (impl → fn → let-block → match-arm block → closure block). Fires. The new
  local arm tops out at depth 4 (impl → fn → if → match-arm block) and is irrelevant to fulfilment.
- `unwrap_used`: `owner_hint.read().unwrap()` (`:475`) and `.write().unwrap()` (`:505`) stay. Fires.

(f) `read_batch` (impl `FeedSourceRead for LineageSource`, expect at lines 567-571 —
`too_many_lines`, `unwrap_used`): remove `local,` from the pattern (line 622) and from the call
(line 629):

```rust
                SpanReader::Sealed {
                    route,
                    target,
                    owner_hint,
                } => {
                    self.sealed_span_page(
                        span,
                        route,
                        target,
                        owner_hint,
                        local_from,
                        budget.remaining(),
                    )
                    .await?
                }
```

Ratchet for the impl-level exception: unchanged reason; metrics only fall (two fewer `path` facts,
`syntax_facts` −2, `scope_lines` −2). `sealed_span_page` is a method call, not an `ExprCall`, so it
is not an ordinary-call fingerprint. `read_batch` stays at ~113 code lines (> 100), so
`too_many_lines` still fires for the impl; `unwrap_used` sites in `frontier`/`closed`/`logicalize`
are untouched.

(g) `build` (expect `excessive_nesting`, lines 277-280): only the field literal is removed; reason
unchanged; metrics fall.

Nothing else in the file changes. `tokio::sync::Mutex` was referenced by path only (no `use` to
remove). `idle_notify: tokio::sync::Notify` stays.

Line accounting: (a) −4; (b) +1 (8 → 9 doc lines); (c) −1; (d) −1; (e) doc 5 → 7 (+2), the
`local` parameter −1, the local arm 50 → 39 (−11); (f) −2. Net −17 ⇒ 988 → ~971 lines. If
rustfmt wraps differently the total moves by a line or two; re-run `wc -l` and keep it ≤ 1,000
(it must not cross the cliff, and a net-negative result is the goal).

### 4.2 `src/dst/tests/livefeed_ownership.rs`

Add the test from §3 (≈ 75 lines) after `livefeed_parked_live_session_is_cut_off_by_engine_close`
(line 372). 645 → ~720 lines. No new imports; `RetirementReason::Shutdown` is available in test
builds (`src/shard_directory.rs:62-69`), `clear_holdoff`/`termination_complete` are `#[cfg(test)]`.

### 4.3 `src/sse/session.rs`, `src/sse/feed.rs`, `src/http/read.rs`

No change. The 100 ms retry arm is correct once the source self-heals; it is what makes the
first page after a reopen succeed.

### 4.4 Commit shape

One commit (no move needed). Suggested subject, in the repo's claim style:

`A sealed span's page resolves its engine on every read, never through a reader cached across pages`

Body: the mechanism (§1.2 in three sentences), "Red first, src/dst/tests/livefeed_ownership.rs,
at the sub2 assertion, not in setup", the deleted state (mutex + pair), file sizes
(source.rs 988 → ~971), the re-decided reason for `sealed_span_page`, and the gate line
(`QUALITY_OK; suite N/0`) only after actually running it. End with the Co-Authored-By line.

---

## 5. Ledgers and docs

| Item | Action |
| --- | --- |
| `docs/refactor/test-inventory.json` | Regenerate: `python3 scripts/test-inventory.py --write` (adds one entry `livefeed_reopened_sealed_span_serves_catch_up`, file `src/dst/tests/livefeed_ownership.rs`, `scenarios: []`). Then `--check` and `--self-test` must pass. The suite floor rises by one automatically. |
| `docs/refactor/test-additions.json` | Not needed (only `--compare` consumes it; recent new-DST commits `2374751d`, `efe12b2e`, `668bc80c`, `6faf4757` touched only test-inventory). |
| `docs/refactor/test-scenario-map.json` | No change (the test is unmapped, like `certification_seal_publish_delay_widens_the_gap`). `scenario-map-report.py --check` unaffected. |
| `docs/refactor/review-mechanisms.json` | No change (no pinned mechanism test or fixture touched; `livefeed_ownership.rs` has no pin). |
| `docs/refactor/WIRE-MATRIX.md` | No change: no wire behaviour changes (same frames, same statuses; the local page is the same `ReadPlan::segment` with the same `Adoption::External` resolve the fill path used). |
| `docs/LIVE-FEED.md` | No change: the SSE contract is unchanged. |
| `docs/quality/owners.json` | No change: no spawn, no env read, no macro DSL, no glob import added. |
| `docs/quality/legacy-*.json` | Do NOT touch (frozen baseline). The `sealed_span_page` `allow(too_many_arguments)` row there predates the `expect` form and is already inert. |
| `scripts/quality/mutation_owners.py` | Conditional, see §6.3. Only if the local `--list` shows a viable diff-scoped mutant in `src/sse/source.rs` that `sse::` tests cannot kill, extend the row to `owner('sse_source', 'src/sse/source.rs', 'sse:: dst_tests::livefeed_ownership::livefeed_reopened_sealed_span_serves_catch_up')` (a single test name keeps each mutant run under the 90 s timeout; the whole module would not). |

Pre-push checklist (the traps this session hit): `wc -l src/sse/source.rs src/dst/tests/livefeed_ownership.rs`;
`scripts/quality.sh` (fmt, clippy `-D warnings`, rustdoc, source gate incl. the exception ratchet,
test-inventory `--check`); `python3 scripts/quality/verification_plan.py --out target/quality-mutations`
against the real merge base and then the mutation driver (or at least
`cargo mutants --list --json --in-diff target/quality-mutations/pr.diff --file src/sse/source.rs --package streams-slate`);
run the new DST test on the UNFIXED tree first and paste the exact red message into the commit body.

---

## 6. What could go wrong

### 6.1 Wire compatibility
None. Bytes on the wire are identical; the sealed page is produced by the same `ReadPlan::segment`
over the same identity/range/budget. The remote protocol (`remote_span_page`, owner hint, redirect
cutoffs) is untouched.

### 6.2 Fleet skew and ownership races
- Remote instances see no change. A mixed fleet (old/new binaries) behaves identically on the wire.
- `owned_here` true but the ring moves between the check and `resolve` → `NotOwner` → remote
  fall-through (unchanged, lines 442-444 today).
- A prefix that is resident here but the ring assigned elsewhere: `resolve` retires it
  ("possession yields to the ring", `src/shard_directory.rs:261-264`) — but that branch is reached
  only when `owned_here` is false, in which case we do not call `resolve` at all (same as today).
- Per-page `stamp_external` re-marks the engine customer-resident on every page; strictly more
  accurate for the R29/R30 sweep custody rule (`install_custody` declines any engine with external
  history, `src/billing.rs:1885-1906`). `ADOPTION_SEQ` is a u64; no overflow concern.
- Anti-flap: after a same-owner close the first pages after the holdoff/termination window get
  `ResolveError::Opening` → `bail!` → the 100 ms retry — bounded by the holdoff (3 s base,
  `src/sharddir.rs:47`), then the reopen serves. This is the intended shape (round-11.x transient =
  retry, never a feed-wide cutoff).

### 6.3 Mutation leg
The diff adds no branch and no comparison. Diff-scoped mutants (`--in-diff`) in `src/sse/source.rs`:
- FnValue for `sealed_span_page` (`Ok(Default::default())` on `ReadPage`, no `Default`) → unviable.
- FnValue for `read_batch` (`SourceBatch` has no `Default`) → unviable.
- FnValue for `build` (`Arc<LineageSource>` has no `Default`) → unviable.
- No binary/unary operators in the changed lines.
cargo-mutants is pinned at 27.1.0 (`quality-tools.toml`); if that version emits genres beyond
FnValue/BinaryOperator/UnaryOperator (e.g. condition or match-arm mutants) inside the changed hunks,
the `sse::` filter cannot kill them (no `sse::` test builds a lineage). The DST test kills
`owned_here → false`/`!owned_here` (the page goes remote, the none-ring rig has no owner → the
"ownership indeterminate" retry loop → no `"r":0`). An `owned_here → true` mutant is behaviourally
equivalent on every path (`resolve` is the authority and refuses foreign prefixes with `NotOwner`,
falling through to remote) — if such a mutant ever appears it must be handled by shrinking the
condition, not by a test. Decide from the `--list` output before pushing (§5 last row).

### 6.4 Flakiness of the red test
- `p_parent != p_child` is by construction (`src/application/topology.rs:502-521` salts the high
  child's route off the parent's prefix; the `""` lane lands in the high child — the existing
  `livefeed_owner_movement_one_redirect_and_typed_cutoffs` asserts the same inequality at line 54).
- The termination barrier is required for determinism: without it a first page could race the
  old Db's close and succeed on the unfixed code. `termination_complete()` is `Phase::Stopped`,
  which is also what `get_or_open` requires before reopening (`src/sharddir.rs:501-509`), so a
  failing barrier surfaces as an assertion rather than a stall.
- Two subscribers on one feed in an owner rig is the established pattern (movement test phase 4).
- Worst-case wall time ≈ 40 s (15 s + 15 s collects + 5 s barrier + 3 s teardown); no unbounded
  wait anywhere.

### 6.5 Behavioural side effects of no cache
- The sealed span's handle is no longer pinned in the engine's map; it may be idle-evicted between
  pages and reloaded with one point read. That is the normal HTTP-read posture and reclaims memory
  the finding called out.
- Concurrent sessions now read the same sealed span in parallel. Sealed data is immutable and every
  read is independent; `owner_hint` (RwLock) is the only remaining shared state in `Sealed`.

### 6.6 Related, out of scope (record as follow-ups, do not fix here)
- `SpanReader::LiveLocal { engine, handle }` (`src/sse/source.rs:205-210`) and `SingleSource`
  (`src/http.rs:3138`) hold an engine captured at build. A same-owner close of the LIVE tail's
  engine (storage fault, not an ownership move) leaves them reading a closed engine: `begin_close`
  wakes parked readers (`src/shard.rs:1905-1913`) but `cut_off()` sees `owned_here == true` and
  re-parks. Sessions then hang until a descriptor change or the lease ends. Same family, separate
  item.
- `ring_read` has no closed check (`src/shard/tail_ring.rs:97-114`); harmless once nothing holds a
  stale handle, but worth a one-line guard if a future reader ever caches handles again.

---

## Skeptic corrections

Checked first-hand against slate @ 71345c03 (read-only; no cargo, no git writes). Verdict:
**sound with corrections**. Every named symbol and signature exists as stated, the red test
compiles against the existing helpers and is red for the stated reason, the change adds no
branch and therefore no mutant, and the ratchet route (re-decide the reason) is the sanctioned
one. The corrections below are concrete; C1, C4, C6 and C7 change what the implementer does.

### C1. Mutation genres — cargo-mutants 27.1.0 DOES delete match arms; resolve §6.3's hedge

- `~/.cargo/registry/src/index.crates.io-*/cargo-mutants-27.1.0/src/mutant.rs:23-35`: the
  genres are `FnValue`, `BinaryOperator`, `UnaryOperator`, `MatchArm`, `MatchArmGuard`,
  `StructField`. `src/visit.rs:645-700`: a `MatchArm` mutant (delete the arm) is generated only
  for a `match` that has a `_` (`Pat::Wild`) arm, and never for an arm with a guard;
  `MatchArmGuard` replaces each `if` guard with `true` and with `false`; `visit.rs:703-` :
  `StructField` only for struct literals with a `..base`. Closure bodies are not mutated
  (`visit_expr_call`/`visit_expr_method_call` at `visit.rs:392-423` only honour `skip_calls`).
- Applied to the diff: the new local-arm `match self.state.shards.resolve(..)` has arms
  `Ok(engine)`, `Err(ResolveError::NotOwner { .. })`, `Err(error)` — no `_` → no `MatchArm`
  mutant. `build`'s `SpanReader::Sealed { route, target, owner_hint }` has no `..` → no
  `StructField`. The remote path's `Some(o) if o != self.state.ownership.instance() => o`
  (today `src/sse/source.rs:477`) yields two `MatchArmGuard` mutants, but `--in-diff` keeps a
  mutant only when its own span touches an affected line (`src/in_diff.rs:117-131`), and the
  affected lines are the inserted lines plus the first surviving line after a deletion
  (`in_diff.rs:213-247`); line 477 is neither. So the selected set is exactly the three
  `FnValue` mutants — `sealed_span_page`, `read_batch`, `build` — all unviable: `ReadPage`
  (`src/application/read.rs:108`), `SourceBatch` (`src/sse/feed.rs:80`) and `LineageSource`
  (`src/sse/source.rs:231`) derive no `Default`. cargo-mutants exits 0 when every selected
  mutant is unviable and `scripts/quality/mutation_driver.py:152` requires only exit 0.
  Expected leg line: `Mutation verification executed 3 selected mutant(s) across 1 registered
  owner(s).` with 3 unviable / 0 missed / 0 timeout.
- **Delete the conditional last row of the §5 table.** Do NOT extend
  `owner('sse_source', 'src/sse/source.rs', 'sse::')` with the DST test name. It is not needed
  for this diff, and it would put a rig test (two 15 s collects, worst case ≈ 40 s) inside the
  per-mutant `--timeout 90` (`mutation_driver.py:52`) of every future `sse_source` run and of
  the nightly full-owner bucket, where a slow runner turns it into a TIMEOUT (a failure, not a
  caught mutant).

### C2. Nesting fulfilment — the plan's claim is right; record the evidence

The pinned Clippy counts `impl`/`trait`/inline-`mod` items as nesting levels:
`ShardDirectory::shutdown` (`src/shard_directory.rs:377-412`) carries a fulfilled
`#[expect(clippy::excessive_nesting)]` whose deepest block is impl → fn → `loop` → `if` →
`return if … { } else { }`, i.e. depth 5 only if `impl` counts. Out-of-line `mod x;` files do
not count (the `for … { if … { break } }` at `src/dst/tests/livefeed_ownership.rs:272-280`
compiles clean under `-D warnings`). After the change `sealed_span_page` keeps exactly ONE
depth-5 site: impl → fn → `let owner = {` → `_ => {` → `.ok_or_else(|| { … })` (today lines
474-491). Do not tidy that closure into a braceless body (`|| anyhow::anyhow!(..)`) while here:
it would remove the only depth-5 site, leave the `excessive_nesting` expectation unfulfilled,
and `unfulfilled_lint_expectations` is DENIED (compile error).

### C3. Line accounting (harmless; the numbers in §4.1/§4.2 are off)

- The new local arm is 40 physical lines (`if owned_here` through its closing `}`; count the
  plan's own snippet §4.1(e)), not 39. Net for `src/sse/source.rs` is −16 → 972, not 971.
- The test block as written is ~97 lines (10 doc + 1 attribute + 85 fn + 1 blank), so
  `src/dst/tests/livefeed_ownership.rs` goes 645 → ~742, not ~720. Both stay under the cliff:
  `limit = min(max(1000, legacy), max(1000, merge-base))` (`scripts/quality/source_rules.py:226-228`)
  with legacy `lines` 987 for source.rs and 676 for the test file → 1,000 for both.

### C4. rustfmt rewraps two lines of the test as written

`    let (a1, eof1) = hub_sse_collect(&mut sub1, 15, |t| lf_record_and_status(t, "\"r\":2")).await;`
is 98 columns → rustfmt keeps it on ONE line (the existing line 137 has the same shape); same
for the `(a2, eof2)` line. Committing the plan's two-line form fails `cargo fmt --all -- --check`
(the first step of `scripts/quality.sh`). Run `cargo fmt --all` before C6.

### C5. The red-determinism rationale for the barrier is wrong on the path the rig takes; keep the barrier

§3/§6.4 say the termination barrier is required because "a first page could race the old Db's
close and succeed". Not on the rig: its absorber (`src/dst/tests/fixture_http.rs:314-320`,
threshold 1 byte / 1 ms / 20 ms tick) absorbed [0,2) long before the split, so sub2's page goes
through `decode_history_range` → `engine.history_partition()` (`src/application/read.rs:753-757`),
which refuses on `is_closed()` (`src/shard.rs:2037-2041`) — a flag stored synchronously inside
`begin_close` (`src/shard.rs:1883`) before `retire` returns (`src/shard_directory.rs:453`). Red
never depends on the Db having finished closing. The barrier still earns its place for the
FIXED run: until the retiring incarnation reaches `Phase::Stopped`, `get_or_open` answers
`shard_closing` (`src/sharddir.rs:501-508, 952-957`) → `ResolveError::Opening` → `bail!` →
100 ms retries; the barrier just removes that retry tail from the timing budget. Reword the
comment above the retire ("joined so the old db is CLOSED before the next page") to say that.

### C6. The inventory hash is over the FINAL text

`docs/refactor/test-inventory.json` entries carry `function_sha256`
(`git show 2374751d -- docs/refactor/test-inventory.json`). Run
`python3 scripts/test-inventory.py --write` only after `cargo fmt` and the last edit of the test
body; any later touch, even a comment, flips `--check` red.

### C7. The serialization half has no red test — state it in the commit body

The finding has two halves; the red test covers only "never evicts". "Holds a tokio Mutex across
the whole page read" cannot be made red deterministically: it needs a failpoint inside
`ReadPlan::execute` (`src/application/read.rs`, a critical prefix; `execute_segment` is
ratcheted under `too_many_lines`/`unwrap_used`, `read.rs:127-142`, so a failpoint there changes
call fingerprints and needs its own reason re-decision), and the rig's `open_park` cannot
distinguish either (with the engine not resident BOTH revisions block in the coalesced open —
the old one under the mutex, the new one without — with the same externally visible timing).
Write in the commit body that the lock removal is proven structurally (after the change
`grep -n Mutex src/sse/source.rs` returns nothing) and is not asserted by a timing test.

### C8. Source-gate merge-base trap (the "run CI's plan before push" trap)

`scripts/quality/common.py:20-29`: locally the exception ratchet and the file-growth ceiling
compare against `git merge-base HEAD origin/slate`; `verification_plan.py:377` diffs the WORKING
TREE against that base. Working DIRECTLY on slate: run `scripts/quality.sh` and
`verification_plan.py` with the change uncommitted or committed-but-unpushed (both compare to the
pre-change tree). After a push HEAD == origin/slate and `exception_growth` becomes vacuous —
a green local run then proves nothing about the ratchet.

### C9. The loom leg is selected (no action)

`src/sse` is in `LIFECYCLE_PREFIXES` (`scripts/quality/verification_plan.py:26`), so `plan.json`
records `loom: true` and the existing loom fixtures run in CI. Nothing to add: the change
removes a lock, it models none.

### C10. Doc wording (rustdoc is clean either way)

§4.1(e) doc: "resolved on EVERY page so a closed and reopened engine is never read twice" →
"resolved on EVERY page, so an engine that closed and reopened under the same owner is read
through its current incarnation".

### C11. One robustness line in the test (optional)

Before the retire add
`assert!(state.shards.is_open(&p_parent), "the sealed span's engine is resident after sub1's local page");`
so a setup surprise (e.g. a future split executor closing the parent — today
`src/application/topology.rs:74` resolves `Adoption::Internal` and never retires it) fails as a
named assertion rather than through the `_ => panic!` arm.

### Confirmations (no change needed)

- Signatures: `ShardDirectory::retire(&self, &str, RetirementReason, impl FnOnce(&Arc<ShardEngine>, EngineIncarnation) -> bool) -> RetireOutcome`
  (`src/shard_directory.rs:433-455`); `RetireOutcome::Retired(Arc<ShardEngine>)` (`:88-97`);
  `RetirementReason::Shutdown` usable in test builds (`:62-69`); `clear_holdoff` (`:205-208`,
  cfg(test)); `is_open` (`:324`); `ShardEngine::is_closed` (`src/shard.rs:1859`);
  `termination_complete` (`:1765`, cfg(test), = `Phase::Stopped` via `src/shard/lifecycle.rs:102`);
  `stream_handle(&self, [u8; 16]) -> Result<Arc<StreamHandle>, slatedb::Error>` (`:2407`, map hit =
  mutex + get + one atomic, miss = one `db.get`); `ReadPlan::segment(&StreamKey, &[u8;16],
  &Arc<StreamHandle>, &Arc<ShardEngine>, ReadRange, Option<&str>, usize, Deliver)`
  (`src/application/read.rs:36-45`); `resolve` fast path = prefix hash + `foreign_owner` RwLock
  read + serving-map read + `stamp_external` (`src/shard_directory.rs:237-260`, `src/billing.rs:1871-1879`).
  Every test helper exists with the used shape (`fixture_livefeed.rs:19,164,194,243,278`,
  `fixture_http.rs:765`, `livefeed_ownership.rs:616`), and the retire + `clear_holdoff` + reopen
  pattern inside one rig is established (`src/dst/tests/runtime_sweep.rs:86-98`,
  `fixture_http.rs:734-760`).
- Red mechanism: `FeedKey = (identity, selector)` (`src/sse/feed.rs:33-36`) so sub2 joins sub1's
  feed; `reconcile_locked` → `install_source` → `AlreadyCurrent` for an equal-length signature
  (`feed.rs:708-728`); `new_with_budget` sets `head = floor = src.frontier()` (`feed.rs:633,642-643`),
  so both subscribers' [0,3) catch-ups are private reads through the same `LineageSource` and can
  never be served from the retained ring; a refresh runs only when `src.closed()`
  (`src/sse/feed/drive.rs:162-165`), and the live child is open; the session's `Err(e)` arm is a
  flat 100 ms sleep with no bound (`src/sse/session.rs:447-464`). Rigs have
  `tail_ring_bytes: 0` (`HttpRigOptions::default` → `ShardConfig::default`, `src/shard.rs:1085`).
  `p_parent != p_child` is by construction: the high child's route is salted until its prefix
  differs (`src/application/topology.rs:502-521`) and the `""` lane's point is stream-independent
  (`src/registry.rs:745-747`), so the existing `assert_ne!` at `livefeed_ownership.rs:54` proves
  it for every stream.
- `SpanReader::Sealed` consumers: constructed at `source.rs:328`, destructured with `local` only
  at `:618-623`; every other match uses `{ .. }` (`:705,720,733,762`).
  `use crate::shard::{ShardEngine, StreamHandle}` (`:16`) stays used by `SingleSource` and
  `LiveLocal`. `tokio::sync::Mutex` appears by path only; `tokio::sync::Notify` remains.
- Ratchet: the contract identity is `(path, qualified, kind, attribute text)`
  (`scripts/quality/source_rules.py:196`); `exception_growth` skips identities absent from the
  merge base (`:207-208`); the new reason matches `"[^";]+;[^";]+;[^";]+"` (`:258`); call
  fingerprints are `target\t<whole expression tokens>` (`tools/quality-syntax/src/scan.rs:242-255`),
  which is why `&handle, &engine` is a new fingerprint and the re-decision is required.
  `read_batch`'s impl-level contract only loses facts (the `local` path fact, two scope lines).
  No active allowance references `src/sse/source.rs` beyond the `path = "source/tests.rs"` row
  (`docs/quality/source-allowances.json:742`); both active diagnostic ledgers are empty;
  `review-mechanisms.json` pins nothing in either touched file.
- Lint fulfilment after the change: `too_many_arguments` 7 inputs incl. `self` > 5;
  `too_many_lines` ≈ 123 code lines in the body (> 100); `excessive_nesting` per C2;
  `unwrap_used` at today's lines 475/505. `read_batch` stays ≈ 113 code lines (> 100).
- Architecture gate: `src/sse/source.rs` is an `sse_core_files` hard owner
  (`docs/refactor/architecture-policy.json:9`); the change adds no `AppState`/`axum`/`Response`/
  `HeaderMap` token (`scripts/architecture-gate.py:101-110`). MT audit: `registry.invalidate(&sref)`
  / `registry.get(&sref)` are not bare-name literals (`scripts/multitenancy-audit.sh` needs `("`).
- WIRE-MATRIX and LIVE-FEED.md mention neither the cache nor per-page resolution; no wire change.
- Scenario map `--check` only validates mapped tests (`scripts/scenario-map-report.py:192-223`);
  an unmapped new test passes. The test name is unique in the crate; the panic thread name is
  `dst::dst_tests::livefeed_ownership::…` (`src/dst/mod.rs:158`, `src/dst/dst_tests.rs:107`).
