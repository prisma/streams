# Feed sources pin one ShardEngine incarnation: a same-owner engine retirement stalls sessions on keep-alives

Repo: /Users/sorenschmidt/code/streams, branch slate @ 71345c03. Read-only verification; nothing edited.

## Verdict on the claim: CONFIRMED (first-hand), with one scoping correction

The stall is real and reachable in production. The one correction: `SweepEviction` is NOT a
same-owner replacement path for a feed's engine (the sweep declines every engine with an
external adoption stamp, and both feed sources resolve their live engine with
`Adoption::External`), so the reachable same-owner closes are the acker's `close_reason`
(fatal store error, or a fence from a sub-tick flap that returns ownership before the session
re-checks), `RequiredExit` (any required worker exits or panics) and explicit `Shutdown`
retirement. `FleetEviction` is paired with an override move, so it is already the WrongOwner
case. Ownership is only half the predicate.

## 1. Mechanism, with file:line evidence

Pinning:

- `src/sse/source.rs:20-32` `SingleSource { engine: Arc<ShardEngine>, handle: Arc<StreamHandle>, route, .. }`, built once at dispatch (`src/http.rs:3138-3149`) from the caller's `engine_for` (External resolve, `src/http.rs:558`).
- `src/sse/source.rs:206-210` `SpanReader::LiveLocal { route, engine, handle }`, built once in `LineageSource::build` (`src/sse/source.rs:336-351`, External resolve).
- `frontier()`/`closed()`/`advance_notify()` read the pinned handle only: `src/sse/source.rs:110-116, 122-124` and `698-735`.

The cut predicates check ownership only:

- `src/sse/source.rs:126-128` `SingleSource::cut_off`: `(!owned_here(..)).then_some(WrongOwner)`.
- `src/sse/source.rs:737-746` `LineageSource::cut_off`: `LiveLocal { route, .. } if !owned_here(..) => Some(WrongOwner)`, `_ => None`.
- `src/sse/source.rs:51-55` and `596-600`: both `read_batch` guards are `if !owned_here(..) { Err(FatalSpanCutoff(WrongOwner)) }`.
- `owned_here` (`src/sse/source.rs:215-217`) is `ownership.is_mine(prefix)`; `is_mine == foreign_owner.is_none()` (`src/ownership.rs:92-99`). A same-owner close never flips it.

The engine close is one-shot and same-owner:

- `ShardEngine::begin_close` (`src/shard.rs:1876-1934`): sets `closed = true` (line 1883) BEFORE `h.notify.notify_waiters()` on every resident handle (1905-1913), then runs `on_close` (1920-1922) which is `notifier.closed(prefix, incarnation)` (`src/bootstrap.rs:505-513`; DST rig identical, `src/dst/tests/fixture_http.rs:292-296`). `notify_closed` (`src/sharddir.rs:716-738`) evicts the resident from the serving map and arms the anti-flap holdoff (3 s base, 60 s cap, `src/sharddir.rs:45-51`). The comment at `src/shard.rs:1893-1904` states the wake contract: the woken session "re-checks ownership (owned_here)". Nothing re-checks the engine.
- Same-owner triggers: `acker_loop` on `status.close_reason` (`src/shard.rs:3149-3152`); `RequiredExit::drop` (`src/shard/lifecycle.rs:126-135`); `ShardDirectory::shutdown` (`src/shard_directory.rs:381-385`). `ShardEngine::is_closed()` exists and is `pub(crate)` (`src/shard.rs:1859-1861`). Handles held by a source are never idle-evicted (`src/shard.rs:2469-2486`, strong_count > 1), so the one wake always reaches the parked session.

What the parked session does with that wake (`src/sse/session.rs`):

- Loop top registers `src_wait` on the pinned handle's notify (496-498), `take_visible` -> `AtHead` (575), `cursor >= frontier` and `!closed` -> status branch (745-761, already reported, nothing sent), `cursor < frontier` false, then the park guard `cur_src.cut_off()` (856-866) -> `None` -> park (871-885). The park's only other wakes are the version watch (no publication), the generation watch (no swap) and `lease_watch.nap()` which is 3600 s for a lease-less session (`src/sse/auth.rs:292-298`). The body keeps emitting `: keep-alive` (`src/sse/auth.rs:493-511`). That is the stall.
- The feed's lifecycle never becomes `Gone` on its own: `Tail::Gone` only arises from `next_source()` on a CLOSED source or a fatal `read_batch` (`src/sse/feed/drive.rs:147-192`, `src/sse/feed.rs:925-943`).

Why a reconnect does not help:

- The reconnect resolves the REPLACEMENT engine through the same route (after the holdoff) and builds a fresh `SingleSource` with `span_sig() == [(seg, 0, None)]`; `FeedRegistry::subscribe` reconciles it into the EXISTING feed (`src/sse/registry.rs:98-107`) via `install_source`, which returns `AlreadyCurrent` for an equal-length signature (`src/sse/feed.rs:715-721`). The new session therefore reads and parks on the dead source too. While any session keeps the feed alive, every reconnect joins the corpse; a reconnect below the frozen head enters the catch-up loop (`src/sse/session.rs:362-466`) whose non-fatal read failures retry every 100 ms forever (447-464).

What a client sees on each `SourceCutoff` today: a nonterminal body end. `count_cutoff` (`src/sse/session.rs:138-151`) increments the typed counter, `FEED_TOPOLOGY_DISCONNECTS` bumps, the producer task returns, `GatedSseBody` yields `None` (`src/sse/auth.rs:489-492`) and the chunked body ends without a `sealed` control (the wire posture in `docs/LIVE-FEED.md:164-168`). `Tail::Gone(reason)` -> `DriveOutcome::IncarnationClosed(reason)` -> the same disconnect at 645-655 and 824-834. No terminal control, no status; clients resume from their cursor, and the gateway/route serves the replacement engine once the holdoff lapses (503 retryable `Opening` before that, `src/shard_directory.rs:287-294`).

## 2. Design

### A1 (recommended, smallest correct): ownership first, then the pinned engine's own close

One predicate, used by both `cut_off` impls and both live-tail `read_batch` guards:

```rust
fn live_tail_cutoff(owned: bool, engine: &ShardEngine) -> Option<SourceCutoff> {
    if !owned {
        return Some(SourceCutoff::WrongOwner);
    }
    engine.is_closed().then_some(SourceCutoff::EngineRetired)
}
```

Why `is_closed()` and NOT the serving map (`ptr_eq(shards.open(prefix), engine)`): `begin_close`
sets `closed` before the one-shot `notify_waiters`, but the serving-map eviction happens
AFTER it (via `on_close`). A woken session that consulted the map could still find the dead
resident, re-park, and never wake again. The flag is the only signal ordered before the wake.

Why one bool + `&ShardEngine`: `clippy.toml` `max-fn-params-bools = 1` makes a two-bool
predicate a `-D warnings` failure, and a new `#[expect(fn_params_excessive_bools)]` for a
four-row table is not worth an exception. The DST tests below cover every row that matters
(owned+open -> None: every parked-session test; owned+closed -> EngineRetired: A/B/C;
moved+closed -> WrongOwner: the existing round-11.4 test; moved+open -> WrongOwner: the
existing movement phases).

Wire: the existing typed disconnect (nonterminal EOF, no terminal control). New typed reason
`SourceCutoff::EngineRetired`, counter `FEED_CUTOFF_ENGINE_RETIRED`, `count_cutoff` arm,
`/v1/debug/load` key `sse_livefeed.cutoff_engine_retired` (flat sibling of the other
`cutoff_*` keys: `bench/canary/livefeed-canary.mjs:673-690` and `bench/fleet/livefeed-cert.mjs`
scrape flat keys with `?? 0`, so an additive sibling is the only compatible shape).

The read guard is load-bearing, not cosmetic: (a) the catch-up loop retries a non-fatal read
failure every 100 ms forever (`session.rs:447-464`), so a joiner below the frozen head on a
dead engine spins until the client leaves; (b) a fatal `read_batch` poisons the feed
(`Lifecycle::Gone(EngineRetired)`, `feed.rs:930-939`) so late joiners cut on their first drive;
(c) it keeps the round-11.2 posture "typed cutoff, never a stale local read".

### A2 (owner-first alternative): the directory owns "replaced", the feed re-sources in place

Make `LiveFeed::reconcile_locked` accept an equal-length compatible signature when the
current source's live tail is retired, bumping the generation so parked sessions re-snapshot
onto the replacement engine without a disconnect (same cursor space: the durable frontier is a
property of the stream, not the engine). The cutoff (A1) is still required, because a feed
whose sessions never reconnect brings no fresh source. A2 therefore adds a second install
path in `feed.rs` (ceilinged at 1200), a new `InstallOutcome`, an un-poisoning rule for
`Lifecycle::Gone`, and head-vs-new-frontier reasoning, to save one reconnect per parked
session on an event that already costs every producer a `Moved` rejection.

Recommendation: A1 now. A2 only if field counters show engine-retire churn with large fan-out.
The engine already is the owner of its own lifecycle; the source asking it `is_closed()` at
the wake it fired is the owner's signal, not a poll.

## 3. Red tests (near-complete Rust)

All three DST tests go in `src/dst/tests/livefeed_ownership.rs` (645 lines; +~175; not a
mutation source; regenerate the inventory). They reuse `http_rig_owner`, `preq`, `PRISMA_KEY`,
`hub_append_lf`, `hub_sse_collect`, `lf_connect`, `lf_record_and_status`, `split_and_await`,
`wait_parked`, `sse_head` (add `sse_head` to the `fixture_livefeed` import) and the file-local
`wait_for_feed_teardown`. Every wait is bounded (10 s collects, 300x10 ms teardown, 200x25 ms
`wait_parked`): each red outcome is an assertion, never a hang (mutation timeout = 90 s per
run, so keep the deadlines as written).

Red procedure (the counter must exist for the tests to compile): apply the scaffolding hunks
first (`EngineRetired` variant, `FEED_CUTOFF_ENGINE_RETIRED`, the `count_cutoff` arm, the
JSON key, the owners.json row), run the three tests -> the failures stated below; then apply
the four guard hunks + the two reason re-decisions -> green. If two commits are wanted, that
is the split; one commit is fine on slate as long as the red run is recorded.

### Test A: parked single-segment session; engine self-closes under the same owner; reconnect lands on the replacement

```rust
/// Review rank 14 (red): a live-tail source pins ONE engine incarnation.
/// When that engine closes under the SAME owner (fatal store, worker
/// exit, sub-tick flap) the parked session was woken once by begin_close,
/// re-checked ownership only, and re-parked on a dead handle's notify:
/// keep-alives forever, and every reconnect rejoined the dead feed
/// (install_source refuses an equal-length signature). The session must
/// take the typed EngineRetired cutoff (nonterminal EOF), the feed must
/// tear down, and a reconnect through the same route must land on the
/// replacement engine and see new data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_under_the_same_owner_cuts_a_parked_session() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xret",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xret", r#"{"p":0}"#).await;
    let sref = state.deployment.raw_adapter_sref("xret");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state.shards.prefix_for(&desc.resolve_segment("").shard_route);
    let load = |body: &[u8]| serde_json::from_slice::<serde_json::Value>(body).unwrap();
    let (st, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert_eq!(st, 200);
    let exposed_before = load(&body)["sse_livefeed"]["cutoff_engine_retired"]
        .as_u64()
        .expect("the engine-retired cutoff is exposed beside the other cutoff reasons");
    let retired_before = crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
        .load(std::sync::atomic::Ordering::Relaxed);

    let mut sub = lf_connect(addr, "xret", "?cursor=now").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(a0.contains("\"upToDate\":true") && !eof0, "parked at the live tail:\n{a0}");

    // The acker's own close path: ownership does NOT move.
    let engine = state.shards.open(&prefix).expect("the live tail's engine is resident");
    engine.begin_close();
    assert!(state.shards.open(&prefix).is_none(), "the close evicted the resident");
    assert!(state.ownership.is_mine(&prefix), "same owner throughout");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "the parked session must take the typed EngineRetired cutoff, not keep-alive forever:\n{a1}"
    );
    assert!(
        !a1.contains("event: data") && !a1.contains("\"sealed\":true"),
        "no stale data, no terminal:\n{a1}"
    );
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
            .load(std::sync::atomic::Ordering::Relaxed)
            > retired_before,
        "the cutoff must be classified EngineRetired"
    );
    let (_, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert!(
        load(&body)["sse_livefeed"]["cutoff_engine_retired"].as_u64().unwrap() > exposed_before,
        "the typed reason must be visible on /v1/debug/load"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;

    // The route reopens (holdoff skipped, test-only) and a resume lands
    // on the REPLACEMENT engine: new appends reach the new session.
    state.shards.clear_holdoff(&prefix);
    let mut again = connect_ready(addr, "xret", "?cursor=now").await;
    let (b0, eof) = hub_sse_collect(&mut again, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(b0.contains("\"upToDate\":true") && !eof, "resume on the replacement:\n{b0}");
    hub_append_lf(addr, "xret", r#"{"p":9}"#).await;
    let (b1, eof) = hub_sse_collect(&mut again, 15, |t| t.contains("\"p\":9")).await;
    assert!(b1.contains("\"p\":9") && !eof, "the replacement engine serves the resume:\n{b1}");
    drop(again);
    wait_for_feed_teardown(&state, 300).await;
}

/// Connect until the route answers 200 (a bounded open on the replacement
/// engine may first answer the retryable 503); the head is consumed so the
/// caller collects the body only.
async fn connect_ready(
    addr: std::net::SocketAddr,
    name: &str,
    query: &str,
) -> tokio::net::TcpStream {
    for attempt in 0..20u32 {
        let mut sck = lf_connect(addr, name, query).await;
        let (status, head) = sse_head(&mut sck).await;
        if status == 200 {
            return sck;
        }
        assert_eq!(status, 503, "only the open-wait refusal may precede the resume:\n{head}");
        drop(sck);
        tokio::time::sleep(std::time::Duration::from_millis(50 * (attempt as u64 + 1))).await;
    }
    panic!("{name}: the replacement engine never served the resume");
}
```

Expected failure on current code (scaffolding applied, guards absent): after the 10 s
collect, `assertion failed: eof1` with message `the parked session must take the typed
EngineRetired cutoff, not keep-alive forever:` followed by a transcript of `: keep-alive`
lines only. (Note: `hub_sse_collect` accumulates the whole response, headers included; the
`connect_ready` helper uses `sse_head` first so the reconnect's collect sees only the body.
Verify that the two-instance rig's bearer mode is Off so `/v1/debug/load` answers 200
without a header, as `admission_maintenance.rs:695` relies on for `http_rig`.)

### Test B: parked lineage session (LiveLocal tail after a split); the child's engine self-closes

```rust
/// Review rank 14 (red), lineage shape: after a split the feed's live
/// tail is a LiveLocal span pinned to the child's engine. Closing that
/// engine under the same owner must cut the parked session with
/// EngineRetired; the sealed predecessor's engine is not the tail and
/// re-resolves per page, so only the tail decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_under_the_same_owner_cuts_a_parked_lineage_session() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xlret",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    hub_append_lf(addr, "xlret", r#"{"h":0}"#).await;
    split_and_await(&state, "xlret", 0).await;
    hub_append_lf(addr, "xlret", r#"{"h":1}"#).await;
    let sref = state.deployment.raw_adapter_sref("xlret");
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let child = desc.resolve_segment("").seg_id;
    let p_child = state.shards.prefix_for(&desc.segment_route_by_id(child).unwrap());
    let p_parent = state.shards.prefix_for(&desc.segment_route_by_id(0).unwrap());
    assert_ne!(p_parent, p_child);
    let retired_before = crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
        .load(std::sync::atomic::Ordering::Relaxed);

    let mut sub = lf_connect(addr, "xlret", "?cursor=beginning").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| lf_record_and_status(t, "\"h\":1")).await;
    assert!(a0.contains("\"h\":1") && !eof0, "parked at the lineage's live tail:\n{a0}");

    let engine = state.shards.open(&p_child).expect("the child's engine is resident");
    engine.begin_close();
    assert!(state.ownership.is_mine(&p_child), "same owner throughout");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "a retired LiveLocal tail must cut the parked lineage session, not keep-alive forever:\n{a1}"
    );
    assert!(
        !a1.contains("event: data") && !a1.contains("\"sealed\":true"),
        "no stale data, no terminal:\n{a1}"
    );
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
            .load(std::sync::atomic::Ordering::Relaxed)
            > retired_before,
        "the lineage cutoff must be classified EngineRetired"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;
}
```

Expected failure on current code: `assertion failed: eof1`, message `a retired LiveLocal tail
must cut the parked lineage session, not keep-alive forever:` + keep-alive transcript, after
10 s.

### Test C: the read guard (a drive that reaches `read_batch` on a retired engine)

```rust
/// Review rank 14 (red), the read side: a session woken by an append
/// drives a read on its pinned engine. If that engine retired between the
/// wake and the read, the read must be the typed EngineRetired cutoff -
/// never a page served from a dead engine's ring (the record resumes
/// from the replacement), never a SourceFailed park. Forced with the
/// drive failpoint so the interleaving is exact, not raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livefeed_engine_retired_before_a_drive_is_a_typed_read_cutoff() {
    let store = mem();
    let (state, addr) = http_rig_owner(store, "inst-b").await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/xrread",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let sref = state.deployment.raw_adapter_sref("xrread");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let prefix = state.shards.prefix_for(&desc.resolve_segment("").shard_route);
    let retired_before = crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
        .load(std::sync::atomic::Ordering::Relaxed);

    crate::failpoints::arm(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread");
    let mut sub = lf_connect(addr, "xrread", "?cursor=now").await;
    let (a0, eof0) = hub_sse_collect(&mut sub, 15, |t| t.contains("\"upToDate\":true")).await;
    assert!(a0.contains("\"upToDate\":true") && !eof0, "parked at the live tail:\n{a0}");
    // The append wakes the session; it stops just before its drive.
    hub_append_lf(addr, "xrread", r#"{"r":1}"#).await;
    wait_parked(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread", 1).await;
    let engine = state.shards.open(&prefix).expect("the live tail's engine is resident");
    engine.begin_close();
    crate::failpoints::release(crate::failpoints::Fp::SseFeedBeforeDrive, "xrread");

    let (a1, eof1) = hub_sse_collect(&mut sub, 10, |_| false).await;
    assert!(
        eof1,
        "the drive on a retired engine must take the typed cutoff, not park:\n{a1}"
    );
    assert!(
        !a1.contains("\"r\":1"),
        "a retired engine must never serve a page; the record resumes from the replacement:\n{a1}"
    );
    assert!(
        crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED
            .load(std::sync::atomic::Ordering::Relaxed)
            > retired_before,
        "the read cutoff must be classified EngineRetired"
    );
    drop(sub);
    wait_for_feed_teardown(&state, 300).await;
}
```

Expected failure on current code: one of two, both assertions: if the dead engine's tail ring
still serves offset 1, `a retired engine must never serve a page; ...` fails (the transcript
contains `"r":1`); if the closed db refuses the scan, the drive is `SourceFailed`, the session
parks and `the drive on a retired engine must take the typed cutoff, not park:` fails after
10 s. With ONLY the park guard applied (partial fix), the first assertion still fails: that is
the red for the read guard specifically.

The `SseFeedBeforeDrive` pause site is `src/sse/session.rs:772-777` (inside `if cursor <
frontier`); `arm`/`release`/`parked` are `src/failpoints.rs:253-282`; the arm-before-connect
shape is `src/dst/tests/livefeed_basics.rs:403-422`. Failpoint arrivals are per name, so no
`gap_lock` is needed.

## 4. Code change, per file, with line budgets

Line counts at 71345c03 (`wc -l`): `src/sse/source.rs` 988, `src/sse/feed.rs` 1200 (ceilinged),
`src/sse/session.rs` 947, `src/sse/auth.rs` 515, `src/http.rs` 3372 (ceilinged),
`src/sse/feed/drive.rs` 214 (untouched), `src/sse/source/spans.rs` 48 (untouched),
`src/dst/tests/livefeed_ownership.rs` 645.

### `src/sse/feed.rs` (1200 -> 1200, must be line-neutral)

Add the variant (+3) and pay with doc compaction inside the same enum (-3), lines 131-151:

```rust
/// Why a feed's source was cut off: the wire behavior is always
/// disconnect-and-resume; the reasons must stay distinguishable in metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceCutoff {
    /// Delete/recreate or descriptor gone: a DIFFERENT incarnation.
    IncarnationChanged,
    /// A lineage span is owned by another instance (409-class).
    WrongOwner,
    /// The topology no longer contains this feed's cursor space.
    IncompatibleTopology,
    /// Round-11.2: the remote owner refused the incarnation-bound target.
    TargetMismatch,
    /// Round-11.2: 401 AFTER the one forced workload-token refresh —
    /// a fleet-auth failure, never a generic source stall.
    FleetAuth,
    /// Round-11.2: a second ownership redirect in one operation.
    RedirectLoop,
    /// The live tail's pinned engine closed under the SAME owner (fatal
    /// store, worker exit, sub-tick flap): resume through the route.
    EngineRetired,
}
```

(The doc block 131-133 goes 3 -> 2, `IncompatibleTopology` 2 -> 1, `TargetMismatch` 2 -> 1.)
Alternative if the reviewer prefers no prose compaction: a preceding verbatim-move commit of
`SourceCutoff` (lines 131-151) into `src/sse/feed/cutoff.rs` with `mod cutoff;` +
`pub(crate) use cutoff::SourceCutoff;` in feed.rs (-19 net), then add the variant there. No
`match` on `SourceCutoff` exists in feed.rs (`#![warn(clippy::wildcard_enum_match_arm)]`), so
the variant needs no arm there; the compiler names the only exhaustive match (`count_cutoff`).

### `src/sse/auth.rs` (515 -> 516)

After line 50:

```rust
    pub(crate) static FEED_CUTOFF_ENGINE_RETIRED: AtomicU64 = AtomicU64::new(0);
```

### `src/sse/session.rs` (947 -> 948; `count_cutoff` carries no `#[expect]`)

In `count_cutoff`, after the `RedirectLoop` arm (line 148):

```rust
        SourceCutoff::EngineRetired => &crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED,
```

### `src/http.rs` (3372 -> 3372; `debug_load` is ratcheted: `#[expect(too_many_lines, cast_possible_truncation)]` at 815-819, so `scope_lines` may not grow)

Line-neutral inside `debug_load`: collapse the two comment lines 904-905 into one and add the
key after line 916.

```rust
            // Bounded cardinality: rows exist only while a project has live feeds (round-10e).
            "project_retention": lf.project_retention.into_iter().map(|(p, reserved, cap_hits)| {
            ...
            "cutoff_redirect_loop": crate::sse::auth::sse_stats::FEED_CUTOFF_REDIRECT_LOOP.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_engine_retired": crate::sse::auth::sse_stats::FEED_CUTOFF_ENGINE_RETIRED.load(std::sync::atomic::Ordering::Relaxed),
```

Ratchet check: the `json!` body is ONE `macro` + ONE `macro-tokens` fact for the syntax gate
(`tools/quality-syntax/src/scan.rs:278-286`); keys inside it add no facts, and the expect
carries no unwrap/expect fingerprints. `scope_lines` and `nested_items` unchanged. rustfmt
leaves this macro body alone (the neighbouring lines are already >100 cols). No reason
re-decision needed.

### `src/sse/source.rs` (988 -> 996; NOT ceilinged but must stay <= 1000)

(a) New predicate after `owned_here` (after line 217), +11 lines (1 blank, 4 doc, 6 code):

```rust

/// The live tail's typed cutoff, ownership FIRST: a moved tail reroutes
/// (WrongOwner) whatever its engine did; an owned tail whose pinned
/// engine closed under this owner (fatal store, worker exit, sub-tick
/// flap) is EngineRetired — the route reopens, so a resume lands live.
fn live_tail_cutoff(owned: bool, engine: &ShardEngine) -> Option<SourceCutoff> {
    if !owned {
        return Some(SourceCutoff::WrongOwner);
    }
    engine.is_closed().then_some(SourceCutoff::EngineRetired)
}
```

(b) `SingleSource::read_batch`, lines 49-55 (7 -> 5, -2):

```rust
        // Round-11.2: a moved live tail is a typed cutoff, never a
        // stale local read — nor is a tail whose engine retired here.
        if let Some(cut) = live_tail_cutoff(owned_here(&self.state, &self.route), &self.engine) {
            return Err(anyhow::Error::new(FatalSpanCutoff(cut)));
        }
```

(97 columns with indent; under rustfmt's 100.)

(c) `SingleSource::cut_off`, lines 126-128 (3 -> 3):

```rust
    fn cut_off(&self) -> Option<super::feed::SourceCutoff> {
        live_tail_cutoff(owned_here(&self.state, &self.route), &self.engine)
    }
```

(d) `LineageSource::read_batch`, LiveLocal arm lines 593-600 (8 -> 7, -1):

```rust
                    // Round-11.2: a moved live tail is NEVER served
                    // from stale local state, nor is a retired one —
                    // typed cutoff (resumable EOF; the gateway reroutes
                    // or the route reopens).
                    if let Some(cut) = live_tail_cutoff(owned_here(&self.state, route), engine) {
                        return Err(anyhow::Error::new(FatalSpanCutoff(cut)));
                    }
```

(e) `LineageSource::cut_off`, lines 737-746 (10 -> 10):

```rust
    fn cut_off(&self) -> Option<super::feed::SourceCutoff> {
        // Only the LIVE tail cuts a parked session off — sealed spans
        // are ownership-dynamic and re-resolve per page.
        match &self.tail().reader {
            SpanReader::LiveLocal { route, engine, .. } => {
                live_tail_cutoff(owned_here(&self.state, route), engine)
            }
            SpanReader::Sealed { .. } => None,
        }
    }
```

Net: +11 -2 -1 = +8 -> 996. Run `wc -l` after rustfmt; if it lands above 998, move the
predicate (a) into `src/sse/source/spans.rs` (48 lines; registered owner `sse_source_spans`,
whose filter then also needs the DST filter below) and import it at line 984
(`use spans::{live_tail_cutoff, locate_in_spans};`).

(f) Ratcheted scopes touched (both impl-wide `#[expect(clippy::unwrap_used, ..)]`, whose
contracts fingerprint every call-site and path under the impl: the new `live_tail_cutoff`
call is a new fingerprint = "accepted exception grew"). Re-decide both reasons; identity is
`(path, item, kind, attribute text)`, so a changed reason is the sanctioned new decision.

`src/sse/source.rs:42-45` ->

```rust
#[expect(
    clippy::unwrap_used,
    reason = "SingleSource; a poisoned stream state may hold a half-advanced durable frontier and the live tail pins one engine incarnation whose close is a typed cutoff; recovering the state could serve a length never made durable and a stale read could serve a retired engine"
)]
```

`src/sse/source.rs:567-571` ->

```rust
#[expect(
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "LineageSource; one batch walks the span chain until the budget or the frontier stops it, the live tail pins one engine incarnation whose close is a typed cutoff, and a poisoned stream state may hold a half-advanced durable frontier; splitting the walk would separate it from its budget, a stale read could serve a retired engine and recovering the state could serve a length never made durable"
)]
```

Both are exactly three `;`-separated parts with no `;` or `"` inside a part (the gate's
`"[^";]+;[^";]+;[^";]+"` check). `sealed_span_page`, `build`, `tail`, `refresh_transition`
and `drive.rs` are untouched.

### `scripts/quality/mutation_owners.py`

The DST tests are the only killers for the new mutants, and the three owner rows whose
sources carry diff hunks run only `sse::`/`http::` today. Extend the filters (whitespace
splits them; the name is a unique substring of the three tests):

```python
    owner('sse_session', 'src/sse/session.rs', 'sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired'),
    owner('http', 'src/http.rs', 'http:: livefeed_engine_retired'),
    owner('sse_source', 'src/sse/source.rs', 'sse:: livefeed_engine_retired'),
```

Mutant accounting (in-diff only):
- `live_tail_cutoff` body -> `None`: A/B/C fail on `eof` (no cutoff). `!owned` -> `owned`:
  A's counter assertion (the cut becomes WrongOwner, `FEED_CUTOFF_ENGINE_RETIRED` unchanged).
  `Option<SourceCutoff>` has no `Default`, so no `Some(..)` mutant is viable.
- `count_cutoff` body -> `()`: every test's `FEED_CUTOFF_ENGINE_RETIRED > before`.
- `debug_load` body -> `Default::default()` (viable: `axum::body::Body: Default`): A's JSON
  parse/`cutoff_engine_retired` assertion.
- `read_batch` bodies: `anyhow::Result<SourceBatch>` has no viable replacement.
- `feed.rs`/`auth.rs`: enum variant and static only; zero mutants, reported as such.
Runtime under mutation: `sse::` unit tests + three DST tests (~10-15 s green; a surviving
mutant costs at most 3x10 s of collect deadlines), inside the driver's 90 s `--timeout`.

## 5. Ledgers and docs to update

- `docs/quality/owners.json`: the new static is a `global` occurrence that fails until owned
  (`scripts/quality/source_rules.py:255-267`; the active allowance file cannot grow past the
  legacy ceiling). Add:

```json
    {
      "category": "global",
      "count": 1,
      "owner": "crate::sse_stats::FEED_CUTOFF_ENGINE_RETIRED",
      "path": "src/sse/auth.rs",
      "reason": "Process instrumentation counter: the typed cutoff canary for a live tail whose pinned engine closed under the same owner, one AtomicU64 beside the other cutoff reasons; a per-runtime copy would hide the reason from /v1/debug/load.",
      "syntax": "AtomicU64"
    },
```

- `docs/refactor/test-inventory.json`: `scripts/test-inventory.py --write` (three new DST
  tests; the full-suite floor rises with them). No pinned mechanism file changes
  (`review-mechanisms.json` pins `fixture_livefeed.rs`, which is untouched).
- `scripts/quality/mutation_owners.py`: the three filter rows above (`test_mutation_owners.py`
  only `assertIn`s the existing filters; adding is safe).
- `docs/refactor/WIRE-MATRIX.md` §2.11: append one sentence: "Mid-stream typed cutoffs
  (owner move, incarnation change, incompatible topology, remote target/auth/redirect
  failures, and a live tail whose pinned engine retired under the same owner) end the body
  WITHOUT a terminal control; clients resume from their cursor; counted per reason under
  `/v1/debug/load` `sse_livefeed.cutoff_*` (`cutoff_engine_retired` added)."
- `docs/LIVE-FEED.md`: wire-semantics table row "Topology transition" (line 117) gains "; a
  live tail whose pinned engine retires under the SAME owner (fatal store, worker exit,
  flap) is disconnect-and-resume too (typed `EngineRetired`; the route reopens after the
  anti-flap holdoff)". Rollback bullet (156-159): add "`cutoff_engine_retired` deltas must
  track `engine_closed` ops events; a delta without an engine close is the regression
  signal". Mention the counter beside `cutoff_wrong_owner` in the ownership paragraph.
- Optional, bench-only: `bench/canary/livefeed-canary.mjs:689` and
  `bench/fleet/livefeed-cert.mjs:629` add `"cutoff_engine_retired"` to the unexpected-cutoff
  list (a healthy canary retires no engines); not part of the gate.
- `scripts/quality/verification_plan.py` selects loom (src/sse is a LIFECYCLE prefix) and
  miri (src/http is a BUFFER prefix) legs automatically; both run the existing models, no
  new loom/miri fixture is needed (no synchronization primitive changes).

## 6. What could go wrong

- Wire compatibility: none on the SSE surface (the disconnect shape is the existing typed
  cutoff; SDKs already resume from cursor). `/v1/debug/load` gains one flat key; both
  campaign scripts read siblings with `?? 0`, so old and new binaries interoperate.
- Fleet skew: the cut fires only on the instance whose engine retired. The reconnect hits the
  same route and eats the anti-flap holdoff (3 s -> 60 s escalating) as 503 `Opening`
  retryables before the replacement opens; that is today's posture for any engine close
  (producers already get `Moved`). A flapping engine turns every subscriber into a reconnect
  per flap, bounded by the holdoff escalation and now visible on the counter instead of
  invisible as keep-alives.
- Ordering invariant the fix depends on: `begin_close` stores `closed` BEFORE
  `notify_waiters` and evicts the map AFTER (src/shard.rs:1880-1922). Reordering those would
  reintroduce a lost wake; say so in the predicate's doc (done) and do not touch shard.rs.
  The session's register -> check -> park order (session.rs:496-498, 856, 871) is the other
  half; unchanged.
- Feed poisoning: a fatal read sets `Lifecycle::Gone(EngineRetired)`; a joiner that attaches
  before teardown is cut on its first loop (park guard) or first drive (`Tail::Gone`), and
  the SDK's next retry finds a fresh feed. Converges by client retry; no server-side loop.
- Flakiness: assert only monotone increases of the process-global counters (parallel tests
  share them); never assert equality on `FEED_CUTOFF_WRONG_OWNER`. Test A's reconnect must
  `clear_holdoff` (test-only) and tolerate 503 open-waits (`connect_ready`); reopening the
  same mem-store shard while the old db closes is the established pattern
  (`runtime_retirement.rs:86-160`). `wait_for_feed_teardown` before the reconnect is
  mandatory, or the reconnect can join the dying feed and be cut too. All waits are bounded.
- Mutation leg: if the `http` row is not extended, a viable `debug_load -> Default::default()`
  mutant survives under `http::` alone (only three unit tests, none touching the endpoint).
  If the predicate moves to `spans.rs`, extend `sse_source_spans` the same way.
- Line ceilings: feed.rs and http.rs must come out at exactly 1200 and 3372; source.rs at
  <= 1000 after rustfmt (budget 996). Run `wc -l` on all five touched sources before the gate.
- Adjacent finding (same root, NOT in this change, budget would put source.rs at 1000):
  `sealed_span_page` caches `(engine, handle)` in `local` (src/sse/source.rs:423-467) and
  reuses it while `owned_here` stays true; a same-owner replacement of a SEALED span's engine
  leaves a dead cached reader whose pages fail as non-fatal `SourceFailed` forever (the
  catch-up loop's 100 ms retries, or a live-loop park that re-fails on every tail wake). Fix
  sketch: before `if cached.is_none()`, `if cached.as_ref().is_some_and(|(engine, _)|
  engine.is_closed()) { *cached = None; }` (+4 lines), with the `sealed_span_page` reason
  re-decided ("... caches the reader and owner hint it used and drops a cached reader whose
  engine retired, ..."), and its own red DST test (lineage, close `p_parent`'s engine, then a
  `?cursor=beginning` reconnect must serve `"h":0` from the reopened parent). File as a
  follow-up item.

## Skeptic corrections

Read-only check of the plan against slate @ 71345c03. Verdict: **sound with
corrections**. Every named function, field and helper exists with the stated
signature; the gate arithmetic holds; the three red tests compile against the
fixtures they name and fail on the current tree for the stated reasons. The
corrections below are (1) one real flake in Test A's reconnect, (2) one
precision fix to Test C's stated red, (3) a log line the fix makes false, and
(4) a handful of confirmations the implementer should not have to re-derive.

### Verified first-hand (no change needed)

- Mechanism: `ShardEngine::is_closed()` is `pub(crate)` (`src/shard.rs:1859`);
  `begin_close` stores `closed` at 1883 inside the `in_flight` guard, fires
  `notify_waiters` on every resident handle at 1905-1913 on EVERY call, and
  runs `on_close` at 1920-1922 only on the first call. `on_close` in both the
  DST rig (`fixture_http.rs:292-298`) and production (`src/bootstrap.rs:505-513`)
  is `notifier.closed(prefix, incarnation)` → `OpenGate::notify_closed`
  (`src/sharddir.rs:716-738`), which evicts the resident and arms the holdoff
  SYNCHRONOUSLY inside the callback, then re-enters `begin_close` (guarded).
  So Test A's `assert!(state.shards.open(&prefix).is_none())` right after
  `engine.begin_close()` is deterministic.
- The body keep-alive is independent of the producer (`src/sse/auth.rs:493-511`
  never wakes the driver task), `lease_watch.nap()` is 3600 s lease-less, and
  no other wake exists: the stall is real and the red for A/B is a 10 s
  collect returning `eof == false`.
- Scoping: `install_custody` (`src/billing.rs:1885-1887`) returns `None` for
  any engine with `last_external_seq != 0`, and both feed sources resolve with
  `Adoption::External` → the sweep never closes a feed's engine. `FleetEviction`
  is preceded by `state.ownership.set_override` (`src/fleet.rs:1040-1049`), and
  the tick's `OwnershipMoved` retire (`src/fleet.rs:835-843`) runs after the
  view changed → both are WrongOwner under ownership-first. The existing
  round-11.4 test (`livefeed_ownership.rs:321-372`) stays green.
- Consumers of `SourceCutoff`: the ONLY exhaustive match is `count_cutoff`
  (`src/sse/session.rs:138-151`, no `#[expect]`); `drive.rs`, `feed.rs`,
  `retry.rs`, `source/tests.rs` only construct/compare variants. `feed.rs`'s
  `#![warn(clippy::wildcard_enum_match_arm)]` has no `SourceCutoff` match to
  complain about.
- Gate arithmetic (`scripts/quality/source_rules.py`): contract identity is
  `(path, qualified, kind, attribute-text)` (line 196) and
  `exception_growth` skips identities absent from the merge base (207-208), so
  re-deciding the two impl-wide reasons is the sanctioned reset. `scope_lines`
  is PHYSICAL (`end_line - line + 1`, line 152). Macro tokens emit exactly one
  `macro` + one `macro-tokens` fact and only `#[...]` groups inside them add
  facts (`tools/quality-syntax/src/scan.rs:278-286`, `scan/macro_attributes.rs`)
  → the `json!` key in `debug_load` adds no fact. Both new reasons match
  `"[^";]+;[^";]+;[^";]+"` (exactly two `;`).
- Line ceilings with `merge_base()` = `origin/slate` or the push `before`
  (`scripts/quality/common.py:20-29`): feed.rs `min(2266+1, 1200) = 1200`,
  http.rs `min(3382+2, 3372) = 3372`, source.rs 1000, session.rs 1000,
  livefeed_ownership.rs 1000. The plan's budgets fit.
- `#[expect(clippy::too_many_lines)]` on `impl FeedSourceRead for LineageSource`
  stays FULFILLED after (d): clippy counts raw source lines of the body span
  (the `#[cfg(test)]` block included), currently 113 code lines → 111 after
  the −2. Not at risk. (`SingleSource`'s impl expect is `unwrap_used` only,
  still fulfilled by `frontier`/`closed`.)
- Statics: `classify` inventories EVERY `static` as `global`
  (`source_rules.py:42-43`); the existing `FEED_CUTOFF_*` rows live in
  `docs/quality/source-allowances.json` (legacy-ceilinged, cannot grow), so the
  new one MUST be an `owners.json` row with a `reason` (`source_gate.py:45-50`).
  `owners.json` is not in `policy.json.immutable_sha256`. Row shape/`syntax:
  "AtomicU64"`/`owner: "crate::sse_stats::FEED_CUTOFF_ENGINE_RETIRED"` match
  the existing allowance rows.
- `/v1/debug/load` answers 200 with no header on `http_rig_owner`: the rig
  builder defaults `auth: None` (`fixture_http.rs:49`) and `AuthMode::Off`
  (`fixture_http.rs:518-521`); `DeploymentBearer::authorizes` with no token is
  `mode == Off` (`src/deployment_bearer.rs:26-33`).
- Fixtures: `hub_sse_collect`, `lf_connect`, `wait_parked` (200×25 ms, panics),
  `sse_head` (10 s deadline, 5 s per read, panics), `lf_record_and_status`,
  `split_and_await`, `hub_append_lf` all exist in `fixture_livefeed.rs` with the
  used signatures; `hreq`/`preq`/`PRISMA_KEY`/`mem`/`http_rig_owner` are already
  imported in `livefeed_ownership.rs:3-11`; only `sse_head` must be added to the
  `fixture_livefeed` import (rustfmt orders it after `split_and_await`).
  `wait_for_feed_teardown` is file-local at 616-624. `state.shards.open`,
  `prefix_for`, `clear_holdoff` (cfg(test)), `resident_incarnation`
  (cfg(test)), `ownership.is_mine(&str)`, `desc.resolve_segment("").shard_route`,
  `desc.segment_route_by_id(u32)`, `deployment.raw_adapter_sref` all exist.
- Test B's `assert_ne!(p_parent, p_child)` holds by construction: the split
  salts the HIGH child's route onto a different prefix than the parent
  (`src/application/topology.rs:494-521`) and lane `""` resolves into the high
  child (the round-11.2 test at `livefeed_ownership.rs:45-53` asserts the same
  inequality with the same shape and is green). Keep it.
- Test C's failpoint: `SseFeedBeforeDrive` has two sites (`session.rs:585` inside
  `if closed`, `session.rs:773` inside `if cursor < frontier`); with
  `?cursor=now` on an empty stream the first is unreachable and the second is
  reached exactly once after the append (arrivals are per name; "xrread" is
  unused elsewhere). `arm` before connect is the `livefeed_basics.rs:403-422`
  shape.
- Mutation engine (pinned cargo-mutants 27.1.0, `~/.cargo/registry/.../src/visit.rs`):
  mutates fn bodies, binary ops, unary `!`/`-` (deleted), match ARM DELETION
  only when a `_` catch-all exists, and match GUARDS → true/false. The plan's
  (e) replaces the guarded `_ =>` match with an exhaustive two-arm match, so
  no arm/guard mutants exist there; `live_tail_cutoff` yields exactly two
  viable mutants (`→ None`, delete `!`); `count_cutoff → ()`;
  `debug_load → Default::default()` (viable: `axum::body::Body: Default`);
  `SourceBatch` has no `Default` so `read_batch → Ok(Default)` is unviable
  (unviable is not a failure; exit 2/3 are). `count == 0` owners (feed.rs,
  auth.rs) are skipped with a printed line (`mutation_driver.py:143-146`).
  Filters are OR'd positional `cargo test` args (`mutation_driver.py:32-39`),
  so the substring `livefeed_engine_retired` selects exactly the three tests.

### Correction 1 (real flake): Test A must wait for the retired engine to TERMINATE before reconnecting

`connect_ready` tolerates 503, but the failure mode is not a 503. In
`OpenGate::get_or_open` (`src/sharddir.rs:491-509`) → `wait_retired`
(`678-703`) the gate finds `closing = Some(old engine's EngineShutdown)` and
AWAITS `engine.wait(open_wait)` — the rig's `open_wait` is 10 s
(`config/model.rs:375`, wired at `fixture_http.rs:487`) — before answering
`shard_closing`. During that await the server sends NO bytes. `sse_head`
(`fixture_livefeed.rs:137-140`) `expect`s each read within 5 s and panics
"head read timed out". The old engine terminates only after its workers stop
(`WORKER_GRACE = 5 s`, `src/shard/lifecycle.rs:10`) and `storage-close`
completes; on a mem store that is usually milliseconds, but a slow worker
exit puts the first reconnect past `sse_head`'s 5 s and the test panics with
a fixture message instead of a test assertion. Fix in Test A, between
`wait_for_feed_teardown(&state, 300).await;` and `state.shards.clear_holdoff(&prefix);`:

```rust
    // The replacement can only open once the retired incarnation has
    // TERMINATED (the gate awaits its shutdown handle before reopening,
    // src/sharddir.rs:678-703); bound that wait here so the reconnect's
    // head arrives inside the fixture's read deadline.
    let retired = engine.shutdown_handle();
    drop(engine);
    for _ in 0..400 {
        if retired.terminated() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(retired.terminated(), "the retired engine must terminate");
```

(`shutdown_handle()` is `pub(crate)` at `src/shard.rs:1761`;
`EngineShutdown::terminated()` at `src/shard/lifecycle.rs:102`. 400×25 ms =
10 s, an assertion, never a hang.) Keep `connect_ready` as the belt for the
holdoff/open-wait 503s; with this wait the first attempt normally answers 200.
Test A's line count becomes ~80, still under the 100-line clippy threshold.

### Correction 2 (prove the replacement): assert a NEW incarnation in Test A

The item asks to verify the reconnect lands on the replacement. `"p":9` after
the reconnect proves liveness, not identity. Add, cheaply and deterministically:

```rust
    let inc_before = state.shards.resident_incarnation(&prefix);   // before begin_close
    ...
    let inc_after = state.shards.resident_incarnation(&prefix);    // after connect_ready
    assert!(inc_before.is_some() && inc_after.is_some() && inc_before != inc_after,
        "the resume must be served by a NEW engine incarnation");
```

(`resident_incarnation` is cfg(test) at `src/shard_directory.rs:201`;
`EngineIncarnation` is `PartialEq` — `runtime_retirement.rs:138` uses
`assert_ne!` on it.)

### Correction 3 (precision): Test C's red on the current tree is deterministic, not "one of two"

`read_frames_until` (`src/shard/record.rs:257-266`) consults the handle's
tail ring FIRST for `Deliver::Durable`, and `dispatch_durable`
(`src/shard.rs:3109-3112`) published `"r":1` into that ring before the wake.
A closed engine's ring is still readable, so on the current tree the drive
SERVES `"r":1` from the retired engine, the session parks, and the 10 s collect
returns `eof1 == false`: the FIRST assertion fails
("the drive on a retired engine must take the typed cutoff, not park:" +
a transcript containing `"r":1`). With ONLY the park guard applied, the ring
page is emitted and the next iteration's park guard cuts, so `eof1 == true`
and the SECOND assertion fails ("a retired engine must never serve a page").
State it that way in the commit message; the `SourceFailed` branch the plan
mentions is not reached because the ring answers before the closed db is
touched.

### Correction 4 (log truth): reword the park-guard log line

`src/sse/session.rs:863` logs `"livefeed ownership moved under a parked
session; disconnecting"` for EVERY park-guard cut; after the change it is
false for `EngineRetired`. Change the literal, line-neutral, to
`"livefeed live tail cut under a parked session; disconnecting"` (the
`?reason` field already carries the typed reason). This is inside `serve`,
whose `#[expect]`s (`session.rs:158-177`) carry no `unwrap_used`/`expect_used`
fingerprints, so a string-literal change alters no contract metric
(`scope_lines`, `nested_items`, `syntax_facts` all unchanged) — no reason
re-decision needed. Optional, but do it: the canary rollback rule in
`docs/LIVE-FEED.md` will otherwise be contradicted by the log.

### Correction 5 (planner side effect, not a failure)

Editing `scripts/quality/mutation_owners.py` sets `tooling = True` in
`verification_plan.plan` (`verification_plan.py:77-79`), which selects the
`properties_fuzz` leg in addition to loom (src/sse) and miri (src/http). §5's
"loom and miri legs" understates the CI cost by one leg; nothing else changes.

### Correction 6 (mutant-kill hygiene, note only)

The three tests run in PARALLEL under one `cargo test` and all bump the
process-global `FEED_CUTOFF_ENGINE_RETIRED`, so a `> before` assertion in one
test can be satisfied by another's bump. I checked every viable in-diff
mutant against this: `live_tail_cutoff → None` and `!owned → owned` misclassify
or skip ALL three sessions at once; `SingleSource::cut_off → None` leaves A
parked (`eof1` false, independent of the counter); `LineageSource::cut_off →
None` leaves B parked; `count_cutoff → ()` bumps nothing; `debug_load →
Default` panics A's JSON parse. No survivor hides behind the shared counter.
Under `!owned → owned`, Test C's `wait_parked` panics after 5 s (the session was
cut before the append) — bounded, an assertion, killed.

### Correction 7 (small compile/format notes)

- (b)/(d)/(e) widths: 97/97/72 columns as written; rustfmt keeps each `if let`
  on one line and keeps (e)'s block arm (a one-line arm would be 118 cols).
  Budget 996 stands; the `spans.rs` fallback is not needed.
- (b) `&self.engine` is `&Arc<ShardEngine>` and (d)/(e) pass `engine:
  &Arc<ShardEngine>`; both deref-coerce to `&ShardEngine` at the call site.
  `SourceCutoff` is already imported at `source.rs:11-14`, so the predicate
  can name `SourceCutoff::WrongOwner` unqualified.
- In `connect_ready`, `50 * (attempt as u64 + 1)` mirrors `seal_ok`
  (`fixture_livefeed.rs:238`); `u64::from(attempt)` is the nicer spelling but
  neither trips an enabled lint.
- The `count_cutoff` arm is 96 columns with its 8-space indent — rustfmt keeps
  it on one line (the `IncarnationChanged` arm is 97).
- `docs/refactor/review-mechanisms.json` pins `fixture_livefeed.rs` only;
  `livefeed_ownership.rs` is inventoried by `test-inventory.json` (three
  `function_sha256` entries to add via `scripts/test-inventory.py --write`), and
  the scenario map does not require new tests to be mapped.
- `src/dst/tests/` is not a critical prefix and not a registered owner
  (`verification_plan.py:22-31`), so the test file needs no owner row — as the
  plan says.

### Design points confirmed (no change)

- Ownership-first is the right order: a fenced-but-not-yet-mirrored close
  (round-11.4's sub-tick case) now cuts as `EngineRetired`, the reconnect eats
  the holdoff 503 and then the 409 redirect once the mirror updates — same
  wire shape, now visible on a counter instead of as keep-alives.
- `is_closed()` rather than the serving map is correct for the reason the plan
  gives: the map eviction happens in `on_close`, AFTER `notify_waiters`.
- The reconnect-joins-the-corpse window is closed in production by the
  3 s holdoff (`src/sharddir.rs:45-51`), during which the dead feed tears
  down; Test A only bypasses it after `wait_for_feed_teardown`.
- Graceful shutdown (`ShardDirectory::shutdown`, `shard_directory.rs:381-385`)
  now cuts parked sessions with `EngineRetired`; the only test that holds an
  SSE session across shutdown (`runtime_isolation.rs:482-509`) asserts the body
  ENDS, so it stays green.
