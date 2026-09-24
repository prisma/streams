# Plan: shared RAW feed clears `need_status` without sending `upToDate` (review rank 13, landing 1)

Repo: `/Users/sorenschmidt/code/streams`, branch `slate` @ `71345c03`. Read-only
verification; nothing in the repo was touched. Scratch measurements live under
`/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/ratchet/`.

## 0. Verdict on the claim

**The bug is real; the review's framing is wrong in two details.**

1. There is no "keyed RAW" SSE subscriber. The public raw route forces the
   default lane (`src/http.rs:2925` `params.key = Some(String::new())`), so
   every raw SSE session is `raw_keyless = true` on lane `""`
   (`src/sse/session.rs:205-207`). The affected subscriber is *any* RAW SSE
   session whose feed has 2+ subscribers.
2. Subscribers sharing a feed share the lane (`feed_key_of`,
   `src/sse/session.rs:902-911`; `FeedKey::keyed/default_lane`,
   `src/sse/feed.rs:38-51`), so "a batch containing only records under A's
   key" cannot happen between two sessions of one feed. What does happen is
   `start_index == records.len()` at `src/sse/feed.rs:865-869`, reached two
   ways: (a) a **match-free batch** (`records` empty: the lane scanned only
   foreign-key records — product appends with `Prisma-Routing-Key` land on the
   same stream the raw route reads, see `sse_follows_lineage_across_split`),
   which hits every subscriber; (b) a session whose cursor (`?offset=`) lies
   past every record of a retained batch but below its `scan_to`.

The mechanism, the missing frame, and the subscriber-count dependence are
exactly as described. Product and raw sessions share one default-lane feed
(`docs/LIVE-FEED.md:107-108`, pinned by
`livefeed_exact_framing_mixed_surfaces_share_one_lane`), which makes the
cheapest two-subscriber rig a raw + product pair.

## 1. Mechanism (file:line evidence)

Session live phase, `src/sse/session.rs`:

- `Take::Batch { batch, start_index }` arm, lines 517-574. `after =
  cursor.max(batch.scan_to)` (524); `head_here = after >= frontier && !closed`
  (525); the loop iterates `batch.records[start_index..]` (527) — empty when
  `start_index == records.len()`; then **lines 559-568**:
  ```rust
  if head_here && ctx.surface == Surface::RawToken {
      need_status = false;           // nothing was sent, yet the status is marked reported
      last_reported = Some(cursor);
      reached_live = true;
  } else {
      need_status = true;
  }
  ```
- The two Solo arms guard on a sent record: lines 707-716 (`head_here &&
  last_off.is_some() && RawToken`) and 812-818 (same). The catch-up arm folds
  the clear into the `at_head` record itself, lines 433-437 (`if at_head {
  need_status = false; ... }`), i.e. also only when a record was sent.
- Consequence: after the buggy clear the session `continue`s (573), takes
  `Take::AtHead` (575), reaches `cursor >= frontier` (579) and skips the
  standalone status because `need_status` is false (749). Nothing else wakes
  it: no heartbeat control exists (comment at 603-605). The RAW client never
  learns it is at the frontier until the next matching record.
- Product is unaffected: the `else` branch keeps `need_status = true` and the
  standalone status goes out at 749-756.

Feed side:

- `take_visible`, `src/sse/feed.rs:827-876`: returns the first retained batch
  with `scan_to > cursor`, `start_index = position(offset >= cursor)
  .unwrap_or(len)` (865-869). `Take` shape at 1117-1128.
- Batches are published from the feed head: `drive_under_permit` →
  `read_and_publish(&src, head)` (`src/sse/feed/drive.rs:117-121`, `tail()`
  147-155 reads `st.head`). An empty page with `scan_to > scan_from` is NOT
  `NoProgress` (948-957) and is retained for a shared feed (1073-1077, charge
  256 bytes for zero records, `charge_for` 214-217). Solo publications return
  `DriveOutcome::Solo { records: [], scan_to }` (976-989).
- A foreign-key record advances the scan: `src/shard/record.rs:288-293` sets
  `last_offset` before the key filter drops the frame; `src/application/read.rs:294-296`
  turns it into `out.last`; `scanned_through` (`read.rs:93-97`) → `scan_to`.
  Lane filter is exact-key: `src/application/read_batch.rs:167`.
- Status semantics, `docs/LIVE-FEED.md:115` ("decided against the durable
  frontier at SEND time"), `:121` (standalone status controls), and the
  session header `src/sse/session.rs:9-10` ("ONE standalone upToDate control,
  deduped by reported position"). The lag contract (`docs/LIVE-FEED.md:46`)
  ties `reached_live` to "an honest upToDate was emitted" — the buggy arm also
  sets `reached_live = true` without one.

Wire trace of the red scenario (raw A + product B on stream `rawshare`, both
parked at next=1 after one foreign append seen solo by A):

```
POST /v1/streams/rawshare/records  Prisma-Routing-Key: ka        -> offset 1, frontier 2
drive (shared): batch { scan_to: 2, records: [] } retained
A (raw):     take_visible(1) -> Batch{start_index 0 == len 0}; head_here; RAW -> need_status=false  => SILENCE
B (product): same batch; else -> need_status=true -> AtHead -> standalone {"nextCursor":..,"upToDate":true}
A solo (before B joined): Solo{records:[],scan_to:1}; last_off None -> need_status=true -> standalone upToDate at next=1
```

## 2. Design

### 2a. Smallest correct fix (RECOMMENDED): the RAW clear rides the `at_head` record

Move the RAW status clear *into* the record loop under `if at_head && RawToken`
(the shape the catch-up arm already uses at 433-437) and set `need_status =
true` once before the loop. A session that sent nothing keeps its status owed
and emits the standalone `upToDate` on the next `AtHead` iteration, exactly as
the two Solo arms and the product surface do.

Why this shape and not the one-line guard `&& start_index < last_i`:
`serve` carries five `#[expect]`s (`src/sse/session.rs:158-177`) and is
ratcheted on `scope_lines`/`nested_items`/`syntax_facts`. The gate counts
**every `syn::Path`** as a fact (`tools/quality-syntax/src/scan.rs:222-225`),
so the one-line guard adds two facts (`start_index`, `last_i`) and trips all
five expectations. I measured both candidates with the repo's own scanner
(`target/debug/streams-quality-syntax`, built 2026-09-21) through
`scripts/quality/source_rules.exception_contracts/exception_growth`:

| candidate | scope_lines | syntax_facts | exception_growth |
|---|---|---|---|
| baseline `serve` | 743 | 875 | — |
| B: `if head_here && start_index < last_i && ctx.surface == ...` | 743 | **877** | 5 failures (`syntax_facts 875 -> 877`) |
| A: restructure below (one comment line) | **743** | **875** | **none** |

Fact accounting for A: removed `head_here, ctx, Surface::RawToken,
need_status, last_reported, Some(+call-site), cursor, reached_live,
need_status` (10) and added `need_status, at_head, ctx, Surface::RawToken,
need_status, last_reported, Some(+call-site), after, reached_live` (10). The
pre-loop `need_status = true;` replaces the old `else` branch. Line count is
neutral only with at most ONE new comment line before the loop (a 4-line
comment measured `scope_lines 743 -> 746`, which fails). File stays 947 lines.

Semantics check, per case (old → new):
- RAW, head_here, ≥1 record sent: clear after loop → clear at the last record
  (`at_head` is true only for `start_index+i+1 == last_i`, `last_reported =
  Some(after)` equals the old `Some(cursor)` since `cursor = after` follows).
- RAW, head_here, no record for this session: **cleared (bug) → owed**; next
  iteration sends `status_ctl(locate(after))` at 749-756 and sets `reached_live`
  there — now consistent with `docs/LIVE-FEED.md:46`.
- RAW, !head_here: `need_status = true` → same.
- Product: `need_status = true` in all cases → same (the inner `if` is false).
- Send failure mid-loop returns before any status bookkeeping in both versions.

No re-decision of any `#[expect]` reason is needed. Fallback texts are in §4 in
case the implementer's diff differs and the ratchet fires.

### 2b. Owner-first alternative (deferred to landing 2)

The three copies of "RAW pairing reports the head only for a record this
session sent" (Batch arm, two Solo arms) plus the catch-up arm are the same
decision written four times; the review's landing 2 is to give it one owner
(a small status machine holding `need_status/last_reported/reached_live` with
`record_sent(at_head)`/`batch_done()` in a new file, e.g.
`src/sse/session/status.rs`, then delete the copies from `serve`). That is the
right end state but it is the larger dedup the task excludes, needs its own
owner row (`src/sse/...` is a critical prefix) and mutation coverage for a new
type. A middle option — a pure predicate `fn raw_head_reported(surface,
head_here, sent) -> bool` called from the arm — is NOT viable: the call adds
+3 facts to `serve`.

**Recommendation: 2a now (one commit: fix + red test + inventory + doc line);
2b as landing 2.**

## 3. Red test (near-complete Rust)

File: `src/dst/tests/sse_delivery.rs` (333 lines → ~455; not ceilinged; it is
in the `sse_session` mutation owner's test filter,
`scripts/quality/mutation_owners.py:135`: `sse:: dst_tests::sse_delivery::
dst_tests::livefeed_swap::` — a test in `livefeed_basics.rs` would NOT count
for the mutation leg). `src/dst/tests/` is not a critical prefix
(`scripts/quality/verification_plan.py:22-31`), no owner row.

Helpers reused: `http_rig`, `engine_shutdown` (`fixture_http`), `hreq`,
`preq`, `PRISMA_KEY`, `RIG_KEY_B64` (`fixture_requests`), `mem`
(`fixture_storage`), `hub_sse_collect`, `lf_connect`, `lf_record_and_status`
(`fixture_livefeed.rs:19,164,278` — untouched, so its sha256 pin in
`docs/refactor/review-mechanisms.json:943` stays valid). Import line change:

```rust
use super::fixture_livefeed::{hub_sse_collect, lf_connect, lf_record_and_status};
```

New test-local helpers (test-only file; each ≤ 20 lines, nesting ≤ 2):

```rust
// ------------------------------------------------------------------
// Review rank 13 (red): a RAW session sharing its feed is owed the
// standalone upToDate a solo one already gets when a batch at the
// durable frontier carries nothing for it.
// ------------------------------------------------------------------

/// A RAW live subscription on `name` from the current frontier
/// (`offset=now`): the raw surface always rides the default lane.
async fn raw_sse_connect(addr: std::net::SocketAddr, name: &str) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut sck = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/stream/{name}?live=sse&offset=now HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nstream-encryption-key: {RIG_KEY_B64}\r\n\r\n"
    );
    sck.write_all(req.as_bytes()).await.unwrap();
    sck
}

/// One product append under routing key `ka`: the default lane scans
/// it and matches nothing — a match-free window at the frontier.
async fn foreign_append(addr: std::net::SocketAddr, name: &str, i: u64) {
    let (st, _, _) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{name}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", "ka"),
        ],
        format!(r#"{{"k":"ka","i":{i}}}"#).as_bytes(),
    )
    .await;
    assert!(st == 200 || st == 204, "foreign append {i}: {st}");
}

/// The raw `streamNextOffset` token a control names for `next` — the
/// encoding `sse_control_ep` uses (segment 0; START when next is 0).
fn raw_next_tok(next: u64) -> String {
    let off = next
        .checked_sub(1)
        .map_or(crate::offsets::Offset::START, |o| crate::offsets::Offset(Some(o)));
    crate::offsets::encode_ep(0, off)
}

/// Payloads of the control frames carrying `upToDate`, in wire order.
/// Frames are `\n\n`-separated; the chunked-transfer framing lines
/// between them carry neither prefix and fall out of the line scan.
fn up_to_date_controls(t: &str) -> Vec<String> {
    t.split("\n\n")
        .filter(|frame| frame.lines().any(|l| l.starts_with("event: control")))
        .filter_map(|frame| frame.lines().find_map(|l| l.strip_prefix("data:")))
        .filter(|d| d.contains("\"upToDate\":true"))
        .map(str::to_owned)
        .collect()
}

/// The default-lane feed of `name` — raw and product sessions share it.
async fn default_lane_feed(
    state: &std::sync::Arc<crate::http::AppState>,
    name: &str,
) -> std::sync::Arc<crate::sse::feed::LiveFeed> {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let key = crate::sse::session::feed_key_of(&desc, &Some(String::new()));
    state
        .livefeed
        .registry()
        .feed_for_test(&key)
        .expect("the default-lane feed exists while sessions are attached")
}
```

The test (≈ 70 code lines, max nesting 1, every wait bounded by
`hub_sse_collect`'s deadline; it fails by assertion, never hangs):

```rust
/// Review rank 13 (red): the shared `Take::Batch` arm cleared the RAW
/// status on `head_here` alone, while the Solo arms guard on a record
/// actually sent — so a RAW session that shared its feed was left
/// without `upToDate` after a batch that carried nothing for it, and
/// wire behaviour depended on the subscriber count. Raw and product
/// sessions share one default-lane feed, so the second subscriber is
/// a product one; the last window also pins the per-surface framing
/// after a shared RECORD (raw: ONE paired control carrying upToDate,
/// product: bare control + standalone status).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = ("content-type", "application/json");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawshare", &[ct], b"").await;
    assert!(st == 200 || st == 201, "create {st}");

    // SOLO control (passes today): a match-free window at the frontier
    // yields the standalone status at next=1 — the Solo arm guards on
    // last_off, so nothing sent means the status is still owed.
    let mut raw = raw_sse_connect(addr, "rawshare").await;
    let (a0, _) = hub_sse_collect(&mut raw, 8, |t| t.contains("upToDate")).await;
    assert!(a0.contains(&raw_next_tok(0)), "raw parks at the head:\n{a0}");
    foreign_append(addr, "rawshare", 0).await;
    let (a1, _) = hub_sse_collect(&mut raw, 8, |t| t.contains(&raw_next_tok(1))).await;
    assert_eq!(
        up_to_date_controls(&a1).len(),
        1,
        "solo raw: ONE standalone upToDate at next=1 for a match-free window:\n{a1}"
    );

    // SHARED: a product session joins the same default-lane feed.
    let mut prod = lf_connect(addr, "rawshare", "?cursor=now").await;
    let (p0, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("upToDate")).await;
    assert!(p0.contains("\"upToDate\":true"), "product parks at the head:\n{p0}");
    assert_eq!(
        default_lane_feed(&state, "rawshare").await.subscriber_count(),
        2,
        "raw and product sessions share ONE feed"
    );

    // THE RED: a second match-free window at the frontier is a shared
    // batch with no record for either session. Both are owed the
    // standalone status at next=2; the raw session was left silent.
    foreign_append(addr, "rawshare", 1).await;
    let (p1, _) = hub_sse_collect(&mut prod, 8, |t| t.contains("\"upToDate\":true")).await;
    assert!(p1.contains("\"upToDate\":true"), "shared product: status at the frontier:\n{p1}");
    let (a2, _) = hub_sse_collect(&mut raw, 5, |t| t.contains(&raw_next_tok(2))).await;
    assert_eq!(
        up_to_date_controls(&a2).len(),
        1,
        "shared raw: ONE standalone upToDate at next=2 for a match-free window (solo got one at next=1):\n{a2}"
    );

    // Paired-control exactness after a shared RECORD: raw folds
    // upToDate into its ONE paired control and never adds a standalone
    // duplicate; product keeps bare control + standalone status.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawshare", &[ct], br#"[{"r":1}]"#).await;
    assert!(st == 200 || st == 204, "raw append {st}");
    let (a3, _) = hub_sse_collect(&mut raw, 8, |t| t.contains(&raw_next_tok(3))).await;
    let (a3x, _) = hub_sse_collect(&mut raw, 2, |_| false).await;
    let a3 = format!("{a3}{a3x}");
    assert!(a3.contains("\"r\":1"), "shared raw: the record:\n{a3}");
    assert_eq!(a3.matches("event: control").count(), 1, "raw: ONE paired control:\n{a3}");
    assert_eq!(
        up_to_date_controls(&a3).len(),
        1,
        "raw: upToDate rides the paired control only (no standalone duplicate):\n{a3}"
    );
    let (p2, _) = hub_sse_collect(&mut prod, 8, |t| lf_record_and_status(t, "\"r\":1")).await;
    assert!(
        lf_record_and_status(&p2, "\"r\":1"),
        "shared product: record, then standalone upToDate:\n{p2}"
    );
    drop(raw);
    drop(prod);
    engine_shutdown(&state).await;
}
```

Notes on determinism: A is provably solo when window 1 lands (B connects only
after `a1`); B's attach is complete before `foreign_append(.., 1)` (its first
`upToDate` was read and `subscriber_count() == 2` is asserted); whichever
session wins the drive, both take the same retained empty batch; no second
append races the frontier check (`after == frontier` at each step). The
`a3x` 2 s drain exists to catch a duplicate standalone control (needed to
kill the `==`→`!=` mutant, see §5); it is the only fixed cost (~2 s).

**Exact red on current code (`71345c03`):** the test reaches the THE RED
block; `p1` passes (product gets its status); `hub_sse_collect(&mut raw, 5,
..)` returns after its 5 s deadline with an empty transcript (the raw session
parked with `need_status = false`); then:

```
thread 'dst::dst_tests::sse_delivery::shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch' panicked at src/dst/tests/sse_delivery.rs:<line of the a2 assert_eq>:
assertion `left == right` failed: shared raw: ONE standalone upToDate at next=2 for a match-free window (solo got one at next=1):

  left: 0
 right: 1
```

With the fix the same wait returns within milliseconds carrying
`event: control\ndata:{"streamNextOffset":"<tok next=2>","streamCursor":"…","upToDate":true}`,
and every later assertion holds (the paired-control shape is what
`livefeed_exact_framing_mixed_surfaces_*` already pin for the shared record
case).

Run: `cargo test --lib dst_tests::sse_delivery::shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch -- --nocapture`
(red first, then green after §4).

## 4. Code change (per file, with budgets)

### `src/sse/session.rs` — 947 lines, NOT ceilinged (limit 1000), budget +0 used; `serve` ratcheted: scope_lines 743 → 743, nested_items 1 → 1, syntax_facts 875 → 875 (measured)

Exact diff (verified against the scanner; the scratch file
`…/scratchpad/ratchet/session_fix.rs` is the full post-fix source):

```diff
@@ -524,6 +524,8 @@
                         let after = cursor.max(batch.scan_to);
                         let head_here = after >= cur_src.frontier() && !cur_src.closed();
                         let last_i = batch.records.len();
+                        // Nothing sent to THIS session = status still owed.
+                        need_status = true;
                         for (i, r) in batch.records[start_index..].iter().enumerate() {
                             let at_head = head_here && start_index + i + 1 == last_i;
                             #[cfg(test)]
@@ -553,19 +555,17 @@
                                 || lease_watch.revoked(&task_state)
                             {
                                 return;
+                            }
+                            if at_head && ctx.surface == Surface::RawToken {
+                                // The paired control already reported the
+                                // head — the standalone status would be a
+                                // duplicate the pinned protocol forbids.
+                                need_status = false;
+                                last_reported = Some(after);
+                                reached_live = true;
                             }
                         }
                         cursor = after;
-                        if head_here && ctx.surface == Surface::RawToken {
-                            // The paired control already reported the
-                            // head — the standalone status would be a
-                            // duplicate the pinned protocol forbids.
-                            need_status = false;
-                            last_reported = Some(cursor);
-                            reached_live = true;
-                        } else {
-                            need_status = true;
-                        }
                         // DRAIN (finding 5): more retained batches may
```

Also update the arm's lead comment at 518-523 in place (same line count) if
desired, e.g. "…carries upToDate when the batch ends at the durable frontier
AND this session sent that record; a batch holding nothing for it leaves the
standalone status owed (as the Solo arms)". Keep it to the existing 6 lines.

Constraints honoured: rustfmt-clean (longest new line 77 cols); no new
unwrap/expect/panic; nesting is already under the fn-level
`excessive_nesting` expectation (the new `if` sits beside the existing
`if !sse_send_billed(..) { return; }` at the same depth); no new macro, effect
or glob (source-allowances rows for `crate::serve` — `tokio::spawn` 1,
`tokio::select` 3, `tokio::pin` 1 — unchanged). Do NOT add more than one
comment line before the loop (each extra line fails `scope_lines`).

Re-measure before pushing (no cargo needed):
`python3 …/scratchpad/ratchet/measure.py …/ratchet/session_base.rs src/sse/session.rs`
must print `--- growth failures` followed by `(end)`.

Fallback ONLY if the ratchet still fires (e.g. the implementer chooses the
one-line guard): all five `serve` reasons must be re-decided, format
`owner; invariant; alternative`:

- too_many_arguments: `serve; the session takes the state, the surface, the descriptor, key, params, headers and lease parts as the handler authorized them and reports a head only for a record it sent; a request struct would restate the authorization`
- too_many_lines: `serve; the session is one subscribe, drive and stream sequence whose teardown depends on which step admitted it and whose status is owed until a record it sent reports the head; splitting it would separate the steps from the teardown they order`
- excessive_nesting: `serve; the driver nests the lease, version and generation waits and the take verdicts inside the drive loop of the spawned task, the head report inside the record it rides; flattening them would separate each wake from the verdict it produces`
- let_underscore_must_use: `serve; the changed waits are only registrations on watches the driver already owns and the head report waits on a sent record; handled results would only restate the registration`
- disallowed_methods: `serve; the driver task is owned by the subscription it drives, ends with it and reports the head only for records it sent; a supervised driver would tie a request-scoped task to the runtime supervisor`

(Prefer 2a; these exist so the fallback is not improvised under CI pressure.)

### `src/dst/tests/sse_delivery.rs` — 333 → ~455 lines (limit 1000)

Import change + helpers + test from §3. Each fn ≤ 100 lines, nesting ≤ 2, no
`#[expect]` needed. `unwrap/expect` are inside `#[cfg(test)]` (the whole
`dst_tests` tree is test-only, as the existing tests in this file rely on).

### `docs/LIVE-FEED.md` — one sentence (contract clarification, not a change)

In the `## Wire semantics` table, row `Status framing` (line 121), append:
"RAW pairing (round 11.8) folds `upToDate` into the paired control of the
LAST record a session itself sends at the frontier; a batch that carries
nothing for a session — a match-free lane window, or a cursor past every
record it holds — leaves the standalone status owed, on solo and shared
feeds alike."

### `docs/refactor/WIRE-MATRIX.md` — no row

The matrix explicitly excludes mid-stream SSE frame sequencing
(`docs/refactor/WIRE-MATRIX.md:231`, "Residual depth limits: (a) SSE
mid-stream frame sequencing … lives in src/sse/session.rs"). The response
head and control vocabularies (line 249) are unchanged. If the reviewer wants
a trace anyway, add one bullet under §4 "Data-dependent behavior": "SSE
(both surfaces): a window at the frontier that carries no record for a
session still yields ONE standalone `upToDate` (raw: `streamNextOffset`
advanced past the scanned range) regardless of subscriber count."

## 5. Ledgers / gates to update or re-run

| Item | Action |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` (new test `shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch`, file `src/dst/tests/sse_delivery.rs`; helpers are not inventoried, `scripts/test-inventory.py:95-131`). The full-suite floor rises by one automatically. |
| `docs/refactor/review-mechanisms.json` | untouched — `fixture_livefeed.rs` (pinned at line 943) is not modified. Keep helpers local to `sse_delivery.rs` for exactly this reason. |
| `scripts/quality/mutation_owners.py` | no change: no new file; `src/sse/session.rs` is owned by `sse_session` (line 135) whose filter already includes `dst_tests::sse_delivery::`. |
| `docs/quality/source-allowances.json` | no change expected (no new macro/effect/glob under `crate::serve`); `scripts/quality.sh` will confirm. |
| `docs/quality/legacy-*.json`, `verification.json` | frozen adoption receipts — do NOT regenerate (`docs/RUST-QUALITY.md:37`). |
| Verification plan | `python3 scripts/quality/verification_plan.py --out <dir>` against the real merge base: `src/sse` is a lifecycle prefix, so the plan selects `loom` (existing loom tests, nothing new) and `mutants` with owner `sse_session`. Run it locally before pushing (memory trap: run CI's plan before push). |
| Mutation kill matrix (`cargo mutants --in-diff`, owner filter `sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap::`, 90 s per mutant) | Mutants on the changed lines: (i) `serve` FnValue → `Default::default()` response: killed by every SSE test in the filter (no `upToDate` ever). (ii) `at_head && …` → `\|\|`: on the product session `at_head \|\| false` clears the status after the shared record → step 4 `lf_record_and_status(&p2, "\"r\":1")` fails after its 8 s bound. (iii) `ctx.surface == Surface::RawToken` → `!=`: raw keeps `need_status` after the paired control → a standalone duplicate arrives in the 2 s `a3x` drain → `event: control` count 2 ≠ 1 and `up_to_date_controls == 2`; on product the same mutant clears the status → (ii)'s assertion also fires. (iv) `need_status = true` / `= false` literal flips are not a cargo-mutants 27.1 genre; the RED step and step 4 would kill them anyway. `reached_live = true` is unobservable here (needs a lag-disconnect leg) but is not mutated. Before pushing, list the selected mutants: `cargo mutants --list --json --in-diff <diff> --file src/sse/session.rs --package streams-slate` and check each against this table. |
| `docs/LIVE-FEED.md` | sentence from §4. |
| `scripts/quality.sh` | fmt, clippy `-D warnings`, rustdoc, `test-inventory`, `scenario-map-report` (no scenario ID required; existing tests in this file carry `"scenarios": []`), `review-evidence`. |
| Conformance / livefeed legs | run the DS conformance leg and `livefeed_basics`/`livefeed_swap` locally; `livefeed_exact_framing_mixed_surfaces_*` pin the unchanged shared-record framing (raw 2 controls / product 4 for two windows). |

## 6. What could go wrong

- **Wire compatibility.** A shared RAW session now receives one standalone
  control (`streamNextOffset` advanced, `upToDate:true`) where it previously
  received nothing. That frame is byte-identical to what a SOLO raw session
  already emits for the same window (Solo arms, 707-716 / 812-818), so any
  client that works against a solo feed already handles it; the pinned
  conformance protocol's "one flag-carrying control per data event" is
  untouched (no data event exists in this window). The raw client's resume
  offset now advances past foreign-key ranges — strictly better on reconnect
  (fewer re-scans), and identical to product.
- **Duplicate-status risk.** None introduced: for a batch with records the
  clear happens on the same record it used to follow; `last_reported ==
  Some(after)` dedupes the `AtHead` path exactly as before. Step 4 of the test
  asserts the no-duplicate shape explicitly.
- **`reached_live` timing.** RAW sessions in the match-free-at-head case now
  become lag-eligible only after the honest standalone `upToDate` (line 759)
  instead of silently at the empty batch. This matches the written lag
  contract (`docs/LIVE-FEED.md:46`) and only delays lag-disconnect eligibility
  by one loop iteration.
- **Fleet skew.** Session-local; no storage, token, cursor or peer protocol
  change. Mixed-version cells differ only in whether the owed frame is sent;
  clients tolerate both today (solo vs shared already differed).
- **Flakiness.** All waits are bounded (`hub_sse_collect` deadlines 2–8 s,
  1 s read timeouts). The RED wait is 5 s only on the buggy code. The
  `subscriber_count()==2` assert removes the "feed silently not shared"
  failure mode. The one-record raw append in step 4 must be the only append
  in flight (it is). Under loaded CI the 2 s duplicate drain is the only
  fixed cost; the whole test is ~3 s green. Do not raise `worker_threads` or
  add `gap_lock` (no process-global counters are read).
- **Ratchet surprises.** If the implementer reflows comments or adds a second
  comment line inside `serve`, `scope_lines` grows and all five expectations
  fail; re-run the scratch `measure.py` before pushing. Do not touch
  `fixture_livefeed.rs` (sha256-pinned).
- **Mutation leg.** The kill matrix depends on the test living in
  `sse_delivery.rs` (owner filter). Moving it to `livefeed_basics.rs` would
  leave the `&&`/`==` mutants unkilled in CI even though the ordinary suite is
  green.

## Skeptic corrections

Verdict: **sound with corrections**. Every load-bearing claim was re-read
against `slate @ 71345c03`; the fix is fact-neutral on the ratchet (I
re-measured it), the red is real and deterministic, the mutant matrix
holds. Three corrections (one real flake risk in the red test, one
measurement-script bug, one gate trap the plan hedges on) and a few
confirmations the implementer can rely on.

### C1 (must fix) — red-test predicates can stop on a TCP-split control frame and assert 0 on the FIXED code

`hub_sse_collect` returns on the first read whose accumulated text
satisfies `done`; the raw control frame is one HTTP chunk but nothing
guarantees one `read()` delivers the whole chunk (the repo's own tests
"drain one more beat" for exactly this, `livefeed_basics.rs:555-562`).
`sse_control_ep` (`src/sse/wire.rs:78-91`) writes the fields in the order
`streamNextOffset`, `streamCursor`, `upToDate` — so at `§3` steps

- `a1`: `hub_sse_collect(&mut raw, 8, |t| t.contains(&raw_next_tok(1)))`
- `a2`: `hub_sse_collect(&mut raw, 5, |t| t.contains(&raw_next_tok(2)))`

the collect can return with the token present and `"upToDate":true` not
yet read, and `up_to_date_controls(..).len()` then reads **0** on a
correct tree — a false red / CI flake. Make the stop predicate the same
thing the assertion counts. Add one test-local helper and use it as both
predicate and assertion input:

```rust
/// A standalone/paired control naming `next` that carries upToDate —
/// the stop condition AND the assertion input, so a TCP read that
/// splits a control between its token and its flag cannot end the
/// collect early (frame order: streamNextOffset, streamCursor, upToDate).
fn status_at(t: &str, next: u64) -> bool {
    let tok = raw_next_tok(next);
    up_to_date_controls(t).iter().any(|d| d.contains(&tok))
}
```

and replace the two collects:

```rust
let (a1, _) = hub_sse_collect(&mut raw, 8, |t| status_at(t, 1)).await;
...
let (a2, _) = hub_sse_collect(&mut raw, 5, |t| status_at(t, 2)).await;
```

Keep the assertions `up_to_date_controls(&a1).len() == 1` /
`up_to_date_controls(&a2).len() == 1` as written. The exact red is
unchanged: on `71345c03` the raw session emits nothing in the window, the
predicate never fires, the 5 s deadline returns `""`, and the `a2`
`assert_eq!` panics with `left: 0 / right: 1` and the plan's message.
Step-4 (`a3`) is already safe: its token is in the *paired* control that
FOLLOWS the data event, and the 2 s `a3x` drain concatenates the rest
before any count is taken. `a0`/`p0`/`p1` stop on the flag itself, which
is written last, so the token/record before it is already in the text.

### C2 (should fix) — `measure.py` prints `{}` for both trees; its growth verdict is still valid

`scratchpad/ratchet/measure.py` filters `ident[1] == 'serve'`, but the
scanner qualifies the item as `crate::serve` (`tools/quality-syntax/src/scan.rs:31,43-45`),
so the two JSON dumps are empty and the 743/875 numbers in §2a/§4 did not
come from that script as-is (they came from `dbg.py`, which prints every
contract). The growth loop compares ALL contracts, so "`--- growth
failures` then `(end)`" remains the correct acceptance line. I re-ran the
contracts on `session_base.rs` vs `session_fix.rs` with the filter fixed
(`'serve' in ident[1]`), through the repo's own
`scripts/quality/source_rules.exception_contracts/exception_growth` and
the prebuilt `target/debug/streams-quality-syntax` (binary 2026-09-21,
scanner source last changed 2026-09-14 → not stale):

```
crate::serve  too_many_arguments       PREV {scope_lines 743, nested_items 1, syntax_facts 875}  CUR {743, 1, 875}
crate::serve  too_many_lines           PREV {743, 1, 875}  CUR {743, 1, 875}
crate::serve  excessive_nesting        PREV {743, 1, 875}  CUR {743, 1, 875}
crate::serve  let_underscore_must_use  PREV {743, 1, 875}  CUR {743, 1, 875}
crate::serve  disallowed_methods       PREV {743, 1, 875}  CUR {743, 1, 875}
--- growth failures
(end)
```

Confirmed independently: `scratchpad/ratchet/session_base.rs` is
byte-identical to the tree's `src/sse/session.rs`, and
`session_fix.rs` differs from it by exactly the §4 diff (+2 at 527, +8 at
558, −10 at 559-568; 947 lines). None of `serve`'s five expectations is
`unwrap_used`/`expect_used`, so no call-site fingerprint is retained for
it (`source_rules.py:159-188`) — only the three counts matter, and
literals (`true`/`false`) are not facts (`scan.rs` emits path,
call-site, method-call, macro, attribute, import, visibility only). The
fixed script is at `scratchpad/ratchet/measure2.py`; use it for the
pre-push re-measure instead of `measure.py`.

### C3 (do it, do not argue it) — write the WIRE-MATRIX bullet

The task's gate list says wire-behaviour changes MUST update
`docs/refactor/WIRE-MATRIX.md`; the plan makes the bullet optional on the
strength of `WIRE-MATRIX.md:231`'s "residual depth limits" note. A shared
RAW client now receives a control it never received before — that is a
wire-visible change regardless of the matrix's self-declared depth. Add
the plan's own bullet under `## 4. Notes` → "Data-dependent behavior
(same route, different wire answer)" (`docs/refactor/WIRE-MATRIX.md:233`),
after the existing "SSE control frames" bullet (line ~240):

```
- SSE (both surfaces): a window at the durable frontier that carries NO record for a session — a match-free default/keyed-lane scan, or a cursor already past every record of a retained batch — still yields ONE standalone `upToDate` control (raw: `streamNextOffset` advanced past the scanned range; product: `nextCursor`), on solo and shared feeds alike; RAW folds `upToDate` into the paired control only for a record the session itself sent (`src/sse/session.rs` Take::Batch/Solo arms).
```

Neither `docs/LIVE-FEED.md` nor `WIRE-MATRIX.md` is sha-pinned by any
gate (`review-mechanisms.json` pins only `fixture_livefeed.rs::hub_append_lf`
by function hash, and `scripts/quality.sh`'s five gates do not hash docs),
so both edits are free.

### C4 (optional, line-neutral) — the arm's lead comment now overstates the rule

`src/sse/session.rs:518-523` still says the last record's control
"carries upToDate when the batch ends at the durable frontier". After the
fix that is true only when this session sent that record. If the
implementer touches it, this 6-line replacement keeps `scope_lines` at
743 (the plan is right that ONE extra line in `serve` trips all five
contracts):

```rust
                        // RAW pairing (round 11.8): the LAST record THIS
                        // session sends carries upToDate when the batch
                        // ends at the durable frontier (the pinned
                        // protocol pairs each data event with ONE
                        // flag-carrying control). A batch holding nothing
                        // for it leaves the standalone status owed.
```

### Confirmations (no change needed)

- **Sites and shapes**: `Take::Batch{batch, start_index}` `feed.rs:1117-1128`;
  `take_visible` `feed.rs:827-876` with `start_index = position(..).unwrap_or(len)`
  at 865-869; `DriveOutcome::Solo{records, scan_to}` 1165-1168; the two
  Solo arms guard on `last_off.is_some()` at `session.rs:707-716` and
  812-818; the catch-up arm clears on the sent `at_head` record at 433-437;
  the standalone status site is 749-760; the buggy clear is 559-568. All
  as cited.
- **No keyed RAW SSE exists**: `read()` forces `params.key = Some(String::new())`
  (`http.rs:2925`) and the only other `SseSurface::Raw` entry
  (`internal_segment_read`, `http.rs:3282-3357`) answers `?live=` with
  400 `live_unsupported`. The plan's reframing of the review item is right.
- **Foreign-key record advances `scan_to` on the default lane**:
  `shard/record.rs:288-293` sets `out.last_offset = Some(off)` for every
  frame BEFORE the key filter drops it; `application/read.rs:293-296`
  folds it into `out.last`; `scanned_through` (`read.rs:93-97`) →
  `scan_to`. `read_and_publish` then takes `scan_to(2) > scan_from(1)` past
  the `NoProgress` guard (`feed.rs:948-957`), charges 256 bytes for the
  empty batch (`charge_for`, 214-217) and retains it (1073-1077). The
  session's wake is the stream-level `handle.notify` (`source.rs:122-124`)
  and `frontier()` is `durable.next` (110-112) — key-agnostic, so the raw
  session does wake on the foreign append.
- **Red is deterministic in both drive orders** (traced): whichever
  session wins the permit, the loser either sees the retained batch on its
  next `take_visible` or parks on `ver_wait` (a `watch` receiver — a bump
  between loop-top registration and the park is not lost) and takes it on
  the wake. The raw session on `71345c03` ends every path at
  `need_status=false, last_reported=Some(2)` and skips 749; nothing else
  writes to its socket (no heartbeat, comment 602-605; the lease nap
  emits no frame).
- **Fix semantics**: the last iterated record always has
  `start_index+i+1 == last_i`, so `at_head == head_here` for it and the
  in-loop clear is equivalent to the old post-loop clear whenever ≥ 1
  record was sent; `Some(after)` equals the old `Some(cursor)` because
  `cursor = after` follows. Product is unchanged (pre-loop
  `need_status = true` replaces the `else`). The DRAIN case (retained
  `{scan_to:5,[r@2]}` then `{scan_to:8,[]}` at frontier 8) now yields the
  owed standalone at 8 instead of silence — consistent with the fix, no
  extra branch. Compiles: `at_head` and `after` are in scope; the
  assignments touch locals, not the `batch` borrow.
- **Mutation matrix**: the driver passes `--file src/sse/session.rs`
  (`mutation_driver.py:48-49`), so the test-local helpers in
  `sse_delivery.rs` are never mutated (a `raw_next_tok -> String` →
  `String::new()` mutant would otherwise survive — irrelevant here). Prior
  receipts (`docs/quality/verification.json`) show only fn-return
  `true/false`, `==`↔`!=`, `&&`↔`||`, arithmetic and `expr→val` genres —
  no `if`-condition or assignment genre, confirming the plan's (iv).
  (ii) `&&→||`: the PRODUCT session at step 4 has `at_head == true` on the
  shared record, clears its status, and `lf_record_and_status(&p2, "\"r\":1")`
  fails after its 8 s bound. (iii) `==→!=`: raw keeps `need_status`,
  emits a standalone duplicate at next=3 caught by the 2 s `a3x` drain
  (`event: control` count 2 ≠ 1); product clears and (ii)'s assertion
  fires too. FnValue on `serve` (`Response: Default`) is killed by every
  test in the filter. Worst-case mutant runtime added by the new test is
  ~13 s, inside the 90 s per-mutant timeout; green runtime ~3 s (the
  `a3x` drain is the only fixed cost).
- **Test-side signatures** all match: `hreq`/`preq(addr, &str, &str, &[(&str,&str)], &[u8]) -> (u16, HashMap, Vec<u8>)`
  (`fixture_requests.rs:6-12, 196-202`; `hreq` injects
  `stream-encryption-key` itself, 16-24); `http_rig(store) -> (Arc<AppState>, SocketAddr)`
  (`fixture_http.rs:119-122`); `mem()` (`fixture_storage.rs:7`);
  `hub_sse_collect(&mut TcpStream, u64, impl Fn(&str)->bool) -> (String, bool)`,
  `lf_connect(addr, &str, &str)`, `lf_record_and_status(&str, &str) -> bool`
  are all `pub(super)` in `fixture_livefeed.rs:19-23,164-168,278-281`;
  `feed_key_of(&StreamDesc, &Option<String>) -> FeedKey` (`session.rs:902-911`);
  `state.livefeed.registry() -> &Arc<FeedRegistry>` (`service.rs:82`),
  `feed_for_test(&FeedKey) -> Option<Arc<LiveFeed>>` (`registry.rs:203`),
  `LiveFeed::subscriber_count() -> u64` (`feed.rs:746`), `crate::sse::feed`
  is `pub(crate)` (`sse.rs:7`); `Offset(pub Option<u64>)`, `Offset::START`,
  `encode_ep(u32, Offset) -> String` (`offsets.rs:12-15,83`);
  `raw_next_tok(next)` reproduces `sse_control_ep`'s `next==0 → START else Some(next-1)`
  exactly (`wire.rs:70-77`); `locate(n)` on a single-segment source is
  `{seg_id: 0, local_after: n}` (`source.rs:130-135`, `lane_seg_id` 35-39).
  The raw `GET /v1/stream/{name}?live=sse&offset=now` + `stream-encryption-key`
  shape is the one `runtime_isolation.rs:495` / `security_workload.rs:48`
  use; raw-created stream + keyed product appends is the shape
  `sse_follows_lineage_across_split` (`sse_delivery.rs:50-100`) already
  exercises. The test path in the red message is
  `dst::dst_tests::sse_delivery::…` (`src/dst/mod.rs:157-158`,
  `dst_tests.rs:202-203`), inside the `#[cfg(test)]` subtree, so
  `unwrap/expect` in the helpers are exact-cfg(test).
- **Gates**: `src/sse/session.rs` is 947 lines with an effective ceiling
  of 1000 (`source_rules.violations`: `min(max(1000,1068), max(1000,947))`),
  so the FILE is not ceilinged — the binding constraint really is
  `serve`'s `scope_lines 743`, and the plan's diff is net-zero inside it.
  `source-allowances.json` rows for `crate::serve` are count-keyed
  (`tokio::spawn 1, tokio::pin 1, tokio::select 3`), not line-keyed; the
  diff adds no macro. Legacy diagnostic ledgers are a frozen baseline and
  `serve`'s lints are already `#[expect]`ed, so line shifts inside the arm
  are harmless. `test-inventory.py --check` rejects "new test requires
  inventory" → `--write` is mandatory (helpers are skipped, `test-inventory.py:108`);
  `scenario-map-report --check` validates scenarios, not tests, so no ID
  is needed; `review-evidence --check` pins nothing in `sse_delivery.rs`.
  `src/dst/tests/` is under no `CRITICAL_PREFIXES` entry
  (`verification_plan.py:22-31`) and is not a registered owner path, so no
  `mutation_owners.py` row. The fallback reason texts in §4 satisfy the
  gate regex `"[^";]+;[^";]+;[^";]+"` (`source_rules.py:258`), and a
  re-decided `disallowed_methods` reason still passes the
  registered-owner check (effect row `crate::serve`/`tokio::spawn`
  exists) — but the recommended diff needs none of them.
