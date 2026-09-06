# R24 — enforceable architecture and mechanism evidence

The structural baseline is exactly `a7e2070f3b4346b3e54d552069ff91c56e900130`.
`docs/refactor/architecture-review-baseline.json` was captured from those Git
objects, including a SHA-256 for every source file. Its content hash is pinned
in `architecture-policy.json`; the capture command refuses to overwrite it.
The older WP-00 diagnostic report and baseline remain historical artifacts.
They cannot make the acceptance gate pass.

`scripts/architecture-gate.py` is now a required CI, commit-gate and local
release-gate check. Every application module and the SSE feed/source/service/
registry/budget core reject HTTP/product imports, `AppState`, Axum and response
types. The scanner masks comments and literals, detects aliases and braced
imports, and checks reverse dependency growth outside transport/composition.
The two SSE HTTP adapters have exact allowed HTTP exports; adding a callback
into HTTP-owned work fails the gate. New modules have a 1,000-line budget and
functions a 200-line budget. Historical overages remain pinned to the reviewed
commit. Every additional exception has a fixed numerical ceiling, owner,
finding, concrete rationale and preserved source obligation. In particular,
the retained shard transaction is visible debt; it is not silently accepted as
a new baseline merely because other responsibilities moved out of HTTP.
All newly extracted application functions now meet the default function
budget; the remaining numerical exceptions describe pre-existing source/test
owners and the concrete safety checks added to them.

`scenario-map-report.py` retains all 189 catalogue IDs and statuses. The map
currently has 142 mapped scenarios: 117 full local mechanisms, 23 partial,
2 external, and 47 unmapped. `scenario-dispositions.json` retains all 78
original partial/unmapped/external obligations, even where this remediation
adds a local mechanism. Every entry records the owner, original procedure,
current evidence and pending work. Historical reader-cache obligations retain
their original IDs and state the replacement mechanism; they are not renamed
away. Current symbol line numbers replace the obsolete monolith positions.

The DST inventory preserves all 410 original tests and records fifteen reviewed
additions (425 total, zero ignored). Five fixture body adaptations have exact
old/new hashes and reasons: valid allocated seal generations and the legal
competing transition at the parked phase-B seam. The new SEC-002 mapping is a
separately recorded metadata adaptation. Two append contract unit tests moved
outside DST retain exact token hashes in `review-unit-relocations.json`.
`test-inventory.py --compare ... --adaptations ... --additions ...` checks
these exact changes against the original manifest; the current `--check` gate
has no adaptation/addition exceptions.

A valid map means source references are internally consistent. It does not
mean every schedule, production backend or runtime was exercised. The
`review-mechanisms.json` manifest separately names the actual fault/frontier,
entered proof, oracle, seed/configuration, exact regression body hashes and
limitations for the required R01/R09/R13–R15/R17–R22 controls. A modified or
ignored Rust regression, changed SDK regression script, missing obligation or
missing disposition fails `review-evidence.py --check`. This source checker
never emits execution certification.

The cold-absorber fixture has separate enforced before/after token hashes from
pre-audit commit `b1864fffaca3f753a34129f95b8f5734cccd0a4a`. Startup recovery
correctly backdates dirty work beyond large age thresholds; the fixture now
pauses its owned history resource before callers append to enforce its promised
cold schedule. No existing test body or assertion changes. Removing the fixture
record, changing either body or omitting its rationale fails the source checker.
The evidence checker includes 29 mutation controls, including four fixture
provenance controls.

## New mechanism controls

- R09: held real spool PUT and catalog LIST operations prove both active
  telemetry tasks finish cooperatively and retain exact batch bytes and the
  incomplete-page cursor. A 513-entry catalog advances in three bounded passes.
  Additional controls cancel entered ops/audit appends and fleet append/CAS
  clear operations; original event ordering, retry IDs, bounded overflow gap
  magnitude and durable outbox sources survive. Two actual database handoff
  tests cover cancellation before completion and after a result is queued.
- R18: at full sketch capacity, 256 repeated known-segment appends visit zero
  unrelated incarnation entries. An epoch replacement must exercise cleanup
  and remove old sibling sketches, hot state and cooldowns. The oracle counts
  actual scan visits instead of inferring constant cost from source shape.
- DUR-005/SEL-022: `r24_prior_group_close_retry_and_fence_wait_on_actual_remote_frontier`
  holds an actual WAL object-store PUT with `FaultStore` seed 2405 and a 5 ms
  SlateDB flush cadence. It observes applied closed state with no Remote tail,
  then enqueues the exact close retry and fence into a later group. The entered
  fence generation is observed and all success replies remain absent. Releasing
  the remote write establishes the Remote tail while the separate dispatch
  hold still withholds replies. Dispatch release and engine reopen prove the
  final closed tail. Existing dispatch-only scenarios remain distinct mappings.
- DUR-014: `r24_exact_set_oracle_rejects_count_preserving_loss_duplicate_swap`
  first proves the acknowledged and observed counts are equal. A missing
  acknowledged record plus duplicate observation must fail the exact-set
  audit; the faithful equal-count set passes.
- SDK-003: one million unique scopes run through the actual `Producer._chain`
  primitive in held cohorts of 128. The test counts all entered operations and
  every injected rejection. Peak map size is exactly 128 and every cohort
  returns to zero, including rejected operations. This exercises sparse-key
  eviction separately from retained producer-state storage.
- SDK-002: the actual five-second Retry-After timer is captured and its
  callback withheld. The operation must be pending before abort, then complete
  and clear the timer before that callback is released. An isolated copy of
  the built SDK with the sleep abort listener removed fails this exact
  assertion; the unchanged SDK passes all 14 retry-classification controls.
  This negative control prevents eventual completion after a full backoff
  from masquerading as prompt cancellation. The mutation never touches the
  repository's built SDK or production source.

Local targeted runs before final integration: both new Rust controls passed
(2 tests, 0 failed/ignored); the million-scope control passed with all four
existing producer-order tests (5 tests, 0 failed). These are development runs,
not final-HEAD acceptance receipts. The existing RC evidence verifier passed
its 15 mutation controls in this real Git checkout. Reporter, mapper and new
architecture/evidence self-tests also passed; final commands and source hashes
must be taken after all source commits settle.

The newer toolchain also exposes historical lints. The security freshness test
now iterates directly over the same versions 2–5, retaining all four waits and
its oracle; its exact body adaptation is recorded. The existing store-timing
EOF branch uses an equivalent guarded match, and the quota report preserves
descending stable order with `sort_by_key(Reverse(...))`. Existing dead-code
fingerprints are accepted only as proven strict subsets or reviewed source
relocations from the original baseline, with the proof in
`clippy-review-dispositions.json`. Newly unused production methods are removed
or given their correct test-only ownership instead of being baselined.

## Final source execution receipts

`review-evidence.py --record-run` requires a clean checkout, a declared
configuration, runtime/toolchain version probes and a minimum positive test
count. It executes the actual command and writes an external JSON receipt and
log containing HEAD, tree, command, versions, configuration, exit status,
concrete pass/fail/ignored counts and log hash. A zero-selected filter, failed
process, changed log, stale HEAD/tree, changed toolchain or dirty source is
rejected. The receipt must live outside the checkout so it cannot change the
source tree it certifies. Configuration is an explicit declaration; the
recorded command and toolchain must be reviewed with it.

For example, after the final commit, with the required toolchain on PATH:

```sh
python3 scripts/review-evidence.py \
  --record-run /private/tmp/streams-final-rust.json \
  --minimum-tests EXPECTED_NON_CAPACITY_COUNT \
  --config-json '{"profile":"release","capacity":"separate"}' \
  --toolchain-command '["rustc","--version"]' \
  --toolchain-command '["cargo","--version"]' \
  -- cargo test --release --lib -- --skip post_split_throughput_scales
python3 scripts/review-evidence.py \
  --verify-run /private/tmp/streams-final-rust.json \
  --config-json '{"profile":"release","capacity":"separate"}'
```

Use the actual discovered test count, not the placeholder. Run the capacity
selector separately on an otherwise idle machine, and record SDK runtime tests
with Node/npm version probes and their actual count. The existing complete
gate, upstream conformance and release-certification scripts remain required
for the scopes they own; this receipt mechanism supplements their evidence.

Independent cryptographic review, actual Prisma Compute/Tigris campaigns,
fleet/load/storage-cost comparisons, official external conformance, and the
applicable Bun/Deno/live-server SDK legs remain **pending** until their own
final-build artifacts exist. The manifest carries explicit owners and required
evidence for each. A local mapped/full scenario is never a claim that these
external acceptance legs passed.

The recursive Rust identity lint now preserves source-relative paths. A nested
`application/creation/product.rs` cannot inherit `src/product.rs` ingress
exemptions, and creation/consumer `deletion.rs` diagnostics remain distinct.
Its controlled test accepts the two real ingress files and rejects nested
product, HTTP, tenant and deletion owners. Both the production scan and the
negative-control test pass when compiled directly with the cached release
`syn`/`quote` dependencies. The actual suite run still supplies final execution
evidence. Two explicit markers identify a pure URL-shape predicate and shard
engine-path scan cursors; neither carries customer stream identity.
