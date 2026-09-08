# Rust quality policy adoption

> This is the adoption receipt at `74e1faae44d8ddebed4ccdacc918ae17e75402b2`. Subsequent cache removal and peer compatibility work is recorded in the [read disposition report](../read-experiments/final-disposition.md). The original counts and invariant evidence below retain their recorded source scope.

The repository owner adopted [the normative policy](../RUST-QUALITY.md) on
8 September 2026. The proposal's structural standards, focused lint profile,
non-growing legacy migration, and invariant-specific verification are retained.
Tool success does not grant source, performance, cryptographic or deployment
acceptance for PR #19, which remains draft.

The exact legacy source is `5bdaf9684197ff84bd544fd0fcd69520001ea196`.
Platform inventories are captured from that commit with the adopted lint profile
and the explicitly recorded scoped-lint overlay, never from the modified code.
The macOS inventory records 3,549 warning occurrences; 3,538 remain after the
initial ownership and safety-comment fixes. Denied diagnostics are never allowed.
The Linux inventory was independently captured by CI and reviewed against the
macOS capture: source tree, every source hash, lint profile, scoped overlay and
compiler pin match. Linux produced the same 3,549/3,538 counts and zero new or
denied diagnostics. The [capture receipt](linux-adoption-receipt.json) identifies
the actual tested PR merge commit and workflow; each target has a frozen table.

Implementation includes exact tool pins, one local/CI entry point, parsed Rust
source rules, occurrence-sensitive diagnostic ratchets, private-owner compiler
fixtures, generated decoder/admission cases, actual handoff-state Loom tests,
Miri-compatible buffer tests, saved fuzz seeds and diff-scoped mutation checks.
The pinned repository-local Cursor skill is unchanged upstream content. The
Cursor path points at the canonical `.agents` copy.

Runtime changes release planning windows and test lock guards before suspension,
give the RSS sampler its actual admission/shard handles, and surface failed
allocator-worker joins. Existing unsafe calls now state their pointer/layout
preconditions. No read algorithm or retirement transition has been rewritten.
Dependency decisions are in [dependencies.md](dependencies.md).

Verification has passed 12 real privacy failures and 10 typed-lint failures with
three legitimate controls, eight syntax-scanner tests, fourteen Python gate tests,
seven properties (1,024 cases each), two bounded Loom models, four compatible
Miri buffer tests, saved decoder seeds and a 121-second fuzz run (26,513,694
inputs). The 52 tests in the isolated owner harness pass; they overlap the root
suite and are not counted as additional server tests. All 33 SDK tests, its
authentication/capability vectors, build and typecheck also pass.

The completed mutation experiment selected 79 exact production mutations:
68 were caught by tests and 11 could not compile; zero survived or timed out.
Every owner had a passing unmodified baseline. Early experiments exposed missing
nonzero-prefix extension cases and batch concatenation/selection cases. The
minimized `(start=1, count=1, extra=1)` extension case is committed, along with a
nonempty-batch regression that checks offsets, payloads and capacity after a
second selection. Two production payload trait implementations existed only to
support test comparisons; removing them and comparing actual bytes removes four
irrelevant mutation sites. No failed experiment is counted as a passing run.
Detailed identities and bounded-check metadata are in [verification.json](verification.json).

The complete local commit gate passed at source revision
`a1d9dabf6ac3933f0115380f0c4c6d0f3294e802`: 938 Rust tests in the full run and
the remaining capacity test in isolation, with zero failures or ignored tests.
The fresh compiler fixtures match the current canonical Rust inputs. The final
exception-gate controls also pass: all compiler lint groups and denied lints are
protected against suppression/downgrades, including nested conditional forms.
Source and diagnostic ratchets pass after that tightening. Rust source did not
change during this last tooling-only check.

CI runs the same quality entry point, real compiler fixtures, generated and
Loom checks, saved fuzz corpus, compatible Miri checks and mutation driver.
The existing full suite, protocol, SDK package, fleet, platform, multitenancy
and capacity jobs remain required. Live status is on [draft PR #19](https://github.com/prisma/streams/pull/19);
this report records local executions and frozen capture provenance rather than
embedding a status that changes after the report commit.

Miri does not cover the allocator/system FFI paths. The initial macOS async Miri
fixture stopped at unsupported `kqueue` setup before assertions; directly polling
the I/O-free buffer checks then passed. Fuzz throughput is instrumentation data,
not a server performance measurement. Raw logs, binaries and generated corpus
remain local under the existing evidence-upload hold.

The compatible verification crate in `tools/quality-invariants` includes the
actual private postings, batch, budget, retained-byte, crypto and tenant modules
through Rust `#[path]`. It preserves the production tests and nominal types;
it neither edits copies of the implementations nor exports a verification API.
It excludes server dependencies whose startup constructors prevent Linux Miri
from reaching the assertions (`fastant` executes unsupported CPUID assembly).
All four buffer tests pass in this harness on macOS.

Mutation scope is generated twice by Git against the same actual merge base:
canonical paths and the lexical `#[path]` prefix retained by cargo-mutants.
Before running, the driver normalizes paths and requires identical selected
function spans, operators and replacements. It therefore tests the same changed
production code through the smaller crate. Full-server mutation attempts were
interrupted during preparation; they do not count as completed experiments.

The standard is applied as the proposal's migration ratchet: 3,538 legacy
warnings and 17 inherited files above 1,000 lines remain visible debt. Denied
lints cannot be grandfathered. Three existing conditional dead-code exceptions
are explicitly registered; every branch of new nested conditional attributes
is checked. Unknown macro DSLs, globs and three pinned historical Rust templates
are reported inventories, not a claim of complete macro/name resolution.

To reproduce locally, use Rustup's Cargo/Rust proxies so root
`rust-toolchain.toml` selects Rust 1.98.1 (a standalone older Cargo is rejected).
Install the exact helper versions with `scripts/install-quality-tools.sh`, add
`target/quality-tools/bin` to `PATH`, and run `scripts/gate.sh`. Install the dated
nightly with Miri and rust-src for `scripts/quality/nightly.sh`; run compiler
fixtures with a fresh `--out` directory and `scripts/quality/mutations.sh` against
`QUALITY_BASE_REF` set to the actual PR target. Raw output stays local.
