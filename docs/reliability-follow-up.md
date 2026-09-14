# Reliability follow-up: mechanisms, findings and acceptance scope

This work continues the [first reliability report](reliability-confidence.md).
The first implementation was committed and pushed as
`db2157e58e2880ef83085770e5fc609bfdca0176` on
`codex/reliability-confidence`, based on `origin/slate` at
`adc2cdc5bdc2dba1aaffabc599d61de3df35fcb1`. The follow-up adds independent
checks at lifecycle, physical reclamation, provider and release-version
boundaries. Source changes and local test results do not lift the existing
performance, cryptographic, deployment or raw-evidence upload holds.

The follow-up implementation and TLS dependency patch are committed and pushed
as `2863bab3e38a5ba81140f3d447a23156c138a616`. Later report-only changes preserve
that candidate's executable source and lockfile.

## First implementation

The first commit added an external fsynced operation journal, exact public-read
oracle and isolated whole-store restore drill; six systematic crash cuts with
24 fresh child processes, including a second crash during recovery and physical
reclamation; and bounded ownership/reclamation models with executable semantic
mutations. The independent evidence exposed two real read defects:

- A cold postings-cache load claimed coverage past its durable target, allowing
  a later absorbed record in the same offset bucket to be omitted. Coverage now
  follows proven durable runs, and superseded loads cannot publish stale state.
- A capped dense index could move a read cursor backward. The reader now falls
  back to the bounded canonical scan when the index cannot prove progress; a
  sparse-index control ensures valid indexed reads remain effective.

Regression controls, compiler boundaries and mutation checks cover those
repairs. The work also corrected a spool recovery test that reused a fenced
writer and SDK packaging checks that could resolve the repository's package
instead of the installed tarball. The
[first report](reliability-confidence.md) records all 38 selected production
mutations: 22 detected by tests and 16 compilation exclusions, with no survivor
or runner timeout after the final iteration. Compilation exclusions are not
counted as semantic detections.

## What was added

| Mechanism | Concrete obligation | Owner and commands |
| --- | --- | --- |
| Authenticated lifecycle replay | Exact acknowledged records across same-name recreation, retained forks and workspace transfer; deletion cannot hide an earlier missing record | [Lifecycle guide](../bench/reliability/LIFECYCLE.md) |
| Checkpoint and SST reclamation | User pins retain physically obsolete SSTs while an unpinned control is deleted; releasing the pin permits deletion; fresh processes still recover exact records | [Checkpoint/GC guide](reliability-checkpoint-reclamation.md) |
| Online single-DB checkpoint copy | A live source advances to seven records while an independently copied checkpoint remains exactly four; missing referenced SST fails both audit and actual reader | [Checkpoint image contract](reliability-checkpoint-reclamation.md#reference-graph-and-negative-control) |
| Provider contracts | Conditional races have one winner, rejected updates leave bytes unchanged, and GET/HEAD/range/LIST/delete agree | [Provider guide](reliability-provider-contracts.md) |
| Actual-binary version transitions | Prior records survive cold process replacement before any retry can repair an omission; actual producer retries remain unique | [Version matrix](reliability-version-transitions.md) |

The external journal and replay remain independent of the server's read planner,
storage decoder, producer state and simulator expectations. Lifecycle events are
serialized and checkpointed, with real project/workspace JWTs on one cell and
one store. Internal workload-authenticated forks are explicitly distinguished
from the customer product API, which has no public fork method. The seed varies
bounded workload order; it does not make Tokio or OS scheduling deterministic.

The physical reclamation fixture calls the pinned SlateDB compactor and GC,
then verifies object absence through HEAD and reads after process death. It
retires only the completed compaction's newly introduced reader-grace pins in
a controlled phase without old readers; it preserves the user's checkpoint.
No production retention timer, runtime experiment switch or service failpoint
was added. Both crash campaigns share one bounded subprocess supervisor.

The version matrix builds unchanged historical source from immutable commits.
It checks the tree, lockfile, crypto source, SlateDB pin, format declarations
and executable hashes. All writing phases must demonstrate actual compression
effects. rc.4 supports frames 2/3, whereas current writers emit 4/5; the
documented hard downgrade prohibition remains in force. The same-format
round trip to the pre-repair revision is a cold-read compatibility experiment,
not approval to deploy its known warm-cache bug.

## Defects found in the verification mechanisms

Independent review and deliberate faults produced concrete failures before the
new acceptance runs:

- Reusing a lifecycle invocation ID could replace an earlier acknowledgement.
  Invocation IDs are now globally unique, with a rejecting replay control.
- Raw fork records could be reordered while healthy keyed reads concealed the
  raw-path violation. Raw order is now checked separately; a raw-only mutation
  fails with an unchanged legitimate keyed-read control.
- End-of-run binary hashes could mislabel a campaign whose executable changed
  between restarts. Lifecycle and version campaigns now verify stable binary
  identity before launches and before accepting receipts.
- Version receipt validation did not initially verify every source field, and
  compression evidence initially omitted later writers. The consumer now
  validates immutable metadata and each paired writing phase.
- The existing provider verifier printed failures while returning success. CAS,
  fencing, WAL-off recovery and clone results now assert their obligations and
  propagate failure. Fencing waits on the actual `WriteHandle::await_durable`
  API and requires `Closed(Fenced)`; successful memory admission is insufficient.
- Constant range bytes and size-filtered LIST entries could conceal stale
  ranges or duplicated keys. Distinguishable ranges and exact key multiplicity
  are now exercised by dedicated wire faults.
- The existing clone fixture used WAL-enabled sources unsupported by projected
  union. It now uses the service's WAL-disabled history posture and explicit
  SST flushes, checking all 500 low, 500 high and 1,001 union records.

The provider proxy uses a 64-connection backlog and correctly framed HTTP/1.1
connections for deliberate concurrent races. A retained earlier attempt failed
on a transport reset in the rig; that failure was not counted as a semantic
fault detection. Every accepted negative requires an engaged fault and the
specific intended failed assertion.

The fresh full gate also rejected Rustls 0.23.41 under newly published
`RUSTSEC-2026-0285`. The lock now selects patched 0.23.45 and its required TLS
dependencies. The [dependency record](quality/dependencies.md) links the upstream
advisory and records the exact changes. No vulnerability exception was added.

## Integration and evidence

`scripts/quality.sh` runs all Python oracle/provenance controls and the finite
models alongside the adopted Rust gates. `scripts/gate.sh` runs the complete
release library suite, the capacity test in isolation, then
`scripts/reliability-campaign.sh`. That script resolves release executable paths
from Cargo artifact messages and runs crash/isolated-restore, provider controls
and authenticated lifecycle checks. Python 3.11+ and Node 22+ are required; the
existing platform emulator needs no npm installation.

CI has named `reliability-recovery` and `reliability-versions` jobs. The historical
matrix has a separate build cache and deadline; it does not silently reuse the
current executable under historical labels. These workflow changes are local
source changes until their remote jobs actually run.

The patched local `scripts/gate.sh` completed with `GATEDONE`. Its source-bearing
release build is recorded in `target/reliability/run.AZefBG/build.jsonl`; the
gate log is `target/reliability/wave2-patched-gate.log`.

| Patched candidate check | Result |
| --- | --- |
| Release library suite | 1,071 passed; zero failures |
| Isolated capacity mechanism | Passed in its separate run |
| Python oracle/provenance controls, including historical reuse | 89 passed |
| Finite protocol models | 29,280 states, 84,686 transitions, all 13 witnesses and 11 semantic fault detections |
| Cold crash, retry and isolated restore | Nine exact checks passed; 66 final acknowledgements restored from 36 objects / 112,837 bytes; physical loss of two registry descriptors was detected |
| Provider campaign | Five positive commands and nine engaged negative controls passed |
| Authenticated lifecycle seeds 17 and 91 | Each passed with 36 acknowledgements, 10 incarnations, 17 exact checkpoints and two SIGKILLs |
| Protocol and SDK | 332 conformance cases passed with six reserved-API skips; 33 SDK unit tests, typecheck and auth/watch vectors passed |
| Installed SDK consumer | 23 checks passed on each of Node 22, Node 18, Bun and Deno using the actual installed tarball |
| Product field gate | 20 checks passed, including an actual automatic split |
| Compiler boundaries | Three positive builds passed; all 12 privacy violations and 16 typed-effect violations were rejected with their expected diagnostics |
| Actual Rust invariants | 20 selected property/concurrency tests passed, including the production handoff implementation's Loom checks |
| Miri and saved decoder corpus | Four Miri tests passed; all six saved corpus files loaded in seven libFuzzer runs |
| Provider verifier unit control | In-memory conditional object contracts passed |
| Immutable candidate version matrix | Four campaigns, 18 fresh servers, 42 cold checks, 112 unique acknowledgements and 40 retained producer retries passed; all seven compression comparisons passed |
| Dependency policy | Advisory/license/source/ban gates passed; no new warning or advisory allowance |

The patched gate, SDK and lifecycle campaigns used a release build made before
the follow-up source commit. Its retained source patch reconstructs all 377
source/build inputs exactly as committed in `2863bab`; its embedded Git revision
still names the first commit. The build-input audit preserves that distinction.
The separate version matrix builds the candidate from its clean immutable
commit. Earlier version and lifecycle receipts remain explicitly historical.

The first-wave 38-candidate mutation results remain tied to the original
dependency graph; the selected algorithm and test files are byte-identical.
Those mutation outcomes are not represented as a rerun against the patched TLS
dependencies.

The final invariant refresh retained failed preflight attempts for the corpus
and allowance-pruning commands: their process PATH omitted the installed pinned
tools. Correcting the PATH allowed the unchanged commands to run. The failed
preflights exercised no corpus input and added no warning or dependency
exception; their logs are retained separately from the successful reruns.

### Local evidence index

These paths are relative to the repository root. The generated receipts bind
commands, logs, source inputs and executable hashes; they are not promises of a
deployed artifact's identity.

| Evidence | Retained path |
| --- | --- |
| Full patched gate | `target/reliability/wave2-patched-gate.log` and adjacent suite/capacity/reliability logs |
| Exact release-build input reconstruction | `target/reliability/follow-up-validation/release-build-input-audit.json` |
| Frozen Python controls | `target/reliability/follow-up-validation/python-final.log` |
| Crash/retry/isolated restore | `target/reliability/run.AZefBG/campaign/receipt.json` |
| Local provider positives and semantic negatives | `target/reliability/run.AZefBG/provider/receipt.json` |
| Authenticated lifecycle seeds and source audit | `target/reliability/lifecycle-rustls-final-validation.json` |
| Protocol, installed SDK and actual-split field checks | `target/reliability/protocol-sdk-rustls-final/validation-receipt.json` |
| Compiler, property/concurrency, Miri, corpus and verifier refresh | `target/reliability/follow-up-invariants/final-receipt.json` |
| Clean candidate and historical binary provenance | `target/reliability/version-binaries-tls-final/build-receipt.json` |
| Four actual-binary transition campaigns | `target/reliability/version-matrix-tls-final/receipt.json` |
| Consolidated final acceptance and artifact hashes | `target/reliability/follow-up-validation/receipt.json` |
| First-wave mutation outcomes and diagnostic audit | `target/reliability/mutations/combined-outcomes.json` and `diagnostic-audit.json` in the same directory |

Raw journals, emulator credential feeds, copied objects and logs remain
under ignored `target/reliability/` directories and are not uploaded.

## Remaining environment-dependent acceptance

Live-provider testing needs an explicitly designated disposable endpoint,
bucket, region and access topology. The executable probe is ready; a local
emulator pass cannot establish the provider's behavior. Separately, a host
power-loss campaign needs an isolated test host and an external journal disk.
SIGKILL and abrupt Rust process exit do not test filesystem or device flush
durability. No such live target or host was designated during this work.

The complete quiescent copy/restore drill includes registry and data objects,
then removes the primary emulator from the recovery path. The new online
checkpoint copy covers one history DB. Whole-service online backup/PITR still
needs coordinated shard watermarks, registry/control-plane metadata, fork
references, outstanding WALs, checkpoint retention and key custody. Independent
per-DB snapshots do not establish a consistent service recovery point. The
operational five-minute RPO remains a target, not a measured guarantee or a
zero-loss claim after total provider loss.

Further protocol work remains distinct: requests completing remotely after a
crash, live mixed-version peer traffic, concurrent lifecycle linearizability,
TTL/physical erasure, arbitrary multi-key fork formats, long production-setting
GC campaigns, and independent storage-protocol/cryptographic review. The finite
models retain their documented bounds; they are not a proof of the whole service.
