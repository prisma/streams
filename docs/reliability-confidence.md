# Reliability confidence additions

These additions target independent evidence about acknowledgment, recovery and
destructive transitions. They exposed a production keyed-read omission and a
non-progressing read cursor; both repairs have focused regressions. Rust toolchain and
dependency pins and existing release holds are unchanged; warning budgets are
not increased.

## Defects found and repaired

A cold keyed read of four durable records used to cache an absence proof through
the end of the 65,536-offset bucket. After a fifth record was absorbed into that
bucket, write-through installation considered it already covered. The scan
returned all five records, while the keyed read returned four and claimed
completion. The new process-recovery fixture reproduced this with the original
production source in debug and release builds. Four focused cache regressions
also failed against the original release implementation.

The cache now caps both its retained runs and its coverage proof at the caller's
durable target. Publication checks that asynchronous loading has not superseded
a newer write-through slice. Gap proofs require complete write admission, and
byte-capped reloads preserve an already-proven prefix. The slice's proof-bearing
fields are private; sibling-module compiler fixtures reject their construction
and mutation.

A dense, valid postings bucket can exceed the index load budget before reaching
the requested cursor. The read owner now uses its existing bounded canonical
envelope scan when the index cannot prove progress. The regression exercises
both matching records and empty filtered pages at a one-record page budget.
Its sparse positive control requires a healthy index to skip other keys within
the same budget, preserving the existing indexed path.

The full suite also exposed a preexisting spool-recovery fixture error: reopening
the spool fenced its old writer, yet the fixture drained through that old owner.
The fixture now installs the recovered owner, verifies exact recovered batch
contents and checks the pending-to-acknowledged transition. Its name and comments
accurately describe durable reopening; actual process-crash evidence comes from
the separate process campaign.

The SDK packaging gate previously invoked its smoke script from inside the SDK
source package. Package self-reference could therefore test the source build
instead of the installed tarball. CI now copies the unchanged script into the
consumer directory before running it on Node, Bun and Deno. Deno uses manual
`node_modules` mode to read that installed package; its automatic installation
mode rejected the npm tarball's `file:` dependency before any smoke checks ran.

## Implemented evidence

| Layer | Mechanism and required evidence | Entry point |
| --- | --- | --- |
| Independent external oracle | Fsynced invocation/outcome journal; exact bytes, logical identity, duplicates, per-key real-time order, ambiguous retries, independent expected stream list; executable positive and negative controls | `python3 -m unittest discover -s bench/reliability -v` |
| Release binaries | Concurrent HTTP clients; actual lost response; SIGKILL and cold process recovery; exact scans/keyed reads with three page budgets; post-recovery progress | `bash scripts/reliability-campaign.sh` |
| Isolated restore | Quiescent copy of all registry/data objects; primary server/store terminated; fresh store restored only from independently retained files; exact client checks; physical registry-loss negative control | Same release campaign |
| Actual Rust recovery | Six selected persistent-state cuts, fresh child processes, a second interruption during recovery metadata retrieval, recovery maintenance, original log-key reclamation and physical original WAL deletion before final cold reads | `cargo test --locked --release --lib process_crashes_preserve_history_through_reclamation -- --nocapture` |
| Finite protocol models | Exhaustive ownership/acknowledgment and history/reclamation transition exploration; positive reachability obligations; eleven executable semantic mutations and retained shortest counterexamples | `python3 scripts/reliability/models/check.py --output target/reliability/models.json` |

The default release campaign uses one namespace and two fresh, non-expiring
streams. It ends with 66 acknowledged logical appends after retry and progress,
and checks three recovery phases at three page budgets. These nine checks do
not exercise concurrent tenant lifecycles or delete/recreate semantics. See the
[external checker workload and boundaries](../bench/reliability/README.md).

The Rust fixture selects these six cuts: acknowledged WAL; history SST write
pending; durable history with the shard boundary held; first durable absorbed
boundary; partial original-key trim; completed original-key trim. The history
fault wrapper is separate from the shard DB store, so a background shard SST
write cannot accidentally satisfy a history cut. Child identity and mechanism
witnesses prevent an empty test selector from passing.

Each case then interrupts a new process during an engaged manifest GET, opens again,
checks exact expected offsets and payloads before new absorption can repair
state, finishes maintenance and appends another record. The fixture flushes the
shard memtable and invokes SlateDB's WAL collector with zero minimum age in its
isolated namespace. Original nonempty WAL paths must be physically absent both
before exit and after a final cold open. This is an original-WAL deletion test;
it does not claim physical old-SST compaction/GC coverage.

## Canonical ownership and review

The external journal owns expected results. It imports no production read
planner, identity derivation or deduplication implementation. The process rig
owns fault injection and lifecycle evidence; it does not calculate expected
records from recovered storage. The Rust fixture uses the existing absorber,
committer, reader, fault store and SlateDB administration interfaces. No new
production failpoint or runtime switch is introduced.

The small models are separate specifications. Their source-symbol mappings and
source hashes expose implementation drift to reviewers; they do not prove
refinement of Rust into the abstract model. The checkpoint model is explicitly
an abstract extension because the operational backup integration is absent.
Crashes erase staged model work; late completion of an object-store request
after process death is outside this finite model.
See [model assumptions and transition mappings](../scripts/reliability/models/README.md).

The repository's pinned `thermo-nuclear-code-quality-review` skill was used for
structural review, including an independent review of the process tests and
gate. That review corrected an ambiguous SST fault witness, a stale-executable
risk with nondefault Cargo target directories, and a retry that could conceal
loss of the known-successful response-discarded write. The new source owner
entries register one test module, two test-only `select!` sites and the generated
property's assertions; they do not grow an adoption baseline or waive compiler
diagnostics.

## Reproduction and scope

Use the exact root toolchain and `scripts/gate.sh`. The common quality gate runs
oracle/restore controls and finite models, the Rust suite runs process crash
cuts, and the final gate runs the release-binary restore drill. CI contains a
named `reliability-recovery` job. Failure at any stage propagates. Evidence is
kept locally; no raw-evidence upload or release certification is performed.

Invariant selection uses the actual `origin/slate` target merge base through
`scripts/quality/verification_plan.py`. The read repairs select registered
production mutation scopes. Changes to the compiler-fixture tooling also select
compiler boundaries, generated properties, saved decoder corpus, Loom and Miri.
The abstract semantic mutations and
independent checker controls are separate experiments with their own mechanisms
and results.

The full release library suite passed 1,069 tests, and the capacity test passed
in its required isolated run. This includes the six crash cuts and all 24 child
processes, ten cache durability/admission controls, both cursor-progress tests,
and the corrected spool-reopen fixture. The independent checker and restore
suite passed all 44 controls. The finite models exhausted 29,280 states and
84,686 transitions, reached all 13 required positive witnesses, and detected
all 11 planted semantic faults. Their final local receipt is
`target/reliability/models-verified.json`.

The actual-target verification plan selected both changed production owners,
compiler boundaries, properties/corpus, Loom and Miri. Compiler fixtures passed
three legitimate-use controls and required the exact diagnostics for 12 privacy
and 16 typed-effect rejection cases. All four selected Miri tests and the saved
postings decoder corpus passed. The common quality gate and warning-pruning
check passed with zero emitted Rust warnings and no adoption-baseline growth.

Production mutation validation accounted for all 38 selected candidates against
the actual target diff: 22 were caught by tests and 16 could not compile, with
no unresolved candidate or runner timeout. Compiler exclusions are not counted
as test detections. The initial history campaign exposed an always-fallback
survivor; the added sparse-index positive control caught it after a fresh
passing baseline. The original failure, exact candidate identities, subsequent
outcomes and source hashes are retained in
`target/reliability/mutations/combined-outcomes.json` and its linked logs.
Two cache resource controls separately caught nonzero-bucket arithmetic and
repeated capped-load mutations that passed the other cache tests.

The local validation also exercises the existing protocol and packaging gates:
332 upstream conformance cases passed, with six explicitly permitted reserved-API
skips; all 33 SDK unit tests passed; the installed tarball passed 23 smoke checks
on each of Node 22, Node 18, Bun and Deno. Credential-cancellation checks passed
on Node 18, Bun and Deno, and the 20-case product field gate exercised a real
automatic split and exact keyed reads across it. Commands, runtime versions,
artifact hashes and the original Deno failure are retained in
`target/reliability/protocol-sdk-final/validation-receipt.json` and its sibling
logs. These local executions do not claim that remote CI has run.

The complete local `scripts/gate.sh` finished with `GATEDONE`, including the
integrated release-binary crash/restore campaign. Its final receipt is
`target/reliability/run.VC0qsB/campaign/receipt.json`: all nine exact-result checks
passed, the response-discarded write survived before retry, all 66 final
acknowledged appends survived isolated restore, and physical deletion of two
stream descriptors triggered the expected checker failure. The independent
copy restored all 35 captured objects, totaling 112,803 bytes. Binary, checker
and retained source-patch hashes were verified. The consolidated evidence index
and retained gate logs are in `target/reliability/final-validation/receipt.json`.

The [follow-up report](reliability-follow-up.md) records subsequent provider
contract controls, actual-binary version fixtures, authenticated lifecycle/fork
replay, physical checkpoint-pinned SST reclamation, and online single-DB copying.
The results above describe the first implementation at `db2157e`; later receipts
identify their own sources and binaries. Live-provider and power-loss evidence,
coordinated whole-service backup/PITR and independent cryptographic acceptance
remain separate. Existing performance, cryptographic, deployment and upload
holds remain independent of local results.
