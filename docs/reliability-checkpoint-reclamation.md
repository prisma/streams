# Checkpoint retention, physical SST reclamation, and cold restoration

The test `checkpoint_pins_old_ssts_until_release_and_restores_exact_cut`
exercises the pinned SlateDB implementation through public APIs, using real
local object-store files and fresh operating-system processes. Its owner is
[`checkpoint_reclamation.rs`](../src/dst/tests/checkpoint_reclamation.rs).
The existing history crash campaign and this campaign share
[`fixture_process.rs`](../src/dst/tests/fixture_process.rs), which owns exact
child-test selection, environment input, evidence files, deadlines and reaping.

## What the campaign requires

The first process writes encrypted history records and postings into two
flushed SSTs. The running writer calls `Db::create_checkpoint` with
`CheckpointScope::All`; a second writer is never opened to obtain a snapshot.
The checkpoint contains exactly four records. Two later records create another
SST that is outside that checkpoint.

The test reads the typed `VersionedManifest`, submits a compaction of its exact
L0 view IDs with `Admin::submit_compaction`, and waits for
`CompactionStatus::Completed`. Completion alone is insufficient: the newly
published manifest must reference nonempty replacement SSTs disjoint from all
input SSTs. The writer explicitly refreshes its manifest.

SlateDB also creates a 15-minute checkpoint over the old input manifest to
protect readers that still use it. Before testing reclamation, the fixture
identifies only the new grace checkpoints, verifies their referenced SST sets
equal the completed compaction's exact input set, and deletes them through
`Admin::delete_checkpoint`. Every preexisting checkpoint must remain. This
controlled phase has opened no reader or iterator, the writer has refreshed,
and all recorded compactions must be completed. This accelerates a known
reader grace period for the fixture; production grace periods are unchanged.

Garbage collection also has time and publication guards. The fixture verifies
that every input SST ID predates the compaction ID and publishes a newer L0
barrier. That barrier contains no history payload. It then invokes the real
collector with zero minimum age for the compacted-SST directory only. This is
a test setting, not a production retention-policy change.

The same GC pass must produce both outcomes:

- Original SSTs referenced by the checkpoint still exist as nonempty objects.
- The unpinned input SST is actually absent, verified by object-store HEAD.

The second outcome is a causal control: a collector that did nothing cannot
pass the checkpoint-retention assertion. Live canonical and keyed history
reads must still return the six exact records with authenticated payloads.

The source writer remains open while the checkpoint manifest and its referenced
SSTs are copied into two independent local object-store roots. A seventh
durable history record is written between object copies. The source must
serve all seven records; neither restored checkpoint may include records that
follow its four-record cut.

The process exits with `std::process::exit(73)` after writing its mechanism
witness. This bypasses Rust destructors, DB close, final flushing and Tokio
shutdown. A fresh process reads the original checkpoint before releasing it,
closes that checkpoint reader, deletes the source pin, refreshes the writer's
manifest, and runs GC again. Every original checkpoint SST must now be
physically absent while the replacement SSTs and all seven live records remain.
It exits abruptly a second time.

A third process checks that original objects remain absent, audits every live
manifest SST reference, reads exact live history through both reader paths,
and restores the four-record checkpoint from the separate copy. No source
cache, DB handle, or background task survives either process boundary.

## Reference graph and negative control

The image inventory comes from SlateDB's public typed manifest APIs:
`l0()`, `compacted()`, `segments()` and `PathResolver::sst_path()`. The copied
manifest is the checkpoint's named immutable version. Every copied object has
an exact byte length and SHA-256 digest recorded independently of the restore
reader. Before restoration, the audit requires the image inventory to equal
the manifest's complete SST-reference set plus its manifest object, requires
the claimed checkpoint root to be present, and verifies every length and hash.

The fixture's image has one checkpoint root and no external or cloned-DB
references. It refuses external DB references because copying their SST paths
alone would not establish their retention or checkpoint ownership. Named
segment references are traversed by the audit, but this campaign's seeded DB
uses the unsegmented tree.

A fourth process deletes one referenced SST from the second copy. It verifies
physical absence, requires the reference/hash audit to fail, and separately
requires actual SlateDB checkpoint opening or reading to fail. The intact
backup is audited again afterward. An empty expected dataset, a missing object
that was never referenced, or a checker that ignores storage errors cannot
satisfy these requirements.

## Scope and operational prerequisites

This is an executable single-DB online checkpoint/copy/restore primitive over
a WAL-disabled history DB. Its checkpoint pin remains on the source until the
copy and audit complete, and restoration uses a fixed `DbReaderMode::Checkpoint`
reader. It neither relies on a graceful source close nor fences the running
writer to capture the image.

It does **not** wire the operational backup system described in
[`RUNBOOK.md`](../RUNBOOK.md) and [`OPERATIONS.md`](../OPERATIONS.md). A service
backup additionally needs a consistent relationship between shard watermarks,
history data, registry metadata, application-fork references, outstanding WALs,
key custody and retention. Independently successful per-DB checkpoints do not
prove that those relationships form a recoverable whole. This campaign proves
no service-wide recovery-point objective, PITR cutoff, provider failover,
application-fork retention, or deletion/erasure policy.

Application forks retain source generations through registry relationships.
Those references are distinct from SlateDB checkpoints and are not created or
released by this test. The SSTs reclaimed here are obsolete physical inputs
whose live data has already been rewritten into a referenced replacement;
the campaign does not claim that deleting an application stream physically
erases its history.

Successful local object operations are assumed to survive process termination.
This is not a power-loss/filesystem-fsync test. SHA-256 detects changes in copied
objects but does not authenticate a maliciously replaced image and digest file.
The payload checks use fixture-held keys to verify exact known plaintext.

## Running the checks

Use the exact toolchain in `rust-toolchain.toml`:

```sh
cargo test --locked --release --lib checkpoint_pins_old_ssts_until_release_and_restores_exact_cut -- --nocapture
cargo test --locked --release --lib process_crashes_preserve_history_through_reclamation -- --nocapture
```

Each child is deadline-bounded, must run the exact named test, and must leave a
phase-specific witness matching the parent's plan. Logs and mechanism state
remain under the reported temporary directory on failure. Successful campaigns
remove their temporary object stores. An ordinary full-suite invocation of a
child helper does nothing unless the parent supplied its explicit plan.
