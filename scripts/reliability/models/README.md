# Executable bounded protocol models

Run from the repository root with Python 3.10 or newer (standard library only):

```sh
python3 scripts/reliability/models/check.py --output /tmp/streams-protocol-models.json
```

The same command replays every checked-in counterexample, exhausts both baseline
state graphs, requires positive reachability witnesses, and checks every semantic
mutation. Any missed mutation, wrong failure mechanism, unreachable required
scenario, or missing source anchor exits nonzero. It does not use Python `assert`
for acceptance, so `python3 -O` retains the checks. Omit `--output` for a quiet
quality-gate run with two summary lines.

The receipt contains bounds, exact state/transition counts, shortest deterministic
witness traces, mutation failures and terminal states, the checked-out revision,
and hashes of model and mapped implementation sources. Hashes record provenance;
they are not frozen baselines. Source anchors detect deleted/renamed mapped
boundaries, **not semantic equivalence**. A reviewer must revisit this mapping when
those owners change. Models neither import production decision logic nor claim
that a production execution has been trace-refined against them.

## Exploration and acceptance

A breadth-first queue exhausts every reachable state; there is no depth cutoff,
random seed, sampling, or success after a timeout. Immutable states and canonical
transition order make shortest witnesses reproducible. Mutations alter executable
transitions, keep the original invariants, and must reach the named invariant
failure. Compile failures, exceptions, and absent transitions do not count as
mutation detection. `counterexamples.json` retains a concrete failing execution
for each mutation and is replayed before exploration.

Current complete baseline results:

| Model | States | Transitions | Required reachability witnesses | Detected semantic mutations |
| --- | ---: | ---: | ---: | ---: |
| Ownership/receipts | 2,102 | 4,941 | 7 | 7 |
| History/retention | 27,178 | 79,745 | 6 | 4 |

These numbers describe these models only. The models are intentionally small,
independent protocol specifications, **not formal verification of the service**.
Exhaustion establishes the stated safety invariants within the finite transition
systems and assumptions below. Positive witnesses establish reachability, not
fairness, eventual progress, or a latency bound.

## A: ownership, commitment, retry, and identity

Bounds: two owners with one retirement/takeover, one stream name, two incarnations,
one logical producer write and at most one retry per incarnation. A write moves
through admission, storage acceptance, applied publication/barrier registration,
durable completion claim, durable effects, and response delivery. Remote durability
is independent of response delivery and applied publication. An accepted operation
may finish after retirement; retirement prevents new applied publication and new
completion claims by that owner. A pre-retirement claim can publish effects and
deliver a receipt after retirement. The latter must be reachable in the safe model.

Safety requirements:

- Every delivered original/retry receipt has a remotely durable record.
- An attached retry also requires its admitting owner's durable effects to have
  been published. Captured retry authority distinguishes that obligation from
  a replacement owner's canonical recovery, even after the original retires.
- Applied publication and completion claims have a live owner at that transition.
- The original admitted incarnation determines the persisted identity even when
  the same stream name has been recreated before storage completes.
- A retry preserves exactly one record for the logical operation.

Required witnesses also include both incarnations completing, original plus retry
receipts for one record, a durable unknown outcome surviving an owner change, and
old work completing after recreation. Separate witnesses require a pending attached retry
to become deliverable only at durable effects and recovered retry receipts even when the old
owner never published applied work. New-owner retries read canonical persisted
producer results; retries on the existing pending group share its barrier.

The model abstracts away storage CAS/fencing internals, HTTP transport, producer
sequence arithmetic, offsets, tenant/project cardinality, channel cancellation,
and recovery implementation. It keeps accepted storage completions separate from
process-local publication; durability is monotonic and storage keys are immutable.
Old-incarnation physical bytes are retained in this model even after logical
recreation, so no retention promise is inferred for a deleted stream. An
undelivered response is unknown, not proof that the write failed. The model does
not assume that owner retirement cancels an already accepted storage operation.

| Abstract transition | Implementation owner and precondition |
| --- | --- |
| Admit/write accepted | `src/shard/transaction/finalize.rs`, `CommitTransaction::write`: storage write precedes `publish(handle.seqnum(), ...)` |
| Applied publication + barrier registration | `src/shard/transaction/publish.rs`, `CommitTransaction::publish`; `CommitHandoff::publication` holds one handoff mutex across mirrors and pending insertion; retired owners reject |
| Retry attaches pending / observes already durable result | `src/shard/transaction/append.rs`, producer duplicate decision; `finalize.rs::join_prior_barrier` takes dispatch gate then `CommitHandoff::attach` |
| Remote durability and completion claim | `src/shard.rs::dispatch_durable` supplies the storage durable sequence to `CommitHandoff::take_durable`; this model uses per-write durability instead of reproducing sequence arithmetic |
| Effects then receipt, including delayed receipt | `src/shard.rs::dispatch_durable` owns drained groups, publishes durable mirrors/ring, then replies after dropping handoff lock |
| Retire / takeover | `src/shard.rs` retirement calls `CommitHandoff::retire`; a new owner represents canonical storage recovery, not a proof of election or provider fencing |
| Recreate / complete old operation | `src/crypto.rs::SegmentHash::for_stream/for_segment`; `src/tenant.rs::storage_hash_input` includes project, name, epoch; admitted identity remains attached to the request |

Semantic mutations remove remote durability before claiming, skip the retry
barrier or attached-retry effects publication, permit publication/claims after
retirement, key storage by the current
name incarnation, or persist a duplicate on retry. Each has its own retained
execution and named invariant failure.

## B: history publication, physical deletion, and retained roots

Bounds: two initially acknowledged rows, five physical objects (two originals,
two history objects, one compacted history object), two absorption boundaries,
two crashes including a crash during recovery, one fork reference, and one
checkpoint root. Object bits are object identities; row positions and absorbed
boundaries are distinct values. A history object can become durable before its
manifest reference, and both can persist before the shard boundary is committed.
Shard absorbed/trim metadata and original tombstones commit atomically. Trimming
uses the previous absorbed boundary, matching the implementation's one-pass lag.
Thus two rows are enough to physically reclaim the first original row.

Every cut before or after an enabled persistence transition is reachable; the two
history object writes can complete in either order and in any allowed subset
before a crash. Atomic object/manifest operations are either absent or wholly
present. Their commit steps do not require a client success response: crashes
with durable but unreferenced objects and durable shard updates are explored.
The model does not invent torn atomic object writes. A crash erases all staged
work, observed flush knowledge, and pending shard publication, while preserving
only durable objects, roots and metadata. Recovery has separate begin/finish
steps, so another crash during recovery is mandatory reachable coverage.
Accepted remote requests completing after process death are not represented:
this model enumerates persistent completion subsets before each crash, and
recovery observes those retained outcomes. Late provider completions require a
separate implementation/provider campaign.

Safety requirements:

- Every active or retained root references physically present objects.
- A durable absorbed boundary has durable replacement objects and manifest
  references for every row below it.
- Every initially acknowledged row still required by the source or a fork has
  an original or history representation after every transition.
- Trim safety never exceeds the durable absorbed boundary.

The checker requires **physical** original removal followed by cold recovery,
physical obsolete-history deletion after compaction, retained checkpoint roots
protecting a tombstoned original, and fork references preserving deleted-source
records. GC ignoring either kind of retention must execute a physical deletion
and fail the root invariant, rather than merely change a candidate list.

| Abstract transition | Implementation owner and precondition |
| --- | --- |
| Stage history rows / persist objects and references / observe flush | `src/history/gather.rs::Absorber::commit`: history write then `part.flush().await?`; with WAL disabled the flush covers L0 objects and manifest publication; object/reference steps expose that dependency's contract |
| Stage shard absorbed boundary | `gather.rs::commit` calls `submit_absorbed_batch_v2` only after history flush returns |
| Commit boundary plus lagged trim | `src/shard/transaction/maintenance.rs::absorbed`: `trim_safe_to.max(prev_absorbed)`; tail and row deletes share the transaction finalized by `finalize.rs::write` |
| Physical deletion and compaction manifest replacement | `src/history.rs::history_settings` configures SlateDB GC; the dependency owns physical SST/manifest deletion, not the Streams trim loop |
| Take/release fork / delete source | `src/application/creation/anchor.rs` installs the source reference; `fork.rs::prepare` checks `source_epoch` and membership; `deletion.rs::delete_transition` retains a source with children and `release_fork_ref` uses the expected incarnation |
| Begin/finish cold recovery | Abstract reload from durable roots; actual task teardown/reopen is covered separately by Rust/DST integration tests |
| Checkpoint capture/release | Abstract retention extension. SlateDB implements checkpoint roots, but the runbook's asynchronous backup/PITR design is not thereby implemented or tested by this model |

The inspected dependency is SlateDB revision
`0717cc1e4e9bad10a4773760f66bac4264ecf05e` from `Cargo.toml`.
Its `slatedb/src/garbage_collector/compacted_gc.rs` reads all manifests referenced
by the latest manifest and its checkpoints before collecting active SSTs;
`manifest_gc.rs` excludes checkpoint-referenced manifests. The model projects
manifest traversal into object-root sets. It omits age filters, listing lag,
compaction low-watermarks, upload protection, multi-DB snapshot coordination,
checkpoint expiry, provider failures, and encryption. Object/reference publishing
is atomic and requires an existing durable object; checking that implementation
and provider precondition remains an independent obligation. This model does not
establish the safety of collecting an in-flight unreferenced upload.

Semantic mutations publish a history reference before its data, commit an
absorbed boundary before history flush, and ignore a checkpoint or fork during
physical deletion. Storage corruption or provider contract violations are outside
the safe model and require separate implementation campaigns.
