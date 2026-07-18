# Storage migration and rollback contract

This is the release contract for persistent formats. A format change is not a
normal rolling-deploy detail: writers stay on the old format until both the new
release and the immediately previous release can safely coexist with every
object the new release may publish.

## Current formats

| surface | readable | writable | rollback boundary |
|---|---:|---:|---|
| live topology/shard/registry | 2 | 2 | pre-v2 pilot data is rejected; this service has no in-place v1 corpus |
| stream cell placement/index | unassigned or 1 | 1 | before adding a second cell, quiesce the sole cell and run `streams-cell-admin`; a managed cell refuses unassigned descriptors |
| history block envelope | legacy 1, bound 2 | 1 or 2 via `HISTORY_BLOCK_WRITE_FORMAT` | deploy the dual reader everywhere with writer 1 before any cell emits 2 |
| encrypted-history integrity baseline | 1 | 1, create-only | baseline must precede readiness enforcement; never infer it without the customer key |
| backup coordination lease / health / retention clock | lease 1 or 2 / health 2 / clock 1 | lease 2 / health 2 / clock 1 | protocol 1 is takeover input only; it is never trusted for readiness or eligible as a production rollback writer |
| recovery point | 1, 2, 3 | 2 or 3 via `BACKUP_WRITE_FORMAT` | one binary release; format-1 and format-2 points remain restorable |

Recovery format 1 stores snapshot-local objects. Format 2 stores shared
SHA-256 blobs and references. Format 3 stores blobs and references below
`formats/3/`, with the coordinator epoch in each physical path and in the
snapshot id. On takeover, unchanged content is streamed through a checksummed
staging object into the new epoch before its inventory is published. A paused
old epoch can therefore delete only its own content paths.

## Single-cell registry to managed cells

Cell placement is an explicit one-way control-plane migration. Create a valid
`cells.json` containing exactly one active target cell, stop every process that
can create streams, and first run `streams-cell-admin` without `--apply`. The
audit is bounded by `--max-descriptors` and refuses pilot descriptors whose
owner is `__legacy__`, placements outside the target cell, corrupt identity,
or a multi-cell directory.

Then rerun with `--apply --confirm-serving-quiesced`. For every tenant it
create-only CASes a one-cell affinity; for every stream it writes the immutable
cell recovery index before CAS-stamping the global descriptor. The command is
restart-safe and performs a second zero-pending audit before success. Only then
may the target start with `CELL_ID`; add a second cell in a later directory
generation after all processes and recovery actors have proven the new index.
There is no automatic fallback that guesses an unassigned descriptor's owner.
After the CAS wave, rollback keeps managed `CELL_ID` mode and the one-cell
directory; do not erase placements or indices. A binary without the separate
registry/cell reader is no longer an eligible rollback target.

Coordination protocol 2 replaces the protocol-1 absolute `lease_until_ms`
with a token, epoch, and renewal sequence. A protocol-2 contender ignores the
legacy deadline—including `i64::MAX`—and conditionally upgrades only after the
exact legacy object content and provider version remain unchanged for six
locally measured monotonic seconds. Protocol-2 health uses relative monotonic
ages and requires an observed post-startup/post-pause lease renewal. Once any
cell writes a protocol-2 lease, a protocol-1 binary cannot parse it and fails
closed. Protocol 2 is therefore the first supported production coordinator
protocol: deploy it everywhere in format-2 recovery mode before the recovery
format 2→3 read-first wave. A pre-protocol-2 binary is not a rollback target.

## Recovery format 2 to 3

1. **Read-first wave.** Deploy this binary everywhere with
   `BACKUP_WRITE_FORMAT=2`. It reads formats 1/2/3 but emits format 2. While in
   this mode the coordinated actor does not delete shared legacy blobs, because
   a prior epoch could still be paused inside an unconditional provider delete.
2. **Prove the reader.** Require a successful format-2 dark restore, green
   rolling scrub, one coordinator takeover, and confirmation that every cell
   runs the new reader. Do not flip a cell with an old backup or restore binary.
3. **Flip one canary cell.** Set `BACKUP_WRITE_FORMAT=3`. Require a format-3
   point, epoch-incremented takeover point, scrub pass, and dark restore. Then
   advance through the deployment waves in `COMPUTE-SPEC.md`.
4. **Retain rollback data.** Keep at least two known-good format-2 points for
   the full rollback window. Format-3 metadata/content is in a top-level root
   that the previous format-2 actor does not list.
5. **Finish.** After the rollback window, format-3 retention may collect legacy
   blobs only when their last reference points at an expired generation.

Rollback by one release is safe before or after the flip only when that release
already implements coordination protocol 2. The previous eligible binary
claims a higher coordinator epoch, ignores the `formats/3/` reference root,
copies primary objects into format-2 paths, and publishes a newer format-2
`latest.json`. Its restore tool can then use that point or an explicitly retained
older format-2 id. Before starting that older binary, set
`BACKUP_RETENTION_SECS` beyond the entire rollback window (the current maximum
is one year) and alert before the oldest point reaches the cutoff: the older
writer predates safe shared-blob GC, whereas this binary disables that GC in
format-2 mode. Return to the new reader before restoring normal retention.
Never roll back more than one release without first proving its read matrix
against the corpus. Never delete format-3 objects to make a rollback appear
successful.

## Encrypted-history block envelope 1 to 2

Legacy envelope 1 is `[random nonce | AES-256-GCM ciphertext]` under the raw
customer stream key with empty AAD. It encrypts payloads but does not bind a
valid block to a stream incarnation when a customer reuses the same key.
Envelope 2 is `[16-byte PRISMA-HIST-V2 marker | random nonce | ciphertext]`.
Its key is HKDF-SHA256 over the customer key with the 32-byte tenant/name/
incarnation storage identity as salt and a fixed history-block domain; the
marker plus that identity is authenticated as AAD. A block copied to another
incarnation therefore fails authentication even when both streams use the same
customer key. The 16-byte marker makes accidental legacy ambiguity a 2^-128
event; a marked block never falls back to legacy decryption after an auth
failure.

The current reader accepts both envelopes and the writer is selected at boot:

1. **Read-first wave:** deploy this binary to every instance with
   `HISTORY_BLOCK_WRITE_FORMAT=1`. Exercise reads, absorbs, compaction,
   primary scrubbing, recovery-point restore, and an instance rollback while
   the corpus remains legacy-writable.
2. **Canary flip:** set `HISTORY_BLOCK_WRITE_FORMAT=2` in one cell. Force
   absorption and compaction, prove the mixed legacy/v2 DB reads through every
   route, inspect primary and recovery objects for payload/key leakage, and
   dark-restore the point. Do not run a pre-dual-reader binary after this step.
3. **Fleet flip:** advance by the deployment waves in `COMPUTE-SPEC.md`.
   Rollback uses the same dual-reader release with writer 1; it can read v2 but
   emits legacy until the incident is resolved. Restoring a pre-flip point is
   the only rollback path for a binary that cannot parse envelope 2.
4. **Finish:** after the rollback window, make writer 2 mandatory. Normal
   SlateDB compaction rewrites remaining legacy blocks under envelope 2; the
   create-only integrity baseline protocol records each new ciphertext object
   before its absorbed frontier becomes authoritative.

No in-place object mutation is permitted. Envelope migration occurs only
through normal fenced SlateDB writes/compaction and is protected by the same
manifest CAS and recovery-point machinery as other history changes.

## Encrypted-history integrity baseline

History SlateDB blocks are encrypted with the customer-supplied stream key;
the service intentionally does not persist that key. On every absorb, a
history-only object-store wrapper conditionally creates the version-1 digest
record under `integrity/history/` from the exact transformed PUT payload before
the SST becomes visible. It then writes the SST; only a later manifest can make
the object live. The keyed writer logically decodes each newly referenced SST
and monotonically finalizes that verification before advancing the absorbed
boundary. A crash at any earlier point leaves the hot range authoritative and
only unreferenced baseline/SST orphans. The background cell scrubber can detect
missing or same-length corrupt ciphertext without key custody. SST paths and
their digest identity are immutable; a conflicting create is corruption, not
an update.

This is a fail-closed rollout boundary. A cell with history SSTs created by a
binary that predates the ledger will remain unready because a keyless process
cannot safely decide that the bytes it first observes are good. Before enabling
the primary-scrub readiness gate on such a cell, run a keyed maintenance pass
for every active stream: open with the customer-provided key, decode all
referenced blocks, create the baseline records, complete a full primary sweep,
and publish/dark-restore a new recovery point. There is no automatic or
recovery-index-based bootstrap because either would re-baseline latent
corruption. A clean cell needs no migration—the first absorb creates the
ledger before its history becomes authoritative.

## Future live-format changes

Live format 2 is the first supported production format; pre-v2 pilot layouts
are deliberately rejected rather than guessed. Any future format N to N+1
must ship in three releases/stages:

1. **Reader:** N remains the only writer. N+1 readers, validation, metrics, and
   an offline verifier ship and bake. Unknown required fields fail closed.
2. **Migrator:** a fenced, restartable actor copies one tenant/shard at a time,
   verifies counts/digests and a checkpointed cut, then conditionally publishes
   a versioned per-object or per-topology cutover marker. N remains the rollback
   shadow; writers never dual-ack two authorities.
3. **Finalizer:** only after the rollback window and a complete dark restore may
   retention remove N. The actor records durable progress, has bounded work per
   pass, and treats corrupt/missing source state as an alarm—not as absence.

For rollback during stage 2, stop new migrations, wait for every in-flight
cutover CAS to resolve, route cut-over objects with the N+1 reader, and roll
back only objects whose marker still names N. A break-glass rewrite must carry
the expected marker version; blind overwrite is forbidden. The first future
live-format change must add a mixed-version canary that exercises every phase,
kill point, and rollback before the migration gate can be Green.
