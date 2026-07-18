# Storage migration and rollback contract

This is the release contract for persistent formats. A format change is not a
normal rolling-deploy detail: writers stay on the old format until both the new
release and the immediately previous release can safely coexist with every
object the new release may publish.

## Current formats

| surface | readable | writable | rollback boundary |
|---|---:|---:|---|
| live topology/shard/registry | 2 | 2 | pre-v2 pilot data is rejected; this service has no in-place v1 corpus |
| recovery point | 1, 2, 3 | 2 or 3 via `BACKUP_WRITE_FORMAT` | one binary release; format-1 and format-2 points remain restorable |

Recovery format 1 stores snapshot-local objects. Format 2 stores shared
SHA-256 blobs and references. Format 3 stores blobs and references below
`formats/3/`, with the coordinator epoch in each physical path and in the
snapshot id. On takeover, unchanged content is streamed through a checksummed
staging object into the new epoch before its inventory is published. A paused
old epoch can therefore delete only its own content paths.

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

Rollback by one release is safe before or after the flip. The previous binary
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
