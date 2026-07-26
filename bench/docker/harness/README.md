# Ladder harness (docker rungs + Compute cluster rung)

These scripts drive the SCALING.md §8 validation ladder. They lived in a
session scratchpad until a cleanup deleted the Compute wrapper's
`index.ts` mid-campaign — they are versioned here so a rerun is always
reproducible.

**Secrets live outside this directory.** Every script reads credentials
from a scratchpad path (`platform-token.txt`, `key.txt`, `bkey-*.json`,
`deploy-obs.sh`); nothing here embeds one. Set `S` to that directory.

| file | role |
|---|---|
| `ladder.sh <tag>` | full pass: fresh world → D1 → D2 → D3 → D4 → D5 |
| `driver.py` | idempotent-producer load generator (docker ports or `CLUSTER_URLS`) |
| `checker.py` | per-key gapless order check across the full segment lineage |
| `setup.sh` | create one scaled stream + seed `desired.json` |
| `showmap.py` | segment-map summary (live/sealed per stream) |
| `cluster-deploy.sh` | create/update the 4 Compute services, emit `cluster-urls.json` |
| `cluster-run.sh` | Compute rung: C1 (split) / C3 (rebalance) / C5 (soak) |

Two invariants the harness enforces, both learned the hard way:

- **D3 must observe a real move.** It resolves the owning instance with
  a READ probe (writes nothing), pauses that instance's absorber, and
  fails if no `rebalancer: moving shard` appears.
- **A pass starts from a stopped fleet.** Servers are stopped before the
  emulator is recreated; otherwise live servers write manifests into the
  fresh bucket referencing wiped SSTs.

## Harness invariants (each one cost a wasted run)

1. **Never edit a script a pass is executing.** Bash reads scripts
   incrementally from a byte offset; an edit shifts the file underneath
   the running process and it resumes mid-token. Passes therefore run
   from an immutable snapshot: copy the harness to `snap-<tag>/`,
   `chmod 555` the scripts (read + execute, NOT writable), leave the
   directory writable for the run log.
2. **A rung that cannot fail proves nothing.** D3 and D4 both passed
   their order checks for several passes while never exercising their
   mechanism. Both now assert: D3 requires a `rebalancer: moving shard`
   line, D4 requires both `FAULT INJECTED` and `resumed crashed split`.
3. **Capture evidence before cleanup destroys it.** D4 recreates the
   containers for its overlay, which discards docker logs — the move and
   fault lines are written into the run log first.
4. **Stop the servers before wiping the world.** Recreating the emulator
   under live servers lets them write manifests into the fresh bucket
   referencing wiped SSTs (poisoned world, compaction 404 loops).
5. **Do not pipe a monitor through `head`.** It block-buffers when
   stdout is a pipe and swallows every event.
6. **Anchor `pgrep` patterns** (`^bash /private.*snap-<tag>/`) or they
   match the monitoring command itself and report a dead run as alive.
7. **Probes must not perturb what they measure.** The D3 owner probe
   appends nothing (a `GET ?limit=1` resolves ownership just as well);
   an append probe left one extra record and failed the order check.
8. **Cloud only:** instances scale to zero (404 while waking — health-
   ping and retry), four services means four URLs (follow
   `Streams-Replay-To` to the owner; a single fixed URL spins forever),
   and no traffic until the ring is stable for 60 s.
