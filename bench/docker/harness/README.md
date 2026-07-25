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
