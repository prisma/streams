# Compute wrapper apps

Prisma Compute runs a Bun app; these download the Rust binary from the
artifacts bucket and exec it. They live in the repo because a deploy must
be reproducible from a clean checkout — these files previously existed
only in scratch directories, and one was deleted by a cleanup mid-campaign
(docs/STAGING.md B1).

| dir | binary | role |
|---|---|---|
| `app-server/` | `streams-slate` (`SERVER_BINARY_S3_KEY`) | stream server / fleet member |
| `app-lb/` | `pilot` (`LB_BINARY_S3_KEY`), `MODE=lb` | rendezvous-hash router; the only client entry point |

Both verify the download is an **x86_64 ELF** before exec. That check is
not politeness: an aarch64 binary deploys "successfully" and crash-loops
into a silent platform zombie (service domain 404, `versions list` says
running, no logs retrievable). Both also re-download every boot, because a
warm instance keeps `/tmp` across versions and would otherwise pin the
previous release.

Distinct env names per role (`SERVER_BINARY_S3_KEY` vs `LB_BINARY_S3_KEY`,
`BIN_S3_*` vs `SLATE_S3_*`) — Compute env vars are project-scoped and
merged, and shared names have twice caused a service to run the wrong
binary (RUNBOOK section 7.3).
