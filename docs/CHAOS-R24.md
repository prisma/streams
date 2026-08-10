# R24 — the backlog bound becomes a real boundary

Response to the review of `0d87ea9b`. Its verdict on R23-1 was that the
mechanism had the right architecture but the wrong source of truth:

> a hard limit must be derived from the durable backlog that moves with
> ownership — not from process-lifetime counters.

That is correct, and I verified every sub-claim against the source before
changing anything.

## Evidence labels

Each claim below is one of: **mechanism** (the code exists), **local
proof** (deterministic test), **field proof** (measured on a real edge),
**open** (not yet established). R23's report blurred these, which is how
a green gate over process-local counters came to read as a safety proof.

## What the review found, verified

| Claim | Verified how |
|---|---|
| Ingest counter bumped before the write succeeds | `shard.rs:3723` — `fetch_add` sits in batch assembly, above `write_with_options` |
| Raw route bypasses the append classifier | route is `/v1/stream/{*name}`, a slash-matching wildcard; `POST /v1/stream/customers/acme` returned `false` |
| `strict_query` wired to one route | single call site, `product.rs:6235` (catalog) |
| Canary keys on PID | `main.rs:863` |

All four were real. The raw-route one was worse than "a miss": my unit
test asserted the wrong behaviour as if it were the contract.

## R24-A — durable, ownership-scoped backlog — *mechanism + local proof*

Each physical shard now carries a durable maintenance row (unabsorbed
bytes, oldest-unabsorbed timestamp) written in the **same committer batch
as the appends it accounts for**, and retired in the absorbed batch.
`src/maintenance.rs` is the in-memory mirror used for admission; the
durable row is authoritative and is reinstalled from the shard DB by the
existing dirty-index rediscovery scan.

The ordering is the fix. The mirror only grows **after**
`write_with_options` returns `Ok`, so a failed group write cannot leave
backlog behind. The four defects, each with a test:

```
a_failed_write_creates_no_backlog
restart_reflects_pre_existing_backlog
ownership_move_transfers_the_contribution
per_shard_bound_names_only_the_offender
age_never_moves_forward_while_work_remains
```

Age is conservative by construction: set when the first outstanding bytes
appear, never moved forward while work remains, cleared on full drain. A
restart may overstate age; it can never reset it.

These tests take a lock, because they mutate process-global state. Before
that, the full suite passed while a filtered run failed — a green result
that proved only thread scheduling.

## R24-B — admission after ownership, per shard — *mechanism + field proof*

The check moved out of global middleware into the append path, after the
descriptor resolves. The non-owner case falls out of the design rather
than needing a special case: the mirror holds only shards this instance
owns, so a shard we do not hold reports no backlog and the request
continues to ownership replay.

Measured on an 8-shard instance, per-shard bound 2 MiB, instance bound
disabled:

```
maintenance_shards: [{prefix "011", unabsorbed_bytes 2162689}, ...]
hot   (shard 011, over bound) -> 503 maintenance_backpressure, retryable
calm1..calm5 (other shards)   -> 200, all admitted
instance latch engaged: false
```

One shard sheds; the rest of the instance keeps serving. That is the
multitenant boundary the review asked for.

## R24-C — work class from the parsed route — *mechanism + local proof*

`is_append_request()` is gone. `WorkClass` is derived from the
`ProductRoute` the handlers actually dispatch on, so the grammar exists
once. The lesson generalizes past this bug: a second copy of route
parsing in a predicate is a copy that will drift, and a test written
against the predicate will pin the drift.

## R24-D — strict query everywhere — *mechanism + local proof*

Route-specific allowlists on records, scan and watch, alongside catalog.
Unknown keys and duplicate scalars now answer 400 on every public route.
`hostile-surface.sh` is 54 checks, 54/54 locally.

## R24-E — canary identity and readyz scope — *mechanism*

Canary key is instance + pid + boot nonce with a create-only write, so
two Firecracker VMs that share a pid cannot silently clobber each other's
probe. `/readyz` is documented as boot readiness with the canary's real
coverage stated (PUT/GET only — not delete, LIST/range, conditional puts,
or every fleet and telemetry prefix).

## Still open

**Read amplification (R23-6) is experimental and must not drive
architecture.** The counters are process-global deltas around a gather,
so they include concurrent customer, registry, billing and fleet traffic;
the denominator includes keys and WriteBatch overhead rather than raw
frame bytes advanced; and `GET_BYTES` uses object metadata size, so a
ranged read bills the whole object. Operation-local accounting
(`StoreIoScope` per gather) is not yet done.

**CHAOS-5 remains open.** The capacity campaign must be rerun against the
durable bound, on a namespace with history depth matching the failing
Singapore state, at the intended supported append rate — with restart or
ownership move at maximum permitted backlog and exact reconciliation of
every acknowledged operation. The earlier 1.04× observation keeps its
caveats: lower offered load, fresh namespace, and history depth drives
read cost. A green "engaged and released" on the pre-R24 counters proved
only the process-local approximation.
