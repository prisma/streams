# Prisma Streams Live Load Tests

These are black-box HTTP load generators for the **live / touch system**
described in `live.md`.

They run against a Durable Streams server process over HTTP, and they intentionally stress real bottlenecks:

- touch processor throughput (CPU, JSON parse, key derivation)
- touch coalescing behavior
- base SQLite WAL growth + retention/GC
- `/touch/wait` concurrency + fanout behavior
- `/touch/wait` keyset size overhead
- template activation / last-seen / retirement / eviction under load
- live metrics stream overhead (`live.metrics`)

Note:

- These CLIs configure the touch processor for the in-memory journal used by Live.

## Prereqs

1. Start the server (local MockR2):

```bash
cd /path/to/prisma-streams

DS_TOUCH_WORKERS=4 \
DS_TOUCH_MAX_BATCH_ROWS=1000 \
DS_TOUCH_MAX_BATCH_BYTES=$((8*1024*1024)) \
  bun run src/server.ts --object-store local --stats --no-auth
```

Notes:

- `DS_ROOT` defaults to `./ds-data` (disk usage grows there).
- Touch coalescing intervals are configured per stream via the `state-protocol`
  profile (these CLIs set it when `--setup` is enabled).

2. Confirm the server is reachable:

```bash
curl -s http://127.0.0.1:8080/health
```

## Test 1: Write-Path Scalability

Script: `experiments/loadtests/live/write_path.ts`

What it stresses:

- State Protocol ingest -> touch generation throughput
- CPU cost of key derivation (tableKey + watchKeys)
- touch coalescing window effectiveness under high/low key cardinality
- WAL/GC gating behavior when touch processing falls behind
- `live.metrics` overhead under sustained write load

### Run (High cardinality: worst-case coalescing)

```bash
bun run experiments/loadtests/live/write_path.ts \
  --stream load.live.write \
  --mode high-cardinality \
  --reset
```

### Run (Low cardinality: best-case coalescing)

```bash
bun run experiments/loadtests/live/write_path.ts \
  --stream load.live.write \
  --mode low-cardinality \
  --reset
```

### Key knobs

- `--steps 1000,5000,10000,20000` and `--step-seconds 60`
- `--producers` and `--batch-events` (client-side throughput tuning)
- `--columns 128` (size of `value` and `old_value` objects)
- `--coarse-interval-ms` and `--coalesce-window-ms`
- Guardrails:
  - `--lag-degrade-offsets` + `--lag-recover-offsets` (hysteresis for coarse-only mode under lag)
  - `--fine-budget-per-batch` (hard cap of fine/template touches per processing batch; coarse table touches still emitted)
  - `--fine-tokens-per-second` + `--fine-burst` (token bucket for bounded fine-touch work)
- `--ttl-ms` (template inactivity TTL on activation)

### What to watch

These are printed periodically by the CLI (via `live.metrics`):

- `touch.touchesEmitted`
- `touch.uniqueKeysTouched`
- `touch.templateTouchesEmitted` vs `touch.tableTouchesEmitted`
- `processor.errors` (should stay 0)
- `processor.lagSourceOffsets` (should stay bounded per step)

Also capture:

- `GET /v1/stream/<stream>/touch/meta` (`mode` + cursor/offsets; retention window exists only in `sqlite` mode)
- process RSS/CPU and `ds-data` disk usage (WAL growth)

Suggested pass/fail (tune for your hardware):

- Pass (per rate step): lag stabilizes (does not grow without bound) and errors remain 0.
- Fail: lag grows without bound for >2 minutes; touch stream stalls; or disk/WAL growth remains uncontrolled after load drops.

## Test 2: Read-Path Scalability

Script: `experiments/loadtests/live/read_path.ts`

What it stresses:

- `/touch/wait` scalability with many concurrent waiters
- keyset-size overhead (small keysets vs huge keysets)
- wakeup fanout behavior (rare invalidations vs broad invalidations)
- template activation + last-seen + retirement/eviction under load
- `live.metrics` overhead when wait volume is high

Implementation note:

- The read-path harness configures `touch.onMissingBefore="skipBefore"` for the
  test stream so touch processing does not wedge if some active templates can’t
  be evaluated from partial row images. (Coarse touches are still emitted; fine
  touches are emitted when fields are present.)
- At high waiter concurrency, it’s often more accurate to **split writer/metrics from waiters** (so the client machine doesn’t starve the metrics long-poll / writer while also running thousands of wait loops). Use `--role`.

### Scenario 1: Many waiters, small key sets (typical live queries)

Low fanout (touch a small fraction of the watched key-domain):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --scenario small \
  --concurrency 200 \
  --keys-per-wait 3 \
  --touch-fraction 0.01 \
  --table-key-entity public.backstop \
  --reset
```

High fanout (invalidate a larger fraction of the watched key-domain):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --scenario small \
  --concurrency 200 \
  --keys-per-wait 3 \
  --touch-fraction 0.50 \
  --table-key-entity public.backstop \
  --reset
```

To model a true **coarse backstop** (tableKey included for the same entity being written), set:

```bash
  --table-key-entity public.posts
```

This makes wakeups much more broadcast-y (any posts write touches the same tableKey), which is useful as a “worst-case fanout” mode.

### Recommended: Split Waiters vs Writer/Metrics (High Concurrency)

1. Setup once (create stream, enable touch, activate templates):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --scenario small \
  --duration-seconds 1 \
  --concurrency 1 \
  --writer-rate 0 \
  --reset
```

2. Terminal A: waiters only (no writer, no metrics):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --role waiters \
  --scenario small \
  --duration-seconds 60 \
  --concurrency 10000 \
  --waiter-processes 40 \
  --waiter-shard-gap-ms 200 \
  --waiter-start-jitter-ms 5000 \
  --graceful-stop \
  --keys-per-wait 3 \
  --wait-timeout-ms 30000 \
  --writer-rate 0 \
  --no-setup \
  --no-activate-templates \
  --no-metrics
```

3. Terminal B: writer + metrics (no waiters):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --role writer \
  --scenario small \
  --duration-seconds 60 \
  --concurrency 1 \
  --keys-per-wait 3 \
  --wait-timeout-ms 30000 \
  --writer-rate 200 \
  --touch-fraction 0.01 \
  --no-setup \
  --no-activate-templates
```

Notes:

- A single Bun process typically tops out around ~256 concurrent TCP connections to one host, which silently caps “true concurrency” for long-poll waiters. `--waiter-processes` shards across processes to reach higher true concurrency.
- At very high concurrency, ramping connections (`--waiter-shard-gap-ms` and `--waiter-start-jitter-ms`) helps avoid thundering-herd connection storms.
- In memory mode, waiters send `keyIds` (uint32) in addition to `keys` so `/touch/wait` can skip per-call key hashing on the hot path.

If you want metrics only (no writer, no waiters):

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --role metrics \
  --duration-seconds 60 \
  --no-setup \
  --no-activate-templates
```

### Scenario 2: Fewer waiters, huge key sets (worst-case keys-per-wait)

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --scenario huge \
  --concurrency 1000 \
  --keys-per-wait 900 \
  --hot-keys 5 \
  --writer-rate 200 \
  --reset
```

This should surface:

- O(keys) per wait call blow-ups
- CPU spikes / latency inflation
- broad waiter inefficiencies (in memory touch mode, large keysets are not indexed per-key)

### Scenario 3: Template churn under load (activation + last-seen + retirement/eviction)

Accelerated TTL example:

```bash
bun run experiments/loadtests/live/read_path.ts \
  --stream load.live.read \
  --scenario churn \
  --concurrency 5000 \
  --keys-per-wait 3 \
  --touch-fraction 0.01 \
  --ttl-ms 30000 \
  --template-churn \
  --churn-every-ms 1000 \
  --churn-activate 5 \
  --churn-drop 5 \
  --reset
```

This scenario activates a near-cap template set across multiple entities and then keeps activating + heartbeating a moving window.

### Pass/Fail targets (suggested)

Scenario 1 (10k waiters, 3 keys each):

- Pass: `live.metrics wait.p95LatencyMs` stays low (try ≤ 50ms for “touched” responses on localhost), error rate stays 0, stale rate near 0 (unless intentionally testing stale).

Scenario 2 (1k waiters, 500–900 keys each):

- Pass: “touched” p95 within a reasonable bound (try ≤ 200ms on localhost); system remains stable (no runaway CPU).

Scenario 3 (churn):

- Pass: templates that are heartbeated never retire; templates that stop being used retire after the inactivity window; eviction does not create large latency spikes relative to baseline.

## Test 4: Selective Fine Shedding (Policy B)

Script: `experiments/loadtests/live/selective_shedding.ts`

What it stresses:

- High write load with many active fine templates
- Policy B admission control (`FINE` vs `COARSE`) under lag pressure
- DS hot-template/hot-key filtering behavior (fine work should collapse after shedding)
- End-to-end targeted wake latency recovery for admitted/hot subscriptions

Roles:

- `setup`: stream/profile/template setup + config file generation
- `writer`: noise+targeted writer + targeted marker emission
- `appkit`: AppKit runtime emulator with phase-based selective shedding
- `metrics`: `/touch/meta` monotonic-counter sampling and phase summaries
- `all`: runs setup + writer + appkit + metrics together (single-process convenience mode)

Behavior notes:

- AppKit role now sends explicit `interestMode` (`fine`/`coarse`) on `/touch/wait`.
- DS applies server-side effective wait-key translation (`fineKey`/`templateKey`/`tableKey`) and returns `effectiveWaitKind` in `/touch/wait` responses.
- For `interestMode=fine` with `templateIdsUsed`, DS also registers template-key fallback IDs so a long-poll started in fine mode still wakes if DS degrades to restricted before the waiter re-issues.
- On policy-driven mode switches, AppKit aborts in-flight long polls and immediately re-issues waits in the new mode (so DS hot-interest state collapses quickly).
- Marker latency matching uses per-fine-key FIFO queues keyed by source-offset progress (`bucketMaxSourceOffsetSeq`), with expiry (`--marker-expiry-ms`, default `5000`).
- Memory-mode `/touch/wait` responses include `flushAtMs` and `bucketStartMs`; Test 4 appkit summaries split latency into:
  - `markerToFlush` (server-side progress to flush)
  - `flushToClient` (delivery/client-side component)
- Marker alignment in current harness is keyed to watched fine keys:
  - writer sends marker `fineKey`
  - appkit accepts markers only for active watched fine keys
  - latency matching is evaluated on touched responses where `effectiveWaitKind=\"fineKey\"`
- Writer can wait on a start barrier (`--start-barrier-fine-waiters`, `--start-barrier-hot-templates`) so writes do not race ahead of waiter registration.

### Setup

```bash
bun run experiments/loadtests/live/selective_shedding.ts \
  --role setup \
  --url http://127.0.0.1:8080 \
  --stream load.live.shedding \
  --templates 128 \
  --hot-templates 16 \
  --config ./tmp/test4_config.json \
  --reset
```

### Split run (4 terminals)

Terminal 1 (writer):

```bash
bun run experiments/loadtests/live/selective_shedding.ts \
  --role writer \
  --url http://127.0.0.1:8080 \
  --config ./tmp/test4_config.json \
  --noise-rate 10000 \
  --targeted-rate 40 \
  --duration-seconds 110 \
  --start-barrier-fine-waiters 50 \
  --start-barrier-hot-templates 16 \
  --start-barrier-timeout-ms 30000
```

Terminal 2 (appkit):

```bash
bun run experiments/loadtests/live/selective_shedding.ts \
  --role appkit \
  --url http://127.0.0.1:8080 \
  --config ./tmp/test4_config.json \
  --subscriptions-hot-per-template 64 \
  --subscriptions-cold-per-template 1 \
  --wait-timeout-ms 30000 \
  --controller-interval-ms 250 \
  --phase-a-seconds 30 \
  --phase-b-seconds 60 \
  --phase-c-seconds 20 \
  --admit-a 128 \
  --admit-b 16 \
  --admit-c 128 \
  --targeted-listen-port 9091 \
  --marker-expiry-ms 5000
```

Terminal 3 (metrics):

```bash
bun run experiments/loadtests/live/selective_shedding.ts \
  --role metrics \
  --url http://127.0.0.1:8080 \
  --config ./tmp/test4_config.json \
  --poll-ms 1000 \
  --duration-seconds 110
```

Terminal 4 (optional observer): `tail -f` the generated summary JSON files in `./tmp/`.

### Single command (recommended for reproducible local runs)

```bash
bun run experiments/loadtests/live/selective_shedding.ts \
  --role all \
  --url http://127.0.0.1:8080 \
  --stream load.live.shedding \
  --duration-seconds 110 \
  --config ./tmp/test4_config.json \
  --out ./tmp/test4_summary.json \
  --reset
```

The script writes a pass/fail block in the summary JSON for:

- lag bounded during phase B
- hot template collapse in phase B
- fine touch-rate drop from phase A -> B
- targeted latency recovery in phase B
- no systemic appkit wait errors

For high load, prefer split-role runs (`writer` + `appkit` + `metrics`) over `--role all`. `all` is useful for deterministic smoke tests but can under-drive writer throughput because all roles share one Bun event loop.

Additional scaling notes:

- A single Bun process can cap effective concurrent client connections to one host. For very high subscription counts, scale out AppKit waiters across multiple processes instead of increasing `--subscriptions-hot-per-template` in one process.
- Test 4 phase deltas (`fineRate*`, `processedDeltaRate*`, `touchesDeltaRate*`, `notifyWakeupsRate*`) are computed from monotonic `/touch/meta` counters by snapshot subtraction, so they remain meaningful under overload even when `live.metrics` is delayed.
- `/touch/meta` now exposes journal flush timing (`lastFlushAtMs`, `flushIntervalMsMaxLast10s`, `flushIntervalMsP95Last10s`) to help diagnose p95 tails in high-load runs.

## Troubleshooting / Tips

- If template activation is being denied or evicted: ensure the stream’s
  `state-protocol` touch config allows it.
  - These CLIs configure a permissive `touch.templates` policy when `--setup`
    is enabled (higher caps, no activation rate limit).
- If you just ran the write-path test (Test 1) at high rates, the touch processor may be backlogged for minutes. For the cleanest read-path numbers (Test 2), restart the server (or use a fresh `DS_ROOT`) so `/touch/wait` latency isn’t dominated by unrelated catch-up work.
- Watch `ds-data/` growth during Test 1:

```bash
du -sh ds-data
```

- You can directly inspect touch meta (cursor/offsets):

```bash
curl -s http://127.0.0.1:8080/v1/stream/load.live.write/touch/meta | jq .
```
