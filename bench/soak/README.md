# Multi-region soak harness

Deploys a Streams server plus a **co-located** load generator into each
Prisma Compute region, ramps load through explicit concurrency tiers, and
harvests both client-observed latency and the server's own object-store
telemetry.

Results of the first full run: [docs/SOAK-REGIONS.md](../../docs/SOAK-REGIONS.md).

It lives in the repo for the same reason `deploy/` does: the previous
version of these scripts existed only in a scratch directory and had to be
rebuilt from scratch for every campaign, losing every fix along the way
(docs/STAGING.md B1, `bench/docker/harness/`).

## Why the generator is in-region

A generator running on the operator's laptop measures *the operator's
distance to the region*. That number is real but it is not ours, and it
swamps the thing under test: at 1 KiB records the append itself costs
30–200 ms depending on region, and a transpacific RTT is the same order.
Co-locating the generator makes the measurement Streams' own roundtrip.

The cost is that you cannot compare regions on "how fast is it from
here" — you compare them on "what does Streams cost a caller already in
this region", which is what a regional deployment actually offers.

## Layout

| file | role |
|---|---|
| `deploy-region.sh <region> server\|gen` | deploy one half of one region's pair |
| `resolve-urls.sh [region] [role]` | re-resolve preview domains from the **running** version |
| `poll.py` | progress line per region **and** a timestamped `/v1/debug/store` snapshot — run on a loop for the whole soak |
| `harvest.py` | collect per-tier client metrics + `/v1/debug/{store,usage,scaler}` → `results.json` |
| `mkreport.py` | render `results.json` as the markdown tables of the report |

## Secrets

Everything secret lives in `$SOAK_HOME`, which **must** be outside the repo
(RUNBOOK §12). Nothing here writes a token, bucket key, or stream key into
the working tree. See the header of `deploy-region.sh` for the expected
file layout.

## Running

```bash
export SOAK_HOME=/some/scratch/dir/soak
for r in us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1; do
  ./deploy-region.sh "$r" server   # sequential: parallel bunx races the package cache
done
for r in us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1; do
  ./deploy-region.sh "$r" gen
done
while true; do python3 poll.py; sleep 45; done   # watch AND snapshot storage
python3 harvest.py && python3 mkreport.py > report-tables.md
```

Deploy **servers first**: `deploy-region.sh <r> gen` reads
`url-server-<r>.txt` to point the generator at its target.

## Invariants

Learned the hard way; a run that violates one of these produces numbers
that look fine and mean nothing.

0. **Campaign scripts must run on macOS bash 3.2.** No `declare -A`
   (associative arrays silently parse keys as arithmetic — soak9's
   monitor died of `us: unbound variable`), no `${arr[@]}` on empty
   arrays without the `+` guard.
1. **Deploy regions sequentially.** Parallel `bunx` invocations race on the
   shared package cache and fail with `EEXIST`.
2. **Never trust a cached URL.** Preview domains are per-version; a
   redeploy retires the old one and it starts answering 503, which reads
   exactly like a boot failure. `resolve-urls.sh` after every deploy.
3. **Confirm load is actually flowing** before walking away — poll once at
   ~60 s and check `ok` is advancing in every region. A generator whose
   binary exited at startup looks identical to one that is warming up.
   (`deploy/supervise.ts` now makes that case self-reporting, but check
   anyway.)
4. **Discard the first window of each tier.** It straddles the concurrency
   step-up and mixes the previous tier's in-flight requests.
5. **Sample `/v1/debug/store` DURING the run, not after.** It is a
   trailing 60 s window, so a post-run harvest returns an empty window and
   every storage cell comes back as a dash. Run `poll.py` on a loop for
   the duration — it writes `store-snaps/<region>-<hhmmss>.json` on every
   pass. The first run of this harness lost its object-store telemetry to
   exactly this and had to fall back on snapshots taken by hand mid-ramp.
6. **Every region gets its own bucket and its own project.** Compute env
   vars are project-scoped and merged; sharing a project across regions
   silently cross-contaminates configuration (RUNBOOK §7.3).
7. **Report the integrity check, not just latency.** Client-accepted
   records must equal server-durable records. A soak that only reports
   percentiles cannot distinguish "fast" from "fast because it dropped
   your writes".
8. **Tear down afterwards, and verify it.** Six projects, six buckets and
   twelve services is real money and none of it expires on its own. Use
   `services destroy`, not `services delete` — delete refuses while
   versions are running, and the project then refuses to delete because
   "active deployments exist". Check the result rather than trusting the
   exit code: the first version of `teardown.sh` piped everything to
   `/dev/null` and cheerfully reported a clean teardown that had removed
   the buckets and nothing else.
