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
| `poll.py` | one-shot progress line per region while a run is in flight |
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
python3 poll.py                    # watch
python3 harvest.py && python3 mkreport.py > report-tables.md
```

Deploy **servers first**: `deploy-region.sh <r> gen` reads
`url-server-<r>.txt` to point the generator at its target.

## Invariants

Learned the hard way; a run that violates one of these produces numbers
that look fine and mean nothing.

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
5. **Every region gets its own bucket and its own project.** Compute env
   vars are project-scoped and merged; sharing a project across regions
   silently cross-contaminates configuration (RUNBOOK §7.3).
6. **Report the integrity check, not just latency.** Client-accepted
   records must equal server-durable records. A soak that only reports
   percentiles cannot distinguish "fast" from "fast because it dropped
   your writes".
7. **Tear down afterwards.** Six projects, six buckets and twelve services
   is real money and they do not expire on their own.
