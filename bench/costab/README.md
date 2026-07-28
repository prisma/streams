# costab: local object-store request-cost A/B

Runs the field soak's shape (soak7 server env, awsbench tier ramp, 25 ms
store) against a local `s3lite`, and compares two builds by **physical
object-store requests by billing class** — the ground truth is s3lite's
`/_s3lite/stats2` ledger (per tier/kind/op, split by status, with a
Class A/B/free rollup at public-Tigris-shaped rules).

```bash
./run-soak.sh <label> <server-binary> <out-dir>          # 30 min
python3 compare.py <baseline-out> <after-out>
```

`SOAK_TIERS` / `SOAK_SECS` override the ramp. The default here is
`1,2,3,4 × 450 s` (30 minutes), NOT the field ladder: a local rig runs
~10× the field's per-tier record rate (no edge RTT, ~32 ms acks), and
the field ladder (`1..64 × 180 s`) drives a single stream far past the
absorber's envelope — both attempts wedged exactly like the known
saturation shape (history flush stalls with the imm memtable pinned over
`max_unflushed_bytes`, RSS climbs through the shed line, everything
429s and never recovers). conc4 ≈ 12 MB/s holds a flat RSS for the full
half hour; conc6+ accumulates.

Measurement invariants:

1. **Same s3lite and generator binaries for both runs**; only the server
   binary differs.
2. **Fresh s3lite (fresh memory, zeroed counters) and a fresh
   `PATH_PREFIX` per run** — cumulative counters need no windowing.
3. Compare medians of the 20 s windows per tier; each tier's last window
   straddles the step-down and is dropped (soak harness invariant 4).
4. Zero errors and decoded == acked×batch in both runs, or the
   comparison is void.
