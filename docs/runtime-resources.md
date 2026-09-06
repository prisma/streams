# Runtime resource ownership (R10)

`RuntimeCaps` constructs the scaler, decoded postings cache, history cache,
absorber budget, packing limit, pause control, and resolved memory report from
configuration. Every engine opened by a runtime receives the same history and
postings handles. A second runtime receives separate resources and capacities;
there is no process-global first-touch configuration. Production runs one
runtime per process, so its configured memory cap still covers all its engines.

Absorber reservations borrow their owner and return both semaphore permits and
accounted bytes on cancellation or drop. The resources live until the runtime
and its last engine release them. Scaler elapsed time uses the runtime's
monotonic clock; persisted creation/record/accounting timestamps retain wall
time. Each shard directory owns readiness independently of process metrics.
A successful open in another runtime cannot heal a failed directory.

Process traffic and historical diagnostic counters intentionally remain global.
They do not select capacities or the readiness verdict. The history flush-stall
fault-injection lever remains process scoped; it is a test/operator fault control,
not configuration or a capacity owner.

Verification: 8 runtime tests, 5 scaler tests, and 13 history tests passed with
Rust 1.98.1. Coverage includes two distinct capacities, shared reservations,
cancellation/release, weak-reference teardown, wall-clock jumps, and idle
expiration. The R17 HTTP regression also passed for both readiness endpoints
following a normal return, error, and panic of a critical task. Full final-tree
checks are recorded separately in the remediation summary.

The final ownership audit also removed the history budget's dependency on the
transport's global body-limit setting. Each HistoryResources stores its own
worst-frame reservation from the validated runtime body limit; absorber seeds,
adaptive caps and memory diagnostics read that same value. A new two-runtime
regression uses 1 MiB and 4 MiB body limits with a zero configured budget and
proves separate floors, capacities and per-gather reservations (passed).
