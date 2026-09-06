# Runtime resource ownership (R10)

`RuntimeCaps` constructs the scaler, decoded postings cache, history cache,
absorber budget, packing limit, pause control, and resolved memory report from
configuration. Every engine opened by a runtime receives the same history and
postings handles. A second runtime receives separate resources and capacities;
these policy handles do not use process-global first-touch configuration. Production runs one
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

The complete R10 ownership pass also makes telemetry cache capacity and billing
readiness progress, usage/rate buckets and backlog maps, store-I/O concurrency
and bulk gates, and operations/audit journals and alerts runtime-owned. All
stores belonging to one runtime share one I/O handle; spool and rollup share
one telemetry cache. Raw and product body collection read the immutable runtime
configuration. A denied request, drained event, successful billing pass, or
refill in one runtime cannot mutate another runtime's decision state.

The physical SlateDB executor intentionally remains process-owned, alongside
OS-resource probes and physical process instrumentation. Its thread count is
immutable once constructed; conflicting late initialization now fails rather
than silently accepting first-touch defaults. Process cumulative diagnostics,
RSS/egress measurements, and the explicit operator flush-stall injection retain
their documented process scope. These are distinct from per-runtime policy.

New controls exercise early unrelated access, distinct capacities, shared local
versus independent remote-runtime permits, cancellation/release, actual spool
and rollup database cache lifetimes, reverse-order journal draining and alert
isolation, raw/product/chunked HTTP body ceilings, exact admitted/committed
counters, weak-reference teardown, and monotonic refill across wall-clock jumps.
The final execution receipts supply the results for this complete source state.
