# Runtime resource ownership (R10)

`RuntimeCaps` constructs the scaler, the shared block cache, the telemetry
resources, the usage and request-work services, the store-I/O handle and the
operations and audit journals from configuration. Every engine opened by a
runtime receives the same shared block cache. A second runtime receives
separate resources and capacities; these policy handles do not use
process-global first-touch configuration. Production runs one runtime per
process, so its configured memory caps still cover all its engines. (Layout 6
retired the decoded postings cache, the history cache, the absorber budget,
its packing limit and its pause control.)

The resources live until the runtime and its last engine release them.
Scaler elapsed time uses the runtime's monotonic clock; persisted
creation/record/accounting timestamps retain wall time. Each shard directory
owns readiness independently of process metrics. A successful open in another
runtime cannot heal a failed directory.

Process traffic and historical diagnostic counters intentionally remain global.
They do not select capacities or the readiness verdict.

Verification at R10: runtime, scaler and resource tests passed with Rust
1.98.1. Coverage includes two distinct capacities, weak-reference teardown,
wall-clock jumps, and idle expiration. The R17 HTTP regression also passed for both readiness endpoints
following a normal return, error, and panic of a critical task.

The complete R10 ownership pass also makes telemetry cache capacity and billing
readiness progress, usage/rate buckets and backlog maps, the store-I/O bulk
(byte) gate, and operations/audit journals and alerts runtime-owned (the pass
also covered a store-I/O count gate, which was removed with
`STORE_MAX_CONCURRENT` on 2026-09-29, edge record #82). All
stores belonging to one runtime share one I/O handle; spool and rollup share
one telemetry cache. Raw and product body collection read the immutable runtime
configuration. A denied request, drained event, successful billing pass, or
refill in one runtime cannot mutate another runtime's decision state.

The physical SlateDB executor intentionally remains process-owned, alongside
OS-resource probes and physical process instrumentation. Its thread count is
immutable once constructed; conflicting late initialization now fails rather
than silently accepting first-touch defaults. Process cumulative diagnostics,
and RSS/egress measurements retain their documented process scope. These are distinct from per-runtime policy.

New controls exercise early unrelated access, distinct capacities, shared local
versus independent remote-runtime permits, cancellation/release, actual spool
and rollup database cache lifetimes, reverse-order journal draining and alert
isolation, raw/product/chunked HTTP body ceilings, exact admitted/committed
counters, weak-reference teardown, and monotonic refill across wall-clock jumps.
The final execution receipts supply the results for this complete source state.
