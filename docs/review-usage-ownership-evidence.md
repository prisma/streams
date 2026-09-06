# R10 — usage policy and remaining process state

`RuntimeCaps.usage` owns one `UsageService`: configured limits, tracked token
buckets and their counter handles, the conservative overflow bucket/counters,
the amortized overflow-scan tick, and the route/segment/shard backlog maps used
by fleet decisions. Every engine opened by that runtime receives the same Arc.
History publishes and clears rows through its engine's owner, while fleet and
HTTP diagnostics read their runtime's owner. Another runtime cannot spend the
first runtime's tokens, inherit its counters or overwrite its backlog rows.

Typed append admission returns the owner's exact counter Arc and carries it
through commit. Close preflight reads the same immutable limits before durable
intent publication. Raw reads, SSE, creation initialization and the product
final-record capacity check use that runtime handle. Refill and idle eviction
use the owned monotonic clock; persisted event timestamps remain wall-clock
values. Counter-generation identifiers remain process-unique integers; they
select no capacity and retain no stream state.

Production has no free usage-policy getter or seed atomics. A `cfg(test)`
compatibility owner supports existing direct storage fixtures that construct
AppendReq without an application runtime. It cannot be called in a production
build. The existing pending-summary shutdown scenario now observes
`engine.usage` instead of the removed process map; its wait schedule and all
assertions are unchanged. Existing pure usage unit scenarios preserve their
names and assertions, with the bucket timestamp and error-message calls adapted
to the explicit clock/limits API.

New controls in `src/usage/runtime_tests.rs` exercise an unrelated earlier
runtime before two differently configured owners, one shared runtime clone,
independent counter Arcs, independent lag/pending maps, weak-reference teardown
while counters remain held, fresh restart, and wall-clock jumps versus explicit
monotonic refill/eviction. Execution is recorded by the final repository gate;
source availability alone does not certify those tests ran. The additional
`runtime_usage` HTTP regression uses the same stream name in two actual rigs
and proves distinct 10/20-byte admission decisions, exact committed request/
plaintext counters, and the shared engine/runtime capability identity.

HTTP fixtures retain their existing real-time rate-refill semantics by giving
the default usage owner the same owned `protocol_clock` used for wire deadlines.
Tests can supply an explicit `ShardConfig.shared_usage` to drive manual time.
The capacity-ratio scenario, limits, workload and assertions remain unchanged;
its protocol timer is not the separate frozen clock used for deterministic
identity/descriptor-time tests.

The final ownership scan distinguishes these categories:

| State | Intended owner and remaining scope |
| --- | --- |
| History/postings/scaler, telemetry DB cache/progress, usage limits/maps, store operation and bulk-byte gates, ops/audit queues and alerts | Explicit runtime handles constructed from configuration; shared among that runtime's engines and controllers. |
| HTTP body ceiling | Read directly from the serving state's validated configuration at collectors and preflight. |
| AppState service slots, BillingService spool slot, RollupSlot, HistoryResources resolved report, SSE feed attribution slots | OnceLock fields inside owned instances; not process globals. |
| Object-store/HTTP timing rings and transferred-byte totals; history/SSE/shard/scaler/refusal totals | Deliberately aggregate process instrumentation, with fixed diagnostic bounds. They no longer initialize runtime capacities or select the readiness verdict. |
| Physical RSS/FD limits and event-loop drift samples | Process measurements; different runtime probes observe the same physical process. |
| Counter-generation/adoption sequences | Process-unique identifiers; no retained per-stream buffers or budget settings. |
| Fixed-option peer HTTP connection pool | Shared process transport infrastructure with constant HTTP/idle settings; does not capture runtime configuration, authorization or topology. |
| SlateDB executor | One process executor, separate from RuntimeCaps' resource budgets. Its configured thread-count/startup contract must be explicit; it is not a claim that two servers own independently sized executors. |
| History flush-stall injection and cfg(test) fault/clock/fixture registries | Deliberate fault controls, not normal runtime capacity or health ownership. |

This scan does not equate all atomics with a defect. It specifically removes
state that changes a runtime's admission, configured capacity, durable-event
buffer attribution or fleet decision because another runtime touched it first.
