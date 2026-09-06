# Runtime and controller remediation evidence

All focused checks below ran locally using isolated Rust 1.98.1 with the repository lockfile. Logs are in `/private/tmp/streams-review-evidence`. Final combined release results are recorded in [the resolution ledger](review-resolution.md).

## R09: bounded controller work

Auth policy sources fetch independently with per-source deadlines; one unavailable source retains its last good state without blocking another source. Cancellation stops an active pass. Six authfeed tests passed.

Fleet repository reads reject corruption and I/O failure. A pass caps object population, individual object bytes, aggregate bytes, parallel reads and duration; controller failure preserves the previous membership rather than publishing an empty fleet. Eager engine adoption rotates a bounded window with bounded concurrency and individual deadlines. Two focused fleet repository tests passed.

Registry active/raw catalog enumeration shares bounded provider-row scanning and descriptor reads. The continuation cursor follows consumed provider rows, including tombstones, rather than successful output count or speculative prefetch. The tombstone-prefix regression passed. Operational dirty-debt alerts use bounded probes and report incomplete/error states as debt.

Billing and history budgets/fairness are described in [storage evidence](review-storage-evidence.md).

## R10: runtime ownership

RuntimeCaps owns enforced local history/cache budgets, pause state, postings resources and scaler resources. Every shard opened by a runtime receives the same history resources, so limits aggregate across its engines. Shard health belongs to an open gate, not a process-global singleton. Resource measurements read the actual runtime owner. Two-runtime resource tests (8), scaler tests (5) and history tests (13) passed.

Production protocol leases and runtime timing use the injected runtime clock. HTTP fixtures explicitly inject a wall clock consistent with their signed credential fixtures, while local runtime-clock tests retain independently controlled monotonic clocks. Diagnostic process totals intentionally retained outside admission are documented in [runtime resources](runtime-resources.md).

## R17: readiness

TaskMonitor reports unready before registration, after shutdown or stop, and after any critical task returns, panics or aborts. Normal noncritical completion does not poison serving. Ten task monitor tests passed. An additional real-router test exercised critical termination at both `/health` and `/readyz` and required HTTP 503 (one test passed).

## R18: scaler bounds and determinism

Cooldown keys include stream incarnation; bounded entries expire through the runtime's monotonic clock. Hot-key summaries aggregate across snapshots before sorting with deterministic tie-breaking, independent of map insertion order. Four focused bound/expiry/order tests passed. Runtime-local ownership and independent-clock checks are additionally covered by R10.

The final R18 audit also checks the normal append path at the 4,096-sketch
capacity: 256 appends to an existing segment of the same incarnation visit
zero entries in incarnation cleanup. First-segment admission and an epoch
change still remove obsolete sibling heat and cooldowns. The test measures
actual cleanup visits and includes an epoch-change positive control; periodic
idle pruning remains amortized every 4,096 appends.

R09 database-open handoffs carry a close-on-drop owner through the result
channel. Cancellation before completion or after a result is queued closes
the eventual database; successful handoff transfers that responsibility to the
caller. The worker retains an in-progress SlateDB open to completion, preserving
the existing OpenGate/reaper contract instead of aborting partially constructed
storage actors. Two real-Db controls exercise delayed and queued abandonment.

The R09 absorber follow-up registers production and composed-fixture pumps with
their shard engine, so `await_terminated` includes absorption. A level-triggered
close signal cancels an active discovery, reservation or gather and clears the
runtime's transient lag rows. Durable dirty markers and absorbed frontiers
remain the retry boundary. Two new controls park the real pump after it consumes
the last gather slot while bytes are held, and inside an actual history SST PUT.
Both require joined termination before releasing the hold, exact permit return,
and restart rediscovery/catch-up without another customer append. Final execution
receipts, rather than the presence of these tests, establish their results.
