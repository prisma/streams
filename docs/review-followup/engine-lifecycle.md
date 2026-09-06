# R17-A engine lifecycle

The committer, acker, optional WAL pump, flush ticker and owned absorber are
required engine roles. Each actual worker future is registered with the existing
TaskSupervisor; no wrapper task owns an independently spawned worker. An
unexpected return, panic or abort fences the engine and retains the first role
failure in directory health. Both `/health` and `/readyz` report that retained
failure, including after the resident is evicted. This policy requires a cell
restart after required-worker loss. Rejected operations and intentional
retirement do not trigger this cell failure policy.

One retained shutdown driver joins workers and then closes the shard and history
stores. The five-second worker grace can request cancellation of remaining
workers, but still joins them. Storage closes are never cancelled by an observer
deadline: SlateDB marks its status closed before completing its internal joins,
so a second close or a closed status cannot establish resource termination.
Observers receive the same terminal report or an explicit ongoing result.
A failed resource close retains the replacement fence and fails directory
readiness. The runtime watchdog observes that failure as well as required-worker
loss. Completed retirements keep a small shutdown receipt, releasing the engine
and its caches.

History initialization also has one owner. Cancelling a request stops its wait;
the engine still joins an in-flight initialization and closes a late result.
Failed initialization attempts retain their typed SlateDB error category and
cannot accumulate completed task handles.

Production retirement starts the driver synchronously. Directory shutdown stops
open admission, retires residents and waits for the retained owners, including
late opens. Production bootstrap invokes it after cancelling and joining runtime
loops, including when the HTTP accept loop returns an error. An exceeded shutdown
deadline is reported as an error; it is not a successful join.

The new regression sources are `src/shard/task_lifecycle_tests.rs` and
`src/dst/tests/runtime_engine_lifecycle.rs`. The three initial regressions failed
against the R06-A source before lifecycle changes. Tests cover a real WAL PUT
held after entry, repeated/cancelled shutdown observers, actual required-role
aborts, real HTTP health, a separate healthy runtime, rejected-operation and
intentional-retirement controls, and a cancelled history open held in its real
manifest PUT. Existing retirement and durability tests remain required.

The absorber controller helper now observes worker termination before releasing
its held barrier, then full store termination before opening the next writer.
All original reservation, lag, durable dirty-marker and exact replay assertions
remain. The evidence manifest pins both versions of this helper and explains the
stronger lifecycle requirement. This document describes implementation and test
scope; execution receipts and final source identity are recorded separately.
