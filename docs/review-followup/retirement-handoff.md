# R17-B — terminal commit handoff

The `89884ab` implementation allowed retirement to drain a predecessor's durability group before a staged duplicate attached, turning applied-only truth into success. It also allowed a successful late write to publish a new group after its acker exited. Both paths were reproduced against the actual pinned backend with a held WAL PUT: two failures and three passing controls.

`CommitHandoff`, under the existing in-flight mutex, now owns terminal state and pending effects. Retirement marks terminal and drains under that guard. A successful write holds the same guard while publishing its internal applied mirrors and registering its group; a retired write instead settles every reply as moved/unknown. Its canonical storage rows may exist and are recovered by the replacement. It does not publish retired applied/producer/queue mirrors, pressure, maintenance, usage, ring, touch or tail effects.

No-write completion takes the existing async dispatch gate before the handoff mutex. It attaches to the last still-owned group, completes against an open queue whose earlier durable publications have finished, or loses to retirement. Queue emptiness cannot override terminal state. A dispatcher claims only proven remote-durable groups under the same handoff; a claim won before retirement retains its valid completion.

Lock order is dispatch gate, handoff mutex, then local maintenance/stream/pressure mirrors. Close never takes the dispatch gate. Storage I/O, encoding, awaits, notifications, callbacks and sender settlement occur outside the synchronous handoff mutex. The single storage write, supervised real workers and retained storage finalizer remain intact.

Five executable regressions cover duplicate attachment on either side of retirement, healthy remote-durability waiting, a valid durable claim before retirement, and late successful mixed data/config/close/billing completion. The last cancels a shutdown observer, observes natural acker termination, requires prompt replies and no stranded groups, joins repeatedly, checks every retired live effect, and recovers exact canonical billing/data/config/close rows in a replacement. Error assertions call the actual append failure mapper and HTTP renderer: 503 `shard_moving`, Retry-After 1, no successful offset and no definite-absence claim.

The duplicate controls retain the backend durability status across retirement. Ordinary database reads can correctly refuse `Closed(Clean)` while the owned close still awaits its held WAL flush; that refusal is not a durability oracle. The open-engine Remote-tail check and the unchanged retained sequence together keep the held-WAL proof independent of shutdown scheduling.

Status: implementation and regression evidence are prepared for reviewer acceptance. Final-source execution receipts are supplied in the follow-up report; this source contract is not an independent verification certificate.
