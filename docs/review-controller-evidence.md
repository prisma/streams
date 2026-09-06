# R09: active scaler and fleet iterations

The scaler and fleet loops now cancel while real work is in flight. The topology protocol and financial append-before-clear order remain in their application owners.

| Owner | Item / byte budget | Time / concurrent work | Continuation and cancellation |
|---|---|---|---|
| Scaler | Existing 4,096 sketches; at most 4,096 queued incarnation hints, with validated bounded tenant/name keys; 16 attempts per turn | Serial execution; 45 seconds per decision, 60 seconds per turn | Timed-out or unreadable pending work rotates to the back. New hints deduplicate by incarnation. A committed topology intent remains authoritative across shutdown and restart. |
| Fleet populations | 4,096 members plus the three known heartbeat-namespace coordination documents (4,099 provider entries), 128 KiB per body, 8 MiB total | Eight independent body fetches; 10-second population deadline | Incomplete reads publish no membership; retry reads the authoritative population. |
| Fleet coordination documents | 8 MiB per streamed body or CAS write; membership/override/URL maps at most 4,096 entries; outboxes at most 64 events | One document operation at a time; 10-second I/O deadline; 45-second total controller pass | Read absence is distinct from failure/corruption. Required desired/override reads complete before atomic ring/override publication. Unknown CAS completion retries from the next observed version and keeps pending events. |
| Fleet eager opens | Existing 16 prefixes per turn, four concurrent open waiters | One second per waiter, inside the same pass deadline | The existing owned open gate retains any underlying open/cleanup; the fleet cursor rotates through prefixes. |

The configuration validator rejects fleet limits outside the same document population bound before topology-size arithmetic or startup. All CAS publications, including outbox clear, reuse the same desired/override semantic validators as reads; an overflowing new override cannot publish a document the owner then refuses to read. The operator view reports unavailable documents as unavailable. The outbox drainer returns actual body/decoding errors instead of treating them as empty outboxes.

The scaler controller uses the existing owned runtime clock for persisted segment age, and every resume keeps the selected incarnation fence. A fresh controller can retry a durable pending transition without relying on the old task's volatile queue. Ordinary read/append topology repair continues to use the same application owner.

New controlled regressions:

- `r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry`: three real production-loop cases hold heartbeat PUT, required overrides GET, or desired CAS PUT. Shutdown must join without forced abort before the hold is released. Failed required reads retain a deliberately different prior ring/override snapshot. Restart completes authoritative publication and retains the deterministic pending event.
- `r09_scaler_cancellation_and_deadlines_preserve_intent_and_rotate_work`: parks after a real durable parent close, cancels the actual iteration, verifies descriptor intent survives, rejects a wrong-incarnation resume, and proves a healthy second stream progresses while the first remains held. Releasing the hold completes the original transition once.
- `r09_fleet_document_body_failures_and_sizes_preserve_authority`: actual body failure, corrupt JSON, misleading metadata with an oversized streamed body, and an oversized desired count are rejected; actual absence alone produces an empty bootstrap view.
- `r09_fleet_document_deadlines_leave_cas_source_retryable`: controlled Tokio time proves the exact read deadline after real GET entry; a held CAS reaches its deadline without erasing the old source and succeeds with the same version after release.
- `r09_fleet_configuration_cannot_publish_an_unreadable_population`: invalid configured counts are rejected and the supported maximum remains valid.

The deadline fixture uses the actual `ObjClass::Fleet` classification for both GET and PUT. Its initial `Other` hold did not enter; the preflight failure was corrected at that fault boundary without weakening deadline, entered-operation or retry assertions.

Additional writer-boundary regressions reject a 4,097th override before any write, verify exact prior bytes and CAS version, and successfully retry a valid update with that unchanged version. Desired count/epoch/event ceilings, malformed JSON and oversized bodies are checked at the same boundary. A 4,096-member population with its three coordination documents is a positive control; one more provider entry remains an error.

Combined all-target release Clippy metadata passed during implementation. Execution results belong to the subsequent clean-HEAD Rust receipt; this source record does not claim those tests have run before that receipt exists. Existing scenario bodies and assertions were retained.
