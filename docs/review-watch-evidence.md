# R07 watch ownership and admission

`application::watch::WatchService` owns descriptor resolution, credential proof,
current policy checks, watch admission, journal observation and append touch
planning. Its inputs name account admission, an unverified capability carrier,
or deployment authority. A verified observation is constructible only inside
the owner. Protocol adapters parse headers/query parameters and render typed
outcomes; they do not repeat the authentication gate.

An account request reuses the request slot held by the HTTP adapter. A verified
capability acquires its own request slot. Both acquire one live-subscription
slot, held through the awaited journal result and released on return or
cancellation. Capability claims never provide audit identity before signature
verification. Missing, expired, malformed and forged capabilities continue to
produce the same refusal regardless of descriptor existence.

The pre-auth lookup limiter is owned by the cached runtime service, with a
1,000-request burst and 500 requests/second refill using monotonic time. The
process-global, first-touch token bucket was removed. Production protocol time
shares the runtime clock; HTTP fixtures explicitly use their JWT/policy feed's
wall-clock domain, separate from their seeded local timer scenarios.

Watch definition/key encoding has one implementation shared by append and
observation. `append_touch` produces the existing deferred committer effect;
publication stays behind durable acknowledgement. The wire still distinguishes
changed, resync and timeout observations, including nullable stream cursors.

Focused local verification includes:

- `account_watch_wait_charges_one_request_and_releases_its_subscription`: a
  project with one request and one subscription slot can complete two sequential
  authenticated waits, and retains zero inflight requests afterward.
- `lookup_budget_is_runtime_local_and_refills_only_with_elapsed_time`: one
  exhausted budget does not affect another; forward/backward wall jumps cannot
  refill it, while 100 ms elapsed time permits exactly 50 more lookups.
- Existing observation scenarios exercise matching durable appends, shard
  closure, a cold process without the encryption key, policy suspension/staleness
  and concurrent capability quota enforcement.
- The body-poll sentinel and 144-case unauthorized watch matrix are retained.

Final aggregate commands and source revision are recorded in the review ledger.
