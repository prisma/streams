# Seal transition ownership (R05)

`application::lifecycle::LifecycleService` owns seal claims, final-record
authority, generation fences, physical segment closes and terminal publication.
It receives registry, topology, clock and certification-delay capabilities.
It has no HTTP state or response dependency. `seal_final` coordinates the typed
append capability; adapters translate its outcome once.

| Observed state | Permitted operation | Durable progress / retry |
|---|---|---|
| Active, topology quiet | Install empty or final-bearing claim | Allocate a fresh generation in the successful registry CAS |
| Topology pending | Resume topology before claiming seal | Persisted split/merge intent survives physical-close failures |
| Initializing | Creation owner completes initialization first | Adapters refuse seal admission while initialization is incomplete |
| Same seal claim | Renew exact operation | New generation exceeds every earlier reservation/fence |
| Other live final-bearing claim | Refuse conflict | Preserve the other operation's promised record |
| Abandoned final-bearing claim | Reserve generation, fence physical committer, inspect closed acknowledgement | Only the winning reservation may install; a committed old final is completed on its owner's behalf |
| Final append ambiguous or cancelled | Leave owed-final claim | Exact retry can discover durable progress; cancellation is never proof of rejection |
| Final append definitively rejected | Release exact incarnation/operation/generation | A renewed or replacement claim cannot be released |
| Final append acknowledged closed | Mark final committed, close remaining live segments | Marking must succeed before unrelated segment closes begin |
| Segments closed, descriptor still sealing | Resume closes and publish terminal state | Idempotent physical closes return frozen offsets |
| Sealed | Exact-operation replay succeeds | Another operation cannot claim the terminal proof |
| Deleted / retained fork debt | Creation/deletion coordinator owns cleanup | Seal never follows a same-name replacement incarnation |

Claim, reservation, renewal, release, final marking and publication use the typed
registry mutation API. Decisions return attempt-local results, so a losing CAS
cannot leak a generation or an `already` flag into a later attempt.

Registry updates and independent storage engines are separate durable steps.
The protocol relies on persisted intent, ordered fences and idempotent replay;
it does not promise an atomic transaction across those stores. The unused
unfenced `seal_descriptor` path was deleted.

The original seal, incarnation, topology and durability integration scenarios
remain traceable in the test inventory. `cancelled_final_preserves_claim_and_only_definitive_retry_releases_it`
adds cancellation after the durable claim but before append completion, followed
by ambiguous and definitive exact retries. The SEL-019 fixture now also proves
the validated descriptor boundary refuses overlapping topology/seal claims;
its stale phase-B check uses a legal competing descriptor state.
