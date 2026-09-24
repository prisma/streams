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
| Same seal claim | Renew exact operation, only once its content is valid on this instance | New generation exceeds every earlier reservation/fence; a retry refused by this instance's limits leaves the claim unrenewed |
| Other live final-bearing claim | Refuse conflict | Preserve the other operation's promised record |
| Abandoned final-bearing claim | Reserve generation, fence physical committer, inspect closed acknowledgement | Only the winning reservation may install; a committed old final is completed on its owner's behalf |
| Final append ambiguous or cancelled | Leave owed-final claim | Exact retry can discover durable progress; cancellation is never proof of rejection |
| Final append definitively rejected | The attempt that installed the claim releases exact incarnation/operation/generation | A renewed or replacement claim cannot be released; a joined or renewed exact retry releases nothing |
| Final append acknowledged closed | Mark final committed, close remaining live segments | Marking must succeed before unrelated segment closes begin |
| Segments closed, descriptor still sealing | Resume closes and publish terminal state | Idempotent physical closes return frozen offsets |
| Sealed | Exact-operation replay succeeds | Another operation cannot claim the terminal proof |
| Deleted / retained fork debt | Creation/deletion coordinator owns cleanup | Seal never follows a same-name replacement incarnation |

## Limit reductions and accepted finals

Ingest capacity (`LIMIT_*`) and the per-record ceiling
(`MAX_RECORD_PAYLOAD_BYTES`) are per instance and may differ during a rolling
configuration change. A final-bearing claim is accepted under the limits of
the instance that installed it, and a later or different instance with lower
limits may neither renew nor release it (TLA-003-F4, TLA-003-F5):

- A raw exact retry validates its content (`parse_content`) before it renews
  the owed claim. A 413 for ingest capacity leaves the claim unrenewed.
- An over-ceiling record stays a deferred refusal, so the committer still
  decides duplicates first. The retry carries the generation it observed
  without renewing. If the original already committed, the retry is
  acknowledged as that duplicate, marks the final and seals. Otherwise it is
  refused and releases nothing.
- Only the attempt that installed the claim releases it on a definitive
  refusal. Its content was valid where the claim was taken, so its refusal
  rests on committer state that every attempt shares.

The accepted final therefore survives a reduction: the installing attempt
commits it, an exact retry on an instance whose limits admit it delivers it,
or, after the lease lapses, the fenced takeover completes a committed final
on its owner's behalf or replaces a claim whose final can no longer commit.

Open obligations: (1) under a reduced ingest capacity, an exact retry of a
final that already committed is refused 413 rather than acknowledged as a
duplicate. The claim is untouched and heals by a capable exact retry or by the
takeover after the lease lapses. (2) If every instance's limits fall below an
accepted final, nothing delivers the record. The collection stays Sealing for
one lease period, then the takeover fences the old generation and a later seal
replaces the claim; no attempt tells the original client that its promise was
cancelled. (3) The product seal validates capacity and ceiling before it
claims or renews (TLA-003-F3), and its definitive refusals still release the
renewed claim. That holds only while product refusals after the claim rest on
committer state alone.

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
