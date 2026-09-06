# Creation and deletion transitions

`application::creation::CreationService` owns registry, shard, read, key, quota,
clock/entropy and billing-attribution capabilities. Protocol adapters parse wire
fields and render `CreateOutcome`, `CreationError` and `ProductCreateError`.
No application lifecycle operation receives HTTP `AppState` or returns an HTTP
response. Product creation's quota reservation commits only for a new registry
winner; idempotent losers release it through Drop.

| Observed state | Command | Durable decision / retry behavior |
|---|---|---|
| Missing | Create | Create-only descriptor write; content/fork/close requests carry `init` before storage work. |
| Initializing, same request and key | Create replay | Re-run seed/anchor/producer-deduped initial append, then publish readiness on that incarnation and claim. |
| Initializing, different request/key | Create | Refuse; elapsed claim age does not publish missing content. |
| Active, same immutable config/key | Create replay | Return the current incarnation. |
| Active or initializing, has children | Delete | Persist soft deletion, retain data/name and parent reference. |
| Active or initializing, no children | Delete | Persist tombstone, close timestamp and parent-release debt in one conditional registry write. |
| Soft-deleted source, last child released | Release child | Atomically remove exact child ID and tombstone, then retry ancestor debt. |
| Tombstone with parent debt | Delete replay | Repeat conclusive parent release; clear only the matching debt on the same incarnation. |
| Tombstone, source reference not yet installed | Delete replay | Keep debt: an absent reference on a live source is inconclusive. |
| Name recreated | Late seed/publication/cleanup/TTL | Incarnation mismatch declines the old operation; new incarnation state is preserved. |
| Sealing or topology transition | New fork anchor | Refuse new anchor. An existing exact anchor can be replayed while retained. |
| Sealed | Create replay | Preserve closed state; final publication remains the seal coordinator's authority. |

Fork stamping, deletion classification and every mutation result are attempt
local. A failed conditional write cannot leak `already` or `hard_deleted` flags
into the winning attempt. Existing records remain invisible behind `init` until
the durable content/closure and fork anchor have completed. A timeout after
submission reports an ambiguous result and leaves that durable marker intact.

Registry writes and independent SlateDB commits are separate durability domains.
Cancellation, lost replies and partial completion retain initialization or
cleanup debt; retry reconciles it. This service does not invent a cross-store
transaction. Billing close submission may fail after tombstoning; the retained
logical close timestamp remains the reconciliation source of truth.

TTL single-flight tracking is scoped to one creation service/runtime. Its Drop
guard removes an in-flight entry on success, error or task cancellation; an old
incarnation's slide cannot update a replacement descriptor.
