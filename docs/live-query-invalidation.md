# Live SQL Query Invalidation Matrix

This document maps relational/SQL query families to the narrowest invalidation
Streams Live can currently provide.

Important:

- Live does not parse SQL.
- Live does not do incremental query maintenance.
- The application must map each SQL query to one or more State Protocol
  entities plus optional templates and fine keys.
- The matrix below is a best-case view. The implementation still has global
  coarsening and false-positive paths listed later in this document.

## Hard Boundary

Streams Live can tell you that a query may be stale. It cannot generally prove
"re-run only when the SQL result actually changed" for arbitrary SQL.

The current exact boundary is narrow:

- exact fine-key invalidation exists only for active single-entity templates
  with 1 to 3 equality fields
- exact result handling also requires a settled cursor; otherwise the query can
  still observe rows ahead of the cursor and rerun later on a conservative wake
- even for those shapes, exact SQL-result invalidation is only possible for a
  narrow set of mappings: full-row reads, membership-only queries, projected
  scalar-field queries whose result depends only on membership plus explicitly
  watched fields, and small application-owned joins built from those exact
  dependencies

Everything else is either:

- exact only with respect to the watched equality partition, not the final SQL
  result, or
- coarse at template or table scope

## Reading The Matrix

- `Best Live mapping` is the narrowest keying the current system can use.
- `Narrowest invalidation scope` describes what a wake means in the best case.
- `Result-exact in best case?` means the wake corresponds to a change that
  really changes the SQL result, ignoring the global false-positive sources
  listed later.

## Query Matrix

| Query family | Example | Best Live mapping | Narrowest invalidation scope | Result-exact in best case? | Notes |
| --- | --- | --- | --- | --- | --- |
| Whole-table full-row read | `SELECT * FROM posts` | `tableKey(posts)` | All rows in one entity | Yes | Any insert, delete, or update on `posts` changes the result. `ORDER BY` without `LIMIT/OFFSET` does not change this. |
| Single-table full-row equality read on 1 to 3 fields | `SELECT * FROM posts WHERE tenantId=? AND status=?` | `watchKey(templateId, args)` plus `templateIdsUsed` | One equality tuple in one entity | Yes | Requires an active template, `interestMode="fine"`, runtime fine mode, usable before-images for updates, and best exactness comes from a small literal `keys` wait with a settled cursor. |
| Small finite union of full-row equality tuples | `SELECT * FROM posts WHERE (tenantId,status) IN ((?,?),(?,?))` | One watch key per concrete tuple | The enumerated equality tuples | Yes | This is inferred from `touch/wait` accepting multiple keys. It stays narrow only while the full tuple set is explicitly enumerated and small. |
| Membership-only equality query on 1 to 3 fields | `SELECT count(*) FROM posts WHERE tenantId=?`, `SELECT EXISTS(...)`, `SELECT id FROM posts WHERE id=?`, `SELECT id FROM posts WHERE tenantId=? ORDER BY id` | `membershipKey(templateId, args)` plus `templateIdsUsed` | Insert/delete/equality-partition-move changes for one tuple | Yes | This key family ignores updates that keep the row in the same equality tuple. It is result-exact only when the SQL result depends on membership of matching row identities, not row contents. That includes stable row-key projections such as `SELECT id WHERE id=?`. Small literal-key waits get the strongest practical exactness. |
| Projected scalar-field equality query on 1 to 3 fields | `SELECT author FROM posts WHERE id=?`, `SELECT id, title FROM posts WHERE tenantId=? ORDER BY id` | `membershipKey(templateId, args)` plus one `projectedFieldKey(templateId, fieldName, args)` per projected scalar field, plus `templateIdsUsed` | One equality tuple in one entity, sensitive only to membership or the listed scalar fields | Yes | Result-exact only when membership and the listed projected scalar fields fully determine the SQL result. Ordering must be stable from row identity or another watched field. Updates need usable before-images to stay exact; `skipBefore` becomes after-only best effort. |
| Application-owned join with a small exact dependency set | `SELECT c.id, c.name, m.id, m.text, m.author, m.createdAt FROM channel c JOIN message m ON m.channelId=c.id WHERE c.id=? ORDER BY m.id` | Union of exact per-entity watch keys, for example `watchKey(channel.id=?)` plus `watchKey(message.channelId=?)`, preheated as a small literal keyset and paired with a settled cursor | Only the exact dependencies the app enumerated | Yes, for that concrete shape | This is not generic SQL join tracking. It works only when the app can decompose the query into exact participating dependencies, keep those keys hot through the query, and each dependency is itself exact for the query result. |
| General projected row read | `SELECT id, title FROM posts WHERE tenantId=? ORDER BY createdAt`, `SELECT metadata FROM posts WHERE id=?` | Watch key(s) when the filter is template-eligible, otherwise `tableKey` | Exact watched tuple(s), otherwise table | No | Queries that depend on untracked fields, non-scalar projected values, or ordering/pagination outside the watched field set still false-wake under generic `watchKey` invalidation. Stable row-key-only projections belong in the membership-only row above. |
| Top-N / pagination / offset query | `SELECT * FROM posts WHERE tenantId=? ORDER BY createdAt DESC LIMIT 20` | Watch key(s) when the filter is template-eligible, otherwise `tableKey` | Exact watched tuple(s), otherwise table | No | Changes inside the watched partition but outside the returned window still wake. |
| Aggregate / `DISTINCT` / `GROUP BY` / `HAVING` / window / value-sensitive derived query on one entity | `SELECT sum(amount) FROM posts WHERE tenantId=?` | Watch key(s) when the filter is template-eligible, otherwise `tableKey` | Exact watched tuple(s), otherwise table | No | Live invalidates from base-row changes. It does not maintain derived relational state beyond membership-only exactness. |
| Equality filter with more than 3 bound fields | `SELECT * FROM posts WHERE a=? AND b=? AND c=? AND d=?` | `tableKey`, or an app-chosen less-specific template if over-invalidation is acceptable | Table, or only a partial equality partition | No | Templates are limited to 1 to 3 fields. |
| Range / inequality / pattern / function / expression predicate | `SELECT * FROM posts WHERE createdAt>=?` | `tableKey(posts)` | Table | No | Current Live does not emit range, prefix, expression, or function-based invalidations. |
| Large finite `IN` / `OR` keyset | `SELECT * FROM posts WHERE id IN (...)` | Many watch keys | Enumerated tuples on the write side, but broad bloom-based matching on the wait side once the keyset is large | No | `touch/wait` allows up to 1024 keys. Large wait sets become broad waiters and false positives increase. |
| Join across multiple entities | `SELECT * FROM posts p JOIN users u ON ...` | `tableKey` for every participating entity the application knows the query depends on | Participating tables | No | There is no join dependency tracker, no multi-table watch key, and no SQL parser. |
| `UNION` / `INTERSECT` / `EXCEPT` / multi-source CTE or expanded view | `SELECT ... FROM posts UNION ALL SELECT ... FROM drafts` | `tableKey` for every participating entity | Participating tables | No | The application must supply the full base-entity dependency set itself. |

## Practical Reading Of The Matrix

- Any query can be supported if the application can at least name the entity or
  entities it depends on and wait on their table keys.
- Exact `watchKey` invalidation is limited to single-entity equality filters on
  1 to 3 fields.
- Exact `membershipKey` invalidation is available for single-entity,
  membership-only equality queries on 1 to 3 fields.
- Exact `projectedFieldKey` invalidation is available for single-entity,
  scalar projected-field equality queries on 1 to 3 fields when the app waits
  on membership plus every result-relevant projected field.
- Small literal fine-key waits can opt into an exact wait path with
  `wait.exact=true`, which avoids the bloom-style and uint32-id false positives
  for recent cursor ranges.
- Exact SQL-result invalidation is narrower still: it is available for
  full-row reads whose result necessarily changes whenever any watched row
  changes, for membership-only queries whose result depends only on the set of
  matching row identities, for scalar projected-field queries whose result
  depends only on membership plus the watched field set, and for a small class
  of application-owned joins whose dependencies can be enumerated exactly.

Examples:

- `SELECT * FROM posts WHERE id=?` can be result-exact.
- `SELECT count(*) FROM posts WHERE tenantId=?` can now be result-exact with
  `membershipKey`.
- `SELECT EXISTS(SELECT 1 FROM posts WHERE tenantId=?)` can now be
  result-exact with `membershipKey`.
- `SELECT id FROM posts WHERE id=?` can be result-exact with `membershipKey`
  because the result depends only on membership of that stable row identity.
- `SELECT author FROM posts WHERE id=?` can be result-exact with
  `membershipKey` plus `projectedFieldKey(author)`.
- `SELECT id, title FROM posts WHERE tenantId=? ORDER BY id` can be
  result-exact with `membershipKey` plus `projectedFieldKey(title)` because the
  result depends only on membership of stable row keys and the projected title.
- the `channel`/`message` join from the live demo can be result-exact if the
  client preheats the exact keyset, uses a settled cursor, and waits on the
  exact per-entity keys for the watched channel row and message partition
- `SELECT id FROM posts WHERE tenantId=? ORDER BY id` can be result-exact with
  `membershipKey` if the result only depends on membership of stable row keys.
- `SELECT * FROM posts WHERE tenantId=? ORDER BY createdAt DESC LIMIT 10` cannot
  be result-exact because rows outside the returned window can still wake it.

## Global Coarsening And False-Positive Sources

The matrix above is the best case. The current implementation has these
cross-cutting cases that make invalidation coarser or force extra re-runs:

| Condition | Effect on invalidation |
| --- | --- |
| Template is not active yet, or the change predates template activation | No fine watch-key invalidation exists for that history. Only coarse table invalidation is reliable. |
| Update is missing a usable `old_value` and `onMissingBefore="coarse"` | Fine update invalidation is suppressed. The update only emits the coarse table touch. |
| Update is missing a usable `old_value` and `onMissingBefore="skipBefore"` | Fine invalidation becomes after-only best effort. `watchKey` can miss the old equality tuple, `membershipKey` can miss rows leaving the old partition, and `projectedFieldKey` can only emit after-side scalar field touches because it cannot compare before vs after. |
| Runtime degrades to `touchMode="restricted"` | Fine waits wake on `templateKey`, which means "some row for this template shape changed", not necessarily the same bound values. |
| Runtime degrades to `touchMode="coarseOnly"` or the client chooses `interestMode="coarse"` | Waits wake on `tableKey`, which means "some row in this entity changed". |
| Wait keyset grows past `memory.keyIndexMaxKeys` (default `32`) | The waiter becomes broad and matching on flush is bloom-based, so false positives increase. |
| Routing keys are collapsed to uint32 key IDs in the journal hot path | Distinct 64-bit routing keys can collide and cause false-positive wakes, except on the small literal fine-key exact path. |
| `maybeTouchedSince*` is a bloom-style fast path | Immediate "already touched" checks are intentionally lossy and can report touched when the exact watched key was not touched, except when recent exact-key history covers the small literal fine-key wait. |
| A journal bucket overflows its unique-key cap | The bucket becomes a broadcast wakeup for all waiters. |
| `touch/meta` cursor is captured while scan lag or the current pending bucket still exists | The query can observe rows newer than the stable flushed cursor coverage, so the next wait may wake even though the rerun result is unchanged. Use `GET /touch/meta?settle=flush` to avoid this internal race. |
| Process restart changes the journal epoch | `stale=true` forces a full rerun and re-subscribe. |

## Current Exactness Preconditions

For the narrow "result-exact" rows in the matrix to remain exact:

- whole-table full-row reads must use table scope and avoid the journal-level
  false-positive paths listed above
- equality-scoped full-row reads must also satisfy all of these:
  - the query is represented as 1 to 3 equality fields per active template
  - the client uses the correct watch key(s) plus `templateIdsUsed`
  - the relevant template was active before the change occurred
  - updates have usable before-images for the template fields
  - the runtime stays out of `restricted` and `coarseOnly`
  - for the strongest practical exactness, the wait uses literal `keys` and the
    keyset stays within the small exact fine-key lane
  - the wait set stays small enough to avoid broad matching if exact waiter
    indexing matters
- membership-only equality queries must also satisfy all of these:
  - the query is represented as 1 to 3 equality fields per active template
  - the client uses the correct membership key(s) plus `templateIdsUsed`
  - the result depends only on membership of matching row identities, not row
    contents
  - updates have usable before-images for those template fields
  - the runtime stays out of `restricted` and `coarseOnly`
  - for the strongest practical exactness, the wait uses literal `keys` and the
    keyset stays within the small exact fine-key lane
  - the wait set stays small enough to avoid broad matching if exact waiter
    indexing matters
- projected scalar-field equality queries must also satisfy all of these:
  - the query is represented as 1 to 3 equality fields per active template
  - the client uses the correct membership key(s), plus one
    `projectedFieldKey` per projected scalar field, plus `templateIdsUsed`
  - the SQL result depends only on row membership plus those watched scalar
    fields; if ordering matters it must be determined by row identity or by
    another watched field
  - updates have usable before-images for the template fields and projected
    fields
  - the runtime stays out of `restricted` and `coarseOnly`
  - for the strongest practical exactness, the wait uses literal `keys` and the
    keyset stays within the small exact fine-key lane
  - the wait set stays small enough to avoid broad matching if exact waiter
    indexing matters
- in all cases:
  - the query is a full-row read over one entity, or a fully enumerated small
    set of equality tuples on one entity, or a scalar projected-field equality
    query whose result depends only on membership plus the watched field set, or
    a join whose exact dependencies are all enumerated explicitly by the
    application
  - exact-result-sensitive small fine keysets are preheated before the query and
    use a settled cursor so writes during the query still emit exact fine keys
  - no uint32 key collision, bloom false positive, or overflow wake occurs
  - the cursor used for the wait is settled enough that the query did not
    observe newer state than the cursor covers, for example from
    `/touch/meta?settle=flush`

If any of those conditions fails, exactness is lost. In the conservative modes
(`coarse`, `restricted`, `coarseOnly`) the system still preserves invalidation
correctness but wakes become coarser and extra SQL reruns are expected.
`skipBefore` is explicitly best-effort and can also miss the old partition on
updates that move rows.
