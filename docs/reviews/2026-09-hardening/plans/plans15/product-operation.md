# Item 73, step 1: ProductOperation replaces the required_scope matrix

Base: `origin/slate` = HEAD = `c397e9e5` (the 8 commits have been pushed since the task was
written; `git merge-base HEAD origin/slate` = `c397e9e5`). `src/product.rs` = 4,205 lines at the base,
so its ceiling is 4,205.

**Summary.** The problem is real but latent. Every operation the entry dispatches today gets its
correct §6.1 scope; the hazard is the fallback arms. The reviewer's step 1 can be built, but it
is **not status-neutral**. Once `required_scope` is deleted, a request that names no operation
has no scope to check. An enforce-mode principal that lacks the old fallback scope then gets
404/405 instead of `403 missing_scope`. That is decision D1 for Søren. The plan therefore has
two commits:

* **Commit 1: lands now.** Test only, no edge change. It pins the §6.1 scope of every operation
  the entry dispatches, and the "authenticate before 404/405" order in enforce mode.
* **Commit 2: HELD until Søren answers D1.** It adds `src/product/operation.rs`
  (`ProductOperation`, `resolve`, an exhaustive `scope()`, the verb list) and moves the gate onto
  it. It deletes `required_scope` and adds an entry-vs-resolve agreement test plus the red test
  for D1.

The status-preserving variant keeps the fallback table for requests that name no operation. I
recommend against it (§2).

---

## 1. Problem (verified on `c397e9e5`)

### 1.1 The matrix is decided separately from the dispatch, and has fallback arms

`src/product.rs:588-644` `required_scope` (doc comment at 582-587). The reviewer's cited lines
are exact on the current tree:

```rust
594:     let read = *method == Method::GET || *method == Method::HEAD;
597:             if *method == Method::PUT {
599:             } else if *method == Method::DELETE || verb == Some("seal") {
601:             } else if verb == Some("scan") {
610:             } else {
611:                 S::MetadataRead            // Collection fallback
612:             }
617:             } else {
618:                 S::RecordsRead             // Records fallback
619:             }
630:             } else {
631:                 S::ConsumersConfigure      // Consumer fallback
632:             }
637:             } else {
638:                 S::WatchesManage           // Watches/Watch fallback (not cited by the reviewer)
639:             }
641:         ProductRoute::Usage { .. } => S::UsageRead,        // any method, any verb
642:         ProductRoute::WatchWait { .. } => return None,
```

The dispatch is a second, independent table in `product_entry` (`src/product.rs:1114-1377`):

| Route | Dispatched `(method, verb)` | Everything else |
|---|---|---|
| Records | `(POST,None)` `(POST,batch)` `(GET,None\|long-poll)` `(GET,sse)` | 405 "records accepts POST (append) and GET (read)" (:1200) |
| Consumer | `(PUT,None)` `(GET,None)` `(DELETE,None)` `(POST,pull)` `(POST,settle)` | 405 (:1259) |
| Usage | `GET`, any verb (the `match method` ignores the verb) | 405 "usage accepts GET" (:1271) |
| Watches / Watch | `GET`, any verb (`if method == Method::GET`) | 405 "watches are read-only (GET)" (:1284, :1297) |
| WatchWait | `GET`, any verb | 405 (:1333) |
| Collection | `(PUT,None)` `(GET,None)` `(DELETE,None)` `(POST,seal)` `(GET,scan)` | 404 `unknown_route` "no such product operation" (:1368) |

**Verified: no live fail-open.** I checked all 18 dispatch arms. Each one gets exactly its §6.1
scope from `required_scope`:

| Operation | Scope |
|---|---|
| create | create |
| metadata, consumer GET, watches, watch | metadata.read |
| delete, seal | lifecycle.manage |
| scan, read, long-poll, SSE | records.read |
| append, batch | records.append |
| consumer PUT/DELETE | consumers.configure |
| pull | consumers.pull |
| settle | consumers.settle |
| usage | usage.read |
| watch wait | none |

**Verified: the matrix answers for requests the entry never serves.** Examples:

* `required_scope(Collection, None, POST)` = `Some(MetadataRead)`. The entry answers 404 `unknown_route`.
* `(Records, Some("batch"), GET)` = `Some(RecordsRead)`. The entry answers 405.
* `(Consumer, None, POST)` = `Some(ConsumersConfigure)`. The entry answers 405.
* `(Watches, None, DELETE)` = `Some(WatchesManage)`. The entry answers 405; no watches route dispatches anything but GET.

**The hazard.** Say someone adds `"compact"` to the verb list and an entry arm
`(Method::POST, Some("compact"))` on Collection. The request would be authorized by `S::MetadataRead`
(:610-612): no compile error and no failing test. A mutation gated by a read scope is fail-open.
The same applies to any new method arm, such as PATCH on a collection.

### 1.2 Use sites (grep over src incl. src/dst and cfg(test), tools/, fuzz/, bench/, scripts/, .github/, docs/)

* **`required_scope`.** Defined at `src/product.rs:588`. Its only caller is `src/product.rs:968`
  (`product_auth_gate`). Its only doc reference is `docs/refactor/WIRE-MATRIX.md:79`. **No test
  names it.** `tools/`, `fuzz/`, `bench/`, `scripts/` and `.github/` have no hits.
* **`product_auth_gate`.** Defined at `src/product.rs:947`, under
  `#[allow(clippy::result_large_err, reason = "transport boundary returns Axum wire response directly; application errors stay compact")]`
  at :943-946. Its only caller is `src/http.rs:1865` (`product_entry_axum_inner`). Ledger rows:
  `docs/quality/source-allowances.json:1588` (active) and `docs/quality/legacy-source.json:2513`
  (frozen ceiling).
* **`strip_verb`.** Defined at :440, with a local `const VERBS: [&str; 7]` at :441-449. Callers:
  :463 (`classify_route`), :967 (gate), :1108 and :1341 (entry).
  `docs/refactor/WIRE-MATRIX.md:77`.
* **`classify_route`.** Defined at :462. Callers: :547 (`watch_capability_carrier`), :965 (gate),
  :1110 (entry). The path is parsed again at :1341 (`canonical_name(strip_verb(&path).0)`). The
  reviewer's "parsed 2-3 times" is real; it is step 2.
* **`route_stream_name`.** Defined at :646, called at :976. Unchanged.
* **`ProductAuthorization::Preflight`.** :928, :955, :1217, and `unreachable!("preflight returned before dispatch")` at :1323-1325.
  The reviewer's point is real; it is step 2.
* **Handlers restate the consumer scopes a third time**, as literals: `src/product.rs:3117`
  (ConsumersConfigure, put), :3169 (MetadataRead, get), :3721 (ConsumersSettle),
  `src/product/consumer_pull.rs:52` (ConsumersPull), `src/application/consumer/deletion.rs:24`
  (ConsumersConfigure). All agree with the gate today. Commit 1's test pins that agreement.
* **Body-visible compound scopes** (not part of the matrix, unchanged): `src/product.rs:1434`
  (watches.manage on create) and `src/application/consumer.rs:481` (dlq.configure).
* **Edge tests touching the matrix today (all dispatched operations):**
  * `security_modes.rs::enforce_mode_gates_the_product_surface`: records append/read, `:scan` metadata-only (:583), catalog, project usage.
  * `security_audit.rs` (:92, GET records `missing_scope` journaled).
  * `security_routes.rs::product_requires_the_account_token`: legacy token mode. The tokenless-401 table is at :493-522 and is the one the reviewer cites.
  * `product/tests.rs::every_auth_refusal_keeps_its_response`: the `auth_failure_response` mapping.

  **No test pins a 403 for a request that names no operation.** scripts/sdk/contracts: no hits
  for `missing_scope`.

---

## 2. Contract decision

**What stays identical in both commits:**

* Every dispatched operation: same scope, same status, same body.
* Off and shadow modes.
* The order grammar (400/404 from `classify_route`) → authenticate (401) → scope (403) → prefix
  (403 `prefix_denied`) → wrapper gates → entry 404/405. For a request that names no operation,
  authentication and the prefix check still come before the entry's 404/405. That is what "auth
  before 404/405" means and what `security_routes.rs:493-522` pins (legacy mode). Commit 1 adds
  the enforce-mode pin.
* Watch-wait GET still carries no scope. Its handler verifies a §15 capability or the stream key.
* Verb-less routes still ignore a verb. `GET …/watches:scan` still lists watches, because
  `resolve` reproduces the entry exactly.

**What cannot stay identical once `required_scope` is deleted (D1, commit 2 only).** Take an
enforce-mode principal whose credential lacks the fallback scope from §1.1, sending a request
that names no operation.

* Today it gets **403 `missing_scope`**, which is journaled as a security denial
  (`crate::audit::tag` → `observe_denial`).
* With commit 2 it gets whatever the rest of the pipeline says. That is usually **404
  `unknown_route` / 405 `method_not_allowed`**, which is not journaled.

Examples: `POST /v1/streams/x` without metadata.read; `GET …/records:batch` or `PATCH …/records`
without records.read; `DELETE …/watches` without watches.manage; `POST …/usage` without
usage.read; `GET …/consumers/c:pull` without consumers.pull.

There is no status-preserving way to delete the fallback. The exact 403-vs-405 split for those
requests depends on the fallback scope and nothing else. Keeping those statuses means keeping
`required_scope`'s whole table for the "no operation" case. That is a third encoding of route ×
verb × method (dispatch, `resolve`, legacy fallback), which is worse than today. So commit 2 is
held for D1 rather than built in a status-preserving form.

**Step 1 alone opens a worse hole unless the entry is tied to `resolve`.** With step 1 alone the
gate uses `resolve` but the entry still has its own `match`. If a future entry arm has no
`resolve` arm, that operation would run with **no scope at all**, which is worse than today's
fallback. So commit 2 must carry the entry-vs-resolve agreement test (T3). T3 must iterate the
real verb list, which is why `VERBS` moves into the new module and is shared with `strip_verb`
and the test.

---

## 3. Tests

All tests go in a new DST module `src/dst/tests/security_operations.rs`. `security_routes.rs`
(801 lines) and `security_modes.rs` (827 lines) cannot absorb ~260 lines under the 1,000-line
ceiling. Shared items in the module:

```rust
//! Product-operation authorization: the §6.1 scope each dispatched operation demands, and
//! the authentication that precedes a route refusal (404/405) for a request naming none.
use super::fixture_auth::{auth_rig, rig_scoped_bearer};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use crate::tenant::Scope;

const PROJECT: (&str, &str) = ("proj-ops", "ws-ops");
const CREDENTIAL: &str = "c-ops";

/// Every operation the product entry dispatches, on a stream that never exists (every
/// handler refuses fast and nothing is created), with the one scope the gate must demand.
const OPERATIONS: [(&str, &str, Option<Scope>); 19] = [
    ("PUT", "/v1/streams/opgrid", Some(Scope::Create)),
    ("GET", "/v1/streams/opgrid", Some(Scope::MetadataRead)),
    ("DELETE", "/v1/streams/opgrid", Some(Scope::LifecycleManage)),
    ("POST", "/v1/streams/opgrid:seal", Some(Scope::LifecycleManage)),
    ("GET", "/v1/streams/opgrid:scan", Some(Scope::RecordsRead)),
    ("POST", "/v1/streams/opgrid/records", Some(Scope::RecordsAppend)),
    ("POST", "/v1/streams/opgrid/records:batch", Some(Scope::RecordsAppend)),
    ("GET", "/v1/streams/opgrid/records", Some(Scope::RecordsRead)),
    ("GET", "/v1/streams/opgrid/records:long-poll", Some(Scope::RecordsRead)),
    ("GET", "/v1/streams/opgrid/records:sse", Some(Scope::RecordsRead)),
    ("PUT", "/v1/streams/opgrid/consumers/c", Some(Scope::ConsumersConfigure)),
    ("GET", "/v1/streams/opgrid/consumers/c", Some(Scope::MetadataRead)),
    ("DELETE", "/v1/streams/opgrid/consumers/c", Some(Scope::ConsumersConfigure)),
    ("POST", "/v1/streams/opgrid/consumers/c:pull", Some(Scope::ConsumersPull)),
    ("POST", "/v1/streams/opgrid/consumers/c:settle", Some(Scope::ConsumersSettle)),
    ("GET", "/v1/streams/opgrid/watches", Some(Scope::MetadataRead)),
    ("GET", "/v1/streams/opgrid/watches/w", Some(Scope::MetadataRead)),
    ("GET", "/v1/streams/opgrid/usage", Some(Scope::UsageRead)),
    ("GET", "/v1/streams/opgrid/watches/w/keys/0011223344556677", None),
];

fn error_code(body: &[u8]) -> String { /* serde_json v["error"]["code"] as String, "" otherwise */ }

/// One request under a credential republished at `grant_version` holding exactly `scopes`.
async fn scoped_call(svc: &crate::auth::AuthService, addr: std::net::SocketAddr,
                     request: (&str, &str), scopes: &str, grant_version: u64) -> (u16, String) {
    let bearer = rig_scoped_bearer(svc, PROJECT, CREDENTIAL, scopes, grant_version);
    let (method, path) = request;
    let (status, _, body) = preq(addr, method, path,
        &[("authorization", bearer.as_str()), ("prisma-encryption-key", PRISMA_KEY)], b"").await;
    (status, error_code(&body))
}
```

(`rig_scoped_bearer` republishes the credential with strictly increasing grant/feed versions,
which `publish_grants` requires. There is no token cache: `verify_customer` intersects token ∩
credential scopes on every request, `src/auth.rs:646-648`. Sending the key makes the consumer
handlers reach their own `ConsumerService::authorize(.., scope)` check, so the "only" leg also
pins handler/gate agreement. The empty body cannot create the stream: `parse_create_doc` refuses
an empty body with 400 `invalid_config`, `src/product.rs:248-255`.)

### Commit 1: pinning tests (green before and after commit 2)

**T1 `dst::dst_tests::security_operations::every_dispatched_operation_demands_exactly_its_scope`**
runs on `auth_rig(PROJECT.0, PROJECT.1, &[CREDENTIAL], None)`, looping over `OPERATIONS`:

* **"Without" leg.** Hold every scope except the row's (`Scope::ALL` minus it).
  * `Some(s)` rows must answer `(403, "missing_scope")`, message `"{method} {path} without {s}"`.
  * The `None` row (watch wait) must *not* answer `missing_scope`.
* **"Only" leg.** Hold just `s`; the answer must not be `missing_scope`. Expected answers are
  404 `not_found` / 400 `invalid_config` / 403 `watch_unauthorized`, all fast on a missing
  stream.
* The body is under 40 lines with nesting 2 (`for` plus the `let … else` for the `None` row).

**T2 `dst::dst_tests::security_operations::a_request_naming_no_operation_authenticates_before_its_404_or_405`**
runs on the same rig, with one full-scope bearer (`Scope::ALL`, grant version 2).

1. **Grammar control.** Tokenless `GET /v1/streams/opgrid/watches/w/extra` must answer
   `(404, "unknown_route")`: `classify_route` runs before authentication.
2. **Undispatchable rows.** Rows (`UNDISPATCHABLE: [(&str, &str, u16, &str); 8]`):

   | Request | Expected with full scope |
   |---|---|
   | `POST /v1/streams/opgrid` | 404 `unknown_route` |
   | `PUT /v1/streams/opgrid:seal` | 404 `unknown_route` |
   | `PATCH …/opgrid/records` | 405 `method_not_allowed` |
   | `GET …/opgrid/records:batch` | 405 |
   | `GET …/opgrid/consumers/c:pull` | 405 |
   | `DELETE …/opgrid/watches` | 405 |
   | `POST …/opgrid/usage` | 405 |
   | `PUT …/opgrid/watches/w/keys/0011223344556677` | 405 |

   For each row:
   * Tokenless must answer `(401, "unauthorized")`, message `"{method} {path} before authentication"`.
   * The full-scope bearer must answer the route's own refusal (above). That leg is the in-test
     non-vacuity control: each row really is refused by route, so the tokenless 401 comes from
     the gate.

**Non-vacuity controls for commit 1** (local edits, reverted before committing):

* **(a) T1 bites on a wrong scope.** Change `src/product.rs:609` `S::RecordsRead` to `S::MetadataRead`.
  T1 fails at the scan row with this output (traced as follows):
  1. The credential lacks records.read but holds metadata.read.
  2. The gate passes, then the prefix check, then `check_read_quota`.
  3. `product_scan` gets the key, the registry misses, and it answers 404 `not_found`
     (`src/product/scan.rs:61-62`).

  ```
  assertion `left == right` failed: GET /v1/streams/opgrid:scan without streams.records.read
    left: (404, "not_found")
   right: (403, "missing_scope")
  ```
* **(b) T2 bites on entry-before-auth.** In `product_auth_gate`, temporarily replace
  `let principal = enforce_customer(state, headers)?;` with a version that answers
  `Ok(ProductAuthorization::Deployment)` when no `authorization` header is present. T2 fails at
  its first row:

  ```
  assertion `left == right` failed: POST /v1/streams/opgrid before authentication
    left: (404, "unknown_route")
   right: (401, "unauthorized")
  ```

  The grammar control is unaffected.

### Commit 2 (held for D1): the agreement test and the red test

**T3 `dst::dst_tests::security_operations::the_entry_dispatches_exactly_the_resolved_operations`.**
This is the reviewer's "red table test", run against the real second decision.

* **Rig.** `http_rig(mem())` (off mode), calling `crate::product::product_entry` directly with
  `ProductAuthorization::Deployment`, empty headers, query and body. There is no HTTP, so any
  method works.
* **Grid** (504 combinations; ~47 reach a handler, all refusing fast on the missing stream):
  * `SHAPES` (7):
    * `opgrid`
    * `opgrid/records`
    * `opgrid/consumers/c`
    * `opgrid/watches`
    * `opgrid/watches/w`
    * `opgrid/watches/w/keys/0011223344556677`
    * `opgrid/usage`
  * Verb slots: `None` plus each of `crate::product::VERBS`, the real list, so a new verb joins
    the grid.
  * Methods: GET, HEAD, POST, PUT, DELETE, PATCH, TRACE, CONNECT, and
    `Method::from_bytes(b"PURGE")`. OPTIONS is left out: it is answered before the gate and the
    entry.
* **Assertion.** `resolve(&route, verb, &method).is_none() == refused_by_route(response)`, with
  message `"{method} {path}: resolve names {resolved:?}"`.
  * `refused_by_route` reads the body only for 404/405. It is true iff `error.code` is
    `unknown_route` or `method_not_allowed`.
  * A dispatched handler's 404 is `not_found` (product `perr`, or raw `err_resp` for DELETE), so
    it counts as dispatched.
* **Structure.** The route is classified once per (shape, verb) with
  `let Ok(route) = classify_route(&path) else { panic!(..) }`. Nesting is 3 (for, for, for),
  under 45 lines.
* **Red status.** It cannot exist before commit 2, since `ProductOperation` does not exist yet.
  It is pinned by the controls in §7 C2.4.

**T4 `dst::dst_tests::security_operations::a_request_naming_no_operation_is_refused_by_route_not_by_scope`.**
This is the behaviour change: **red on the current tree.** It runs on `auth_rig`, with rows
`NO_OPERATION_WITHHELD: [(&str, &str, Scope, u16, &str); 6]` (method, path, withheld scope,
refusal):

| Request | Withheld scope | Refusal |
|---|---|---|
| `POST /v1/streams/opgrid` | MetadataRead | 404 `unknown_route` |
| `PUT /v1/streams/opgrid:seal` | Create | 404 `unknown_route` |
| `GET …/records:batch` | RecordsRead | 405 `method_not_allowed` |
| `GET …/consumers/c:pull` | ConsumersPull | 405 |
| `DELETE …/watches` | WatchesManage | 405 |
| `POST …/usage` | UsageRead | 405 |

Each row uses `scoped_call` with `Scope::ALL` minus the withheld scope, and asserts
`(status, code) == (refused, code)`, message `"{method} {path} without {withheld} names no operation"`.

**Exact red output**, with T4 applied to `c397e9e5` before the product edit. For the first row the
gate calls `required_scope(Collection, None, POST)`. That is not PUT, not DELETE, and has no
seal/scan verb, so it returns `MetadataRead` (:610-612). The principal lacks it, so it gets
`auth_failure_response(MissingScope)` → `perr(403, "missing_scope", …)`:

```
thread '…a_request_naming_no_operation_is_refused_by_route_not_by_scope' panicked at src/dst/tests/security_operations.rs:<line>:<col>:
assertion `left == right` failed: POST /v1/streams/opgrid without streams.metadata.read names no operation
  left: (403, "missing_scope")
 right: (404, "unknown_route")
```

**Green after commit 2.** The path is:

1. `resolve` returns `None`, so there is no scope check.
2. The prefix check passes (All), then project admission.
3. The POST memory gate lets an idle project through, and the empty body buffers.
4. In the entry, `reject_legacy_inputs` passes (only `authorization` and `prisma-encryption-key` are set).
5. Collection `(POST, None)` hits `_ =>` and answers 404 `unknown_route` (:1365-1371).

---

## 4. Edits, in commit order

### Commit 1: "Every dispatched product operation is pinned to the one scope the gate demands" (lands now)

| File | Change | Lines |
|---|---|---|
| `src/dst/tests/security_operations.rs` | new: header, imports, `PROJECT`, `CREDENTIAL`, `OPERATIONS`, `UNDISPATCHABLE`, `error_code`, `scoped_call`, T1, T2 | ~150, under the 1,000-line ceiling |
| `src/dst/dst_tests.rs` | `#[path = "tests/security_operations.rs"]` / `mod security_operations;` between `security_noninterference` and `security_policy` | 270 → 273 |
| `docs/quality/owners.json` | by-path row (§6) | +8 |
| `docs/refactor/test-inventory.json` | `scripts/test-inventory.py --write`: +2 entries | |

Production source is untouched. No `#[expect]` or `#[allow]` scope is touched. Test functions stay
under 100 lines, nesting under 4, and 5 arguments or fewer (`scoped_call` has 5). They use no
`_ =>` on domain enums.

### Commit 2: "The auth gate authorizes the product operation a request names, and a request naming none is refused by route" (HELD for D1)

**New `src/product/operation.rs`, ~110 lines.**

```rust
//! Which product operation a request names is decided once, here. The auth gate authorizes
//! that operation's §6.1 scope, and the entry dispatches exactly these operations
//! (`security_operations::the_entry_dispatches_exactly_the_resolved_operations`). A request
//! that names none has no scope to lack; the entry refuses it (404/405) after authentication.
//! A separate route × verb × method scope matrix used to answer with fallback arms for
//! requests nothing dispatched, so a new entry arm inherited whatever those arms guessed.
use axum::http::Method;

use super::ProductRoute;          // NOT crate::product::…: that is a reverse edge (§6)
use crate::tenant::Scope;

/// The suffixes the grammar splits off a final segment. Anything else after a colon stays
/// part of the collection name, because a colon is legal inside one.
pub(crate) const VERBS: [&str; 7] = ["batch", "long-poll", "sse", "pull", "settle", "seal", "scan"];

/// One variant per dispatch arm of `product_entry`, so a new operation cannot reach the gate
/// without `scope` deciding its scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProductOperation {
    Create, Metadata, Delete, Seal, Scan,
    Append, AppendBatch, Read, Subscribe,
    ConsumerPut, ConsumerGet, ConsumerDelete, ConsumerPull, ConsumerSettle,
    WatchList, WatchGet, WatchWait, Usage,
}

impl ProductOperation {
    /// `None` exactly when the entry answers 404/405 for this route, verb and method.
    pub(crate) fn resolve(route: &ProductRoute, verb: Option<&str>, method: &Method) -> Option<Self> {
        let get = *method == Method::GET;
        match route {
            ProductRoute::Collection { .. } => match (method.clone(), verb) {
                (Method::PUT, None) => Some(Self::Create),
                (Method::GET, None) => Some(Self::Metadata),
                (Method::DELETE, None) => Some(Self::Delete),
                (Method::POST, Some("seal")) => Some(Self::Seal),
                (Method::GET, Some("scan")) => Some(Self::Scan),
                _ => None,
            },
            ProductRoute::Records { .. } => match (method.clone(), verb) {
                (Method::POST, None) => Some(Self::Append),
                (Method::POST, Some("batch")) => Some(Self::AppendBatch),
                (Method::GET, None | Some("long-poll")) => Some(Self::Read),
                (Method::GET, Some("sse")) => Some(Self::Subscribe),
                _ => None,
            },
            ProductRoute::Consumer { .. } => match (method.clone(), verb) {
                (Method::PUT, None) => Some(Self::ConsumerPut),
                (Method::GET, None) => Some(Self::ConsumerGet),
                (Method::DELETE, None) => Some(Self::ConsumerDelete),
                (Method::POST, Some("pull")) => Some(Self::ConsumerPull),
                (Method::POST, Some("settle")) => Some(Self::ConsumerSettle),
                _ => None,
            },
            // The entry ignores a verb on these routes; so does the operation.
            ProductRoute::Watches { .. } => get.then_some(Self::WatchList),
            ProductRoute::Watch { .. } => get.then_some(Self::WatchGet),
            ProductRoute::WatchWait { .. } => get.then_some(Self::WatchWait),
            ProductRoute::Usage { .. } => get.then_some(Self::Usage),
        }
    }

    /// §6.1. Scopes that depend on the body (watch definitions on create, a DLQ link on a
    /// consumer) are checked where the body is parsed. `None` only for the watch wait: its
    /// handler verifies a §15 capability or the stream key, and the gate still applies the
    /// prefix grant.
    pub(crate) fn scope(self) -> Option<Scope> {
        Some(match self {
            Self::Create => Scope::Create,
            // Reading a consumer's config/positions is stream metadata; changing it is
            // configuration.
            Self::Metadata | Self::ConsumerGet | Self::WatchList | Self::WatchGet => Scope::MetadataRead,
            Self::Delete | Self::Seal => Scope::LifecycleManage,
            // `:scan` pages back decrypted record bodies: a bulk record read, so a
            // metadata-only credential that also holds the stream key cannot export records,
            // and revoking records.read cuts record access.
            Self::Scan | Self::Read | Self::Subscribe => Scope::RecordsRead,
            Self::Append | Self::AppendBatch => Scope::RecordsAppend,
            Self::ConsumerPut | Self::ConsumerDelete => Scope::ConsumersConfigure,
            Self::ConsumerPull => Scope::ConsumersPull,
            Self::ConsumerSettle => Scope::ConsumersSettle,
            Self::Usage => Scope::UsageRead,
            Self::WatchWait => return None,
        })
    }
}
```

Notes on `operation.rs`:

* **Lint profile.**
  * `resolve` is ~35 lines, nesting 2.
  * The outer match on the domain enum `ProductRoute` is exhaustive with no `_`. The inner `_`
    arms are over `(Method, Option<&str>)`, not domain enums.
  * `scope` is exhaustive with no wildcard. Arms are merged by body, for `match_same_arms`.
  * The inner matches mirror `product_entry`'s `(method.clone(), verb.as_deref())` idiom, so the
    two tables can be read side by side.
  * The `VERBS` array is formatted vertically by rustfmt (array width 66 > 60), 9 lines.
* **Dead code.** None: `resolve`, `scope` and `VERBS` all have production users.

**`src/product.rs` (ceiling 4,205; target about 4,138, a net change of −67):**

1. **:440-454 `strip_verb`.** Delete the local `const VERBS` (−9). The guard
   `Some((p, v)) if !v.contains('/') && VERBS.contains(&v) => (p, Some(v)),` stays
   byte-identical and resolves through the import below.
2. **:582-645.** Delete `required_scope`, its doc comment and the trailing blank line (−64). The
   `:scan` and consumer-read comments move into `scope()`.
3. **:943-946.** The `#[allow(clippy::result_large_err, reason = "transport boundary …")]` on
   `product_auth_gate` is **re-decided** as
   `#[expect(clippy::result_large_err, reason = "product_auth_gate; every refusal is the finished wire response the product wrapper returns unchanged, before it reads a body; a compact error would be rendered into that same response at its one call site")]`.
   * It has exactly two `;` and no `"`.
   * It follows the precedent in `src/product/internal.rs:21-24`, where `expect(result_large_err)`
     on `Result<_, Response>` is fulfilled.
   * Line count is unchanged (4 lines).
   * Why it is needed: the old allow ratchets `scope_lines` and `syntax_facts` on the gate. The
     gate edit adds `Some(scope)`, `operation.scope()` (method-call, method-call-site, path) and 3
     comment/code lines. Recount of the let-chain: 11 → 15 facts. That fails "accepted exception
     grew". A wrapper that only restores the fact count is forbidden (RUST-QUALITY "never
     introduce a wrapper solely to satisfy a lint"), so the remedy is the new reason.
4. **:961-970, the gate.** Comment +3 lines, code +1 line:

   ```rust
           // §9 order: the exact route parses FIRST (grammar errors are
           // not authentication outcomes), then authenticate, then
           // authorize the named operation's scope + prefix. A request
           // that names no operation has no scope to lack: the entry
           // refuses it (404/405) only after this authentication and
           // prefix check. No legacy fallback: in enforce the customer
           // token is the only product credential.
           let route = classify_route(path)?;
           let principal = enforce_customer(state, headers)?;
           let (_, verb) = strip_verb(path);
           if let Some(operation) = ProductOperation::resolve(&route, verb, method)
               && let Some(scope) = operation.scope()
               && let Err(e) = principal.require(scope)
           {
   ```

   Everything after it, including `require_stream(route_stream_name(&route))`, is unchanged.
5. **:4027-4034 area.** Add `mod operation;` and
   `pub(crate) use operation::{ProductOperation, VERBS};` (+2). The re-export is needed because
   the DST test names `crate::product::{ProductOperation, VERBS}`; the gate and `strip_verb`
   use them locally, so it is not an unused import.

**Ratcheted scopes (the import-alias trap).**

* **Nothing `product_entry` or `product_list` references is moved or renamed.** Their
  `#[expect(clippy::unwrap_used)]` fingerprints (`:1049-1052` and `:3883-3886`) keep every
  call-site and path fact: `classify_route`, `strip_verb`, `canonical_name`, `perr`,
  `enforce_customer`, `auth_failure_response`, `project_admission` and `strict_query` all stay in
  `product.rs`. **`strip_verb` and `classify_route` must not move** in this commit.
* **The new import aliases only `ProductOperation` and `VERBS`.** No ratcheted scope mentions
  either name.
* **The only exception scope touched is `product_auth_gate`,** and its reason is re-decided.
  Metrics are position-free (counts and fingerprints), so the −73-line shift above the other
  scopes changes nothing.
* **No other ceilinged file changes.** `http.rs` is untouched.

**`src/dst/tests/security_operations.rs`.** Add:

* `SHAPES`
* T3 and its helper `refused_by_route`
* `NO_OPERATION_WITHHELD` and T4
* imports: `axum::http::{HeaderMap, Method}`, `axum::response::Response`, `bytes::Bytes`,
  `crate::product::{ProductAuthorization, ProductOperation, VERBS, classify_route, product_entry}`,
  `super::fixture_http::http_rig`, `super::fixture_storage::mem`

This takes it to ~260 lines.

**Docs.** `docs/refactor/WIRE-MATRIX.md`:

* `:77`: the verb list now lives at `VERBS` in `src/product/operation.rs`, read by `strip_verb`.
* `:79`: "enforce: customer JWT + the named operation's scope (`ProductOperation::scope`,
  `src/product/operation.rs`) + prefix; a request naming no operation needs no scope and is
  refused 404/405 by the entry after authentication". This replaces the `required_scope` /
  `src/product.rs:584-640` reference.
* `:184-185`: add "(after authentication and the prefix check)".

---

## 5. Mutation analysis

**Neither commit selects the mutation leg.**

* **Commit 1** changes `src/dst/**` only.
* **Commit 2** changes `src/product.rs`, `src/product/operation.rs` and `src/dst/**`.
* No path is under `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:22-31`).
  `src/product/operation.rs` does not start with `src/product_cursor`.
* No path is registered in `scripts/quality/mutation_owners.py`.
* `http.rs` is not touched.
* Expected `plan.json`: `"mutants": false`, `"mutation_source_files": []`, `"miri": false`,
  `"properties_fuzz": false`.

For the record, and if Søren later wants `src/product/operation.rs` registered as
`owner('product_operation', 'src/product/operation.rs', 'dst_tests::security_operations::')`,
every viable cargo-mutants 27.1 mutant is killed:

| Mutant | Killer |
|---|---|
| `resolve -> Option<ProductOperation>` replaced with `None` | T3: the first dispatched combination, `GET opgrid`: `left: true, right: false`. T1 would also fail: every "without" leg reaches a handler. |
| Each deleted `resolve` inner-match arm (arm deletion is possible because each inner match has `_ =>`; 14 arms) | T3 at that combination (resolve None, entry dispatched), and T1's without-leg for that row |
| `==` → `!=` in `let get = *method == Method::GET` | T3: GET on watches is now `None` but dispatched; POST on watches is now `Some` but refused |
| `scope -> Option<Scope>` replaced with `None` | T1: every `Some` row's without-leg answers the handler's code instead of `(403, "missing_scope")` |
| Scope arm swaps | Not generated: `scope()` has no wildcard, so cargo-mutants does not delete its arms |
| `Some(Default::default())` | Not generated: `ProductOperation` and `Scope` implement no `Default` |
| `VERBS` | Not mutated: a const |
| The gate's `&&` | Not mutated: let-chain `&&` is syntax, not a binary expression |

No equivalent mutants. No timing loops: every request refuses on a missing stream without
waiting (no key → 400 before any wait; missing descriptor → 404/403 before any wait).

---

## 6. Ledgers

**Commit 1:**

* **`docs/quality/owners.json`.** A by-path row next to the other `src/dst/dst_tests.rs` rows
  (after `quota_read_volume`/`billing_readiness`, ~:2289-2297):

  ```json
  {"category": "by-path-module", "count": 1, "owner": "crate::security_operations",
   "path": "src/dst/dst_tests.rs",
   "reason": "Product-operation authorization scenarios; real HTTP requests against an enforce-mode rig pin the scope each dispatched operation demands and the authentication that precedes a route refusal; compiled and executed with DST.",
   "syntax": "path = \"tests/security_operations.rs\""}
  ```
* **`docs/refactor/test-inventory.json`.** Run `python3 scripts/test-inventory.py --write`,
  which adds 2 entries with `scenarios: []`, then `--check`.
* **Unchanged:**
  * `src/dst/tests/README.md`: the `security_*` glob in the "Multitenancy and authorization" row covers the module.
  * `docs/refactor/test-scenario-map.json` / `scenario-dispositions.json`: nothing renamed or deleted.
  * `docs/refactor/review-mechanisms.json`: no pinned test touched.

**Commit 2:**

* **`docs/refactor/test-inventory.json`.** `--write` adds 2 entries (T3, T4). T1 and T2 hashes
  are unchanged. If a shared helper edit changes T1/T2 bytes, `function_sha256` hashes the test
  body only; confirm `git diff` shows exactly 2 added entries.
* **`docs/quality/source-allowances.json`.** Remove the now-stale row at :1585-1591
  (`crate::product_auth_gate`, `allow (clippy :: result_large_err , reason = "transport boundary …")`).
  Use `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune` or a hand
  delete of that one object. `legacy-source.json` stays frozen.
* **`docs/refactor/architecture-policy.json`: no change.**
  * `src/product/operation.rs` has no `crate::http`/`crate::product` reverse edge, because it
    uses `super::ProductRoute`.
  * It needs no transport rationale: `usage.rs` and `internal.rs` are precedent.
  * The `product_entry` budget of 318 is untouched.
* **`docs/refactor/WIRE-MATRIX.md`:** as in §4.
* **Unchanged:** `scripts/quality/mutation_owners.py` (not a critical prefix; optional row in §5),
  `scripts/mt-audit-baseline.txt` (no audited pattern touched), `owners.json`.

---

## 7. Controls

These are for the implementer; this planner ran none of them.

**C1, commit 1:**

1. **Run the new tests.**
   `scripts/test-leg.sh target/quality/ops-c1.log --exact dst::dst_tests::security_operations::every_dispatched_operation_demands_exactly_its_scope --exact dst::dst_tests::security_operations::a_request_naming_no_operation_authenticates_before_its_404_or_405 -- --locked --lib security_operations::`
   → `test result: ok. 2 passed`, and tests_ran OK.
2. **Non-vacuity controls (a) and (b) from §3.** Each fails with the exact assertion output
   shown there. Revert, then rerun 1.
3. **Existing edge tests stay green.**
   `cargo test --locked --lib security_modes:: security_routes:: security_audit:: product::tests::`
   → all ok.
4. **Quality.**
   `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl`
   then `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl` → no
   `unregistered source occurrence` for `path = "tests/security_operations.rs"` and no growth
   lines.
5. **Inventory.** `python3 scripts/test-inventory.py --check` → `test-inventory: OK (523 tests, …)`.
6. **Mutation plan.**
   `cargo build --locked -p streams-quality-syntax && python3 scripts/quality/verification_plan.py --out target/quality-plan`
   → `"mutants": false`.
7. **Full gate.** `scripts/quality.sh` → `QUALITY_OK`.

**C2, commit 2 (only after D1 = yes):**

1. **Red first.** Add T4 only, with no product edit.
   `cargo test --locked --lib security_operations::a_request_naming_no_operation_is_refused_by_route_not_by_scope`
   → FAILS with exactly the output in §3.
2. **Apply the product edits and T3.** Then:
   `scripts/test-leg.sh target/quality/ops-c2.log --exact dst::dst_tests::security_operations::every_dispatched_operation_demands_exactly_its_scope --exact dst::dst_tests::security_operations::a_request_naming_no_operation_authenticates_before_its_404_or_405 --exact dst::dst_tests::security_operations::the_entry_dispatches_exactly_the_resolved_operations --exact dst::dst_tests::security_operations::a_request_naming_no_operation_is_refused_by_route_not_by_scope -- --locked --lib security_operations::`
   → 4 passed. T1 and T2 are **unchanged and still green**: that is the pinning evidence that
   every dispatched operation's scope and the auth-before-404/405 order survived.
3. **Pre-delete cross-check (scratch only, not committed).** Before deleting `required_scope`,
   run a throwaway `#[test]` over T3's grid. It asserts that for every dispatched combination,
   `ProductOperation::resolve(..).and_then(ProductOperation::scope) == required_scope(..)` →
   ok. Also print the undispatchable combinations where `required_scope` is `Some`: that list is
   exactly D1's blast radius. Delete the scratch test.
4. **T3 non-vacuity controls** (local edits, reverted):
   * **(i)** Delete the `(Method::GET, Some("scan")) => Some(Self::Scan)` arm. T3 fails:
     ```
     assertion `left == right` failed: GET opgrid:scan: resolve names None
       left: true
      right: false
     ```
     T1 also fails at the scan row with `left: (404, "not_found")`.
   * **(ii)** Add a bogus `(Method::POST, None) => Some(Self::Metadata)` on Collection. T3 fails
     at `POST opgrid: resolve names Some(Metadata)`, with `left: false, right: true`.
5. **Other suites.**
   `cargo test --locked --lib security_modes:: security_routes:: security_audit:: product::` and
   `cargo test --locked --lib review_security` → all ok.
6. **Ratchet.** Clippy JSON as in C1.4, then
   `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl` → no
   `accepted exception grew`, no `file growth: src/product.rs`, and no
   `exception needs owner; invariant; alternative`. The only prune is the one stale
   `product_auth_gate` allow row (§6). Also check `wc -l src/product.rs` ≤ 4205 (expected ~4138).
7. **Architecture.** `python3 scripts/architecture-gate.py --check` → OK. In particular there is
   no `reverse dependency growth: src/product/operation.rs`.
8. **Docs and the rest.**
   `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
   → ok. Then C1.5-C1.7.

---

## 8. Out of scope

* **Step 2.** The gate returning the operation, the entry dispatching on it, and deleting the
  second `classify_route` (:1110), the `canonical_name(strip_verb(..))` reparse (:1341),
  `ProductAuthorization::Preflight` and its `unreachable!` (:1323-1325). This is not trivially
  safe:
  * It rewrites `product_entry`, which sits under `#[expect(unwrap_used)]` fingerprints and the
    `too_many_lines`/`excessive_nesting` ratchets, plus architecture budget 318.
  * It changes the gate's signature at `src/http.rs:1865`. `src/http` is a critical prefix, so
    that would select a mutation leg.
  * With T3 in place, step 2 turns into a mechanical follow-up.
* **Handlers restating consumer scopes as literals** (:3117, :3169, :3721, `consumer_pull.rs:52`,
  `consumer/deletion.rs:24`). Step 2 can pass `operation.scope()`. T1's "only" leg already pins
  their agreement with the gate.
* **The verb-ignoring quirk on watches/watch/watch-wait/usage** (`GET …/watches:scan` lists
  watches). It is preserved exactly. Turning it into 404/405 would be a separate edge change.
* **Watch-wait by bearer carries no scope** (existing §15 design; the key or capability is the
  authority). Unchanged.
* **Stale doc comments nearby, which step 2 or a cleanup can take:**
  * `shadow_observe_request` (:559-561) still says "the auth gate itself runs twice by design
    (wrapper + entry defense-in-depth)". The entry no longer calls the gate.
  * The misplaced "Ok(None)/Ok(Some(p))/Err" paragraph above `project_admission` (:752-755)
    describes the gate's old return type.

---

## 9. Decisions for Søren

* **D1 (blocks commit 2).** In enforce mode, should a request that names no product operation
  skip the scope check?
  * **What "names no operation" means:** the entry answers it 404 `unknown_route` / 405
    `method_not_allowed`. Examples: `POST /v1/streams/x`, `PATCH …/records`,
    `GET …/records:batch`, `DELETE …/watches`, `POST …/usage`, `GET …/consumers/c:pull`,
    `PUT x:seal`.
  * **What changes:** it would still be authenticated (401) and prefix-checked (403
    `prefix_denied`). A verified principal lacking the old fallback scope then gets 404/405
    instead of **403 `missing_scope`**, and that refusal stops being journaled as a security
    denial.
  * **What does not change:** every dispatched operation's status, the ordering above, and
    off/shadow modes.
  * **Recommendation: yes.** The 403 names a scope for an operation that does not exist. Deleting
    it is what makes `required_scope`'s fallback arms (the fail-open hazard) go away.
  * **If no:** do not land commit 2, and do not build the status-preserving variant: it keeps the
    whole fallback table as a third encoding. Instead land a test-only guard. It would be an
    off-mode grid like T3, but its oracle would be commit 1's `OPERATIONS` table plus
    "GET on the verb-less routes", with the verb list hoisted where `product.rs` can spare the
    lines. A new entry arm would then force a row, and T1 would demand that row's scope.

---

## Skeptic corrections (C1..C10)

Everything below was checked read-only on `c397e9e5`. `git rev-parse origin/slate` = `c397e9e5`, and
`src/product.rs` is 4,205 lines both there and at `5d9d517f`, so the ceiling of 4,205 holds. These
claims were confirmed and need no change:

* the quoted `required_scope` lines (582-644), the gate (943-977), `strip_verb`/`VERBS` (440-454) and
  `classify_route` (462);
* the complete use-site lists (`required_scope` has one caller, :968; `product_auth_gate` has one,
  `src/http.rs:1865`; `ProductAuthorization::Preflight` is used at :955/:1217/:1323; `VERBS` is local
  to `strip_verb`);
* the 47-dispatched / 504-combination grid count;
* T4's red trace and its green trace;
* control (a) of T1, including its output;
* the stale allowance row (`docs/quality/source-allowances.json:1585-1591`), plus the
  `source_rules.py` finding that a changed reason is a new identity (so `exception_growth` skips it)
  and that the stale row must be pruned (`source_gate.py:60-62`);
* the net −67 lines in `product.rs`;
* no reverse edge: `architecture-gate.py:34-49` matches `super::(http|product)\b` case-sensitively,
  so `super::ProductRoute` is not an edge;
* no mutation leg: `src/product.rs` and `src/product/operation.rs` match no `CRITICAL_PREFIXES`
  entry (`verification_plan.py:22-31`) and no `mutation_owners.py` row;
* inventory 521 → 523;
* the README glob `security_*` (`src/dst/tests/README.md:16`).

**C1 (blocking for commit 1): T2's grammar control is wrong, so T2 is red on the current tree.**
Tokenless `GET /v1/streams/opgrid/watches/w/extra` does NOT reach the "watch names are one path
segment" 404:

1. `split_subresource("opgrid/watches/w/extra")` (`src/application/names.rs:75-101`) matches no
   shape. `take=4` needs n>4, `seg[2]="w"` is not `watches`, and `seg[3]="extra"` is none of
   records/watches/usage. It returns `None`.
2. `classify_route` therefore returns `Ok(Collection { name: "opgrid/watches/w/extra" })`.
   `ProductStreamName::try_from` accepts it: the final segment is not reserved and
   `split_subresource` is `None` (`names.rs:37-58`).
3. The gate then authenticates and answers **(401, "unauthorized")**, not (404, "unknown_route").

In fact the `wrest.contains('/')` 404 at `product.rs:510-517` is unreachable through
`split_subresource`. Replace the control with tokenless `GET /v1/streams/opgrid/consumers/c:x`:
`strip_verb` keeps `:x` (not a verb), `split_subresource` gives `("opgrid", "consumers/c:x")`, and
`valid_consumer_name` refuses the ':' (`names.rs:115-126`). That yields **(400,
"invalid_consumer_name")** from `classify_route` (:474-481) before `enforce_customer`. Control (b)
still leaves the new control unaffected, as the plan says. (`WIRE-MATRIX.md:187` "bad watch URL
shapes → 404" overstates the same thing. Note it; it is not this item's edit.)

**C2: T1's watch-wait row is vacuous as written.** "Every scope except the row's" with `None` means
`Scope::ALL`, so the "must not answer `missing_scope`" assertion can never fail, even if the gate
began demanding a scope for the watch wait. Hold **no** scopes for the `None` row (`scopes = ""`).
`ScopeSet::parse("")` = `EMPTY`, and `verify_customer` does not reject an empty `scope` claim
(`src/auth.rs:567-585`). The expected answer is still 403 `watch_unauthorized` from the handler.

**C3: the "only" leg does not pin handler/gate agreement for consumer DELETE.**
`product_consumer_delete` answers 400 `missing_consumer_version` (`src/product.rs:3653-3664`)
before `application::consumer::delete` reaches `access.require(.., ConsumersConfigure)`
(`src/application/consumer/deletion.rs:24`). So §1.2's "Commit 1's test pins that agreement" and
§8's "T1's only leg already pins their agreement" are false for deletion.rs:24. The fix is to send
`("prisma-consumer-version", &crate::product::consumer_version_token(&[0; 16], 1))` on T1's requests.
It is re-exported at `product.rs:3008`, and `parse_consumer_version` accepts any 24-byte token
(`consumer.rs:354-366`). The other handlers ignore the header, and `reject_legacy_inputs` does not
list it (`product.rs:125-136`). DELETE then reaches the require and answers 404 on the missing
stream.

**C4: `Scope` has no `Display`** (`src/tenant.rs:486-538`; only `Debug` and `as_str`). The messages
`"{method} {path} without {s}"` (T1) and `"… without {withheld} names no operation"` (T4) do not
compile as written. Use `s.as_str()`. The expected red outputs already print `streams.records.read`
/ `streams.metadata.read`, which is `as_str()` spelling, not `Debug`.

**C5 (missed documented contract and ledger for commit 2 / D1).** `docs/MULTITENANCY.md` §6.1
(lines 485-499) is the normative scope matrix. Two of its rows are honored today **only** by the
fallback arms commit 2 deletes:

* **"HEAD/read/scan/SSE records → `streams.records.read`" (:488).** The entry dispatches no HEAD.
  The gate still demands records.read for HEAD because of `read = GET || HEAD` (:594) and the
  Records non-POST arm (:617-619).
* **"Create/delete watch → `streams.watches.manage`" (:497).** The only enforcement for a
  non-GET watches/watch request is the fallback at :637-639. Watch creation via the create body
  is the separate check at :1434.

After commit 2, a HEAD on records, or a DELETE on a watch, from a principal lacking those scopes
gets 405 instead of 403. D1 must name these two §6.1 rows explicitly: they are spec text, not an
accident of the matrix. Commit 2 must also edit `docs/MULTITENANCY.md` §6.1 so that HEAD and watch
delete are marked not served on the product surface (refused by route after authentication), or
D1 must choose to keep them. This is in addition to the `WIRE-MATRIX.md` edits in §4.

**C6: D1 has a second observable consequence the plan omits.** Once the scope check is skipped, an
undispatchable POST/PUT from a principal lacking the old fallback scope passes several steps
before its 404/405 (`src/http.rs:1869-1911`):

* project admission, where it holds an inflight slot;
* the POST memory gate;
* the reserved-name guard;
* **body buffering, up to `max_request_body_bytes`.**

Today it is refused 403 before any of that. `docs/MULTITENANCY.md` §9 (:699-712) orders "authorize
scope" before "enforce request body limits". The widening is bounded: a principal that holds the
fallback scope already buffers today. It still belongs in D1's "what changes" list. Offer the
alternative too: step 2's gate returning `Option<ProductOperation>` lets the wrapper skip
buffering when it is `None`.

**C7: §2's claim about `security_routes.rs:493-522` is overstated.** Its 15 rows are all dispatched
operations in legacy-token mode (`http_rig_auth`). None of them can answer 404/405, so it pins
"tokenless is 401", not "auth before 404/405". T2 (with C1 applied) is the first real pin of that
order. Say so in the commit message rather than citing :493-522 as existing coverage.

**C8: control commands C1.3 and C2.5 are not runnable as written.** `cargo test --locked --lib
security_modes:: security_routes:: security_audit:: product::tests::` passes four positional
TESTNAMEs, and cargo accepts one. Use `cargo test --locked --lib -- security_modes:: security_routes::
security_audit:: product::tests::`, since libtest takes several filters after `--`, or run one
`scripts/test-leg.sh` per filter. The same applies to C2.5 (`… security_audit:: product::`).

**C9: T3 has a residual gap to state, not to fix now.** T3 checks only
`resolve(..).is_none() == refused_by_route(..)`. A future entry arm whose `resolve` arm names the
**wrong** variant would pass T3; for example, `(POST, Some("compact")) => Some(Self::Read)` puts a
mutation under records.read. Only a new T1 `OPERATIONS` row would catch it. Put that in the
`OPERATIONS` doc comment ("a new `ProductOperation` variant needs a row here") and in the commit
message. Step 2, where the entry dispatches on the operation, is what closes it.

**C10 (wording): `publish_grants` requires only a non-decreasing `feed_version`**
(`src/auth/publication.rs:449-451`, `<` regresses). The credential high-water check refuses a lower
`grant_version` (:173-175). Strictly increasing versions, as the plan uses, satisfy both, so there
is no change to the tests, only to the sentence under §3.

### Verdict: **ready-with-corrections**

Commit 1 lands after C1 (otherwise T2 fails on the current tree), C2, C3 and C4. Commit 2 stays held
for D1. D1 must be restated with C5 (the two §6.1 rows and the `MULTITENANCY.md` edit) and C6 (body
buffering and admission) before Søren answers. The approach needs no rework: making the step-1
refactor status-neutral is impossible, so holding it for a decision is right, and `product.rs`
shrinks.
