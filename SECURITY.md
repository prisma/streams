# Security policy

Report suspected vulnerabilities through Prisma's private
security/escalation channel; do not put tenant data, credentials,
exploit details, or bucket coordinates in a public issue.

## Identity and authorization model (implemented)

The multi-tenant model described in [docs/MULTITENANCY.md](./docs/MULTITENANCY.md)
is implemented and enforced — it is not roadmap:

- **Customer credentials** are short-lived JWTs, JWKS-verified against
  the Control-Plane publisher (`STREAMS_AUTH_MODE=enforce`), carrying
  scoped grants intersected with the credential's feed-published grant
  at the same `grant_version` — neither a stale cache nor a widened
  token can grant beyond the other.
- **Policies, grants, and signing keys arrive as versioned feeds** with
  monotonic-publication rules: high-water marks survive snapshot
  omissions, so a stale replay cannot restore a previous owner,
  reactivate a revoked credential, resurrect removed scopes, or
  reintroduce a retired signing key; a workspace change is structurally
  bound to an `ownership_version` increment.
- **Long-lived subscriptions re-prove authorization for their whole
  life**: every SSE connection carries a lease checked at the response-
  body boundary on generation change and deadline, terminates at token
  or credential expiry, and can never start on authorization that was
  already invalidated. Fleet-internal (workload-JWT) subscriptions are
  bounded by their token's expiry too.
- **Fleet identity** is short-lived workload JWTs under the release
  posture; the static bridge token is a named legacy posture that boot
  validation refuses there.

## Release checks

Every release runs `cargo deny check` against the locked dependency graph
(`scripts/release-gate.sh`). Unknown registries and Git sources, wildcard
requirements, OpenSSL/native-TLS dependencies, unapproved licenses, known
vulnerabilities, and yanked crates fail the gate. Duplicate major versions
are reported for review.

2026-07-21 first run: upgraded `object_store` 0.14.0→0.14.1 /
`quick-xml` 0.40.1→0.41.0 (two remotely reachable XML CPU/memory DoS
advisories, RUSTSEC-2026-0194/0195) and `spin` 0.10.0→0.10.1 (yanked).

## Audited RustSec exceptions

The exact exceptions live in `deny.toml`; each must be reviewed on every
SlateDB/foyer dependency update.

| advisory | dependency path | why it is not currently exploitable | removal condition |
|---|---|---|---|
| RUSTSEC-2025-0141 | SlateDB 0.14.1 → foyer → bincode 1.3.3 | unmaintained status, not a reported vulnerability | upgrade SlateDB/foyer when it removes bincode 1.x |
| RUSTSEC-2024-0436 | SlateDB 0.14.1 → foyer → paste | unmaintained build-time macro crate | same |

## Surface posture

Two independent factors: authentication gates each surface, and the
per-stream encryption key gates the data — a leaked token cannot
decrypt. Keys never persist server-side; storage and backups are
ciphertext.

- `/v1/streams/*` (product API): customer credentials only.
- `/v1/stream/*` (pinned Durable Streams surface) and `/v1/internal/*`:
  INTERNAL-ONLY under shared-cell enforcement — fleet identity per
  exact operation, never customer tokens.
- `/operator` and `/v1/debug/*`: bearer-gated in every non-off mode
  (SR-5); the operator dashboard serves operational metadata only —
  never stream names, tenant identifiers, tokens, keys, or signed URLs.

Never commit tokens, keys, or presigned URLs — deploy tooling keeps
them in a local scratch directory outside the repo.
