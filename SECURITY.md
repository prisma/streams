# Security policy and dependency exceptions

Prisma Streams (slate) is a pilot, not GA. Report suspected vulnerabilities
through Prisma's private security/escalation channel; do not put tenant
data, credentials, exploit details, or bucket coordinates in a public issue.

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

## Operator-facing posture

Two independent factors: the bearer token gates the API, the per-stream
encryption key gates the data — a leaked token cannot decrypt. Keys never
persist server-side; storage and backups are ciphertext. The unsecured
`/operator` dashboard serves operational metadata only (enforced in
`src/operator.rs`): never stream names, tenant identifiers, tokens, keys,
or signed URLs. Never commit tokens, keys, or presigned URLs — deploy
tooling keeps them in a local scratch directory outside the repo.

The multi-tenant identity/authn/z roadmap (JWKS-verified scoped tokens,
revocation, per-customer admission) is deferred work tracked in
[AWS-readyness.md §4](./AWS-readyness.md).
