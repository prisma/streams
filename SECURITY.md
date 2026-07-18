# Security policy and dependency exceptions

Prisma Streams is not GA on this branch. Report suspected vulnerabilities
through Prisma's private security/escalation channel; do not put tenant data,
credentials, exploit details, or recovery-provider coordinates in a public
issue. Operators must rotate affected JWT principals, object-store credentials,
and customer-directed stream keys according to the incident scope, preserve
audit/recovery evidence, and follow `RUNBOOK.md` and `OPERATIONS.md`.

## Release checks

Every release runs `cargo deny check` against the locked dependency graph for
the production musl target and the development macOS target. Unknown registries
and Git sources, wildcard requirements, OpenSSL/native-TLS dependencies,
unapproved licenses, known vulnerabilities, and yanked crates fail CI. Duplicate
major versions are reported for review. Dependabot opens weekly Cargo and
GitHub Actions updates; updates do not bypass the same gate.

`streams-at-rest-check` and `scripts/ci-at-rest-inspection.sh` provide the
first-party primary/recovery payload-and-key leakage control. They do not replace
the independent encryption-envelope review required by
`AWS-QUALITY-GATE.md`.

## Audited RustSec exceptions

The exact exceptions live in `deny.toml`; each is narrow and must be reviewed
on every SlateDB/foyer or JWT dependency update and at least monthly while this
branch is a release candidate.

| advisory | dependency path | why it is not currently exploitable | removal condition |
|---|---|---|---|
| RUSTSEC-2025-0141 | SlateDB 0.14.1 → foyer 0.22.3 → bincode 1.3.3 | unmaintained status, not a reported vulnerability; no safe compatible foyer replacement | upgrade SlateDB/foyer when it removes bincode 1.x |
| RUSTSEC-2024-0436 | SlateDB 0.14.1 → foyer-memory 0.22.3 → paste 1.0.15 | unmaintained macro dependency; no runtime parser or network surface | upgrade foyer when it replaces paste |
| RUSTSEC-2023-0071 | jsonwebtoken 10.4.0 → rsa 0.9.10 | the timing oracle concerns RSA private-key operations; the service loads public JWKS material and performs verification only | remove when jsonwebtoken's RustCrypto backend adopts a fixed RSA crate, or remove RS256 support |

The initial gate found reachable `quick-xml 0.40.1` CPU and memory denial-of-
service advisories through the S3 client. The lockfile is upgraded to
`object_store 0.14.1` / `quick-xml 0.41.0`. It also found a yanked `spin 0.10.0`
through `crc-fast 1.10.0`; the compatible graph is pinned to `crc-fast 1.7.1`,
which removes that dependency. These findings are fixed rather than excepted.

## Key and format boundaries

Customer root keys arrive on requests, are never serialized, and zeroize when
`StreamKey` values drop. Shard frames bind authenticated headers and stream
identity. History envelope 2 binds its derived key and AAD to the 32-byte
tenant/name/incarnation storage identity. Existing cells must follow the
read-first writer-1 → writer-2 procedure in `STORAGE-MIGRATIONS.md`; a binary
without the dual reader is not a rollback target after the first v2 block.

Control metadata, offsets, timestamps, and routing keys are not confidential.
Tenant payloads and root keys are. Customers must not place secrets in routing
keys, stream names, or other documented metadata fields.
