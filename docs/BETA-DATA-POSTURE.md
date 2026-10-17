# Prisma Streams public beta: data posture

This page states what the public beta does with a customer's data, as
the code at the time of writing does it. It is the engineering half of
the beta's terms. **The legal statements a customer agrees to are to be
reviewed by Prisma legal**; the sections that need that review are
marked. Where this page and a design document disagree, this page
describes what runs, and the design document describes a target.

Limits, error answers and retry rules are in
[BETA-REFERENCE.md](BETA-REFERENCE.md).

## Where data lives

- One beta cell is one server on Prisma Compute and one Tigris bucket
  (`SLATE_S3_BUCKET`) in the cell's Compute project. Every stream's
  records, the registry of stream descriptors, the usage ledgers and the
  usage rollup are objects under one prefix (`PATH_PREFIX`) of that
  bucket. Source: `RUNBOOK.md` §11 ("Storage layout", "Usage
  databases").
- Projects, credentials and the allow-list are kept in a separate
  control bucket, and the access policy the cell reads in a separate
  feeds bucket (`deploy/cell-admin/README.md`, "The state in buckets").
- A bucket takes its Compute project's region when it is created, and a
  project cannot be moved to another region (`docs/OPS-RELEASE.md`,
  bucket placement rule). **The production region is not chosen yet**
  (NEXT-WORK "Public beta" item 8); it will be named here before the
  first customer is admitted. There is no multi-region copy.
- *To be reviewed by Prisma legal:* whether this is a data-residency
  commitment, and which sub-processors (Prisma Compute, Tigris) are
  named.

## What is encrypted, and with which keys

- **Record content.** The client sends its stream key with every
  request that reads or writes records (`Prisma-Encryption-Key`). The
  server derives per-routing-key subkeys from it and stores records in
  pages encrypted with AES-256-GCM-SIV (`src/crypto_page.rs`,
  `docs/crypto-page-v6.md`, `SPEC.md` §3.7). The server never writes the
  stream key to storage. It keeps a stream's key in memory for at most
  15 minutes after the last request that carried it (`KeyCache`,
  `KEY_TTL` in `src/history.rs`), and then forgets it.
- **What is not encrypted.** Each page's header is stored in clear: its
  routing key, first offset, record count, timestamp and key version.
  The registry keeps each stream's descriptor as plain JSON: its name,
  project, creation time, expiry, fork links and a fingerprint derived
  from the key (`PersistedDescriptor` in `src/registry.rs`;
  `StreamKey::fingerprint` in `src/crypto.rs`, which does not reveal the
  key). Object names and sizes are visible in the bucket. Routing keys
  are metadata, not secrets (`SPEC.md` C10).
- **Usage data.** Metering records (`_usage`) name the project and the
  stream; they, the operations journals and the audit journal are
  encrypted under the cell's own key, `USAGE_STREAM_KEY`, which the
  operator holds (`src/billing.rs`, `RUNBOOK.md` §12). The usage rollup
  and the read spool are databases in the same bucket that hold metering
  figures by project and stream; they are not encrypted under a customer
  key.
- **Catalog cursors.** `STREAMS_CURSOR_KEY` signs the cursors of stream
  listings (`src/product.rs`, `product_list`). It encrypts nothing.
  Record cursors are signed with a key derived from the stream key
  (`src/product_cursor.rs`).

## What the operator can and cannot read

- With the bucket's keys alone, the operator cannot read record content:
  it is encrypted under keys derived from the customer's stream key,
  which is not stored. The operator can see stream names, routing keys,
  sizes, timestamps and usage.
- The server decrypts records while it serves a request that carries the
  key, and JSON and raw reads return plaintext over TLS. So content is
  unreadable to the operator **at rest**, not end to end: someone who
  controls the running server can see keys and content while requests
  are served. A client that wants content no server can read must
  encrypt it before it appends.
- The operator's own credentials (`AUTH_TOKEN`, the `/operator` and
  `/v1/debug` surfaces) show operational metadata and per-stream usage
  counters, never record content: they carry no stream key
  (`docs/refactor/WIRE-MATRIX.md` §3).
- The issuer keeps an audit of management actions with the Prisma user
  id and e-mail address of the person who acted
  (`deploy/issuer/audit.ts`). It is meant to be kept 13 months; the rule
  that deletes older entries is not set yet (NEXT-WORK item 3). *To be reviewed by Prisma legal:* the
  privacy notice for these.

## Retention

- Records are kept until their stream is deleted or expires. There is no
  per-record maximum age yet. A stream may be created with an expiry
  (`expiry: {idle}` or `{at}`), after which it ends as a deleted stream
  does (`docs/refactor/WIRE-MATRIX.md` §2.2). Per-stream retention is
  planned with the next storage layout's stage 3b (NEXT-WORK item 2,
  design B).
- A project stores at most its stored-bytes limit, 10 GiB by default
  ([BETA-REFERENCE.md](BETA-REFERENCE.md)).

## Erasure, as built

- **At once:** a delete writes a tombstone in the registry
  (`src/application/creation/deletion.rs`). From then on the stream
  answers `404` or `410`, and nothing reads its records through the API.
  The server holds no stored copy of the stream key, so the remaining
  bytes are ciphertext nobody at Prisma can decrypt from storage. This
  is crypto-erasure from the operator's side. A customer who keeps the
  stream key could still decrypt a copy of those objects; destroying the
  key completes the erasure.
- **Later:** the encrypted objects, the descriptor's tombstone and the
  page headers stay in the bucket. No code deletes a deleted stream's
  rows yet (`docs/READINESS.md`, "Physical reclamation of hard-deleted
  incarnations (TLA-019-F3)"); reclamation (cost lever E11) is planned
  with the next storage layout's stage 3b, is not built, and whether it
  must exist before the beta opens is undecided (NEXT-WORK item 2). Until it exists, the stream's name,
  routing keys and sizes stay visible to the operator (`SPEC.md` G8).
- Usage records about a deleted stream stay in the usage ledgers and the
  rollup: they are Prisma's metering record.
- *To be reviewed by Prisma legal:* whether this meets a deletion request
  under GDPR or a DPA. If the beta accepts personal data under a DPA,
  "no physical erasure" may block it.

## Availability and backups

- **One instance.** Each cell is one server with no spare. A deploy, a
  restart or a crash stops every project on the cell until it is back.
- **No SLA.** There is no availability or latency commitment during the
  beta. *To be reviewed by Prisma legal:* the disclaimer wording.
- **Backups:** none beyond the object store's own durability until the
  durability posture (NEXT-WORK "Public beta" item 18) is decided. The
  backup, point-in-time recovery and restore design in `OPERATIONS.md`
  §2 is not built (`RUNBOOK.md` §11). An operator error or a defect that
  deletes or corrupts objects cannot be undone today.
- No data reset is planned for the beta: the owner chose to open it on
  the next storage layout, layout 6 (NEXT-WORK "What the public beta
  is"), so no layout change forces one. Layout 6 is not built yet; the
  code at this tip writes layout 5 (`LAYOUT_VERSION` in
  `src/registry.rs`).
