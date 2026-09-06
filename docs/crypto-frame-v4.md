# R01: frame v4/v5 encryption and rollout

New writes use RustCrypto `aes-gcm-siv` 0.11.1 implementing RFC 8452 AES-256-GCM-SIV. The existing AES-256-GCM reader remains only for retained frame versions 2 and 3. No legacy version is reinterpreted. Layout namespace version 4 and the frame's version byte are independent.

## Key and invocation domain

The existing stream subkey remains HKDF-SHA256 with stream key as IKM, stream epoch as salt, and routing-key bytes, zero separator and LE32 key version as info. A second HKDF-SHA256 uses that subkey as IKM, the 16-byte physical segment identity as salt, and the fixed ASCII info `prisma-streams/frame/v4/aes-256-gcm-siv`. This separates new cipher keys from legacy keys and from sibling segments. Every encryption invocation obtains 12 nonce bytes directly from `OsRng`; these bytes are stored in the authenticated header. No volatile or durable logical offset is claimed to count encryption invocations.

Splits/merges and forks use different physical segment identities. Writer replacement, rollback, reopen and changed payloads at an old offset obtain fresh nonces even within the same segment. Producer duplicates return their stored result before encryption; copies to history and retransmission preserve encoded frames. Rotation changes the stream subkey. Old ciphertext remains decryptable under its original key/version and segment identity.

A repeated random nonce does not recreate ordinary GCM's shared-keystream failure: RFC 8452's misuse-resistant construction authenticates the message before deriving the encryption stream. Identical input under an accidentally repeated nonce can reveal equality. The production policy never intentionally reuses a nonce. Fixed nonces are exposed only inside the private testable encoder, used for RFC vectors and forced-misuse regressions.

## Format and compatibility

New frame bytes: version (4 raw, 5 compressed), offset BE64, timestamp BE64, key version BE32, routing length BE16, routing bytes, nonce[12], ciphertext length BE32, ciphertext plus 16-byte tag. AAD is physical segment identity concatenated with the entire header including nonce. Legacy v2/v3 omit the stored nonce, use the original stream subkey and derive their nonce from offset. Both old and new compressed versions decompress only after authentication.

A new encryption of the same record intentionally produces different bytes. Canonical persisted/history copies remain byte-identical. The logical read adapter that synthesizes frames from plaintext produces a fresh v4/v5 envelope; consumers must compare decoded record identities/content rather than assume re-encryption reproduces a legacy envelope. JSON/raw plaintext contracts are unchanged. Frame-aware clients must accept v4/v5 before writers resume.

Deployment is a compatible retained-data migration with a coordinated writer pause: drain writes, deploy all readers/owners and frame consumers capable of v2–v5, then resume writers. Old binaries must not resume after any new frame is written. This change does not execute that deployment or silently discard old data.

## Usage bounds and external acceptance

RFC 8452 section 9 recommends random nonces and gives quantitative limits dependent on message length, nonce repetition and authentication queries. Operational policy for this migration is at most 2^32 new encryption invocations per derived segment/routing/version key, counting re-encryption and rolled-back invocations, and at most 32 MiB payload plus 64 KiB metadata per invocation. Rotate/resegment well before the invocation bound. This deliberately conservative operating envelope still needs cryptographic review of the exact AAD and deployment workload; the counter is an operational limit, not a claimed crash-proof persisted invocation counter. The library additionally rejects inputs outside the primitive's limits. The service's request-size limits remain the ingestion admission control.

External acceptance remains pending: independent review of this integration and its operating envelope; inventory of operated namespaces and retained legacy ciphertext; response to any identified historical key/nonce reuse; the coordinated rollout and pinned-backend split/merge/fork/restart campaign. No claim is made that changing algorithms or rotating a key repairs historical disclosure. No operated environment was accessed by this remediation task.

Primary construction reference: [RFC 8452](https://www.rfc-editor.org/rfc/rfc8452), sections 4–6 and 9, and Appendix C.2. The committed Rust regression checks the first published AES-256-GCM-SIV vector rather than modeling a different language implementation.
