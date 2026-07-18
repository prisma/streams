# Technical verification results (README §8 V-items)

Run 2026-07-08 against live Tigris bucket `slate-sin` (single-region
Singapore) from a machine in Thailand — all absolute latencies include
~40–60 ms cross-border RTT; in-region numbers will be several-fold lower.
Harness: [src/bin/verify.rs](./src/bin/verify.rs)
(`verify latency|cas|fence|waloff|clone|idle`) plus the standard server +
bench binaries, and `compress_ratio.ts` for V6.

Baseline network: 10 KiB PUT p50 **79 ms**, GET p50 **60 ms** from here.

## V2 — Tigris conditional writes ✅ PASS (the critical one)

- `PutMode::Create` on fresh key: OK, returns ETag.
- `PutMode::Create` on existing key: rejected `AlreadyExists`; content
  untouched.
- `PutMode::Update` with correct ETag: OK; with stale ETag: rejected
  `Precondition`.
- Two concurrent `Create`s on one key: **exactly one winner**.

SlateDB's fencing (WAL SSTs and manifests via `PutMode::Create`,
confirmed in `tablestore.rs`/`retrying_object_store.rs`) is fully supported
by Tigris. Guarantee G6 stands on verified ground.

## Fencing, end-to-end ✅ PASS (live)

Two `Db` writers on one path: B opened, read A's durable data; A's next
write failed immediately (`Closed error: detected newer DB client`, 0 ms);
the zombie write never became visible to B.

Full server on Tigris: boot 4.5 s; durable append ack ~60 ms; c=64 append
bench: **478 req/s, p50 133 ms, p99 301 ms, zero errors**; ack→durability
gap 0 ms (by construction). All data intact after hard kill + restart.

**Finding F1 — WAL replay bound matters in practice:** restart after the
bench (≈500 accumulated WAL SSTs, default `max_wal_flushes_before_l0_flush
= 4096`) took **25 s** — WAL replay is sequential GETs. With the spec's
prescribed cap of 64 this bounds to ~1–4 s (sub-second in-region). The
server must set this cap explicitly; it is not a tuning nicety but the
shard-move/crash-recovery budget.

## V1 — Block-transformer coverage ✅ PASS (source-verified)

`compress_and_transform` in `format/sst.rs` is applied to **data blocks,
the composite filter block, the index block, and the stats block**, and the
read path decodes each through the transformer. Routing keys inside `k!`
index entries are therefore encrypted in history-tier SSTs. No key-hashing
workaround needed (L6 resolved).

## V3 — `wal_disable` ✅ PASS, with a required pattern

Feature flag exists (not default; now enabled in Cargo.toml along with
`zstd`). Behavior on live Tigris:

- non-durable put: 0.2 ms; explicit `flush()` (memtable→L0 with WAL off):
  697 ms; all data present after reopen.

**Finding F2:** with WAL off, `await_durable: true` **hangs indefinitely**
for a small memtable — there is no timer-driven memtable flush below
`l0_sst_size_bytes`. The absorber must therefore write its batch with
`await_durable: false` and conclude with an explicit `flush()` before
advancing `absorbed_through`. (This is the natural shape anyway; recorded
so nobody "simplifies" it into a hang.)

## V7 / V8 — clone split & union ✅ PASS (V8 partially)

Parent DB with 1,000 keys across two ranges (1,010 objects):

- Split into two projection-ranged clones: children created with **2
  objects each** (genuinely zero-copy), contents exactly partitioned,
  post-split writes isolated.
- Union of the two children (one carrying a post-split write) into a merged
  DB: all keys visible, including the child's new write.
- Timing at cross-border latency: split 4.9 s, union 4.6 s (sequential
  metadata round trips; expect ~1 s in-region).

**V8 status:** zero-copy referencing confirmed; clones pin the parent via a
checkpoint. *Not yet exercised:* the long-horizon path where children
compact away parent references and the retired parent's objects become
GC-eligible. Needs a soak test before shard splits run unattended in
production. **Finding F3:** at V10, the split pause budget must be planned
against in-region clone time (~1 s), and the quiesce window overlaps the
append timeout comfortably only in-region — cross-region shard splits would
need a longer hold or async cutover.

## V4 — idle per-open-DB overhead ⚠️ REAL AT DEFAULTS → ✅ TUNED (V4b)

8 idle DBs (default manifest poll 1 s, default compactor/GC), 60 s:
**8.26 object-store ops/s per DB** (~$10/month per idle open database at
Tigris Class B prices — ~$250/month for a 24-shard instance before any
traffic).

**V4b — measured with the D23 profile** (`manifest_poll_interval = 60 s`,
`compactor_options: None`, `garbage_collector_options: None`; safe because
fencing correctness comes from CAS write failures, not polls, and
compaction/GC run detached or piggybacked): 8 idle DBs, 90 s →
**0.03 ops/s per DB** — a **275× reduction**, ≈ $0.04/month per open shard
log; a 24-shard instance idles at ~$1/month. Closed DBs cost zero. The
per-open cost for transient history-DB opens remains to be measured, but at
~3 ops/open it is amortized by the bytes-or-age absorption thresholds.

## V6 — compression ratios ✅ TARGET MET (no dictionaries)

20k synthetic records per corpus, zstd level 3:

| strategy | evlog (372 B avg) | chat (234 B avg) |
|---|---:|---:|
| per-record zstd + AEAD (shard log) | 15.5% | 24.1% |
| 5-record commit batch + AEAD | 71.4% | 76.4% |
| **64 KiB block zstd (history tier)** | **90.5%** | **92.4%** |
| 256 KiB block zstd (old TS impl) | 91.2% | 92.8% |

The history tier reaches the ~90% target within half a point of the old
256 KiB segments — D5/D8 validated. Shard-log compression is weak as
expected and accepted (transient window only).

## V9 — deterministic-nonce envelope (desk review) ⚠️ SOUND WITH TWO MANDATES

Scheme: `subkey = HKDF-SHA256(streamKey, info = routingKey ‖ keyVersion)`;
AES-256-GCM per record; nonce = 96-bit encoding of the record offset; AAD =
(stream hash, offset, timestamp, routing key, key version).

- **Uniqueness argument:** within one (streamKey, keyVersion, routingKey),
  the nonce is the offset, and an offset is assigned to exactly one record
  by G3 (single committer, contiguous assignment, fencing). Re-encryption
  during history reads reproduces the *same* plaintext under the same
  (subkey, nonce) — identical ciphertext, no information leak beyond the
  (already public) equality.
- **Mandate 1 — stream re-creation:** delete + recreate of the same stream
  name restarts offsets at 0. Reusing the *same* streamKey would reuse
  (subkey, nonce) pairs on new plaintexts — catastrophic for GCM. Rule:
  **a stream identity change always mints a new streamKey** (enforced by
  the key service; additionally bind a per-creation `streamEpoch` into the
  HKDF info so even a violated rule fails closed).
- **Mandate 2 — rotation bumps keyVersion** (new subkeys, nonce space
  reset is safe); the version rides in the record header/AAD.
- GCM per-key data limits (~2³² records per subkey for comfortable margins)
  are far above per-routing-key volumes; the per-stream fan-out into
  subkeys helps here.
- Residual leakage: record boundaries, sizes, offsets, timestamps, routing
  keys — all declared metadata (C10). Acceptable.

Independent cryptographic review still recommended before GA; the design
has no identified flaw with the two mandates in force.

## V5 / V10 — deferred

V5 (CDN long-poll coalescing) awaits the CDN choice (O3). V10 (split pause
under load) awaits the split implementation; F3 above gives the first
timing input.

## Summary

| item | verdict |
|---|---|
| V1 transformer coverage | ✅ pass (source) |
| V2 Tigris CAS | ✅ pass (live) |
| V3 wal_disable | ✅ pass; absorber must explicit-flush (F2) |
| V4 idle overhead | ✅ 8.26 ops/s/db default → **0.03 ops/s tuned** (V4b, D23 profile) |
| V6 compression | ✅ 90.5–92.4% at 64 KiB blocks, no dictionaries |
| V7 clone/union | ✅ pass (live, zero-copy) |
| V8 parent GC | ◐ pinning confirmed; long-horizon GC needs soak test |
| V9 crypto | ⚠️ sound with new-key-on-recreation + keyVersion mandates |
| fencing e2e | ✅ pass (live) |
| new finding F1 | WAL replay cap is the recovery budget — set it to 64 |
