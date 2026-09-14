# Bounded actual-binary version transitions

This fixture checks retained records and producer retries across **sequential,
writer-paused server replacements**. Each process runs an actual release-profile
binary built from an immutable source revision. It complements the in-process
peer wire-contract tests; it does not certify a rolling fleet, live mixed peers,
connection draining, a cloud provider, or deployment rollback.

## The reviewed revisions and the hard downgrade boundary

| Label | Verified source revision | Writer frames | Meaning |
| --- | --- | --- | --- |
| `release` | `685ea0354f123864154b46e585f9c22664929763` (`v0.2.0-rc.4`) | 2 / 3 | Latest release tag observed on origin when this fixture was added |
| `prior` | `adc2cdc5bdc2dba1aaffabc599d61de3df35fcb1` | 4 / 5 | Starting `origin/slate` revision, before the reliability repair |
| `current` | Explicit immutable revision supplied to the builder | 4 / 5 | Candidate server source |

All three use layout namespace 4 and SlateDB revision
`0717cc1e4e9bad10a4773760f66bac4264ecf05e`. Layout namespace and encrypted-frame
version are independent. The builder records each Cargo lockfile hash, source
tree, crypto source hash, package version, SlateDB pin, compiler host/version,
and exact binary SHA-256. It builds historical source without patches using the
root's pinned Rust 1.98.1 toolchain. rc.4 did not have a root toolchain file.
These are locally rebuilt source revisions, **not** the historical certified
x86_64-musl release artifact or a claim to reproduce that artifact's hash.

Commit `031ae5b09596ee629f87e20e019e545d3b5868ca` changed new encryption from
AES-GCM frames 2/3 to AES-GCM-SIV frames 4/5. Current readers retain support for
2/3; rc.4 readers do not support 4/5. The existing
[frame migration contract](crypto-frame-v4.md) requires a coordinated writer
pause and says: **“Old binaries must not resume after any new frame is written.”**
The released-binary fixture therefore runs rc.4 → rc.4 → current → current.
It never starts rc.4 after current has written to that namespace. There is no
legacy-writer switch, format downgrade, error-swallowing compatibility adapter,
or inferred “safe refusal” from an arbitrary old-binary HTTP error.

The `prior` source can read frames 4/5 but retains the warm keyed-cache omission
repaired in the first reliability commit. Its round trip checks a precisely
bounded **cold-read frame/layout compatibility** path. A pass does not make that
old revision safe to deploy or certify rollback to it.

## Run and retained evidence

Run from the repository root. Choose a fresh output directory for each attempt:

```sh
python3 bench/reliability/version_build.py \
  --out target/reliability/version-binaries \
  --target-dir target/reliability/version-build \
  --current-revision HEAD
python3 bench/reliability/version_transition.py \
  --build-receipt target/reliability/version-binaries/build-receipt.json \
  --out target/reliability/version-matrix
```

`HEAD` is resolved once to an immutable commit. Uncommitted source is excluded;
the test receipt names the actual revision tested. The builder uses detached
worktrees in a temporary directory outside the current Cargo workspace and an
isolated target directory. It checks ancestry, the reviewed release-tag target,
source cleanliness and authoritative Cargo executable paths. It copies binaries
out before building the next revision. Clean temporary source trees are removed
after successful builds; failed build logs and source paths remain available.

When only the candidate revision changes, the builder can reuse the two exact
historical executables from an earlier successful build:

```sh
python3 bench/reliability/version_build.py \
  --out target/reliability/version-binaries-next \
  --target-dir target/reliability/version-build \
  --current-revision HEAD \
  --reuse-historical-builds target/reliability/version-binaries/build-receipt.json
```

Reuse validates the entire previous receipt against immutable Git metadata and
all executable hashes, and requires the same compiler identity and host. It
copies only `release` and `prior`, checking the copied bytes against the original
hashes. The new receipt preserves and hashes the previous receipt and marks the
historical reuse explicitly. The new current server and object-store fixture
always build from the newly resolved candidate commit; historical reuse never
substitutes an older current binary or a previous matrix result.

The matrix requires full immutable commit IDs and checks every recorded source
tree, package version, lockfile hash, crypto hash, SlateDB dependency and format
pin against that exact Git revision. It refuses changed executable bytes,
duplicate executable hashes under different version labels, and unreviewed
historical source or frame pins.
It rechecks the build receipt and binaries after running. Success is written
only after all four campaigns pass; a failed run retains process logs and the
client journal but has no successful final receipt. Unit tests include deliberate
binary replacement and mislabeled-version controls.

## What each campaign exercises

Both plans run once with `FRAME_COMPRESS=0` and once with the existing production
`FRAME_COMPRESS=1` setting and compressible input. Compressed-frame versions are
writer-source pins; the fixture does not decode storage SSTs to count individual
frame versions. Every paired writing phase, including the current writer, must
report fewer actual encoded ingest bytes with compression enabled for the same
record count and payload lengths; merely
setting an ignored environment variable cannot satisfy that witness.
Each run uses its own fresh s3lite process and namespace, two
streams, two routing keys, producer identities, explicit sequence numbers and
byte-exact client-recorded payloads.

* **Released forward:** rc.4 writes; a fresh rc.4 verifies and advances; current
  verifies legacy retained records and retries their producer sequence before
  adding new records; a fresh current verifies the mixed retained data.
* **Same-format round trip:** prior writes; a fresh prior verifies and advances;
  current verifies and advances; a fresh prior verifies and advances; a fresh
  current verifies all records.

Every cold verification happens **before** retry or new append on that process.
The external, fsynced journal determines expected streams and exact records;
server enumeration cannot shrink the expectation. The independent checker
walks both public full scans and every expected routing key at three page byte
budgets (1,024, 4,096 and 65,536). Missing or duplicated records, changed payloads,
wrong identities, broken per-key order and stalled pagination fail the run.
Acknowledged retrying operations retain the same external logical identity, so
any duplicate insertion fails the next cold exact-read check.

After each writing phase, the fixture requires positive ingest bytes, recorded
absorption at least equal to this process's ingest, and zero reported unabsorbed
bytes, plus actual nonempty history SST objects. Those diagnostics witness the
maintenance intervention; they are not the correctness oracle. The next fresh
binary's independent exact reads establish retained readability. This does not
prove physical deletion of every original WAL object; the separate crash and
reclamation campaign covers that boundary.

Each serving process receives SIGKILL and must exit with the expected signal
before the next version starts. The fixture never overlaps writers. It retains
every process log, every cold journal snapshot and head, each phase's revision
and binary hash, the maintenance witnesses, and a final quiescent object copy
with exact names and SHA-256 hashes. No evidence leaves the machine.

## Remaining operational holds

A live rolling-upgrade claim still requires the exact deployed binaries and
provider configuration, inter-version peer requests in both directions, drained
connections, owner handoff under traffic, and a compatible rollback destination.
The current pre-launch clean-namespace and layout migration contracts remain in
force. This local matrix does not invent compatibility for unexercised descriptor shapes,
pre-layout-4 namespaces, checkpoint expiry, multi-database snapshots, or an
object-store request that finishes after a process has crashed.

## Recorded local result, 14 September 2026

The full matrix passed with current source
`db2157e58e2880ef83085770e5fc609bfdca0176`, the first reliability commit. All
three binaries were rebuilt from their immutable source under Rust 1.98.1 on
aarch64-apple-darwin. The successful run contained four campaigns, 18 fresh
server processes, 42 independent cold-read checks, 112 final unique acknowledged
operations, and 40 retained producer retries. Every paired writing phase showed
fewer encoded bytes with compression enabled; the two current-writer phases
fell from 6,180 bytes to 2,381 and 2,387 bytes respectively.

The retained local receipts are
`target/reliability/version-binaries-final/build-receipt.json` and
`target/reliability/version-matrix-final/receipt.json`. The latter includes all
phase checks, intervention witnesses and fixture source hashes; process logs,
client snapshots and final object copies remain alongside it. These local
artifacts are generated evidence, not committed fixtures. The eight provenance
and compression controls passed, including rejection of a moving `HEAD` claim,
each falsified source-metadata field, replaced/duplicated executable bytes, and
an ignored compression setting in the current writing phase.

The actual server binary hashes were:

| Revision label | SHA-256 |
| --- | --- |
| release | `99280e15b7ed38f0ecb8b3927820114dde85cab0cc8c3875a4e0f16a39eaac61` |
| prior | `12b94918ac0582ab2763869ee544ce22c8a994cb13391d09cc9309d1d91c478b` |
| current | `60f6c3bd63fcd7715e5701cef3451eafc96efce11c9f45064103b30bfbba141e` |

No deployed release or real object-store account was exercised, and the rc.4
downgrade prohibition and prior revision's known warm-cache defect remain.

## Retest of the patched candidate

The full matrix also passed for candidate
`2863bab3e38a5ba81140f3d447a23156c138a616`, after the lockfile update to rustls
0.23.45, rustls-webpki 0.103.15, aws-lc-rs 1.18.1 and aws-lc-sys 0.45.0. The
builder verified and reused the two historical binaries with their original
hashes above, preserved the previous build receipt, and rebuilt the current
server and s3lite from the new immutable source. This preserves the initial
matrix as historical evidence while testing the updated candidate separately.

The candidate server SHA-256 is
`600cb57ddc5ac02be077750910cc6691e37927f59403acf4eaf23547152154f5`.
Its build receipt is
`target/reliability/version-binaries-tls-final/build-receipt.json`, and its
matrix receipt is `target/reliability/version-matrix-tls-final/receipt.json`.
All four campaigns passed: 18 fresh server processes, 42 independent cold-read
checks, 112 final unique acknowledged operations, 40 retained producer retries,
and 22 verified process kills including the four object stores. All seven
paired writing phases demonstrated compression savings; both current-writer
phases fell from 6,180 to 2,384 encoded bytes.

The new run archives the exact five executed fixture source files alongside
their hashes. The initial matrix's matching source bytes remain preserved in
its own `fixture-source` directory. All temporary source worktrees and serving
processes were removed after successful completion. The compatibility scope
and deployment holds above remain unchanged; TLS handshakes are outside this
loopback-HTTP persistence fixture.
