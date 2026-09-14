# External receipt and recovery checks

`independent.py` is a Python standard-library client and correctness oracle. It
imports no server code, read planner, routing implementation, producer-state
implementation, simulator expectation, or aggregate receipt calculation. Its
expected streams and operations come only from an independently retained client
journal. `reliability.py` generates a bounded concurrent public-HTTP workload or
checks an existing journal after an externally controlled recovery.

Run the oracle's executable negative controls:

```sh
python3 -m unittest discover -s bench/reliability -p 'test_independent.py' -v
```

The controls exercise missing acknowledgements masked by ambiguous commits at
equal total count, duplicate commits, exact byte corruption, wrong tenant,
project, incarnation and routing key, phantom operations, successful truncated
pagination, missing streams, rejected writes becoming visible, ordering errors,
and journal corruption/truncation. They also require legitimate controls to pass:
ambiguous writes may be present or absent, retries retain one logical identity,
later rejection does not erase earlier ambiguity, overlapping operations may
appear in either order, and different routing keys have no global order.

## Public HTTP campaign

Create a namespace configuration outside the repository. Credentials are read
from environment variables and are never written into the journal:

```json
[
  {
    "tenant": "test-tenant",
    "project": "test-project",
    "base_url": "http://127.0.0.1:8090",
    "token_env": "RELIABILITY_TOKEN",
    "key_env": "RELIABILITY_KEY"
  }
]
```

`tenant` and `project` are external identity labels. The supplied credential must
actually address that namespace; the checker does not infer identity from bearer
contents or trust a server's tenant enumeration. Add a second entry with its own
credential to exercise same-name streams, colliding producer IDs and identical
application payloads across namespaces. Both may deliberately use the same
encryption key, so isolation errors cannot hide behind decryption failure.

```sh
python3 bench/reliability/reliability.py drive \
  --config /independent-disk/namespaces.json \
  --journal /independent-disk/campaign.jsonl \
  --streams 2 --keys 2 --writers 2 --records 50 --page-bytes 4096

# Kill every serving process without graceful shutdown. Restart from the
# authoritative persistent store with all old processes and caches gone.
# The caller/rig must establish and retain evidence of those fault mechanisms.

python3 bench/reliability/reliability.py check \
  --config /independent-disk/namespaces.json \
  --journal /independent-disk/campaign.jsonl \
  --page-bytes 4096 --report /independent-disk/after-cold-recovery.json
```

Repeat `check` at multiple byte budgets, after compaction/ownership movement,
and against an isolated restore by changing the configuration's server URL.
The check command never creates streams or appends. Streams and journal paths
must be fresh for `drive`; existing stream creation is refused even if the
server reports success. There is no automatic deletion or evidence cleanup.

The driver uses current product routes: `PUT /v1/streams/{name}`, idempotent
`POST .../records`, keyed durable `GET .../records`, and `GET ...:scan`.
Bytes-format streams contain a canonical identity envelope plus the exact
base64-encoded application payload. The full envelope bytes are compared with
the invocation. Scan records preserve individual record boundaries via
`valueB64`; keyed reads independently reconstruct the envelope stream. Cursors
are opaque strings. The checker neither decodes their offsets nor discovers
stream/segment identity by interrogating internal server state.

## What a pass establishes

For every journalled, successfully created stream, including an empty stream:

* Every acknowledged logical operation appears exactly once with the recorded
  tenant/project, stream incarnation label, routing key, producer, sequence and
  exact payload bytes. Identical user payloads in distinct logical appends remain
  distinct operations.
* A complete pre-commit refusal cannot become visible. HTTP 400/401/403/404/405/
  410/413/415/422 are classified as rejections; 408, 409, 429, 5xx, redirects,
  unexpected statuses and transport failures are conservatively ambiguous.
  Product success is HTTP 200, including deduplicated replay. Any success imposes
  the durability obligation even if its receipt body subsequently fails schema
  validation. Incomplete HTTP framing is an ambiguous transport outcome.
* Any earlier ambiguous attempt keeps the logical operation possible even when
  a later retry is rejected. A successful retry requires the same operation
  exactly once. An invocation without a persisted outcome is ambiguous.
* Successful completion before another invocation establishes a required order
  within that routing key. Overlapping operations are not ordered by response
  arrival. Per-producer sequences must increase. No order across keys is assumed.
* Complete snapshot scans and complete keyed walks agree. Missing/repeated
  cursors, a false completion header, malformed bodies, read errors, missing
  registry entries and page-limit exhaustion never count as completion. Explicit
  `Prisma-Scan-Complete: true` / `Prisma-Up-To-Date: true` is required.

Each invocation is persisted before its request is sent. Each observed outcome
is persisted before the recorder returns to its caller. Every event is hashed
into a chain, written and `fsync`ed, then its head receipt is atomically replaced
and its directory `fsync`ed. Preserve **both** `campaign.jsonl` and
`campaign.jsonl.head`. A partial write, changed byte, missing/reordered line or
whole-line tail truncation fails verification. A client crash between the two
durability steps is incomplete evidence and fails closed. The single writer
holds a process lock; concurrent threads serialize journal writes, and checks
refuse to read an active writer's journal.

## Complementary lifecycle, provider and version campaigns

The append-only oracle below retains its narrow contract. The separate
[lifecycle campaign](LIFECYCLE.md) journals authorized deletion/recreation,
authenticated project isolation, internal workload forks and workspace transfer.
Deletion requires a preceding exact-data checkpoint; it cannot erase an earlier
missing acknowledgement from the expected history.

[Provider contracts](../../docs/reliability-provider-contracts.md) run the
canonical Rust verifier against a real local emulator and deliberately broken
wire behavior. [Version transitions](../../docs/reliability-version-transitions.md)
build three immutable source revisions and cold-read prior data before issuing
retries or new writes. [Checkpoint reclamation](../../docs/reliability-checkpoint-reclamation.md)
uses real compaction, physical SST deletion and a fixed online checkpoint copy.
Each mechanism has its own scope and evidence; none implicitly certifies the
unimplemented whole-service online backup path.

## Boundaries and retained evidence

This first workload uses fresh, non-expiring streams and producer epoch 1. The
incarnation is a client-generated creation identity, not a reconstructed server
epoch. Deletion, recreation, TTL expiry and arbitrary producer-epoch transitions
are not supported operations; no journal event may silently waive a receipt
obligation. Wrong-incarnation data is covered by negative controls, but a pass
does not claim a generated delete/recreate lifecycle campaign. Fork/checkpoint
retention and backup creation need separate protocol campaigns.

Verification requires a quiescent state: no new appends, unresolved surviving
server requests, or concurrent lifecycle mutations during the scans. Scan/key
disagreement fails rather than being normalized away. The driver stops a writer
after bounded retries fail to acknowledge an operation; it never skips a
producer sequence. A later safety check may pass for that partial workload, but
the failed workload run is still failed progress evidence.

Journal hashes detect accidental damage; they are not signatures or proof
against an adversary able to rewrite the journal and its head. Retain the head
digest independently alongside the report if that threat matters. The checker
holds records in memory and bounds HTTP response size (32 MiB), request timeout
and pages; it is intended for bounded correctness campaigns, not production
bulk export. Keep the journal, reports, keys and binary/configuration provenance
outside the storage domain being destroyed. A local emulator or process-kill
pass does not establish a real provider's consistency contract, mixed-version
compatibility, production backup RPO, cryptographic acceptance or release safety.

## Reproducible local release-binary drill

From the repository root, with the pinned Rust toolchain:

```sh
bash scripts/reliability-campaign.sh
```

The script requires Node 22 or newer for the existing platform emulator. It
builds all three release executables and takes their actual paths from
Cargo's artifact messages, including when Cargo uses a custom target directory.
It creates a fresh evidence directory under `target/reliability/run.*/campaign`.
It also runs the provider contract/fault campaign and the authenticated
lifecycle campaign with seed 17 in sibling `provider` and `lifecycle` evidence
directories. The full local `scripts/gate.sh` runs it, and CI has a named recovery job. The
lightweight oracle/restore controls and bounded protocol models run in
`scripts/quality.sh`.

`local_campaign.py` owns all subprocesses and uses only loopback endpoints and
disposable `s3lite` storage. Its concurrent clients use two streams, two routing
keys and two producers per key. It forwards one real HTTP append through a
proxy that consumes the successful upstream response and disconnects without
returning any response to the client. The journal must classify that attempt as
ambiguous. After SIGKILL and a fresh server start, the rig separately requires
that known-successful operation to remain visible before retry can conceal any
loss. Retrying the same logical operation must then leave exactly one copy; a
subsequent append must also make progress.

Every verification drains both scans and keyed reads at byte budgets 1,024,
4,096 and 65,536. The writer is stopped during checks; immutable copies of the
journal and its head are saved for each phase. After a second cold recovery,
the server is killed, every stored object is copied to files with an identity
and SHA-256 manifest, and the original storage process is killed. The drill
restores into a fresh empty storage process using only those retained files,
then starts a new server and verifies the same client receipts. All copied
bytes are validated before restore begins and read back after upload.

Finally, the drill deletes actual restored registry descriptor objects, proves
their physical absence and the expected HTTP 404, starts a fresh reader, and
requires the external checker to reject the missing journalled streams. This
control prevents a loss of complete stream identities from silently reducing
the expected dataset. The original backup and journal are retained unchanged.

`receipt.json` records binary/checker hashes, the pinned Rust and SlateDB
versions, the retained working-tree patch digest, workload/configuration bounds, SIGKILL and lost-response witnesses,
copy counts, phase checks and the detected registry-loss control. Logs, copied
objects and journal snapshots remain local; no evidence is uploaded.

This is a **quiescent complete-copy restore drill**, with the application key
held by the independent client. It establishes neither online checkpoint/PITR
correctness nor an asynchronous backup recovery-point objective. It does not
implement the operational backup path described as unwired in `RUNBOOK.md`.
The copied registry and data must be retained together. The separate campaigns
above add local provider, fork, SST GC and version-transition evidence. Live
provider consistency, total provider loss, customer-key recovery and coordinated
whole-service online backup remain separate acceptance work.
