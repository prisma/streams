# Authenticated lifecycle and retained-fork evidence

`lifecycle_campaign.py` runs the existing release server, local S3 emulator and
platform emulator as separate processes. It checks exact client-journaled record
identities across same-name deletion/recreation, retained default-stream forks,
workspace ownership transfer and two abrupt server restarts. `lifecycle.py` owns
the client lifecycle facts and replays every retained HTTP read checkpoint; it
imports no production planner, storage decoder, simulator or producer logic.
The ordinary [independent oracle](README.md) still owns payload, identity,
per-key ordering, duplicate detection and pagination checks.

## Run and replay

Prerequisites are the pinned release `streams-slate` and `s3lite` binaries,
Python 3.11 or newer and Node.js 22. The existing platform emulator imports
only Node built-ins and its local schema validator; no npm install is needed. Root quality/gate tooling owns builds; this command does not
compile Rust or install dependencies. All subprocesses use loopback addresses
and independently selected ports. The output directory must not exist.

```sh
python3 -m unittest discover -s bench/reliability -p 'test_lifecycle.py' -v
python3 bench/reliability/lifecycle_campaign.py \
  --out target/reliability/lifecycle-seed-17 --seed 17 --cycles 2 --node node
python3 bench/reliability/lifecycle.py \
  target/reliability/lifecycle-seed-17/lifecycle.jsonl
```

`--server` and `--s3lite` select already-built binaries. A failed child, HTTP
contract mismatch, incomplete journal, missing witness or changed fixture source
fails the run. `receipt.json` is created only after final offline replay succeeds.
The receipt records binary hashes, the base Git revision, executable fixture
source hashes, Node version, workload parameters and actual SIGKILL witnesses.
Binaries are hashed before boot and checked again before every child launch and
before accepting the receipt; a replacement during the run cannot silently
change its version identity. Exact fixture sources are copied to `source/`; dirty/untracked Python owners are
therefore retained independently of the base Git revision. To replay that exact
owner after the working tree changes:

```sh
python3 target/reliability/lifecycle-seed-17/source/bench/reliability/lifecycle.py \
  target/reliability/lifecycle-seed-17/lifecycle.jsonl
```

This is local evidence; it neither deploys nor uploads artifacts. The private output directory contains emulator key/token
feeds needed by the fixture. Credentials are excluded from journal events,
receipts and printed commands. Preserve the existing independent raw-evidence
upload hold; do not publish this directory as an unrestricted CI artifact.

## Exact workload and public/internal boundaries

Both customer projects run on **one cell and one object-store namespace** with
`STREAMS_AUTH_MODE=enforce`, workload identity and release posture enabled. Each
project has its own real JWT credential, grant and workspace ownership policy.
They use identical collection names, routing keys, producer IDs and application
payload bytes, and deliberately share the encryption key. Separation therefore
has to come from authenticated project identity. The `tenant` field in envelopes
is a client label; project/workspace JWT authorization is the actual server
isolation boundary. No deployment-bearer fixture is presented as multitenancy.

The seed determines the order of project lifecycle cycles and the order of keyed
appends. It does not control OS scheduling, generated UUIDs, cryptographic keys
or binary timing. Defaults are seed **17** and **two cycles** per project;
`--cycles` accepts 1 through 20. A second run with seed **91** exercises another
bounded order. This is serialized stateful testing, not exhaustive concurrency
or randomized fault-schedule exploration.

The run performs these transitions:

1. Create `same-name` and `fork-source` in both projects. Append two records per
   key (`alpha` and `beta`) to each `same-name` and four default-key records to
   each source. Read every known stream at both 1,024 and 65,536 byte budgets.
2. For each project and cycle, first prove an acknowledged tail cursor works.
   Verify exact current data before deletion, verify deleted metadata is absent,
   recreate the same name with a new client incarnation and reset producer
   sequences. Require the previously valid old-incarnation cursor to return 400.
   Check exact new data and the unaffected project's data again.
3. Capture projectA's nonempty default-key source prefix and its opaque raw
   boundary. Create a fork, append separately to the parent and child, and prove
   the child contains exactly the inherited prefix plus its own tail. Parent
   records after the boundary must not appear in the child.
4. Delete the parent, require its name to remain blocked while the live child
   retains it, and retry the exact original fork creation after parent deletion.
   SIGKILL the server, reopen it against the same object store and verify the
   inherited bytes with cold server caches.
5. Delete the last child, recreate the parent name and append with a fresh
   producer sequence. Transfer projectA to a new workspace through the existing
   platform emulator. Require old credential exchange to stop during transfer,
   then old JWT access to be refused while a newly issued owner JWT reads and
   appends the same project data. ProjectB must remain exact and unchanged.
6. SIGKILL and reopen once more, then replay all live/deleted identities and all
   retained checkpoints from the closed external journal.

The product API currently has **no public fork-creation method**. Raw
`/v1/stream/` is an internal workload-authenticated surface in enforce mode, and
addresses the server's deployment project. The fixture sets the deployment
project to projectA and uses a separate workload client solely for raw GET/PUT.
It verifies that a real customer JWT cannot read that raw source (401/403).
Customer product reads/appends/deletes continue to use their respective JWTs.
The workload token allows `raw-read` and `raw-lifecycle` in addition to the
existing `telemetry-append` and `segment-read` server requirements. No raw calls
are routed on projectB's behalf, and no raw append capability is requested.

Forks here cover the existing **default-key raw stream** contract, not implicit
whole-collection or multi-key forks. Their exact bytes are checked through raw
reads and customer product **keyed default-key** reads. Ordinary collections are
checked through product scans and keyed reads. Product scan inheritance is not
assumed. Ownership transfer means workspace authorization ownership; this
campaign does not claim physical shard placement movement or fleet failover.

With `cycles=2`, acceptance requires **36 acknowledged logical operations,
10 successfully created incarnations, four final live collections, 17 exact
checkpoints at both budgets and two actual SIGKILLs**. Required witnesses include
nonempty inheritance, parent deletion with a live fork, pinned-name refusal,
fork retry after parent deletion, same-name recreation, previously-valid cursor
refusal, completed ownership transfer, retired-owner refusal and customer raw
refusal. Empty workloads cannot produce a successful lifecycle replay.

## Journal and oracle contract

Each data-plane create, append, delete and fork invocation is persisted and
fsynced before the request is sent. The response is likewise persisted before
its transition is accepted. Logical append IDs and individual invocation IDs
are separate; invocation IDs are globally unique. A later rejected retry cannot
replace an earlier acknowledgement. An acknowledged append is required exactly
once; a definite rejection must be absent; an ambiguous append may be present
once or absent until a later acknowledgement makes it required. Malformed
success receipts remain in the journal and make replay fail.

Create/delete/fork ambiguity stops the campaign. This bounded oracle does not
invent a new incarnation after an unresolved lifecycle outcome. Destruction
requires a complete exact checkpoint at the current mutation generation, so an
acknowledged missing record cannot be hidden by deleting its name. A fork's
inherited operation IDs come from the independent acknowledged default-key
prefix, never server enumeration or a copied server metadata model.

Checkpoints retain every GET's namespace label, method, exact path, status,
headers and response bytes. Offline replay regenerates the expected requests
from client facts and rejects omitted, reordered or extra observations. Missing
streams, payload or identity changes, duplicates, order violations, cross-project
records, stale-incarnation records and incomplete pagination fail closed. Every
known routing key plus the default key is read; expected streams derive from
client creation events, not server catalog enumeration. Deleted names with no
replacement must also be absent from metadata reads. Read page walks have a
finite 10,000-page bound; exhausting it is failure, never completion.

Workspace transfer retains the concrete management completion receipt and an
old-JWT refusal observation; those control-plane setup calls do not use the
data-plane invocation grammar. The fixture source and live positive controls
establish which credential was sent. Offline replay does not verify JWT
signatures or pretend a namespace label itself proves authorization.

The journal uses the ordinary fsynced hash chain plus independent head file.
Retain both `lifecycle.jsonl` and `lifecycle.jsonl.head` outside the serving
processes' failure domain. Corruption, partial lines and whole-line truncation
are errors. A quiescent checkpoint is not an isolated backup: this campaign
restarts the server against the same live emulator. The separate restore and
provider-contract campaigns retain their distinct responsibilities.

## Executable negative controls and limits

`test_lifecycle.py` includes 30 public-wire/model tests. Controls deliberately
swap project clients, drop acknowledged records before deletion, retain deleted
metadata, inject retired-incarnation bytes, omit fork prefixes, insert parent
records after the cut, duplicate/reorder/corrupt fork frames (including raw-only reorder with a
healthy keyed control), truncate keyed/raw
reads, forge unchecked boundaries, reuse acknowledged invocation IDs, corrupt
ack receipts, and rehash tampered checkpoint observations. The latter checks
prove semantic validation rather than merely detecting a broken hash chain.
Positive controls include exact namespace separation, nonempty inherited bytes,
child producer sequence reset, successful fork retry after deletion, parent
reuse after final-reference release, and ambiguous-commit deduplicated retry.

Passing these bounded seeds is evidence for these mechanisms and concrete
release binaries. It does not establish all interleavings, arbitrary customer
payload formats, concurrent delete/append linearizability, provider durability,
host power loss, multi-cell ownership movement, TTL/compaction reclamation or
all historical upgrade formats. Retained-fork logical reference release is
observed through name reuse; no physical SST/object reclamation is inferred.

The retained local validation for this change ran seeds 17 and 91 with two cycles.
Both passed the required 36 acknowledgements/10 incarnations/17 checkpoints/two
SIGKILLs and retained 401 GET responses for independent replay. Their project
cycle orders differ. The receipts are
`target/reliability/lifecycle-final-17b/receipt.json` and
`target/reliability/lifecycle-final-91/receipt.json`; the combined source, journal
and privacy audit is `target/reliability/lifecycle-final-validation.json`.
The complete Python reliability suite passed 87 tests, including 30 lifecycle
controls (`target/reliability/lifecycle-all-python-final.log`). The raw-only
reordering negative was run against the unchecked raw-order branch first and
failed because no `CheckError` was raised; that baseline is retained at
`target/reliability/lifecycle-raw-order-baseline.log`. Adding the raw default-key
order check made the control pass. These local artifact paths are evidence from
this session, not files expected to be committed or uploaded.
