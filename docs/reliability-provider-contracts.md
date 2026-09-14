# Storage-provider contract verification

The canonical `verify` binary checks provider behavior through the same pinned
`object_store` and SlateDB clients used by the service. The `contract` command
fails with a nonzero exit code on an incorrect response, unexpected error,
missing object, mismatched bytes or incomplete observation. It does not infer
success from printed diagnostics.

## Contracts exercised

- Concurrent conditional creates have exactly one winner, every loser has the
  expected rejection, and the stored bytes are exactly the winner's payload.
- Concurrent conditional updates from one version have exactly one winner.
  Reusing the stale version must fail and must leave the winner's bytes intact.
- Each successful overwrite is immediately visible through exact GET, HEAD
  size, range GET and complete prefix LIST. Generations have different sizes and
  different bytes inside the requested range. LIST must contain exactly one
  matching key, independently of its size, and that size must be current.
- A successful deletion is immediately absent from GET, HEAD and prefix LIST.

The existing `cas` command uses that same conditional-write implementation.
Previously, several failures printed `UNEXPECTED` or `FAIL` and returned success.
The `fence`, `waloff` and `clone` commands now also require their actual expected
values. Fencing requires SlateDB's typed `Closed(Fenced)` reason, then a cold
reopen verifies both owners' acknowledged bytes and absence of zombie bytes.
Both acknowledged writes and the attempted stale-owner write await the pinned
API's `WriteHandle::await_durable`; merely accepting a write into memory is not
a fencing or durability result. The first strict run caught this stale API
assumption in the old harness.
Clone checks drain the complete 500-record low projection, 500-record high
projection and 1,001-record union, including child-write isolation. Sources use
the WAL-disabled history configuration and explicit SST flushes required by
the pinned SlateDB projection/union API. The previous fixture used default
WAL-enabled writers, which could leave unsupported source WALs behind. These
checks exercise normal provider semantics; they are not host power-loss tests.

## Local positive and negative controls

Build with the exact root toolchain:

```sh
cargo build --locked --release --bin verify --bin s3lite
python3 bench/reliability/provider_campaign.py \
  --verify target/release/verify --s3lite target/release/s3lite \
  --out target/reliability/provider-local
```

The campaign owns a fresh loopback emulator and runs all five verifier commands.
An HTTP proxy then deliberately removes create/update conditions, corrupts
returned bytes, hides or duplicates a listed object, serves stale range bytes,
acknowledges deletion without deleting, or changes bytes behind a rejected write.
Every negative run must observe its
fault, exit unsuccessfully and name the intended failed check. The legacy `cas`
entry point has a separate false-green regression. Generic connection failures
do not count as these negative controls.

The stale-range and duplicate-LIST controls were added after an independent
review found that a constant range prefix and filtering duplicates by the
expected size could otherwise conceal those defects.

The receipt contains each command, exit status, log hash, engagement count and
binary hashes. All fixtures and evidence remain local. The proxy cannot target
a non-loopback store. The normal verifier keeps its tiny generated objects
under a fresh random sub-prefix for inspection; only its own visibility-test
object is deleted as part of the contract.

## Run against a disposable provider namespace

Use the deployment's actual endpoint, bucket configuration, credentials, region
and access topology. Supply credentials through the standard AWS environment or
credential mechanism; do not paste secrets into commands or evidence files.
There is no implicit deployment target in this receipt-producing entry point:

```sh
python3 bench/reliability/provider_probe.py \
  --verify target/release/verify \
  --endpoint "$TEST_S3_ENDPOINT" --bucket "$TEST_S3_BUCKET" \
  --region "$TEST_AWS_REGION" --prefix reliability-disposable \
  --out target/reliability/provider-live
```

The bucket must already exist. Run separately from each relevant region/access
path and retain separate receipts. A passing local emulator result does not
establish a live provider contract. A passing live probe observes only these
bounded object operations; replication durability, ambiguous in-flight
completions, IAM/version-retention policy, correlated provider incidents,
power-loss and sustained capacity require separate evidence. The process
campaign's lost-response write is covered by the
[independent client oracle](../bench/reliability/README.md).

Live execution requires an explicitly designated disposable target. No live
provider or host power-cut run is implied by adding these tools. See the
[operations requirements](../OPERATIONS.md),
[reliability evidence](reliability-confidence.md) and
[checkpoint/restore scope](reliability-checkpoint-reclamation.md).
