#!/bin/bash
# Build BOTH campaign binaries and upload them, verifying each with a
# ranged GET — not HEAD alone (the Bun/S3 wrapper has masked provider
# errors before). The 2026-08-11 run deployed servers whose binary key
# did not exist and generators whose binary was never built; both
# presented as edge 404s indistinguishable from platform routing
# failures. This script makes that class impossible.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
S=${SOAK_HOME:?set SOAK_HOME}
BIN_TAG=${BIN_TAG:?set BIN_TAG (campaign.sh exports it)}

echo "== building streams-slate + awsbench (x86_64-musl)"
# R29: pin the embedded build identity to the EXACT commit being
# shipped (build.rs env override; also recorded in the manifest so
# verify-running can compare /v1/debug/load and /readyz identity).
export BUILD_GIT_COMMIT=$(cd "$ROOT" && git rev-parse HEAD)
# ONE timestamp for the compile stamp AND the manifest (R30: the
# upload clock could never equal the compile clock; SOURCE_DATE_EPOCH
# threads the same value into build.rs).
export BUILD_UNIX=$(date +%s)
export SOURCE_DATE_EPOCH="$BUILD_UNIX"
export STREAMS_GIT_COMMIT="$BUILD_GIT_COMMIT"
(cd "$ROOT" && cargo zigbuild --release --target x86_64-unknown-linux-musl --bin streams-slate)
(cd "$ROOT/bench/awsbench" && cargo zigbuild --release --target x86_64-unknown-linux-musl --bin awsbench)

SERVER_BIN="$ROOT/target/x86_64-unknown-linux-musl/release/streams-slate"
GEN_BIN="$ROOT/bench/awsbench/target/x86_64-unknown-linux-musl/release/awsbench"
# ELF x86_64 check: byte 18 == 0x3e. An aarch64 binary deploys fine and
# crash-loops as a silent zombie (prisma-compute-x86-64 lesson).
for b in "$SERVER_BIN" "$GEN_BIN"; do
  m=$(xxd -s 18 -l 1 -p "$b")
  [ "$m" = "3e" ] || { echo "FATAL: $b is not x86_64 (e_machine=$m)"; exit 1; }
done

python3 - "$SERVER_BIN" "bin/streams-$BIN_TAG-x64" "$GEN_BIN" "bin/awsbench-$BIN_TAG-x64" <<'PY'
import boto3, hashlib, json, os, sys
S = os.environ["SOAK_HOME"]
r = lambda f: open(os.path.join(S, f)).read().strip()
c = boto3.client("s3", endpoint_url=r("artifact-endpoint.txt"),
    aws_access_key_id=r("binid.txt"), aws_secret_access_key=r("binsec.txt"),
    region_name="auto")
bucket = r("artifact-bucket.txt")
manifest = {}
pairs = [(sys.argv[1], sys.argv[2]), (sys.argv[3], sys.argv[4])]
for path, key in pairs:
    data = open(path, "rb").read()
    sha = hashlib.sha256(data).hexdigest()
    c.put_object(Bucket=bucket, Key=key, Body=data)
    # Ranged GET verification: first+last 16 bytes must match what we
    # uploaded. HEAD alone has lied before.
    head = c.get_object(Bucket=bucket, Key=key, Range="bytes=0-15")["Body"].read()
    tail = c.get_object(Bucket=bucket, Key=key,
        Range=f"bytes={len(data)-16}-{len(data)-1}")["Body"].read()
    assert head == data[:16] and tail == data[-16:], f"ranged-GET mismatch for {key}"
    manifest[key] = {"sha256": sha, "bytes": len(data),
                     "gitCommit": os.environ.get("BUILD_GIT_COMMIT", ""),
                     "buildUnix": os.environ.get("BUILD_UNIX", "")}
    print(f"  uploaded+verified {key}  {len(data)} bytes  sha256 {sha[:16]}")
run_id = os.environ.get("SOAK_RUN_ID", "adhoc")
os.makedirs(f"{S}/results/{run_id}", exist_ok=True)
json.dump(manifest, open(f"{S}/results/{run_id}/binaries.json", "w"), indent=1)
print(f"  manifest -> results/{run_id}/binaries.json")
PY
