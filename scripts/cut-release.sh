#!/bin/bash
# Cut a shared cell's release (public-beta deploy-q5 triage B5): one commit,
# built and staged once into $STREAMS_RELEASES/<tag>/ (default
# ~/.streams-releases), never overwritten, and the only thing
# deploy/composer/deploy-release.sh deploys. Its layout and its rules are
# deploy/composer/release.ts's.
#
#   SOAK_HOME=~/.streams-soak scripts/cut-release.sh <commit> <tag> --cells DIR
#
# --cells DIR holds the operator's cells files, <stage>.env for each stage
# the release may deploy to (scratch, cert, production; provision-cell.py
# and `cell-admin init --cell-env` write them); each is copied into the
# release as deploy/cells/<stage>.env, and nothing else of DIR is read.
# $SOAK_HOME holds the artifact bucket's files, as build-upload.sh reads
# them (artifact-endpoint.txt, artifact-bucket.txt, binid.txt, binsec.txt).
#
# In order:
#   1. refuses an existing release directory, results/<tag>/ or a cut of the
#      same tag in progress, before anything else: a tag is cut once;
#   2. scripts/dev/ci-status.sh <commit> must print CI_GREEN (AGENTS.md §1:
#      CI green is evidence, never memory);
#   3. a clean detached worktree at <commit>, beside the release directory;
#   4. the x86_64-musl streams-slate and s3lite, with STREAMS_GIT_COMMIT,
#      BUILD_UNIX and SOURCE_DATE_EPOCH pinned (build-upload.sh's identity);
#   5. deploy/composer's packages (`bun install --frozen-lockfile`) and the
#      entry bundled into one file (D0 finding 2);
#   6. the commit's own deploy/composer/release.ts stages the release:
#      every repository file byte-identical to the commit's, both binaries
#      x86_64 ELFs, release.json, the rules;
#   7. the server uploaded to the artifact bucket as
#      releases/<tag>/streams-slate, verified by ranged GETs, and
#      $SOAK_HOME/results/<tag>/binaries.json in build-upload.sh's shape,
#      which `rc-certify.sh --shape single-cell --artifact-run <tag>` reads;
#   8. the staged directory moved to its place.
# A failure removes the worktree and the half-staged directory.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
refuse() { echo "cut-release: refused: $*" >&2; exit 1; }
usage() { echo "usage: scripts/cut-release.sh <commit> <tag> --cells DIR" >&2; exit 2; }

[ $# -eq 4 ] && [ "$3" = --cells ] || usage
COMMIT_ARG=$1 TAG=$2 CELLS=$4
[[ "$TAG" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || refuse "tag $TAG is not [A-Za-z0-9._-]"
RELEASES=${STREAMS_RELEASES:-$HOME/.streams-releases}
FINAL=$RELEASES/$TAG
CUT=$RELEASES/.cut-$TAG
S=${SOAK_HOME:?set SOAK_HOME, which holds the artifact bucket files and results/}
RESULTS=$S/results/$TAG

# 1.
[ ! -e "$FINAL" ] || refuse "$FINAL exists: a release directory is never overwritten"
[ ! -e "$RESULTS" ] || refuse "$RESULTS exists: tag $TAG was cut before"
[ ! -e "$CUT" ] || refuse "$CUT exists: another cut of $TAG is running, or one failed and left it (remove it)"
[ -d "$CELLS" ] || refuse "--cells $CELLS is not a directory"
CELLS=$(cd "$CELLS" && pwd)
for f in artifact-endpoint.txt artifact-bucket.txt binid.txt binsec.txt; do
  [ -f "$S/$f" ] || refuse "$S/$f is missing (the artifact bucket, as build-upload.sh reads it)"
done
COMMIT=$(git -C "$ROOT" rev-parse --verify "$COMMIT_ARG^{commit}") || refuse "$COMMIT_ARG is not a commit"

# 2.
STATUS=$("$ROOT/scripts/dev/ci-status.sh" "$COMMIT" 2>&1) || true
printf '%s\n' "$STATUS"
printf '%s\n' "$STATUS" | grep -qx CI_GREEN || refuse "CI is not green for $COMMIT (ci-status.sh did not print CI_GREEN)"

# 3.
mkdir -p "$RELEASES"
mkdir "$CUT"
TREE=$CUT/tree
cleanup() {
  git -C "$ROOT" worktree remove --force "$TREE" 2>/dev/null || true
  rm -rf "$CUT"
}
trap cleanup EXIT
git -C "$ROOT" worktree add --detach "$TREE" "$COMMIT" > /dev/null
[ -z "$(git -C "$TREE" status --porcelain --ignored)" ] || refuse "the worktree at $COMMIT is not clean"

# 4.
export STREAMS_GIT_COMMIT=$COMMIT BUILD_GIT_COMMIT=$COMMIT
BUILD_UNIX=$(date +%s)
export BUILD_UNIX SOURCE_DATE_EPOCH=$BUILD_UNIX
(cd "$TREE" && cargo zigbuild --locked --release --target x86_64-unknown-linux-musl --bin streams-slate --bin s3lite)
BINS=$TREE/target/x86_64-unknown-linux-musl/release

# 5.
(cd "$TREE/deploy/composer" && bun install --frozen-lockfile)
ENTRY_JS=$CUT/entry.js
(cd "$TREE" && bun build deploy/composer/streams/entry.ts --target=bun --format=esm --outfile "$ENTRY_JS")

# 6.
STAGED=$CUT/release
(cd "$TREE" && bun deploy/composer/release.ts stage --tree "$TREE" --commit "$COMMIT" --tag "$TAG" \
  --build-unix "$BUILD_UNIX" --cells "$CELLS" --server "$BINS/streams-slate" --s3lite "$BINS/s3lite" \
  --entry-js "$ENTRY_JS" --node-modules "$TREE/deploy/composer/node_modules" --out "$STAGED")

# 7.
python3 -E - "$STAGED/deploy/composer/streams/streams-slate" "releases/$TAG/streams-slate" "$S" "$RESULTS" \
  "$COMMIT" "$BUILD_UNIX" <<'PY'
import hashlib, json, os, sys
import boto3
path, key, soak, results, commit, build_unix = sys.argv[1:]
r = lambda f: open(os.path.join(soak, f)).read().strip()
c = boto3.client("s3", endpoint_url=r("artifact-endpoint.txt"), aws_access_key_id=r("binid.txt"),
                 aws_secret_access_key=r("binsec.txt"), region_name="auto")
bucket = r("artifact-bucket.txt")
data = open(path, "rb").read()
c.put_object(Bucket=bucket, Key=key, Body=data)
head = c.get_object(Bucket=bucket, Key=key, Range="bytes=0-15")["Body"].read()
tail = c.get_object(Bucket=bucket, Key=key, Range=f"bytes={len(data) - 16}-{len(data) - 1}")["Body"].read()
if head != data[:16] or tail != data[-16:]:
    sys.exit(f"cut-release: refused: the ranged GETs of {key} do not return what was uploaded")
manifest = {key: {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
                  "gitCommit": commit, "buildUnix": build_unix}}
os.makedirs(results)
with open(os.path.join(results, "binaries.json"), "w") as f:
    json.dump(manifest, f, indent=1)
print(f"uploaded and verified {key} ({len(data)} bytes); {results}/binaries.json")
PY

# 8.
[ ! -e "$FINAL" ] || refuse "$FINAL appeared during the cut"
mv "$STAGED" "$FINAL"
echo "cut-release: $TAG is $FINAL (commit $COMMIT); deploy it with deploy/composer/deploy-release.sh"
