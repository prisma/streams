#!/bin/bash
# SR2-5/SR3-4: release-candidate promotion — the ONE certification
# boundary. `slate` stays an unprotected integration branch; a RELEASE
# CANDIDATE is an annotated tag minted only from a certified tree.
#
#   scripts/promote-rc.sh <commit-ish> <rc-tag> \
#     [--capacity-run <id> --handoff-run <id> ...]   # rc-certify field legs
#   scripts/promote-rc.sh <commit-ish> <rc-tag> \
#     --shape single-cell --artifact-run <id> ...      # single-cell, fleet off
#
# Everything after the tag goes to scripts/rc-certify.sh as given; its
# usage names both shapes' arguments (docs/OPS-RELEASE.md §7).
#
# Round-3 finding 4 hardening:
#   * every local gate runs in a CLEAN DETACHED WORKTREE at the exact
#     SHA — never the (possibly dirty) working directory;
#   * the worktree is verified pristine with porcelain v1 INCLUDING
#     untracked files before and after the gates;
#   * the GitHub check (scripts/required-checks.sh) asserts every
#     attempt of EVERY required named job in the push runs at that SHA,
#     not one run's overall conclusion;
#   * rc-certify.sh (the exact-binary certification path: local gate,
#     x86_64-musl binary sha as RC identity, capacity + handoff
#     manifests) is SUBSUMED when field-run ids are supplied — there is
#     one definition of "certified", not two;
#   * the tag records the server-binary digest, the SDK tarball's
#     sha256 and npm integrity (sha512), and the explicitly
#     NOT-certified legs; the tarball itself is kept in
#     target/rc/<rc-tag>/, which scripts/publish-sdk.sh publishes.
set -euo pipefail
cd "$(dirname "$0")/.."
COMMIT=${1:?usage: promote-rc.sh <commit-ish> <rc-tag> [<rc-certify.sh arguments, either shape> | --field-legs manifest.json]}
TAG=${2:?usage: promote-rc.sh <commit-ish> <rc-tag> [<rc-certify.sh arguments, either shape> | --field-legs manifest.json]}
shift 2
CAP_ARGS=()
FIELD_LEGS=""
if [ "${1:-}" = "--field-legs" ]; then
  # rc.3-class evidence (round-12 review): the certification is a set
  # of directed field legs + an exact-artifact canary run, carried in
  # a prepared evidence manifest. The manifest names the certified
  # server binary sha256 (the artifact the canary battery actually
  # exercised), every field run id with its stage-manifest hash, the
  # profile and harness hashes, and the tag's explicit exclusions.
  FIELD_LEGS=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
  [ -s "$FIELD_LEGS" ] || { echo "FAIL: --field-legs manifest $2 missing"; exit 1; }
  shift 2
fi
[ $# -gt 0 ] && CAP_ARGS=("$@")
SHA=$(git rev-parse --verify "$COMMIT^{commit}")
# A tag that exists is refused now, not after hours of gates.
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "FAIL: tag $TAG exists"
  exit 1
fi
# The SDK tarball of the tag's commit is packed into, and kept in, its own
# directory; scripts/publish-sdk.sh publishes exactly that file. A
# directory left by an earlier promotion is never overwritten, and one this
# run makes is removed when it mints no tag (the EXIT trap).
SDK_DIR="$PWD/target/rc/$TAG"
SDK_DIR_MADE=""
if [ -e "$SDK_DIR" ]; then
  echo "FAIL: $SDK_DIR exists (an earlier promotion's kept SDK tarball); remove it if no tag $TAG was minted from it"
  exit 1
fi

echo "== promote-rc: $SHA -> $TAG =="

echo "== 1/4 required jobs at $SHA =="
# Every attempt of every required job in the push runs at the SHA
# succeeded (scripts/required-checks.sh, the owner's round-3 call 8; its
# header gives the rule). scripts/publish-sdk.sh reads the tag's commit by
# the same script, so publishing applies exactly this certification.
CHECKS_OUT=$(scripts/required-checks.sh "$SHA") || { echo "$CHECKS_OUT"; echo "FAIL: a required job is not green at $SHA"; exit 1; }
echo "$CHECKS_OUT"
REQUIRED_JOBS=$(printf '%s\n' "$CHECKS_OUT" | sed -n "s/^REQUIRED_CHECKS_GREEN $SHA: //p")
[ -n "$REQUIRED_JOBS" ] || { echo "FAIL: scripts/required-checks.sh printed no REQUIRED_CHECKS_GREEN for $SHA"; exit 1; }

echo "== 2/4 clean detached worktree at $SHA =="
WORKTREE=$(mktemp -d /tmp/promote-rc.XXXXXX)
cleanup() {
  git worktree remove --force "$WORKTREE" 2>/dev/null || true
  if [ -n "$SDK_DIR_MADE" ] && ! git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    rm -rf "$SDK_DIR"
  fi
}
trap cleanup EXIT
git worktree add --detach "$WORKTREE" "$SHA" >/dev/null
DIRTY=$(git -C "$WORKTREE" status --porcelain=v1 --untracked-files=all)
[ -z "$DIRTY" ] || { echo "FAIL: fresh worktree not pristine:"; echo "$DIRTY"; exit 1; }

echo "== 3/4 certification inside the worktree =="
if [ ${#CAP_ARGS[@]} -gt 0 ]; then
  # Full exact-binary certification incl. field-campaign manifests.
  (cd "$WORKTREE" && SOAK_HOME="${SOAK_HOME:?rc-certify needs SOAK_HOME}" \
    bash scripts/rc-certify.sh "${CAP_ARGS[@]}")
else
  echo "NOTE: no field-run ids supplied — running the LOCAL half only;"
  echo "      the tag will record capacity/handoff as NOT certified."
  (cd "$WORKTREE" && bash scripts/release-gate.sh && bash scripts/multitenancy-audit.sh)
fi
(cd "$WORKTREE" && node scripts/mt-noisy-campaign.mjs)
DIRTY=$(git -C "$WORKTREE" status --porcelain=v1 --untracked-files=all | grep -v "^?? target" | grep -v "^?? sdk/node_modules" || true)
[ -z "$DIRTY" ] || { echo "FAIL: gates modified the tree:"; echo "$DIRTY"; exit 1; }

echo "== 4/4 artifact digests =="
# The RC's server identity is the FIELD-CERTIFIED campaign artifact
# (review round 3): rc-certify.sh writes a machine-readable manifest
# and its server_sha256 is what the tag records. The locally rebuilt
# binary is hashed as a reproducibility measurement only — a differing
# local SHA (arch, libc, build metadata) never replaces the certified
# one, and the tag says which is which.
LOCAL_SHA=$(shasum -a 256 "$WORKTREE/target/release/streams-slate" | cut -d' ' -f1)
CERT_MANIFEST="$WORKTREE/target/rc-certify-manifest.json"
if [ -n "$FIELD_LEGS" ]; then
  # Evidence-manifest mint: the certified identity is the exact
  # artifact the canary battery ran (never the local macOS rebuild).
  # Round-13 fix 2: NOTHING in the manifest is trusted — every claim
  # is independently recomputed by the verifier (which gate.sh
  # self-tests with per-field mutations on every commit).
  : "${RC_ARTIFACT:?field-legs mode needs RC_ARTIFACT (the exact x86_64-musl binary)}"
  : "${RC_CANARY_LOG:?field-legs mode needs RC_CANARY_LOG}"
  python3 scripts/verify-rc-evidence.py "$FIELD_LEGS" --sha "$SHA" --repo . \
    --results "${SOAK_HOME:?field-legs mode needs SOAK_HOME}/results" \
    --binary "$RC_ARTIFACT" --canary-log "$RC_CANARY_LOG"
  SRV_SHA=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['server_binary_sha256'])" "$FIELD_LEGS")
  SRV_SHA_NOTE="exact-artifact canary-certified (evidence independently verified)"
  REPRO_LINE="local rebuild sha256 $LOCAL_SHA differs from the certified x86_64-musl artifact (expected across arch/libc; the certified SHA governs)"
  [ "$LOCAL_SHA" = "$SRV_SHA" ] && REPRO_LINE="local rebuild reproduces the certified artifact byte-for-byte"
elif [ ${#CAP_ARGS[@]} -gt 0 ]; then
  [ -s "$CERT_MANIFEST" ] || { echo "FAIL: rc-certify.sh left no manifest at $CERT_MANIFEST"; exit 1; }
  SRV_SHA=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['server_sha256'])" "$CERT_MANIFEST")
  SRV_SHA_NOTE="field-certified campaign artifact (rc-certify manifest)"
  if [ "$LOCAL_SHA" = "$SRV_SHA" ]; then
    REPRO_LINE="local rebuild reproduces the certified artifact byte-for-byte"
  else
    REPRO_LINE="local rebuild sha256 $LOCAL_SHA differs from the certified artifact (expected across arch/libc; the certified SHA governs)"
  fi
else
  SRV_SHA=$LOCAL_SHA
  SRV_SHA_NOTE="LOCAL build only — no field-certified artifact for this tag"
  REPRO_LINE="no field certification: local SHA recorded, NOT a certified artifact"
fi
# The build has just run, so the pack skips prepack's second build. Its
# --json answer names the tarball and npm's integrity, which must be the
# sha512 of the bytes kept: the digest the registry reports after a publish.
mkdir -p "$SDK_DIR"
SDK_DIR_MADE=1
SDK_PACK=$(cd "$WORKTREE/sdk" && npm ci >/dev/null 2>&1 && npm run build >/dev/null 2>&1 &&
  npm pack --ignore-scripts --json --pack-destination "$SDK_DIR" 2>/dev/null) ||
  { echo "FAIL: the SDK build or pack failed in $WORKTREE/sdk"; exit 1; }
SDK_NAME=$(printf '%s' "$SDK_PACK" | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["filename"])')
SDK_NPM_INTEGRITY=$(printf '%s' "$SDK_PACK" | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["integrity"])')
SDK_TGZ="$SDK_DIR/$SDK_NAME"
case "$SDK_NAME" in
  ''|*/*) echo "FAIL: npm pack named no tarball ($SDK_NAME)"; exit 1 ;;
esac
[ -f "$SDK_TGZ" ] || { echo "FAIL: npm pack left no $SDK_TGZ"; exit 1; }
SDK_SHA=$(shasum -a 256 "$SDK_TGZ" | cut -d' ' -f1)
SDK_INTEGRITY=$(python3 -c 'import base64, hashlib, sys; print("sha512-" + base64.b64encode(hashlib.sha512(open(sys.argv[1], "rb").read()).digest()).decode())' "$SDK_TGZ")
if [ "$SDK_INTEGRITY" != "$SDK_NPM_INTEGRITY" ]; then
  echo "FAIL: npm pack reported integrity $SDK_NPM_INTEGRITY, but $SDK_TGZ is $SDK_INTEGRITY"
  exit 1
fi

if [ ${#CAP_ARGS[@]} -gt 0 ]; then
  # rc-certify.sh states what its shape certified and, for a single-cell
  # RC, which fleet legs it waived (scripts/quality/test_rc_certify.py).
  FIELD_LINE=$(python3 -E -c 'import json, sys; print("\n- ".join(json.load(open(sys.argv[1]))["tag_lines"]))' "$CERT_MANIFEST")
else
  FIELD_LINE="capacity + handoff campaigns: NOT certified by this tag"
fi

if [ -n "$FIELD_LEGS" ]; then
  MSGFILE=$(mktemp /tmp/rc-tagmsg.XXXXXX)
  python3 - "$FIELD_LEGS" "$TAG" "$SHA" "$SRV_SHA" "$SDK_SHA" "$SDK_NAME" "$REPRO_LINE" "$SDK_INTEGRITY" > "$MSGFILE" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
tag, sha, srv, sdk, sdk_name, repro, sdk_integrity = sys.argv[2:9]
print(f"release candidate {tag}\n")
print(f"Certified at {sha} (all gates ran in a clean detached worktree):")
print("- every attempt of every required job green in the push runs at the SHA")
print("- local release gate + bare multitenancy audit")
print("- noisy-neighbor campaign at contract scale (locked thresholds)")
print("- exact-artifact canary battery on the tagged binary "
      f"(log sha256 {m['exact_artifact_canary']['log_sha256']})")
print(f"\nProfile under certification: {m['profile']['file']} "
      f"sha256 {m['profile']['sha256']}")
print("  " + " ".join(m["profile"]["pins"]))
print(f"Harness identity (git tree oids at the tag): "
      + " ".join(f"{k}={v}" for k, v in m["harness_trees"].items()))
print("\nDirected field legs (all EXACT reconciliation, zero lost acked records):")
for leg in m["field_legs"]:
    print(f"- {leg['leg']}: run {leg['run_id']} stage {leg['stage']} "
          f"[{leg['finding']}] stage-manifest sha256 {leg['stage_sha256']}")
print("\nArtifacts:")
print(f"- server binary sha256: {srv}  [exact-artifact canary-certified]")
print(f"- reproducibility:      {repro}")
print(f"- sdk tarball sha256:   {sdk} ({sdk_name})")
print(f"- sdk tarball sha512:   {sdk_integrity} (npm integrity)")
print("\nNOT certified by this tag (schedule before GA):")
for x in m["not_certified"]:
    print(f"- {x}")
PY
  git tag -a "$TAG" "$SHA" -F "$MSGFILE"
  rm -f "$MSGFILE"
  echo "PROMOTE_RC_OK: annotated tag $TAG at $SHA (push with: git push origin $TAG)"
  echo "NOTE: the SDK tarball packed at the certified commit is kept at $SDK_TGZ (scripts/publish-sdk.sh $TAG)"
  exit 0
fi

git tag -a "$TAG" "$SHA" -m "release candidate $TAG

Certified at $SHA (all gates ran in a clean detached worktree):
- every attempt of every required job green in the push runs at the SHA: $REQUIRED_JOBS
- local release gate + bare multitenancy audit
- noisy-neighbor campaign at contract scale (locked thresholds)
- $FIELD_LINE

Artifacts:
- server binary sha256: $SRV_SHA  [$SRV_SHA_NOTE]
- reproducibility:      $REPRO_LINE
- sdk tarball sha256:   $SDK_SHA ($SDK_NAME)
- sdk tarball sha512:   $SDK_INTEGRITY (npm integrity)

NOT certified by this tag (schedule before GA):
- fleet owner-movement campaign at scale
- billing reconciliation against a live month
- external security review
- FLEET_AUTH_MODE=workload deployment posture (the binary refuses
  static under STREAMS_RELEASE_POSTURE=1; the platform must mint the
  JWTs)"
echo "PROMOTE_RC_OK: annotated tag $TAG at $SHA (push with: git push origin $TAG)"
echo "NOTE: the SDK tarball packed at the certified commit is kept at $SDK_TGZ (scripts/publish-sdk.sh $TAG)"
echo "NOTE: protect the rc/GA tag pattern from force updates in the repo settings,"
echo "      and prefer signing (git tag -s) once a release key exists."
