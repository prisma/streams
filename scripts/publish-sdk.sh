#!/bin/bash
# Publish the SDK tarball a release candidate's tag pins (promote-rc's pack
# of the certified commit; no gate runs the tarball itself, CI smoked an
# equivalent pack) to npm. Owner-run: publication is a release, which
# stays the owner's hold.
#
#   scripts/publish-sdk.sh <rc-tag>             # checks, then npm publish
#   scripts/publish-sdk.sh --dry-run <rc-tag>   # the checks only (steps 1-3)
#
# The tarball is the one scripts/promote-rc.sh packed at the tag's commit
# and kept in target/rc/<rc-tag>/; the tag message names it and records its
# sha256 and npm integrity (sha512). The steps:
#   1. the tag is annotated, and CI is green at its commit
#      (scripts/dev/ci-status.sh prints CI_GREEN);
#   2. the kept tarball's sha256 and integrity equal the tag's, it is the
#      @prisma/streams package of the version the tag is a release
#      candidate of (v<version>-rc.<n>: the SDK and the server move in
#      lockstep), its README.md holds no placeholder of the beta (a line
#      with a <...> and "published with the beta" or "before publishing"),
#      and its CHANGELOG.md's first release heading is "## <version>";
#   3. its version is not on npm yet;
#   4. with no npm token in the environment, `npm publish` under the beta
#      dist-tag: npm asks the owner's account for its 2FA code, so no token
#      is in argv, the environment or CI;
#   5. the registry's integrity for the version equals the tarball's, and
#      the package's dist-tags are printed (where `latest` points is the
#      owner's call after this readback);
#   6. the release-notes line.
set -euo pipefail
cd "$(dirname "$0")/.."
PACKAGE=@prisma/streams
DIST_TAG=beta
# Every npm call names the public registry: an @prisma scoped registry in
# an npmrc (an internal one, GitHub Packages) would otherwise take the
# publish and the readback, and nothing would reach npmjs.org.
REGISTRY=https://registry.npmjs.org/
READBACK_TRIES=${PUBLISH_SDK_READBACK_TRIES:-12}
READBACK_SECS=${PUBLISH_SDK_READBACK_SECS:-5}
DRY_RUN=false
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=true
  shift
fi
TAG=${1:?usage: publish-sdk.sh [--dry-run] <rc-tag>}
fail() {
  echo "PUBLISH_SDK_FAIL: $*"
  exit 1
}
ERR=$(mktemp "${TMPDIR:-/tmp}/publish-sdk.XXXXXX")
trap 'rm -f "$ERR"' EXIT

echo "== 1/6 annotated tag $TAG and CI at its commit =="
[ "$(git cat-file -t "refs/tags/$TAG" 2>/dev/null)" = tag ] || fail "$TAG is not an annotated tag"
SHA=$(git rev-parse --verify "refs/tags/$TAG^{commit}")
CI_OUT=$(scripts/dev/ci-status.sh "$SHA") || { echo "$CI_OUT"; fail "CI is not green at $SHA"; }
echo "$CI_OUT"
[ "$(printf '%s\n' "$CI_OUT" | tail -n 1)" = CI_GREEN ] || fail "scripts/dev/ci-status.sh printed no CI_GREEN for $SHA"

echo "== 2/6 the kept tarball is the one the tag pins =="
MSG=$(git for-each-ref --format='%(contents)' "refs/tags/$TAG")
SHA256_LINE=$(printf '%s\n' "$MSG" | grep -E '^- sdk tarball sha256: +[0-9a-f]{64} \(' || true)
SHA512_LINE=$(printf '%s\n' "$MSG" | grep -E '^- sdk tarball sha512: +sha512-' || true)
[ -n "$SHA256_LINE" ] && [ "$(printf '%s\n' "$SHA256_LINE" | wc -l | tr -d ' ')" = 1 ] ||
  fail "$TAG's message records no single sdk tarball sha256"
[ -n "$SHA512_LINE" ] && [ "$(printf '%s\n' "$SHA512_LINE" | wc -l | tr -d ' ')" = 1 ] ||
  fail "$TAG's message records no single sdk tarball sha512 (a tag promote-rc minted before it kept the tarball)"
TAG_SHA256=$(printf '%s\n' "$SHA256_LINE" | sed -E 's/^- sdk tarball sha256: +([0-9a-f]{64}) \(.*$/\1/')
NAME=$(printf '%s\n' "$SHA256_LINE" | sed -E 's/^- sdk tarball sha256: +[0-9a-f]{64} \((.*)\)$/\1/')
TAG_INTEGRITY=$(printf '%s\n' "$SHA512_LINE" | sed -E 's|^- sdk tarball sha512: +(sha512-[A-Za-z0-9+/=]+).*$|\1|')
case "$NAME" in
  ''|*/*|.*) fail "$TAG names no tarball file ($NAME)" ;;
esac
TGZ="$PWD/target/rc/$TAG/$NAME"
[ -f "$TGZ" ] || fail "$TGZ is missing: run this where scripts/promote-rc.sh minted $TAG"
SHA256=$(shasum -a 256 "$TGZ" | cut -d' ' -f1)
INTEGRITY=$(python3 -c 'import base64, hashlib, sys; print("sha512-" + base64.b64encode(hashlib.sha512(open(sys.argv[1], "rb").read()).digest()).decode())' "$TGZ")
[ "$SHA256" = "$TAG_SHA256" ] || fail "$TGZ has sha256 $SHA256; $TAG records $TAG_SHA256"
[ "$INTEGRITY" = "$TAG_INTEGRITY" ] || fail "$TGZ has integrity $INTEGRITY; $TAG records $TAG_INTEGRITY"
MANIFEST=$(tar -xzOf "$TGZ" package/package.json) || fail "$TGZ holds no package/package.json"
NAME_IN_TGZ=$(printf '%s' "$MANIFEST" | python3 -c 'import json, sys; print(json.load(sys.stdin)["name"])')
VERSION=$(printf '%s' "$MANIFEST" | python3 -c 'import json, sys; print(json.load(sys.stdin)["version"])')
[ "$NAME_IN_TGZ" = "$PACKAGE" ] || fail "$TGZ is $NAME_IN_TGZ, not $PACKAGE"
# npm versions are immutable: a version, a placeholder or an unreleased
# changelog published under the beta cannot be withdrawn and published again.
RC_N=${TAG#"v$VERSION-rc."}
case "$RC_N" in
  "$TAG"|''|*[!0-9]*) fail "$TGZ is $PACKAGE@$VERSION, but $TAG names no release candidate of it (v$VERSION-rc.<n>)" ;;
esac
README=$(tar -xzOf "$TGZ" package/README.md 2>/dev/null) || fail "$TGZ holds no package/README.md"
PLACEHOLDER=$(printf '%s\n' "$README" | grep -nE '<.*(published with the beta|before publishing)|(published with the beta|before publishing).*>' | head -n 1 | cut -d: -f1 || true)
[ -z "$PLACEHOLDER" ] || fail "$TGZ README.md line $PLACEHOLDER is a placeholder to fill before publishing"
CHANGES=$(tar -xzOf "$TGZ" package/CHANGELOG.md 2>/dev/null) || fail "$TGZ holds no package/CHANGELOG.md"
RELEASE=$(printf '%s\n' "$CHANGES" | grep -m 1 '^## ' || true)
[ "$RELEASE" = "## $VERSION" ] || fail "$TGZ CHANGELOG.md's first release heading is '$RELEASE', not '## $VERSION'"
echo "ok   $NAME: $PACKAGE@$VERSION, sha256 $SHA256, $INTEGRITY"

echo "== 3/6 $PACKAGE@$VERSION is not on npm yet =="
# npm answers E404 for a missing package (and, from npm 11, a missing
# version); older npm answers a missing version with nothing.
if VIEW=$(npm view "$PACKAGE@$VERSION" version --registry "$REGISTRY" 2>"$ERR"); then
  [ -z "$VIEW" ] || fail "$PACKAGE@$VERSION is already published"
elif grep -q 'E404' "$ERR"; then
  :
else
  cat "$ERR"
  fail "npm view $PACKAGE@$VERSION failed"
fi
echo "ok   not published"

if $DRY_RUN; then
  echo "PUBLISH_SDK_DRY_RUN_OK: would publish $TGZ as $PACKAGE@$VERSION under the dist-tag $DIST_TAG"
  exit 0
fi

echo "== 4/6 npm publish (npm asks for your 2FA code) =="
[ -z "${NPM_TOKEN:-}" ] || fail "NPM_TOKEN is set: unset it; the owner publishes with npm's 2FA, never a token"
[ -z "${NODE_AUTH_TOKEN:-}" ] || fail "NODE_AUTH_TOKEN is set: unset it; the owner publishes with npm's 2FA, never a token"
# Counted, not `grep -q`: a grep that stops reading at its first match
# kills env with SIGPIPE, and under pipefail that status read as "none".
# The count prints no value.
AUTH_VARS=$(env | grep -ciE '^npm_config_[^=]*_auth' || true)
[ "$AUTH_VARS" = 0 ] ||
  fail "an npm_config_*_auth* variable is set: unset it; the owner publishes with npm's 2FA, never a token"
npm publish "$TGZ" --tag "$DIST_TAG" --access public --registry "$REGISTRY"

echo "== 5/6 the registry's integrity and dist-tags =="
REMOTE=""
tries=0
while [ -z "$REMOTE" ] && [ "$tries" -lt "$READBACK_TRIES" ]; do
  tries=$((tries + 1))
  REMOTE=$(npm view "$PACKAGE@$VERSION" dist.integrity --registry "$REGISTRY" 2>/dev/null || true)
  [ -n "$REMOTE" ] || sleep "$READBACK_SECS"
done
[ -n "$REMOTE" ] || fail "the registry names no integrity for $PACKAGE@$VERSION after $READBACK_TRIES reads"
[ "$REMOTE" = "$INTEGRITY" ] || fail "the registry's integrity for $PACKAGE@$VERSION is $REMOTE; the tarball's is $INTEGRITY"
echo "ok   $PACKAGE@$VERSION $REMOTE"
npm dist-tag ls "$PACKAGE" --registry "$REGISTRY"

echo "== 6/6 release notes =="
echo "RELEASE_NOTES: $PACKAGE@$VERSION (npm dist-tag $DIST_TAG) published to $REGISTRY from release candidate $TAG at $SHA: $NAME sha256 $SHA256, integrity $INTEGRITY"
echo "PUBLISH_SDK_OK"
