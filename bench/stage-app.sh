#!/bin/bash
# Stage one deploy/<app> wrapper into the campaign app directory a Compute
# deploy runs from (`--path .`), so that the directory holds exactly the
# repo's sources plus the installed node_modules. Every campaign deploy
# path calls this first.
#
#   bench/stage-app.sh app-server "$SOAK_HOME/app-server-eu-central-1"
#
# Why (item 39, plan decision D11): campaign scripts deploy from copies
# under $SOAK_HOME, because those carry node_modules and the repo does not.
# Nothing refreshed a copy once it existed, so a copy staged before item 39
# kept the wrapper that holds every death. Behind it, streams-slate's exit 1
# after a critical loop's exit (item 38) becomes a 500 diagnostic the
# platform never replaces, where the old binary at least kept serving.
#
# And the deploy ships the whole directory: a stray file ships with it.
# Bun loads a `.env` (or `.env.local`, ...) it finds there on its own, so a
# stale dotfile would change the deployed configuration behind the
# environment the operator exported and compared (second external review).
#
# So, in this order, before anything is installed or replaced:
#   1. The source: every entry of deploy/<app> except node_modules,
#      hidden ones included, must be a regular file (not a symlink).
#   2. The existing staged directory, if any: every entry, hidden ones
#      included, must be one of those file names as a regular file, or
#      node_modules as a real directory. Anything else (a dotfile, another
#      directory, a symlink) is refused, and named, for the operator to
#      remove by hand: this script deletes nothing it did not stage.
#   3. A FRESH directory next to it gets exactly the source files; the old
#      node_modules moves in only if an install succeeded for exactly this
#      manifest (a marker in node_modules records the manifest's hash),
#      otherwise `bun install` (frozen when bun.lock exists) runs there, its
#      output shown, and a failure refuses the deploy.
#   4. The fresh directory must then list exactly the source files plus
#      node_modules, byte-identical, before it replaces the staged one.
# The marker proves an install once succeeded for a manifest; it is not an
# integrity check of node_modules.
set -euo pipefail
APP=${1:?usage: stage-app.sh <app-server|app-lb|app-gen> <staged app directory>}
DIR=${2:?usage: stage-app.sh <app-server|app-lb|app-gen> <staged app directory>}
case "$APP" in
  app-server|app-lb|app-gen) ;;
  *) echo "stage-app: unknown wrapper app '$APP'" >&2; exit 1 ;;
esac
SRC="$(cd "$(dirname "$0")/.." && pwd)/deploy/$APP"
[ -f "$SRC/index.ts" ] && [ -f "$SRC/supervise.ts" ] || {
  echo "stage-app: $SRC is not a wrapper app (no index.ts/supervise.ts)" >&2; exit 1; }
DIR="${DIR%/}"
refuse() { echo "stage-app: $*; refusing to deploy" >&2; exit 1; }

# Every entry of a directory, hidden ones included, one name per line.
entries() { find "$1" -mindepth 1 -maxdepth 1 -exec basename {} \; | LC_ALL=C sort; }

# 1. The source allowlist.
FILES=()
while IFS= read -r name; do
  [ "$name" = node_modules ] && continue
  { [ -f "$SRC/$name" ] && [ ! -L "$SRC/$name" ]; } ||
    refuse "deploy/$APP/$name is not a regular file; extend stage-app.sh"
  FILES+=("$name")
done < <(entries "$SRC")
allowed() {
  local f
  for f in "${FILES[@]}"; do [ "$f" = "$1" ] && return 0; done
  return 1
}

# 2. The existing staged directory.
if [ -L "$DIR" ]; then refuse "$DIR is a symlink"; fi
if [ -e "$DIR" ]; then
  [ -d "$DIR" ] || refuse "$DIR is not a directory"
  STRAY=""
  while IFS= read -r name; do
    path="$DIR/$name"
    if [ "$name" = node_modules ]; then
      { [ -d "$path" ] && [ ! -L "$path" ]; } || STRAY="$STRAY $name"
    elif ! allowed "$name" || [ ! -f "$path" ] || [ -L "$path" ]; then
      STRAY="$STRAY $name"
    fi
  done < <(entries "$DIR")
  [ -z "$STRAY" ] ||
    refuse "$DIR holds entries deploy/$APP does not (the deploy would ship them):$STRAY; remove them"
fi

# 3. A fresh directory with exactly the source files.
FRESH="$DIR.staging.$$"
rm -rf "$FRESH"
mkdir -p "$FRESH"
trap 'rm -rf "$FRESH"' EXIT
for name in "${FILES[@]}"; do cp "$SRC/$name" "$FRESH/$name"; done
MANIFEST=$(cat "$FRESH/package.json" "$FRESH/bun.lock" 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
if [ "$(cat "$DIR/node_modules/.stage-app-installed" 2>/dev/null)" = "$MANIFEST" ]; then
  mv "$DIR/node_modules" "$FRESH/node_modules"
else
  LOCKED=()
  [ -f "$FRESH/bun.lock" ] && LOCKED=(--frozen-lockfile)
  (cd "$FRESH" && bun install ${LOCKED[@]+"${LOCKED[@]}"}) || refuse "bun install failed for deploy/$APP"
  [ -d "$FRESH/node_modules" ] || refuse "bun install left no node_modules for deploy/$APP"
  echo "$MANIFEST" > "$FRESH/node_modules/.stage-app-installed"
fi

# 4. Exactly the sources plus node_modules, then swap.
[ "$(entries "$FRESH")" = "$(printf '%s\n' "${FILES[@]}" node_modules | LC_ALL=C sort)" ] ||
  refuse "the fresh staging directory is not exactly deploy/$APP plus node_modules"
for name in "${FILES[@]}"; do
  cmp -s "$SRC/$name" "$FRESH/$name" || refuse "$FRESH/$name differs from deploy/$APP/$name"
done
if [ -e "$DIR" ]; then
  OLD="$DIR.replaced.$$"
  mv "$DIR" "$OLD"
  mv "$FRESH" "$DIR"
  rm -rf "$OLD"
else
  mv "$FRESH" "$DIR"
fi
trap - EXIT
echo "staged deploy/$APP -> $DIR (${#FILES[@]} files and node_modules)"
