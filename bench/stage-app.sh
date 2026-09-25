#!/bin/bash
# Stage one deploy/<app> wrapper into the campaign app directory a Compute
# deploy runs from (`--path .`), and refuse to go on unless the staged
# sources are the repo's. Every campaign deploy path calls this first.
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
# It copies every file of deploy/<app> except node_modules (index.ts,
# supervise.ts, downloader.ts, package.json, bun.lock) over the staged
# copy, runs `bun install` until one has succeeded for the staged manifest
# (a marker in node_modules records it, so a failed install is retried by
# the next run, and its output is shown), and exits non-zero if any
# source still differs or the staged directory holds a file the repo does
# not (`--path .` would deploy it; remove it by hand).
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
mkdir -p "$DIR"

CHANGED=""
for f in "$SRC"/*; do
  name=$(basename "$f")
  [ "$name" = node_modules ] && continue
  if [ ! -f "$f" ]; then
    echo "stage-app: deploy/$APP/$name is not a regular file; extend stage-app.sh" >&2
    exit 1
  fi
  if ! cmp -s "$f" "$DIR/$name"; then
    cp "$f" "$DIR/$name"
    CHANGED="$CHANGED $name"
  fi
done
# Installed means: an install succeeded for exactly this manifest.
MANIFEST=$(cat "$DIR/package.json" "$DIR/bun.lock" 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
MARKER="$DIR/node_modules/.stage-app-installed"
if [ "$(cat "$MARKER" 2>/dev/null)" != "$MANIFEST" ]; then
  LOCKED=""
  [ -f "$DIR/bun.lock" ] && LOCKED=--frozen-lockfile
  if ! (cd "$DIR" && bun install $LOCKED); then
    echo "stage-app: bun install failed in $DIR; refusing to deploy" >&2
    exit 1
  fi
  echo "$MANIFEST" > "$MARKER"
fi

# The refusal: whatever happened above, deploy only the repo's sources.
for f in "$SRC"/*; do
  name=$(basename "$f")
  [ "$name" = node_modules ] && continue
  if ! cmp -s "$f" "$DIR/$name"; then
    echo "stage-app: $DIR/$name differs from deploy/$APP/$name; refusing to deploy" >&2
    exit 1
  fi
done
EXTRA=""
for f in "$DIR"/*; do
  name=$(basename "$f")
  [ "$name" = node_modules ] && continue
  [ -e "$SRC/$name" ] || EXTRA="$EXTRA $name"
done
if [ -n "$EXTRA" ]; then
  echo "stage-app: $DIR holds files deploy/$APP does not:$EXTRA; the deploy would ship them; remove them and rerun" >&2
  exit 1
fi
echo "staged deploy/$APP -> $DIR (updated:${CHANGED:- nothing})"
