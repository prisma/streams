#!/usr/bin/env bash
# build.sh: the rig's Linux binaries and image from this checkout.
# Builds streams-slate and s3lite from this checkout (so the server reports
# its commit) and bench with habanero's population mode for aarch64 Linux
# (glibc 2.39) with cargo zigbuild, niced and on 4 jobs, then the image
# `streams-ramp`. The population mode (population-mode.patch, from
# prisma/habanero#771's examples/edge-bench/services/bench-modes.patch) is
# applied to a scratch copy of HEAD under target/, never to this checkout.
# Also s3lite for the host (bin/s3lite-host): the legs whose store outgrows the
# container VM run it there (rig.HostStore).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
triple=aarch64-unknown-linux-gnu
build() { # dir bins...
  local dir=$1; shift
  local args=()
  for b in "$@"; do args+=(--bin "$b"); done
  (cd "$dir" && CARGO_TARGET_DIR=$root/target CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4} nice -n 19 \
    cargo zigbuild --locked --release --target "$triple.2.39" "${args[@]}")
}
mkdir -p "$here/bin"
build "$root" streams-slate s3lite
cp "$root/target/$triple/release/streams-slate" "$root/target/$triple/release/s3lite" "$here/bin/"
(cd "$root" && CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4} nice -n 19 cargo build --locked --release --bin s3lite)
cp "$root/target/release/s3lite" "$here/bin/s3lite-host"
src=$root/target/ramp-src
rm -rf "$src" && mkdir -p "$src"
git -C "$root" archive HEAD | tar -x -C "$src"
(cd "$src" && git init -q && git apply "$here/population-mode.patch")
# The scratch copy builds into its own target dir: one source path per dir.
(cd "$src" && CARGO_TARGET_DIR=$root/target/ramp-bench CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4} nice -n 19 \
  cargo zigbuild --locked --release --target "$triple.2.39" --bin bench)
cp "$root/target/ramp-bench/$triple/release/bench" "$here/bin/bench"
docker build -q -t streams-ramp "$here"
