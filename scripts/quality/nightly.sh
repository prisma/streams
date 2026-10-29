#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/lib/python.sh
NIGHTLY=$(python3 -c 'import tomllib; print(tomllib.load(open("quality-tools.toml","rb"))["nightly"])')
HOST=$(rustc "+$NIGHTLY" -vV | sed -n 's/^host: //p')
[[ -n "$HOST" ]]
# Every fuzz target in fuzz/Cargo.toml, each with its saved corpus.
FUZZ_TARGETS=(seal_directory segment_chunk)
case "${1:?expected miri, corpus, or fuzz}" in
  miri)
    cargo "+$NIGHTLY" miri test --locked -p streams-quality-invariants --lib retained_bytes::tests
    cargo "+$NIGHTLY" miri test --locked -p streams-quality-invariants --lib application::read_batch::tests::o2a_
    ;;
  corpus)
    for target in "${FUZZ_TARGETS[@]}"; do
      cargo "+$NIGHTLY" fuzz run "$target" --target "$HOST" "fuzz/corpus/$target" -- -runs=0
    done
    ;;
  fuzz)
    # A writable copy avoids automatically committing new unreviewed seeds.
    FUZZ_OUT=${QUALITY_FUZZ_OUT:-target/quality-fuzz}
    for target in "${FUZZ_TARGETS[@]}"; do
      mkdir -p "$FUZZ_OUT/$target/corpus" "$FUZZ_OUT/$target/artifacts"
      cp "fuzz/corpus/$target/"* "$FUZZ_OUT/$target/corpus/"
      cargo "+$NIGHTLY" fuzz run "$target" --target "$HOST" "$FUZZ_OUT/$target/corpus" -- \
        -max_total_time=120 -timeout=10 -max_len=32769 -rss_limit_mb=2048 \
        "-artifact_prefix=$FUZZ_OUT/$target/artifacts/"
    done
    ;;
  *) echo 'unknown verification mode' >&2; exit 2 ;;
esac
