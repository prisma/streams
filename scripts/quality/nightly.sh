#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
NIGHTLY=$(python3 -c 'import tomllib; print(tomllib.load(open("quality-tools.toml","rb"))["nightly"])')
HOST=$(rustc "+$NIGHTLY" -vV | sed -n 's/^host: //p')
[[ -n "$HOST" ]]
case "${1:?expected miri, corpus, or fuzz}" in
  miri)
    cargo "+$NIGHTLY" miri test --locked --lib retained_bytes::tests
    cargo "+$NIGHTLY" miri test --locked --lib application::read_batch::tests::o2a_
    ;;
  corpus)
    cargo "+$NIGHTLY" fuzz run postings --target "$HOST" fuzz/corpus/postings -- -runs=0
    ;;
  fuzz)
    # A writable copy avoids automatically committing new unreviewed seeds.
    FUZZ_OUT=${QUALITY_FUZZ_OUT:-target/quality-fuzz}
    mkdir -p "$FUZZ_OUT/corpus" "$FUZZ_OUT/artifacts"
    cp fuzz/corpus/postings/* "$FUZZ_OUT/corpus/"
    cargo "+$NIGHTLY" fuzz run postings --target "$HOST" "$FUZZ_OUT/corpus" -- \
      -max_total_time=120 -timeout=10 -max_len=32769 -rss_limit_mb=2048 \
      "-artifact_prefix=$FUZZ_OUT/artifacts/"
    ;;
  *) echo 'unknown verification mode' >&2; exit 2 ;;
esac
