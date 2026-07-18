#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19503}"
STREAMS_PORT="${STREAMS_PORT:-18093}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""

cleanup() {
  if [[ -n "${STREAMS_PID}" ]]; then
    kill "${STREAMS_PID}" 2>/dev/null || true
    wait "${STREAMS_PID}" 2>/dev/null || true
  fi
  if [[ -n "${S3_PID}" ]]; then
    kill "${S3_PID}" 2>/dev/null || true
    wait "${S3_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
KEY="$(${TARGET_DIR}/streams-keys generate)"
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket conformance --region auto \
  --access-key-id test --secret-access-key test --initial-shards 4 \
  --flush-interval-ms "${CONFORMANCE_FLUSH_INTERVAL_MS:-10}" \
  --conformance-default-key "${KEY}" \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!

attempts=0
until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    tail -100 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done

# Pin the external contract rather than silently accepting upstream changes.
# Package 0.3.6's CLI passes a node_modules path to Vitest 4, which Vitest
# excludes even when explicitly filtered. Copy the published bundled runner
# unchanged into the scratch directory and invoke the pinned runner directly.
(
  cd "${TMP_DIR}"
  npm install --no-save --no-audit --no-fund \
    @durable-streams/server-conformance-tests@0.3.6 vitest@4.0.17 >/dev/null
  cp -R node_modules/@durable-streams/server-conformance-tests/dist conformance-dist
  mv conformance-dist/test-runner.js conformance-dist/test-runner.test.js
  vitest_args=(
    run conformance-dist/test-runner.test.js
    --no-coverage
    --reporter="${CONFORMANCE_REPORTER:-dot}"
    --passWithNoTests=false
  )
  if [[ -n "${CONFORMANCE_TEST_FILTER:-}" ]]; then
    vitest_args+=(-t "${CONFORMANCE_TEST_FILTER}")
  fi
  CONFORMANCE_TEST_URL="${STREAMS_URL}" \
    ./node_modules/.bin/vitest "${vitest_args[@]}"
)
