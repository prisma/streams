#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19564}"
STREAMS_PORT="${STREAMS_PORT:-18164}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-release-soak"
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
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix release-soak-ci \
  --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --absorb-bytes 1 --absorb-age-secs 1 \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!

attempts=0
until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done

export SOAK_STREAM_KEY
SOAK_STREAM_KEY="$(${TARGET_DIR}/streams-keys generate)"
export SOAK_AUTH_TOKEN="${AUTH_TOKEN}"
export SOAK_OPERATOR_TOKEN="${AUTH_TOKEN}"
python3 scripts/release-soak.py \
  --url "${STREAMS_URL}" --metrics-url "${STREAMS_URL}" \
  --metrics-url "http://localhost:${STREAMS_PORT}" \
  --bench-bin "${TARGET_DIR}/bench" --evidence "${TMP_DIR}/evidence.json" \
  --release-id ci-short --target-label hermetic-s3lite \
  --instance-class local-process --storage-provider s3lite \
  --duration-secs 3 --warmup-secs 0 --monitor-secs 1 --drain-secs 8 \
  --concurrency 4 --streams 2 --payload-bytes 128 --allow-short \
  --max-p99-ms 2000 --max-p999-ms 5000 \
  >"${TMP_DIR}/soak.stdout"

python3 - "${TMP_DIR}/evidence.json" <<'PY'
import json
import os
import pathlib
import sys

evidence = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert evidence["format_version"] == 1
assert evidence["status"] == "pass"
assert evidence["workload"]["short_run"] is True
assert evidence["target"]["metrics_targets"] == 2
assert evidence["monitor"]["samples"] >= 6
assert all(item["passed"] for item in evidence["checks"].values())
raw = pathlib.Path(sys.argv[1]).read_text()
assert os.environ["SOAK_STREAM_KEY"] not in raw
assert os.environ["SOAK_AUTH_TOKEN"] not in raw
assert os.environ["SOAK_OPERATOR_TOKEN"] not in raw
PY

if python3 scripts/release-soak.py \
  --url "${STREAMS_URL}" --metrics-url "${STREAMS_URL}" \
  --bench-bin "${TARGET_DIR}/bench" --evidence "${TMP_DIR}/rejected.json" \
  --release-id ci-rejected-budget --target-label hermetic-s3lite \
  --instance-class local-process --storage-provider s3lite \
  --duration-secs 1 --warmup-secs 0 --monitor-secs 1 --drain-secs 0 \
  --concurrency 1 --streams 1 --payload-bytes 64 --allow-short \
  --min-req-per-sec 1000000000000 --max-p99-ms 5000 --max-p999-ms 5000 \
  >"${TMP_DIR}/rejected.stdout"; then
  echo "release soak accepted an impossible throughput budget" >&2
  exit 1
fi
python3 - "${TMP_DIR}/rejected.json" <<'PY'
import json
import pathlib
import sys

evidence = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert evidence["status"] == "fail"
assert evidence["checks"]["throughput"]["passed"] is False
PY

echo "target-hardware release-soak harness smoke passed"
