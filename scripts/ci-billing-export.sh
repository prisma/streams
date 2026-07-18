#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19566}"
STREAMS_PORT="${STREAMS_PORT:-18166}"
PROXY_PORT="${PROXY_PORT:-18167}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PROXY_URL="http://127.0.0.1:${PROXY_PORT}"
AUTH_TOKEN="billing-export-ci"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
PROXY_PID=""

cleanup() {
  if [[ -n "${PROXY_PID}" ]]; then
    kill "${PROXY_PID}" 2>/dev/null || true
    wait "${PROXY_PID}" 2>/dev/null || true
  fi
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

metric_value() {
  local name="$1"
  curl --fail --silent "${STREAMS_URL}/v1/debug/metrics" \
    -H "authorization: Bearer ${AUTH_TOKEN}" |
    sed -n "s/^${name} //p"
}

wait_ready() {
  local attempts=0
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      tail -160 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

# Fail only the first exporter append path, immediately and outside the
# storage stack. This makes the retry assertion deterministic while preserving
# the exact HTTP body and idempotency headers observed on both attempts.
: >"${TMP_DIR}/fail-export"
python3 - "${PROXY_PORT}" "${STREAMS_URL}" "${TMP_DIR}" <<'PY' \
  >"${TMP_DIR}/proxy.log" 2>&1 &
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import urllib.error
import urllib.request

port = int(sys.argv[1])
target = sys.argv[2]
root = Path(sys.argv[3])
fail_flag = root / "fail-export"


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_PUT(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        marker = "|".join(
            self.headers.get(name, "")
            for name in ("producer-id", "producer-epoch", "producer-seq")
        ).encode()

        if self.command == "POST" and fail_flag.exists():
            (root / "failed.body").write_bytes(body)
            (root / "failed.headers").write_bytes(marker)
            response = b'{"error":"injected exporter failure"}'
            self.send_response(503)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
            return

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"host", "content-length", "connection"}
        }
        request = urllib.request.Request(
            target + self.path,
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            upstream = urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as error:
            upstream = error

        response = upstream.read()
        if self.command == "POST":
            (root / "success.body").write_bytes(body)
            (root / "success.headers").write_bytes(marker)
        self.send_response(upstream.status)
        content_type = upstream.headers.get("content-type")
        if content_type:
            self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_args):
        pass


ThreadingHTTPServer(("127.0.0.1", port), Proxy).serve_forever()
PY
PROXY_PID=$!

METRICS_KEY="$(${TARGET_DIR}/streams-keys generate)"
SOURCE_KEY="$(${TARGET_DIR}/streams-keys generate)"
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --s3-request-timeout-ms 500 \
  --path-prefix billing-export --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --metrics-key "${METRICS_KEY}" --metrics-lb-url "${PROXY_URL}" \
  --metrics-auth-token "${AUTH_TOKEN}" --metrics-customer-id __legacy__ \
  --metrics-export-interval-secs 3 --require-metrics-export \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
wait_ready

auth=(-H "authorization: Bearer ${AUTH_TOKEN}")
source=(-H "stream-encryption-key: ${SOURCE_KEY}" -H 'content-type: application/json')
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/source" \
  "${auth[@]}" "${source[@]}" -d '[]' >/dev/null
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/source" \
  "${auth[@]}" "${source[@]}" -d '{"bill":1}' >/dev/null

attempts=0
until [[ "$(metric_value streams_billing_export_healthy)" == "0" ]]; do
  attempts=$((attempts + 1))
  if (( attempts > 150 )); then
    echo "billing export failure did not become observable" >&2
    tail -160 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
(( $(metric_value streams_billing_export_failures_total) > 0 ))
rm -f "${TMP_DIR}/fail-export"

attempts=0
until [[ "$(metric_value streams_billing_export_healthy)" == "1" ]]; do
  attempts=$((attempts + 1))
  if (( attempts > 250 )); then
    echo "billing exporter did not retry its pending interval" >&2
    tail -160 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
cmp "${TMP_DIR}/failed.body" "${TMP_DIR}/success.body"
cmp "${TMP_DIR}/failed.headers" "${TMP_DIR}/success.headers"
[[ -s "${TMP_DIR}/failed.headers" ]]

attempts=0
until curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/__metrics__" \
    "${auth[@]}" -H "stream-encryption-key: ${METRICS_KEY}" \
    >"${TMP_DIR}/billing.json" && grep -q '"stream":"source"' "${TMP_DIR}/billing.json"; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "retry-stable interval did not reach the encrypted metrics stream" >&2
    exit 1
  fi
  sleep 0.1
done
grep -q '"appends":1' "${TMP_DIR}/billing.json"
[[ "$(grep -o '"process_id"' "${TMP_DIR}/billing.json" | wc -l | tr -d ' ')" == "1" ]]
[[ "$(metric_value streams_billing_export_configured)" == "1" ]]

# The exporter stream is excluded only for its exact configured customer/name;
# it must not meter itself into an endless one-record-per-interval loop.
sleep 4
curl --fail --silent "${STREAMS_URL}/v1/stream/__metrics__" "${auth[@]}" \
  -H "stream-encryption-key: ${METRICS_KEY}" >"${TMP_DIR}/billing-later.json"
[[ "$(grep -o '"process_id"' "${TMP_DIR}/billing-later.json" | wc -l | tr -d ' ')" == "1" ]]

echo "retry-stable non-recursive encrypted billing export passed"
