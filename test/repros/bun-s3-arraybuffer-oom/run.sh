#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK_NAME="bun-s3-arraybuffer-oom-${RANDOM}-${RANDOM}"
MINIO_NAME="bun-s3-arraybuffer-oom-minio-${RANDOM}-${RANDOM}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"
BUN_IMAGE="${BUN_IMAGE:-oven/bun:1.3.11}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_PASSWORD:-minioadmin}"
BUCKET="${BUCKET:-streams-rss-test}"
PAYLOAD_MIB="${PAYLOAD_MIB:-8}"
TARGET_RSS_MIB="${TARGET_RSS_MIB:-900}"
MAX_ITERATIONS="${MAX_ITERATIONS:-256}"
REPORT_EVERY="${REPORT_EVERY:-8}"
EXIT_ON_THRESHOLD="${EXIT_ON_THRESHOLD:-1}"

cleanup() {
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$NETWORK_NAME" >/dev/null
docker run -d --rm \
  --name "$MINIO_NAME" \
  --network "$NETWORK_NAME" \
  -e "MINIO_ROOT_USER=$MINIO_USER" \
  -e "MINIO_ROOT_PASSWORD=$MINIO_PASSWORD" \
  "$MINIO_IMAGE" \
  server /data --console-address :9001 >/dev/null

for _ in $(seq 1 120); do
  if docker run --rm --network "$NETWORK_NAME" --entrypoint sh "$MC_IMAGE" -lc \
    "mc alias set local http://$MINIO_NAME:9000 $MINIO_USER $MINIO_PASSWORD >/dev/null && mc ready local >/dev/null"; then
    break
  fi
  sleep 0.25
done

docker run --rm --network "$NETWORK_NAME" --entrypoint sh "$MC_IMAGE" -lc \
  "mc alias set local http://$MINIO_NAME:9000 $MINIO_USER $MINIO_PASSWORD >/dev/null && mc mb -p local/$BUCKET >/dev/null 2>&1 || true"

docker run --rm \
  --memory=1g \
  --memory-swap=1g \
  --network "$NETWORK_NAME" \
  -e "S3_TEST_ENDPOINT=http://$MINIO_NAME:9000" \
  -e "S3_TEST_BUCKET=$BUCKET" \
  -e "S3_TEST_ACCESS_KEY_ID=$MINIO_USER" \
  -e "S3_TEST_SECRET_ACCESS_KEY=$MINIO_PASSWORD" \
  -e "PAYLOAD_MIB=$PAYLOAD_MIB" \
  -e "TARGET_RSS_MIB=$TARGET_RSS_MIB" \
  -e "MAX_ITERATIONS=$MAX_ITERATIONS" \
  -e "REPORT_EVERY=$REPORT_EVERY" \
  -e "EXIT_ON_THRESHOLD=$EXIT_ON_THRESHOLD" \
  -v "$ROOT_DIR:/work" \
  -w /work \
  "$BUN_IMAGE" \
  bun run repro.ts
