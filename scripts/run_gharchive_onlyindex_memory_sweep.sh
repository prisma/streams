#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_SCRIPT="${ROOT_DIR}/scripts/run_gharchive_memory_sampler_experiment.sh"
SUMMARY_SCRIPT="${ROOT_DIR}/scripts/summarize_memory_sampler.ts"
REPORT_DATE="$(date '+%Y-%m-%d')"
REPORT_PATH="${1:-${ROOT_DIR}/experiments/reports/per-index-memory-sweep-${REPORT_DATE}.md}"
RUN_ROOT="${RUN_ROOT:-/tmp/streams-gharchive-onlyindex-sweep}"
DS_ROOT_BASE="${DS_ROOT_BASE:-/tmp/lib/prisma-streams-onlyindex-sweep}"
PORT="${PORT:-8870}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT}}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-60}"
INGEST_SAMPLES="${INGEST_SAMPLES:-0}"
RECOVERY_SAMPLES="${RECOVERY_SAMPLES:-2}"
MAX_INGEST_SECONDS="${MAX_INGEST_SECONDS:-900}"
SAMPLER_INTERVAL_MS="${SAMPLER_INTERVAL_MS:-1000}"
EXACT_TARGET_SEGMENTS="${EXACT_TARGET_SEGMENTS:-16}"
COMPANION_TARGET_SEGMENTS="${COMPANION_TARGET_SEGMENTS:-12}"
AUTO_TUNE_MB="${AUTO_TUNE_MB:-4096}"
BUN_BIN="${BUN_BIN:-$(command -v bun || true)}"

if [[ -z "${BUN_BIN}" ]]; then
  echo "bun binary not found in PATH" >&2
  exit 1
fi

default_selectors=(
  exact:eventType
  exact:ghArchiveId
  exact:actorLogin
  exact:repoName
  exact:repoOwner
  exact:orgLogin
  exact:action
  exact:refType
  exact:public
  exact:isBot
  col:eventTime
  col:public
  col:isBot
  col:commitCount
  col:payloadBytes
  col:payloadKb
  fts:eventType
  fts:repoOwner
  fts:action
  fts:refType
  fts:title
  fts:message
  fts:body
  agg:events
)

if [[ -n "${SELECTORS:-}" ]]; then
  selectors=("${(@s: :)SELECTORS}")
else
  selectors=("${default_selectors[@]}")
fi

mkdir -p "$(dirname "${REPORT_PATH}")" "${RUN_ROOT}" "${DS_ROOT_BASE}"

slugify() {
  local value="$1"
  value="${value//:/-}"
  value="${value//\//-}"
  echo "${value}"
}

json_value() {
  local file="$1"
  local path="$2"
  "${BUN_BIN}" -e '
    const fs = require("node:fs");
    const file = process.argv[1];
    const path = process.argv[2];
    const root = JSON.parse(fs.readFileSync(file, "utf8"));
    let current = root;
    for (const key of path.split(".")) {
      if (key === "") continue;
      if (current == null) {
        current = null;
        break;
      }
      current = current[key];
    }
    if (current == null) process.stdout.write("");
    else if (typeof current === "object") process.stdout.write(JSON.stringify(current));
    else process.stdout.write(String(current));
  ' "${file}" "${path}"
}

{
  echo "# GH Archive Per-Index Memory Sweep"
  echo
  echo "- date: ${REPORT_DATE}"
  echo "- base_url: ${BASE_URL}"
  echo "- sample_interval_seconds: ${SAMPLE_INTERVAL_SECONDS}"
  echo "- recovery_samples: ${RECOVERY_SAMPLES}"
  echo "- max_ingest_seconds: ${MAX_INGEST_SECONDS}"
  echo "- exact_target_segments: ${EXACT_TARGET_SEGMENTS}"
  echo "- companion_target_segments: ${COMPANION_TARGET_SEGMENTS}"
  echo
  echo "| selector | uploaded_segments | companion_objects | exact_index_count | peak_rss_bytes | peak_jsc_heap_size_bytes | recovery_rss_bytes | top_phase | run_dir |"
  echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |"
} > "${REPORT_PATH}"

for selector in "${selectors[@]}"; do
  slug="$(slugify "${selector}")"
  run_dir="${RUN_ROOT}/${slug}"
  ds_root="${DS_ROOT_BASE}/${slug}"
  stream_prefix="gharchive-onlyindex-${slug}"
  stream_name="${stream_prefix}-all"
  report_run_path="${run_dir}/report.md"
  server_log_path="${run_dir}/server.log"
  ingester_log_path="${run_dir}/ingester.log"
  sampler_path="${run_dir}/sampler.jsonl"
  summary_json_path="${run_dir}/summary.json"
  db_path="${ds_root}/wal.sqlite"
  target_segments="${COMPANION_TARGET_SEGMENTS}"
  if [[ "${selector}" == exact:* ]]; then
    target_segments="${EXACT_TARGET_SEGMENTS}"
  fi

  rm -rf -- "${run_dir}" "${ds_root}"
  mkdir -p "${run_dir}" "${ds_root}"

  export PORT BASE_URL AUTO_TUNE_MB SAMPLE_INTERVAL_SECONDS INGEST_SAMPLES RECOVERY_SAMPLES SAMPLER_INTERVAL_MS MAX_INGEST_SECONDS
  export DS_ROOT="${ds_root}"
  export GHARCHIVE_NOINDEX=0
  export GHARCHIVE_ONLYINDEX="${selector}"
  export TARGET_UPLOADED_SEGMENTS="${target_segments}"
  export STREAM_PREFIX="${stream_prefix}"
  export DS_DB_PATH="${db_path}"

  zsh "${RUN_SCRIPT}" "${report_run_path}" "${server_log_path}" "${ingester_log_path}" "${sampler_path}"

  sampler_files=("${sampler_path%.jsonl}"*.jsonl(N))
  if (( ${#sampler_files[@]} == 0 )); then
    echo "missing sampler files for ${selector}" >&2
    exit 1
  fi
  "${BUN_BIN}" run "${SUMMARY_SCRIPT}" "${sampler_files[@]}" > "${summary_json_path}"

  uploaded_segments="$(sqlite3 -noheader "${db_path}" "select coalesce(uploaded_segment_count, 0) from streams where stream='${stream_name}';" 2>/dev/null | tr -d '[:space:]')"
  companion_objects="$(sqlite3 -noheader "${db_path}" "select count(*) from search_segment_companions where stream='${stream_name}';" 2>/dev/null | tr -d '[:space:]')"
  exact_index_count="$(sqlite3 -noheader "${db_path}" "select count(*) from secondary_index_state where stream='${stream_name}';" 2>/dev/null | tr -d '[:space:]')"
  peak_rss_bytes="$(json_value "${summary_json_path}" "peak_process_rss_bytes")"
  peak_jsc_heap_size_bytes="$(json_value "${summary_json_path}" "peak_jsc_heap_size_bytes")"
  recovery_rss_bytes="$(json_value "${summary_json_path}" "last_process_rss_bytes")"
  top_phase="$(json_value "${summary_json_path}" "by_primary_phase.0.phase")"

  {
    echo "| ${selector} | ${uploaded_segments:-0} | ${companion_objects:-0} | ${exact_index_count:-0} | ${peak_rss_bytes:-} | ${peak_jsc_heap_size_bytes:-} | ${recovery_rss_bytes:-} | ${top_phase:-} | ${run_dir} |"
    echo
    echo "## ${selector}"
    echo
    echo "- run_dir: ${run_dir}"
    echo "- report: ${report_run_path}"
    echo "- summary: ${summary_json_path}"
    echo "- stream: ${stream_name}"
    echo "- uploaded_segments: ${uploaded_segments:-0}"
    echo "- companion_objects: ${companion_objects:-0}"
    echo "- exact_index_count: ${exact_index_count:-0}"
    echo "- peak_rss_bytes: ${peak_rss_bytes:-}"
    echo "- peak_jsc_heap_size_bytes: ${peak_jsc_heap_size_bytes:-}"
    echo "- recovery_rss_bytes: ${recovery_rss_bytes:-}"
    echo "- top_phase: ${top_phase:-}"
    echo
  } >> "${REPORT_PATH}"
done
