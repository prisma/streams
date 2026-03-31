#!/bin/zsh
set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "usage: $0 <report_path> <phase> <pid> <stream> <url> [db_path]" >&2
  exit 1
fi

REPORT_PATH="$1"
PHASE="$2"
PID="$3"
STREAM="$4"
BASE_URL="$5"
DB_PATH="${6:-${DS_DB_PATH:-${DS_ROOT:+${DS_ROOT}/wal.sqlite}}}"
if [[ -z "${DB_PATH}" ]]; then
  DB_PATH="/tmp/lib/prisma-streams/wal.sqlite"
fi

timestamp="$(date '+%Y-%m-%d %H:%M:%S %Z')"
os_name="$(uname -s 2>/dev/null || echo unknown)"

health_output="$(curl -sS --connect-timeout 1 --max-time 20 -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' "${BASE_URL}/health" 2>&1 || true)"
index_output="$(curl -sS --connect-timeout 1 --max-time 20 -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' "${BASE_URL}/v1/stream/${STREAM}/_index_status" 2>&1 || true)"
ps_output="$(ps -o pid=,ppid=,%cpu=,%mem=,rss=,vsz=,etime=,command= -p "${PID}" 2>&1 || true)"
if [[ "${os_name}" == "Darwin" ]]; then
  top_output="$(top -l 1 -pid "${PID}" -stats pid,command,cpu,time,threads,ports,rsize,mem,rprvt,vsize,state 2>&1 || true)"
elif [[ "${os_name}" == "Linux" ]]; then
  top_output="$(COLUMNS=200 top -bn1 -p "${PID}" 2>&1 || true)"
else
  top_output="unsupported top mode for os=${os_name}"
fi
stream_sql="$(sqlite3 -header -column "${DB_PATH}" "select stream, next_offset, sealed_through, uploaded_through, uploaded_segment_count, pending_rows, pending_bytes, wal_rows, wal_bytes, segment_in_progress, last_append_ms, last_segment_cut_ms from streams where stream='${STREAM}';" 2>&1 || true)"
segment_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as total_segments, sum(case when r2_etag is not null then 1 else 0 end) as uploaded_segments from segments where stream='${STREAM}';" 2>&1 || true)"
pending_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as pending_segments from segments where stream='${STREAM}' and r2_etag is null;" 2>&1 || true)"
companion_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as companion_objects from search_segment_companions where stream='${STREAM}';" 2>&1 || true)"
index_sql="$(sqlite3 -header -column "${DB_PATH}" "select indexed_through from index_state where stream='${STREAM}';" 2>&1 || true)"
exact_sql="$(sqlite3 -header -column "${DB_PATH}" "select min(indexed_through) as min_exact_indexed_through, max(indexed_through) as max_exact_indexed_through, count(*) as exact_index_count from secondary_index_state where stream='${STREAM}';" 2>&1 || true)"

{
  echo
  echo "### ${timestamp} | ${PHASE}"
  echo
  echo '```text'
  echo "[platform]"
  echo "os=${os_name}"
  echo
  echo "[health]"
  echo "${health_output}"
  echo
  echo "[index_status]"
  echo "${index_output}"
  echo
  echo "[ps]"
  echo "${ps_output}"
  echo
  echo "[top]"
  echo "${top_output}"
  echo
  echo "[stream]"
  echo "${stream_sql}"
  echo
  echo "[segments]"
  echo "${segment_sql}"
  echo
  echo "[pending_segments]"
  echo "${pending_sql}"
  echo
  echo "[companions]"
  echo "${companion_sql}"
  echo
  echo "[routing_index]"
  echo "${index_sql}"
  echo
  echo "[exact_indexes]"
  echo "${exact_sql}"
  echo '```'
} >> "${REPORT_PATH}"
