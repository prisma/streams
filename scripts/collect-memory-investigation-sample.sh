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
search_browse_output="$(
  curl -sS --connect-timeout 1 --max-time 20 --get \
    --data-urlencode 'q=type:pushevent owner:prisma*' \
    --data-urlencode 'size=1' \
    --data-urlencode 'sort=offset:desc' \
    --data-urlencode 'track_total_hits=false' \
    -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' \
    "${BASE_URL}/v1/stream/${STREAM}/_search" 2>&1 || true
)"
search_fts_output="$(
  curl -sS --connect-timeout 1 --max-time 20 --get \
    --data-urlencode 'q=message:PushEvent' \
    --data-urlencode 'size=1' \
    --data-urlencode 'sort=offset:desc' \
    --data-urlencode 'track_total_hits=false' \
    -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' \
    "${BASE_URL}/v1/stream/${STREAM}/_search" 2>&1 || true
)"
search_col_output="$(
  curl -sS --connect-timeout 1 --max-time 20 --get \
    --data-urlencode 'q=public:true payloadBytes:>=800' \
    --data-urlencode 'size=1' \
    --data-urlencode 'sort=offset:desc' \
    --data-urlencode 'track_total_hits=false' \
    -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' \
    "${BASE_URL}/v1/stream/${STREAM}/_search" 2>&1 || true
)"
filter_exact_output="$(
  curl -sS --connect-timeout 1 --max-time 20 --get \
    --data-urlencode 'format=json' \
    --data-urlencode 'filter=ghArchiveId:"1136888046"' \
    -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' \
    "${BASE_URL}/v1/stream/${STREAM}" 2>&1 || true
)"
aggregate_output="$(
  curl -sS --connect-timeout 1 --max-time 20 \
    -H 'content-type: application/json' \
    -X POST \
    --data-raw '{"rollup":"events","from":"2011-02-17T10:00:00.000Z","to":"2011-02-17T11:00:00.000Z","interval":"1h","group_by":["eventType"]}' \
    -w '\nTIME_TOTAL=%{time_total}\nHTTP_CODE=%{http_code}\n' \
    "${BASE_URL}/v1/stream/${STREAM}/_aggregate" 2>&1 || true
)"
ps_output="$(ps -o pid=,ppid=,%cpu=,%mem=,rss=,vsz=,etime=,command= -p "${PID}" 2>&1 || true)"
if [[ "${os_name}" == "Darwin" ]]; then
  top_output="$(top -l 1 -pid "${PID}" -stats pid,command,cpu,time,threads,ports,rsize,mem,rprvt,vsize,state 2>&1 || true)"
elif [[ "${os_name}" == "Linux" ]]; then
  top_output="$(COLUMNS=200 top -bn1 -p "${PID}" 2>&1 || true)"
else
  top_output="unsupported top mode for os=${os_name}"
fi
stream_sql="$(sqlite3 -header -column "${DB_PATH}" "select stream, logical_size_bytes, next_offset, sealed_through, uploaded_through, uploaded_segment_count, pending_rows, pending_bytes, wal_rows, wal_bytes, segment_in_progress, last_append_ms, last_segment_cut_ms from streams where stream='${STREAM}';" 2>&1 || true)"
segment_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as total_segments, sum(case when r2_etag is not null then 1 else 0 end) as uploaded_segments from segments where stream='${STREAM}';" 2>&1 || true)"
pending_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as pending_segments from segments where stream='${STREAM}' and r2_etag is null;" 2>&1 || true)"
companion_sql="$(sqlite3 -header -column "${DB_PATH}" "select count(*) as companion_objects from search_segment_companions where stream='${STREAM}';" 2>&1 || true)"
index_sql="$(sqlite3 -header -column "${DB_PATH}" "select indexed_through from index_state where stream='${STREAM}';" 2>&1 || true)"
exact_sql="$(sqlite3 -header -column "${DB_PATH}" "select min(indexed_through) as min_exact_indexed_through, max(indexed_through) as max_exact_indexed_through, count(*) as exact_index_count from secondary_index_state where stream='${STREAM}';" 2>&1 || true)"

health_time_total="$(printf '%s\n' "${health_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
health_http_code="$(printf '%s\n' "${health_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
index_time_total="$(printf '%s\n' "${index_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
index_http_code="$(printf '%s\n' "${index_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
search_browse_time_total="$(printf '%s\n' "${search_browse_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
search_browse_http_code="$(printf '%s\n' "${search_browse_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
search_fts_time_total="$(printf '%s\n' "${search_fts_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
search_fts_http_code="$(printf '%s\n' "${search_fts_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
search_col_time_total="$(printf '%s\n' "${search_col_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
search_col_http_code="$(printf '%s\n' "${search_col_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
filter_exact_time_total="$(printf '%s\n' "${filter_exact_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
filter_exact_http_code="$(printf '%s\n' "${filter_exact_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
aggregate_time_total="$(printf '%s\n' "${aggregate_output}" | awk -F= '/^TIME_TOTAL=/{print $2; exit}')"
aggregate_http_code="$(printf '%s\n' "${aggregate_output}" | awk -F= '/^HTTP_CODE=/{print $2; exit}')"
ps_rss_kb="$(printf '%s\n' "${ps_output}" | awk 'NF >= 5 {print $5; exit}')"
ps_cpu_percent="$(printf '%s\n' "${ps_output}" | awk 'NF >= 3 {print $3; exit}')"
stream_summary="$(sqlite3 -separator '|' -noheader "${DB_PATH}" "select coalesce(logical_size_bytes, 0), coalesce(uploaded_segment_count, 0), coalesce(pending_bytes, 0), coalesce(next_offset, 0), coalesce(sealed_through, 0), coalesce(uploaded_through, 0) from streams where stream='${STREAM}';" 2>/dev/null || true)"
stream_logical_size_bytes="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 1 {print $1; exit}')"
stream_uploaded_segment_count="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 2 {print $2; exit}')"
stream_pending_bytes="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 3 {print $3; exit}')"
stream_next_offset="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 4 {print $4; exit}')"
stream_sealed_through="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 5 {print $5; exit}')"
stream_uploaded_through="$(printf '%s' "${stream_summary}" | awk -F'|' 'NF >= 6 {print $6; exit}')"
exact_summary="$(sqlite3 -separator '|' -noheader "${DB_PATH}" "select coalesce(min(indexed_through), ''), coalesce(max(indexed_through), ''), count(*) from secondary_index_state where stream='${STREAM}';" 2>/dev/null || true)"
exact_min="$(printf '%s' "${exact_summary}" | awk -F'|' 'NF >= 1 {print $1; exit}')"
exact_max="$(printf '%s' "${exact_summary}" | awk -F'|' 'NF >= 2 {print $2; exit}')"
exact_count="$(printf '%s' "${exact_summary}" | awk -F'|' 'NF >= 3 {print $3; exit}')"

{
  echo
  echo "### ${timestamp} | ${PHASE}"
  echo
  echo '```text'
  echo "[sample_summary]"
  echo "logical_size_bytes=${stream_logical_size_bytes:-}"
  echo "uploaded_segment_count=${stream_uploaded_segment_count:-}"
  echo "pending_bytes=${stream_pending_bytes:-}"
  echo "next_offset=${stream_next_offset:-}"
  echo "sealed_through=${stream_sealed_through:-}"
  echo "uploaded_through=${stream_uploaded_through:-}"
  echo "exact_min_indexed_through=${exact_min:-}"
  echo "exact_max_indexed_through=${exact_max:-}"
  echo "exact_index_count=${exact_count:-}"
  echo "health_http_code=${health_http_code:-}"
  echo "health_time_total=${health_time_total:-}"
  echo "index_http_code=${index_http_code:-}"
  echo "index_time_total=${index_time_total:-}"
  echo "search_browse_http_code=${search_browse_http_code:-}"
  echo "search_browse_time_total=${search_browse_time_total:-}"
  echo "search_fts_http_code=${search_fts_http_code:-}"
  echo "search_fts_time_total=${search_fts_time_total:-}"
  echo "search_col_http_code=${search_col_http_code:-}"
  echo "search_col_time_total=${search_col_time_total:-}"
  echo "filter_exact_http_code=${filter_exact_http_code:-}"
  echo "filter_exact_time_total=${filter_exact_time_total:-}"
  echo "aggregate_http_code=${aggregate_http_code:-}"
  echo "aggregate_time_total=${aggregate_time_total:-}"
  echo "ps_cpu_percent=${ps_cpu_percent:-}"
  echo "ps_rss_kb=${ps_rss_kb:-}"
  echo
  echo "[platform]"
  echo "os=${os_name}"
  echo
  echo "[health]"
  echo "${health_output}"
  echo
  echo "[index_status]"
  echo "${index_output}"
  echo
  echo "[search_browse]"
  echo "${search_browse_output}"
  echo
  echo "[search_fts]"
  echo "${search_fts_output}"
  echo
  echo "[search_col]"
  echo "${search_col_output}"
  echo
  echo "[filter_exact]"
  echo "${filter_exact_output}"
  echo
  echo "[aggregate]"
  echo "${aggregate_output}"
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
