#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import re
import sys
from pathlib import Path


HEADER_RE = re.compile(r"^### (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+) \| (.+)$")


def parse_timestamp(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S %Z")


def parse_report(path: Path) -> list[dict[str, str]]:
    lines = path.read_text().splitlines()
    sections: list[dict[str, str]] = []
    current_ts: dt.datetime | None = None
    current_phase: str | None = None
    in_summary = False
    current: dict[str, str] | None = None

    for line in lines:
        header = HEADER_RE.match(line)
        if header:
            current_ts = parse_timestamp(header.group(1))
            current_phase = header.group(2)
            in_summary = False
            current = None
            continue
        if line.strip() == "[sample_summary]":
            in_summary = True
            current = {
                "timestamp": current_ts.isoformat(sep=" ") if current_ts else "",
                "phase": current_phase or "",
            }
            sections.append(current)
            continue
        if in_summary:
            if not line.strip():
                in_summary = False
                current = None
                continue
            if "=" in line and current is not None:
                key, value = line.split("=", 1)
                current[key.strip()] = value.strip()
    return sections


def to_int(value: str) -> int:
    return int(value) if value else 0


def to_float(value: str) -> float:
    return float(value) if value else 0.0


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize_soak_report.py <report_path>", file=sys.stderr)
        return 1

    sections = parse_report(Path(sys.argv[1]))
    if not sections:
        print("no sample_summary sections found", file=sys.stderr)
        return 1

    first_ts = dt.datetime.fromisoformat(sections[0]["timestamp"])
    prev_ts: dt.datetime | None = None
    prev_bytes: int | None = None

    print(
        "| Timestamp | Phase | Elapsed Min | Logical GB | Delta GB | Ingest MiB/s | Browse ms | Browse tail ms | Browse indexed ms | Browse fts get ms | Browse fts decode ms | Browse fts estimate ms | Browse scanned ms | Col ms | Exact ms | Agg ms | FTS ms | Index ms | Health ms | RSS GiB | Exact Max | Uploaded Segments |"
    )
    print(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )

    for section in sections:
        ts = dt.datetime.fromisoformat(section["timestamp"])
        elapsed_min = (ts - first_ts).total_seconds() / 60.0
        logical_bytes = to_int(section.get("logical_size_bytes", "0"))
        logical_gb = logical_bytes / 1_000_000_000
        delta_gb = ""
        ingest_mib_s = ""
        if prev_ts is not None and prev_bytes is not None:
            delta_bytes = logical_bytes - prev_bytes
            delta_secs = max((ts - prev_ts).total_seconds(), 1.0)
            delta_gb = f"{delta_bytes / 1_000_000_000:.3f}"
            ingest_mib_s = f"{delta_bytes / delta_secs / (1024 * 1024):.2f}"
        browse_ms = to_float(section.get("search_browse_time_total", "0")) * 1000
        browse_tail_ms = to_float(section.get("search_browse_scanned_tail_time_ms", "0"))
        browse_indexed_ms = to_float(section.get("search_browse_indexed_segment_time_ms", "0"))
        browse_fts_get_ms = to_float(section.get("search_browse_fts_section_get_ms", "0"))
        browse_fts_decode_ms = to_float(section.get("search_browse_fts_decode_ms", "0"))
        browse_fts_estimate_ms = to_float(section.get("search_browse_fts_clause_estimate_ms", "0"))
        browse_scanned_ms = to_float(section.get("search_browse_scanned_segment_time_ms", "0"))
        col_ms = to_float(section.get("search_col_time_total", "0")) * 1000
        exact_ms = to_float(section.get("filter_exact_time_total", "0")) * 1000
        agg_ms = to_float(section.get("aggregate_time_total", "0")) * 1000
        fts_ms = to_float(section.get("search_fts_time_total", "0")) * 1000
        index_ms = to_float(section.get("index_time_total", "0")) * 1000
        health_ms = to_float(section.get("health_time_total", "0")) * 1000
        rss_gib = to_int(section.get("ps_rss_kb", "0")) / (1024 * 1024)
        exact_max = section.get("exact_max_indexed_through", "") or "0"
        uploaded_segments = section.get("uploaded_segment_count", "") or "0"
        print(
            f"| {ts.strftime('%Y-%m-%d %H:%M:%S')} | {section.get('phase', '')} | {elapsed_min:.1f} | {logical_gb:.3f} | {delta_gb} | {ingest_mib_s} | {browse_ms:.1f} | {browse_tail_ms:.1f} | {browse_indexed_ms:.1f} | {browse_fts_get_ms:.1f} | {browse_fts_decode_ms:.1f} | {browse_fts_estimate_ms:.1f} | {browse_scanned_ms:.1f} | {col_ms:.1f} | {exact_ms:.1f} | {agg_ms:.1f} | {fts_ms:.1f} | {index_ms:.1f} | {health_ms:.1f} | {rss_gib:.2f} | {exact_max} | {uploaded_segments} |"
        )
        prev_ts = ts
        prev_bytes = logical_bytes

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
