#!/usr/bin/env python3
"""Sorted, de-duplicated clippy warning fingerprints (message :: file).

Line numbers are deliberately excluded — insertions shift them without
changing what the warning IS. Duplicate (message, file) pairs collapse:
--all-targets re-emits the same crate warnings per target, and that
multiplicity is build-graph noise, not new defects. The gate compares
this output against scripts/clippy-baseline-fingerprints.txt (the
reviewed allowlist) with comm(1).
"""
import re
import sys

lines = open(sys.argv[1]).read().splitlines()
fps = set()
for i, l in enumerate(lines):
    if l.startswith("warning: ") and "generated" not in l:
        msg = l[9:]
        loc = ""
        for j in range(i + 1, min(i + 4, len(lines))):
            m = re.search(r"--> ([^:]+):", lines[j])
            if m:
                loc = m.group(1)
                break
        fps.add(f"{msg} :: {loc}")
for fp in sorted(fps):
    print(fp)
