#!/usr/bin/env bash
# Local fixtures only. Evidence stays on disk; this is not provider certification.
set -euo pipefail
cd "$(dirname "$0")/.."
RELIABILITY_PARENT=${RELIABILITY_PARENT:-target/reliability}
mkdir -p "$RELIABILITY_PARENT"
RELIABILITY_RUN=$(mktemp -d "$RELIABILITY_PARENT/run.XXXXXX")
cargo build --locked --release --bin streams-slate --bin s3lite \
  --message-format=json > "$RELIABILITY_RUN/build.jsonl"
# Cargo's artifact paths remain authoritative with CARGO_TARGET_DIR or a
# configured target triple; never accidentally run a stale default-path binary.
python3 - "$RELIABILITY_RUN" <<'PY'
import json
from pathlib import Path
import subprocess
import sys

run = Path(sys.argv[1])
artifacts = {}
for line in (run / 'build.jsonl').read_text().splitlines():
    item = json.loads(line)
    if item.get('reason') != 'compiler-artifact':
        continue
    name = item['target']['name']
    if name in {'streams-slate', 's3lite'} and item.get('executable'):
        if Path(item['manifest_path']).resolve() != Path('Cargo.toml').resolve():
            raise SystemExit('unexpected release artifact owner')
        artifacts[name] = item['executable']
if set(artifacts) != {'streams-slate', 's3lite'}:
    raise SystemExit('Cargo did not identify both release executables')
subprocess.run([sys.executable, 'bench/reliability/local_campaign.py',
                '--out', str(run / 'campaign'), '--server', artifacts['streams-slate'],
                '--s3lite', artifacts['s3lite']], check=True)
PY
printf 'RELIABILITY_OK: %s/campaign/receipt.json\n' "$RELIABILITY_RUN"
