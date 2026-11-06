#!/usr/bin/env bash
# tenants.sh COUNT DIR: COUNT single-stream projects on a cell-admin cell,
# for the ramp's enforced auth (#149's "owned by one tenant, or each by its
# own"). Writes DIR/feeds/{keys,policies,grants}.json (the servers mount
# them read-only and enforce them), DIR/tokens.txt (one customer token a
# line, project i's on line i: the population bench gives stream i token
# i), and DIR/cell (cell-admin's state). Uses bench/k2cost/tenants-admit.ts
# (cell-admin's own admission, tokens and feeds); a manifest class holds at
# most 9,999 projects, so COUNT is spread over classes r0, r1, ...
# Tokens and the workload JWT live 12 h: rerun tenants.sh before a run (rig.tenants
# does), which keeps the admitted cell and re-mints them. The cell's signing key
# and the tokens stay in DIR (mode 0600), never in the repository or a log.
set -euo pipefail
count=${1:?count}; dir=${2:?dir}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$dir" && chmod 700 "$dir"
manifest=$dir/manifest.json
python3 - "$count" "$manifest" <<'PY'
import json, sys
count, path = int(sys.argv[1]), sys.argv[2]
classes, left, k = [], count, 0
while left > 0:
    n = min(9999, left)
    classes.append({"class": f"r{k}", "count": n, "role": "compliant", "streams": 1, "prefix": "r/", "phases": []})
    left -= n; k += 1
manifest = {"name": f"ramp-{count}", "seed": 7, "duration": 3600, "classes": classes}
json.dump(manifest, open(path, "w"))
PY
[ -f "$dir/cell.pem" ] || K2_CELL_KEY=$dir/cell.pem bun -e 'require("node:fs").writeFileSync(process.env.K2_CELL_KEY,
  require("node:crypto").generateKeyPairSync("ed25519").privateKey.export({ type: "pkcs8", format: "pem" }),
  { mode: 0o600 })'
# One tenant owns the cell (dedicated-cell.env, k = 1: one project at the
# instance's bounds); a tenant each shares it (shared-cell.env, k = 8).
profile=shared-cell.env max=""; [ "$count" = 1 ] && profile=dedicated-cell.env max="--max-projects 1"
admit=$root/bench/k2cost/tenants-admit.ts
# A rerun keeps the admitted cell and re-mints what expires (12 h).
[ -f "$dir/admitted" ] || bun "$admit" admit --manifest "$manifest" --state "$dir/cell" --key-file "$dir/cell.pem" \
  --cell-id ramp-cell --deployment-project proj_ramp_deploy --account acct_ramp $max \
  --profile "$root/deploy/profiles/compute-1g.env,$root/deploy/profiles/$profile" > "$dir/admit.log" 2>&1
touch "$dir/admitted"
bun "$admit" tokens --manifest "$manifest" --state "$dir/cell" --key-file "$dir/cell.pem" \
  --out "$dir/tokens.json" >> "$dir/admit.log" 2>&1
bun "$admit" feeds --state "$dir/cell" --out "$dir/feeds" >> "$dir/admit.log" 2>&1
# The fleet's workload identity (a release-posture fleet refuses the static
# token): a workload key in the keys feed, audience prisma-streams-internal,
# and feeds/workload.jwt signed by it for 12 h (the ramp's runs are shorter),
# as the platform emulator mints one (platform-demo/src/emulator.mjs).
K2_FEEDS=$dir/feeds bun -e '
const { generateKeyPairSync, sign, randomBytes } = require("node:crypto");
const fs = require("node:fs");
const dir = process.env.K2_FEEDS;
const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const kid = "streams-workload-" + randomBytes(4).toString("hex");
const keys = JSON.parse(fs.readFileSync(dir + "/keys.json", "utf8"));
keys.keys.push({ kid, alg: "EdDSA", aud: "prisma-streams-internal",
  pem: publicKey.export({ type: "spki", format: "pem" }) });
fs.writeFileSync(dir + "/keys.json.tmp", JSON.stringify(keys));
fs.renameSync(dir + "/keys.json.tmp", dir + "/keys.json");
const b64u = (x) => Buffer.from(x).toString("base64url");
const now = Math.floor(Date.now() / 1000);
const head = b64u(JSON.stringify({ alg: "EdDSA", typ: "JWT", kid }));
const body = b64u(JSON.stringify({ iss: "https://auth.prisma.io", aud: "prisma-streams-internal",
  sub: "ramp-fleet", cell_id: "ramp-cell",
  operations: ["telemetry-append", "segment-read", "segment-close", "seal-fence"],
  nbf: now - 60, exp: now + 12 * 3600 }));
const sig = b64u(sign(null, Buffer.from(head + "." + body), privateKey));
fs.writeFileSync(dir + "/workload.jwt", head + "." + body + "." + sig, { mode: 0o644 });
'
python3 - "$dir" <<'PY'
import json, os, sys
d = sys.argv[1]
tokens = [t["token"] for t in json.load(open(os.path.join(d, "tokens.json")))]
path = os.path.join(d, "tokens.txt")
with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
    f.write("\n".join(tokens) + "\n")
if len(tokens) == 1:
    # #149's one tenant ran without project quotas binding; cell-admin's
    # dedicated project carries the instance's bounds (1,411 requests/s,
    # 512 in flight, 65,536 streams), which a 60x-compressed ramp reaches
    # long before the cell's own limits. The ramp measures the cell, so the
    # one project's quotas are lifted in its policy feed (a k = 1 server
    # applies the feed as given: src/auth/ceiling.rs `ceiling`).
    pp = os.path.join(d, "feeds", "policies.json")
    feed = json.load(open(pp))
    feed["projects"][0]["quotas"].update({
        "requests_per_sec": 10_000_000, "max_inflight_requests": 1_000_000,
        "max_streams": 100_000_000, "append_bytes_per_sec": 10_000_000_000,
        "read_bytes_per_sec": 10_000_000_000, "queued_append_bytes": 4_294_967_296})
    json.dump(feed, open(pp + ".tmp", "w"))
    os.replace(pp + ".tmp", pp)
print(f"tenants: {len(tokens)} projects, feeds in {d}/feeds")
PY
