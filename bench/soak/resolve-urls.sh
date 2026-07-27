#!/bin/bash
# Re-resolve preview domains from the RUNNING version of each service.
#
# Preview domains belong to a version, not a service. After a redeploy the
# cached URL still looks plausible but answers 503 -- which is easy to
# misread as a boot failure. This is the only supported way to learn a
# service's current URL.
#
#   ./resolve-urls.sh                 # every region+role in $SOAK_HOME
#   ./resolve-urls.sh <region> <role> # just one
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
# Prefer the CLI's own stored login (it auto-refreshes). A static token
# file is only a fallback — a copied OAuth access token expires in about
# an hour, which silently killed every deploy of a soak run mid-campaign
# (2026-07-27: five WARN "failures" that were all a 401 in disguise).
if [ -s "$S/platform-token.txt" ]; then
  export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
fi
REGIONS=${1:-${SOAK_REGIONS:-us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1}}
ROLES=${2:-"server gen"}
for r in $REGIONS; do
  for role in $ROLES; do
    [ -f "$S/svc-$role-$r.txt" ] || continue
    d=$(bunx --bun @prisma/compute-cli versions list \
          --project "$(cat "$S/proj-$r.txt")" \
          --service "$(cat "$S/svc-$role-$r.txt")" 2>/dev/null \
        | awk '$2=="running"{print $3; exit}')
    if [ -n "$d" ]; then
      echo "https://$d" > "$S/url-$role-$r.txt"
      echo "$r/$role -> https://$d"
    else
      echo "$r/$role -> NO RUNNING VERSION (a failed deploy leaves a version-less shell)" >&2
    fi
  done
done
