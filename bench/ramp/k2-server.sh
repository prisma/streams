#!/usr/bin/env bash
# k2-server.sh --listen 127.0.0.1:PORT: bench/k2cost/run-local.sh's server, in a
# sized Linux container (the rig's image, streams-ramp), for the packing
# points per instance size. run-local starts K2_SERVER_BIN under `env -i`
# with the server's posture file sourced; this wrapper takes that
# environment, minus its own knobs, into the container:
#   K2_DOCKER_CPUS, K2_DOCKER_MEM   the container's --cpus and --memory
#   K2_DOCKER_NAME                  the container's name, set by run-local per
#                                   server (its PID spec names it, so scrape.py
#                                   reads the container's cgroup CPU)
#   K2_DOCKER_BIN                   the docker CLI (run-local's PATH is /usr/bin:/bin)
# The store at 127.0.0.1 is reached as host.docker.internal; the secrets
# directory the posture file names (feeds, tokens) is mounted at its own path,
# read-only. The server listens on PORT inside and is published on PORT.
set -euo pipefail
[ "${1:-}" = --listen ] || { echo "k2-server.sh: --listen HOST:PORT expected" >&2; exit 2; }
port=${2##*:}
name=${K2_DOCKER_NAME:?K2_DOCKER_NAME}
envfile=$(mktemp)
trap 'rm -f "$envfile"' EXIT
# The server's environment, as the posture file set it (this wrapper's own
# knobs and the shell's left out), with the store's host rewritten.
env | grep -E '^[A-Z][A-Z0-9_]*=' | grep -vE '^(K2_DOCKER_[A-Z_]+|PATH|HOME|PWD|SHLVL|_)=' \
  | sed -E 's#(http://)(127\.0\.0\.1|localhost):#\1host.docker.internal:#' > "$envfile"
mounts=()
for d in $(grep -oE '=/[^ ]+' "$envfile" | cut -c2- | xargs -n1 dirname 2>/dev/null | sort -u); do
  [ -d "$d" ] && mounts+=(-v "$d:$d:ro")
done
docker=${K2_DOCKER_BIN:-/usr/local/bin/docker}
"$docker" rm -f "$name" >/dev/null 2>&1 || true
exec "$docker" run --rm --name "$name" --cpus "${K2_DOCKER_CPUS:-1}" --memory "${K2_DOCKER_MEM:-1g}" \
  --memory-swap "${K2_DOCKER_MEM:-1g}" --add-host host.docker.internal:host-gateway \
  -p "127.0.0.1:$port:$port" --env-file "$envfile" ${mounts[@]+"${mounts[@]}"} \
  streams-ramp:latest streams-slate --listen "0.0.0.0:$port"
