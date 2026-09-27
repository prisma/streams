# Sourced (never executed) by the gate entry points and the dev tools.
#
# The gate scripts need Python >= 3.11 as `python3` (they import tomllib),
# but macOS ships 3.9 as /usr/bin/python3, first on PATH. When the python3
# on PATH is older, link the newest installed 3.11+ as `python3` into
# target/python3-shim and put that first on PATH, so nested `python3`
# calls in every script get it too. CI's python3 is new enough: no-op there.
# This selects the interpreter only; no check changes.
_streams_python_ok() {
  "$1" -c 'import sys; sys.exit(sys.version_info < (3, 11))' >/dev/null 2>&1
}
if ! _streams_python_ok python3; then
  _streams_python=
  for _streams_candidate in python3.14 python3.13 python3.12 python3.11 \
      /opt/homebrew/bin/python3.14 /opt/homebrew/bin/python3.13 \
      /opt/homebrew/bin/python3.12 /opt/homebrew/bin/python3.11 \
      /usr/local/bin/python3.13 /usr/local/bin/python3.12 /usr/local/bin/python3.11; do
    _streams_found=$(command -v "$_streams_candidate" 2>/dev/null) || continue
    if _streams_python_ok "$_streams_found"; then
      _streams_python=$_streams_found
      break
    fi
  done
  if [[ -z "$_streams_python" ]]; then
    echo 'QUALITY_FAIL: python3 >= 3.11 is required (the gates import tomllib); install one, e.g. brew install python@3.12' >&2
    return 1
  fi
  _streams_shim="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/python3-shim"
  mkdir -p "$_streams_shim"
  ln -sf "$_streams_python" "$_streams_shim/python3"
  export PATH="$_streams_shim:$PATH"
  unset _streams_found _streams_shim
fi
unset _streams_python _streams_candidate
unset -f _streams_python_ok
