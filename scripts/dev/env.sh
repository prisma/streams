# Source from zsh or bash in the repository root before running repo tools
# by hand:   . scripts/dev/env.sh && python3 scripts/quality/formal.py check
#
# The gate entry points (scripts/quality.sh, gate.sh, test-leg.sh,
# quality/mutations.sh, quality/nightly.sh) already select their Python;
# this is for direct `python3 scripts/...` calls and manual `cargo kani`.
# It puts a Python >= 3.11 first on PATH as python3 (macOS /usr/bin/python3
# is 3.9 and lacks tomllib), adds the pinned formal/quality tools
# (target/quality-tools/bin: kani, cargo-kani) and ~/.cargo/bin, unsets
# RUSTUP_TOOLCHAIN (the pins come from rust-toolchain.toml and
# quality-tools.toml), and warns about variables scripts/quality.sh refuses.
_streams_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))' >/dev/null 2>&1; then
  for _streams_candidate in python3.14 python3.13 python3.12 python3.11 \
      /opt/homebrew/bin/python3.13 /opt/homebrew/bin/python3.12 /opt/homebrew/bin/python3.11; do
    if command -v "$_streams_candidate" >/dev/null 2>&1 \
        && "$_streams_candidate" -c 'import sys; sys.exit(sys.version_info < (3, 11))' >/dev/null 2>&1; then
      mkdir -p "$_streams_root/target/python3-shim"
      ln -sf "$(command -v "$_streams_candidate")" "$_streams_root/target/python3-shim/python3"
      PATH="$_streams_root/target/python3-shim:$PATH"
      break
    fi
  done
fi
PATH="$_streams_root/target/quality-tools/bin:$HOME/.cargo/bin:$PATH"
export PATH
unset RUSTUP_TOOLCHAIN
for _streams_var in RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC CLIPPY_CONF_DIR; do
  if [ -n "$(printenv "$_streams_var" 2>/dev/null)" ]; then
    echo "env.sh: $_streams_var is set; scripts/quality.sh refuses to run with it" >&2
  fi
done
python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))' >/dev/null 2>&1 \
  || echo 'env.sh: no python3 >= 3.11 found (brew install python@3.12)' >&2
unset _streams_root _streams_candidate _streams_var
