#!/usr/bin/env python3
"""Build immutable historical and current server revisions for a local matrix."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib

import local_store

ROOT = Path(__file__).resolve().parents[2]
RELEASE = "685ea0354f123864154b46e585f9c22664929763"
PRIOR = "adc2cdc5bdc2dba1aaffabc599d61de3df35fcb1"
TOOLCHAIN = "1.98.1"
SLATEDB = "0717cc1e4e9bad10a4773760f66bac4264ecf05e"


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def source_pins(crypto, registry):
    """Read the actual reviewed constant declarations; fail on source drift."""
    declarations = ((crypto, "FRAME_VER", "u8"), (crypto, "FRAME_VER_Z", "u8"),
                    (registry, "LAYOUT_VERSION", "u32"))
    values = []
    for source, name, kind in declarations:
        match = re.search(r"\bconst " + name + ": " + kind + r" = (\d+);", source)
        if match is None:
            raise ValueError(f"reviewed source constant no longer resolves: {name}")
        values.append(int(match[1]))
    return {"writer_frames": values[:2], "layout_namespace": values[2]}


def source_metadata(revision):
    if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("source revision must be a full immutable commit ID")
    if git("rev-parse", "--verify", revision + "^{commit}") != revision:
        raise ValueError("source revision must resolve to its exact commit")
    files = {name: subprocess.check_output(["git", "show", f"{revision}:{name}"], cwd=ROOT)
             for name in ("Cargo.toml", "Cargo.lock", "src/crypto.rs", "src/registry.rs")}
    manifest = tomllib.loads(files["Cargo.toml"].decode())
    return {"revision": revision, "tree": git("rev-parse", revision + "^{tree}"),
            "package_version": manifest["package"]["version"],
            "slatedb": manifest["patch"]["crates-io"]["slatedb"],
            "cargo_lock_sha256": hashlib.sha256(files["Cargo.lock"]).hexdigest(),
            "crypto_source_sha256": hashlib.sha256(files["src/crypto.rs"]).hexdigest(),
            **source_pins(files["src/crypto.rs"].decode(), files["src/registry.rs"].decode())}


def read_builds(path):
    receipt = json.loads(path.read_bytes())
    if receipt.get("schema") != 1 or receipt.get("profile") != "release":
        raise ValueError("an immutable release-build receipt is required")
    builds = receipt["builds"]
    if set(builds) != {"release", "prior", "current"}:
        raise ValueError("all three reviewed source revisions are required")
    if builds["release"]["revision"] != RELEASE or builds["prior"]["revision"] != PRIOR:
        raise ValueError("historical revisions differ from the reviewed matrix")
    if len({build["revision"] for build in builds.values()}) != 3:
        raise ValueError("three distinct revisions are required")
    server_hashes = set()
    for label, build in builds.items():
        if (build["layout_namespace"] != 4
                or build["writer_frames"] != ([2, 3] if label == "release" else [4, 5])):
            raise ValueError("layout/frame pins differ from the reviewed matrix")
        actual = source_metadata(build["revision"])
        if any(build.get(name) != value for name, value in actual.items()):
            raise ValueError("recorded metadata differs from actual immutable source")
        if build["slatedb"]["rev"] != SLATEDB:
            raise ValueError("unreviewed SlateDB pin")
        expected = {"streams-slate", "s3lite"} if label == "current" else {"streams-slate"}
        if set(build["artifacts"]) != expected:
            raise ValueError("build receipt lacks its exact executable set")
        for artifact in build["artifacts"].values():
            binary = Path(artifact["path"])
            if not binary.is_absolute() or not binary.is_file():
                raise ValueError("build receipt must identify an existing absolute binary path")
            if hashlib.sha256(binary.read_bytes()).hexdigest() != artifact["sha256"]:
                raise ValueError("executable bytes changed after the recorded build")
        server_hashes.add(build["artifacts"]["streams-slate"]["sha256"])
    if len(server_hashes) != 3:
        raise ValueError("different labels must not run the same executable bytes")
    return receipt


def reuse_historical(path, destination, rustc):
    """Copy only verified historical artifacts; a current candidate always builds."""
    raw = path.read_bytes()
    previous = read_builds(path)
    if previous != json.loads(raw):
        raise ValueError("historical build receipt changed while being validated")
    if previous.get("rustc") != rustc:
        raise ValueError("historical reuse requires the same compiler identity and host")
    historical = {}
    for label in ("release", "prior"):
        build = previous["builds"][label]
        original = build["artifacts"]["streams-slate"]
        source = Path(original["path"])
        data = source.read_bytes()
        if hashlib.sha256(data).hexdigest() != original["sha256"]:
            raise ValueError("historical executable changed while being copied")
        binary = destination / f"{label}-streams-slate"
        local_store.durable_write(binary, data)
        binary.chmod(source.stat().st_mode & 0o777)
        historical[label] = {**build, "artifacts": {"streams-slate": {
            "path": str(binary), "sha256": original["sha256"]}}}
    receipt_path = destination / "reused-build-receipt.json"
    local_store.durable_write(receipt_path, raw)
    return historical, {"receipt_sha256": hashlib.sha256(raw).hexdigest(),
                        "receipt_path": str(receipt_path)}


def build(args):
    current = git("rev-parse", "--verify", args.current_revision + "^{commit}")
    if git("rev-parse", "v0.2.0-rc.4^{commit}") != RELEASE:
        raise RuntimeError("release tag no longer resolves to the reviewed revision")
    revisions = {"release": RELEASE, "prior": PRIOR, "current": current}
    if len(set(revisions.values())) != 3:
        raise RuntimeError("version matrix requires three distinct source revisions")
    for revision in (RELEASE, PRIOR):
        subprocess.run(["git", "merge-base", "--is-ancestor", revision, current],
                       cwd=ROOT, check=True)
    args.out.mkdir(parents=True, exist_ok=False)
    local_store.durable_write(args.out / "build-driver.py", Path(__file__).read_bytes())
    args.target_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, RUSTUP_TOOLCHAIN=TOOLCHAIN,
               CARGO_TARGET_DIR=str(args.target_dir))
    cargo = str(Path.home() / ".cargo/bin/cargo")
    rustc = subprocess.check_output([str(Path.home() / ".cargo/bin/rustc"), "-Vv"],
                                   env=env, text=True)
    receipt = {"schema": 1, "rustc": rustc, "profile": "release", "builds": {}}
    if args.reuse_historical_builds is not None:
        receipt["builds"], receipt["historical_reuse"] = reuse_historical(
            args.reuse_historical_builds, args.out, rustc)
    # Old revisions have no workspace declaration; nesting them beneath this
    # checkout would accidentally inherit the current root's Cargo workspace.
    sources = Path(tempfile.mkdtemp(prefix="streams-version-sources-")).resolve()
    local_store.durable_write(args.out / "source-directory.txt", str(sources).encode())
    for label, revision in revisions.items():
        if label in receipt["builds"]:
            print(f"Reusing verified {label} {revision}", flush=True)
            continue
        worktree = sources / label
        subprocess.run(["git", "worktree", "add", "--detach", str(worktree), revision],
                       cwd=ROOT, check=True)
        metadata = source_metadata(revision)
        if (metadata["writer_frames"] != ([2, 3] if label == "release" else [4, 5])
                or metadata["layout_namespace"] != 4 or metadata["slatedb"]["rev"] != SLATEDB):
            raise RuntimeError("actual source pins differ from the reviewed transition matrix")
        names = {"streams-slate", "s3lite"} if label == "current" else {"streams-slate"}
        command = [cargo, "build", "--locked", "--release", "--jobs", "2",
                   "--message-format=json"]
        for name in sorted(names):
            command += ["--bin", name]
        print(f"Building {label} {revision}", flush=True)
        with (args.out / (label + "-build.jsonl")).open("xb") as out, \
                (args.out / (label + "-build.log")).open("xb") as error:
            subprocess.run(command, cwd=worktree, env=env, stdout=out, stderr=error, check=True)
        artifacts = {}
        for line in (args.out / (label + "-build.jsonl")).read_text().splitlines():
            item = json.loads(line)
            if item.get("reason") != "compiler-artifact" or not item.get("executable"):
                continue
            name = item["target"]["name"]
            if name not in names:
                continue
            if Path(item["manifest_path"]).resolve() != (worktree / "Cargo.toml").resolve():
                raise RuntimeError("Cargo artifact belongs to a different manifest")
            destination = args.out / (label + "-" + name)
            shutil.copy2(item["executable"], destination)
            artifacts[name] = {"path": str(destination),
                               "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}
        if set(artifacts) != names:
            raise RuntimeError(f"missing authoritative Cargo executable paths: {label}")
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=worktree):
            raise RuntimeError("historical source changed during its build")
        receipt["builds"][label] = {**metadata, "artifacts": artifacts}
        # The exact commit/tree, lockfile and binary hashes remain in the receipt.
        # Remove only this tool's clean detached source checkout, never a user checkout.
        subprocess.run(["git", "worktree", "remove", str(worktree)], cwd=ROOT, check=True)
    local_store.durable_write(args.out / "build-receipt.json",
                              json.dumps(receipt, indent=2).encode())
    sources.rmdir()
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="new artifact directory")
    parser.add_argument("--target-dir", type=Path, required=True,
                        help="isolated Cargo target, never the concurrent main checkout target")
    parser.add_argument("--current-revision", default="HEAD")
    parser.add_argument("--reuse-historical-builds", type=Path,
                        help="verified previous full build receipt; copies release/prior only")
    args = parser.parse_args()
    args.out, args.target_dir = args.out.resolve(), args.target_dir.resolve()
    if args.target_dir == ROOT / "target":
        parser.error("use an isolated target directory")
    build(args)
    print(args.out / "build-receipt.json")


if __name__ == "__main__":
    main()
