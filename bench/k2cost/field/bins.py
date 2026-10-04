#!/usr/bin/env python3
"""Upload the field binaries to the artifact bucket (pattern:
bench/soak/build-upload.sh), and compile the in-region generator.

    bench/k2cost/field/bins.py --tag TAG [--k2gen DIR] [--variant glibc|musl]

- streams-slate and pilot come only from ~/.streams-k2/bin/<TAG>/
  (x86_64-musl, built from the commit its git-commit.txt names; TAG, or
  K2_BIN_TAG, is that commit's short hash): their sha256 must match that
  directory's SHA256SUMS, their ELF e_machine (byte 18) must be 0x3e.
- k2gen is compiled from DIR/k2gen.ts (default: this tree's
  bench/k2cost) with `bun build --compile --target=bun-linux-x64` into
  ~/.streams-k2/bin/k2gen/<variant>-<sha12>/. Compute's image is glibc
  (the wrappers' `instance shape:` line, 2026-10-02): the
  bun-linux-x64-musl build (`--variant musl`) needs /lib/ld-musl-x86_64.so.1
  plus libstdc++ and libgcc_s, and cannot exec there.
- Keys: bin/k2c-streams-<TAG>-x64, bin/k2c-pilot-<TAG>-x64,
  bin/k2c-k2gen-<variant>-<sha12>-x64. Each PUT is verified with ranged
  GETs of its first and last 16 bytes; an object that already exists with
  the same size and the same first/last bytes is not uploaded again.
- The manifest (key -> sha256, bytes, git commit, build time, source) is
  written to $K2_FIELD_HOME/bins.json, which deploy.py and gen.py read.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess

import botocore.exceptions

import fieldlib as F

BIN_HOME = os.path.expanduser("~/.streams-k2/bin")
K2GEN_DEFAULT = os.path.abspath(os.path.join(F.HERE, ".."))
K2GEN_SOURCES = ("k2gen.ts", "common.ts", "produce.ts", "consume.ts", "churn.ts", "corpus.ts")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def check_x86_64(path: str, data: bytes) -> None:
    if data[:4] != b"\x7fELF" or data[18] != 0x3E:
        F.die(f"{path} is not an x86_64 ELF (e_machine byte 18 = {data[18]:#x}): an aarch64 binary "
              "crash-loops as a silent zombie")


def upload(client, bucket: str, key: str, data: bytes) -> str:
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] == len(data):
            first = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-15")["Body"].read()
            last = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={len(data) - 16}-{len(data) - 1}")["Body"].read()
            if first == data[:16] and last == data[-16:]:
                return "present (size and ranged GETs match)"
    except botocore.exceptions.ClientError:
        pass
    F.put_verified(client, bucket, key, data)
    return "uploaded and verified by ranged GETs"


def release_binaries(tag: str) -> list:
    d = os.path.join(BIN_HOME, tag)
    sums = {}
    with open(os.path.join(d, "SHA256SUMS"), encoding="utf-8") as f:
        for line in f:
            h, name = line.split()
            sums[name] = h
    commit = open(os.path.join(d, "git-commit.txt")).read().strip()
    built = open(os.path.join(d, "build-unix.txt")).read().strip()
    out = []
    for name, short in (("streams-slate", "streams"), ("pilot", "pilot")):
        path = os.path.join(d, name)
        data = open(path, "rb").read()
        if sha256(data) != sums[name]:
            F.die(f"{path}: sha256 differs from SHA256SUMS")
        check_x86_64(path, data)
        out.append((f"bin/k2c-{short}-{tag}-x64", data, {"gitCommit": commit, "buildUnix": built, "source": path}))
    return out


def k2gen_binary(src_dir: str, variant: str):
    srcs = {}
    for name in K2GEN_SOURCES:
        with open(os.path.join(src_dir, name), "rb") as f:
            srcs[name] = sha256(f.read())
    src_digest = sha256("".join(f"{k}:{v}\n" for k, v in sorted(srcs.items())).encode())[:12]
    target = "bun-linux-x64-musl" if variant == "musl" else "bun-linux-x64"
    outdir = os.path.join(BIN_HOME, "k2gen", f"{variant}-{src_digest}")
    os.makedirs(outdir, exist_ok=True)
    out = os.path.join(outdir, "k2gen")
    if not os.path.exists(out):
        bun = subprocess.run(["bun", "--version"], capture_output=True, text=True).stdout.strip()
        F.say(f"  compiling k2gen ({target}, bun {bun}) from {src_dir}")
        subprocess.run(["bun", "build", "--compile", f"--target={target}", os.path.join(src_dir, "k2gen.ts"),
                        "--outfile", out], check=True, cwd=outdir)
    data = open(out, "rb").read()
    check_x86_64(out, data)
    bun = subprocess.run(["bun", "--version"], capture_output=True, text=True).stdout.strip()
    meta = {"source": src_dir, "sourceSha256": srcs, "target": target, "bun": bun}
    return f"bin/k2c-k2gen-{variant}-{src_digest}-x64", data, meta


def main() -> None:
    F.banner("bins")
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=os.environ.get("K2_BIN_TAG"),
                    help="the binaries' directory under ~/.streams-k2/bin (or K2_BIN_TAG)")
    ap.add_argument("--k2gen", default=K2GEN_DEFAULT, help="directory holding k2gen.ts and its modules")
    ap.add_argument("--variant", choices=("musl", "glibc"), default="glibc")
    a = ap.parse_args()
    if not a.tag:
        F.die("name the binaries: --tag TAG or K2_BIN_TAG (a directory under ~/.streams-k2/bin)")
    client, bucket = F.artifact_s3()
    manifest = F.read_json(os.path.join(F.FIELD, "bins.json"), {}) or {}
    items = release_binaries(a.tag) + [k2gen_binary(a.k2gen, a.variant)]
    for key, data, meta in items:
        what = upload(client, bucket, key, data)
        manifest[key] = {"sha256": sha256(data), "bytes": len(data), "role": key.split("-")[1], **meta,
                         "verified": F.utc()}
        F.say(f"  {key}: {len(data)} bytes sha256 {sha256(data)[:16]} - {what}")
    manifest["_latest"] = {"streams": items[0][0], "pilot": items[1][0], "k2gen": items[2][0]}
    F.write_json(os.path.join(F.FIELD, "bins.json"), manifest)
    F.say(f"  manifest -> {os.path.join(F.FIELD, 'bins.json')}")


if __name__ == "__main__":
    main()
