#!/usr/bin/env python3
"""Run the executable provider contract with explicit targets and a local receipt."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib
import urllib.parse

from local_store import durable_write

ROOT = Path(__file__).resolve().parents[2]
CHECKS = {"single-create-winner", "single-update-winner", "winner-exact-bytes",
          "stale-update-rejected", "overwrite-read-head-range-list", "delete-get-head-list"}


def endpoint(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
            or (parsed.scheme == "http" and parsed.hostname != "127.0.0.1")):
        raise argparse.ArgumentTypeError("endpoint must be an HTTPS origin, or an owned loopback HTTP origin; no credentials/query/path")
    return value.rstrip("/")


def contract_result(stdout, prefix, concurrency):
    markers = [line.removeprefix("PROVIDER_CONTRACT_OK ") for line in stdout.decode(errors="replace").splitlines()
               if line.startswith("PROVIDER_CONTRACT_OK ")]
    if len(markers) != 1:
        return None
    try:
        result = json.loads(markers[0])
        valid = (isinstance(result, dict) and result.get("concurrency") == concurrency
                 and isinstance(result.get("prefix"), str)
                 and result["prefix"].startswith(prefix.strip("/") + "/contract-")
                 and isinstance(result.get("checks"), list)
                 and all(isinstance(item, str) for item in result["checks"])
                 and set(result["checks"]) == CHECKS and len(result["checks"]) == len(CHECKS))
        return result if valid else None
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path, required=True)
    parser.add_argument("--endpoint", type=endpoint, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--prefix", required=True, help="explicit disposable namespace; each probe adds a random sub-prefix")
    parser.add_argument("--concurrency", type=int, default=8, choices=range(2, 65))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.verify = args.verify.resolve(strict=True)
    binary_hash = hashlib.sha256(args.verify.read_bytes()).hexdigest()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    command = [str(args.verify), "contract", "--s3-endpoint", args.endpoint,
               "--bucket", args.bucket, "--region", args.region, "--prefix", args.prefix,
               "--n", str(args.concurrency)]
    timed_out = False
    try:
        result = subprocess.run(command, capture_output=True, timeout=180)
        stdout, stderr, exit_code = result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired as error:
        stdout, stderr, exit_code, timed_out = error.stdout or b"", error.stderr or b"", None, True
    durable_write(args.out / "stdout.log", stdout)
    durable_write(args.out / "stderr.log", stderr)
    contract = contract_result(stdout, args.prefix, args.concurrency)
    binary_unchanged = hashlib.sha256(args.verify.read_bytes()).hexdigest() == binary_hash
    passed = exit_code == 0 and contract is not None and not timed_out and binary_unchanged
    pins = tomllib.loads((ROOT / "quality-tools.toml").read_text())
    receipt = {"status": "passed" if passed else "failed", "endpoint": args.endpoint,
               "bucket": args.bucket, "region": args.region, "requested_prefix": args.prefix,
               "exit_code": exit_code, "timed_out": timed_out, "contract": contract,
               "binary_sha256": binary_hash, "binary_unchanged": binary_unchanged,
               "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "configured_repository_pins": {"rust": pins["rust"], "slatedb": pins["slatedb"]},
               "provenance": "binary digest and repository configuration; establish reproducible build provenance separately",
               "scope": "observed object API contract at this endpoint/configuration; no provider durability, power-loss or cross-region guarantee"}
    durable_write(args.out / "receipt.json", json.dumps(receipt, indent=2).encode())
    if not passed:
        raise SystemExit(f"provider contract failed; evidence: {args.out}")
    print(args.out / "receipt.json")


if __name__ == "__main__":
    main()
