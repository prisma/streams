"""Quiescent object copying for the isolated s3lite restore drill.

This is intentionally loopback-only, unsigned, and not an operational S3 backup.
The caller must kill every writer before capture. Object names never become
filesystem paths; a manifest and SHA-256 bind every copied object to its name.
"""
import hashlib
import json
import os
from pathlib import Path
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def local_url(base):
    parsed = urllib.parse.urlsplit(base)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment):
        raise ValueError("restore drill requires an isolated http://127.0.0.1 endpoint")
    return base.rstrip("/")


def request(base, path, data=None):
    req = urllib.request.Request(local_url(base) + path, data=data)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read()


def bucket_path(bucket):
    return "/" + urllib.parse.quote(bucket, safe="") + "/"


def objects(base, bucket):
    """Read complete ListObjectsV2 output, rejecting ambiguous pagination."""
    found, tokens, token = {}, set(), None
    for _ in range(10000):
        query = {"list-type": "2", "max-keys": "1000"}
        if token is not None:
            query["continuation-token"] = token
        root = ET.fromstring(request(base, bucket_path(bucket) + "?"
                                     + urllib.parse.urlencode(query)))
        for element in root.iter():
            element.tag = element.tag.rsplit("}", 1)[-1]
        for item in root.findall("Contents"):
            key, etag = item.findtext("Key"), item.findtext("ETag")
            if not key or not etag or key in found:
                raise ValueError("invalid/duplicate listed object")
            found[key] = (etag, int(item.findtext("Size")))
        truncated = root.findtext("IsTruncated")
        if truncated == "false":
            return found
        token = root.findtext("NextContinuationToken")
        if truncated != "true" or not token or token in tokens:
            raise ValueError("incomplete object listing")
        tokens.add(token)
    raise ValueError("object listing exceeded page bound")


def durable_write(path, data):
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    sync_directory(path.parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def capture(base, bucket, directory):
    directory = Path(directory)
    directory.mkdir()
    sync_directory(directory.parent)
    before = objects(base, bucket)
    if not before:
        raise ValueError("refusing vacuous empty backup")
    entries = []
    for number, (key, (_, size)) in enumerate(sorted(before.items())):
        data = request(base, bucket_path(bucket) + urllib.parse.quote(key, safe="/"))
        if len(data) != size:
            raise ValueError("object changed during quiescent copy")
        filename = f"{number:08d}.object"
        durable_write(directory / filename, data)
        entries.append({"key": key, "file": filename, "size": size,
                        "sha256": hashlib.sha256(data).hexdigest()})
    if before != objects(base, bucket):
        raise ValueError("storage changed while copying; writers must be stopped")
    manifest = {"version": 1, "objects": entries}
    durable_write(directory / "manifest.json", json.dumps(manifest, sort_keys=True).encode())
    sync_directory(directory)
    return {"objects": len(entries), "bytes": sum(item["size"] for item in entries)}


def validate(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    if manifest.get("version") != 1 or not manifest.get("objects"):
        raise ValueError("invalid or empty backup manifest")
    keys, files, entries = set(), set(), []
    for number, entry in enumerate(manifest["objects"]):
        key, filename = entry["key"], entry["file"]
        if (not isinstance(key, str) or not key or key in keys
                or filename != f"{number:08d}.object" or filename in files):
            raise ValueError("invalid backup object identity")
        data = (directory / filename).read_bytes()
        if len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError(f"backup object integrity failure: {filename}")
        keys.add(key)
        files.add(filename)
        entries.append((key, data))
    if {path.name for path in directory.iterdir()} != files | {"manifest.json"}:
        raise ValueError("backup has unaccounted files")
    return entries


def restore(base, bucket, directory):
    # Validate ALL bytes before any mutation. Destination is an empty emulator
    # created by the caller; refuse to overwrite even an unrelated bucket.
    entries = validate(directory)
    stats = json.loads(request(base, "/_s3lite/stats"))
    if stats.get("objects") != 0:
        raise ValueError("restore requires a fresh empty s3lite process")
    for key, data in entries:
        req = urllib.request.Request(
            local_url(base) + bucket_path(bucket) + urllib.parse.quote(key, safe="/"),
            data=data, method="PUT")
        with urllib.request.urlopen(req, timeout=30) as response:
            response.read()
    restored = objects(base, bucket)
    if set(restored) != {key for key, _ in entries}:
        raise ValueError("restored object set differs from manifest")
    for key, data in entries:
        if request(base, bucket_path(bucket) + urllib.parse.quote(key, safe="/")) != data:
            raise ValueError("restored object bytes differ from backup")
    return {"objects": len(entries), "bytes": sum(len(data) for _, data in entries)}
