#!/usr/bin/env python3
"""RC field-evidence verifier (round-13 release-boundary fix 2).

promote-rc.sh --field-legs used to TRUST the evidence manifest: it
checked only that the file existed and that the canary field said
PASS, then copied every claimed identity into the tag message. This
verifier independently recomputes every claim; the mint refuses on
the first mismatch.

  verify-rc-evidence.py MANIFEST --sha TAG_SHA --repo DIR \
      --results DIR --binary FILE --canary-log FILE
  verify-rc-evidence.py --self-test

Checks (review list, verbatim):
  manifest source commit == the intended tag SHA;
  profile sha256 == sha256 of that file IN THE TAG TREE, and every
    recorded pin line appears verbatim in that content;
  harness tree ids == `git rev-parse SHA:path` for each path;
  every field-run directory and stage manifest exists, its sha256
    matches, its reconciliation recomputes to zero lost acked records
    (holes excusable only at unacked/shed q's; victim-class tail
    shortfall is loss; a leg may declare tail_short_class
    "noisy-catchup" to permit NOISY-class catch-up-in-progress only);
  legs marked gates_required have a PASS verdict in eval-<stage>.json;
  legs recording a commit/server sha match the run's binaries.json;
  the exact-artifact binary's sha256 matches server_binary_sha256;
  the canary log's sha256 matches, it ends LIVEFEED_CANARY_OK, and
    the sha it names is the SAME binary.

--self-test builds a synthetic evidence world against the current
repo HEAD, verifies it PASSES, then mutates every field independently
and proves each mutation fails promotion. gate.sh runs it on every
commit.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def git(repo, *args):
    return subprocess.check_output(["git", "-C", repo, *args],
                                   stderr=subprocess.DEVNULL).decode()


def expand(ranges):
    return set(q for a, b in ranges for q in range(a, b + 1))


def recompute_integrity(stage_path, tail_short_class):
    """Zero-lost-acked, recomputed from the raw stage JSON — never
    from any summary field. Returns a list of failures."""
    fails = []
    lines = json.load(open(stage_path))
    dones = [l for l in lines if l.get("phase") == "cert_done"]
    if not dones:
        return [f"{os.path.basename(stage_path)}: no cert_done"]
    d = dones[-1]
    r = d["recon"]
    writer = r.get("writerStreams") or {}
    noisy_from = r.get("noisyFrom")
    if r.get("dupsTotal", 0):
        fails.append(f"duplicates {r['dupsTotal']}")
    if r.get("wrongStreamTotal", 0):
        fails.append(f"wrong-stream {r['wrongStreamTotal']}")
    for t_s, wa in writer.items():
        t = int(t_s)
        sv = (r.get("subStreams") or {}).get(t_s)
        if not wa.get("acked"):
            continue
        acked = expand(wa["acked"])
        if sv is None:
            continue  # no parked subs on this stream for this gen
        if sv.get("holeCount", 0) > 0:
            holes = expand(sv.get("holes") or [])
            distinct = sv.get("holeQsDistinct", sv["holeCount"])
            if len(holes) < distinct:
                fails.append(f"stream {t}: hole union truncated")
            elif holes & acked:
                fails.append(f"stream {t}: {len(holes & acked)} acked records in holes")
        final_q = wa["acked"][-1][1]
        if sv.get("minLast", final_q) < final_q:
            tail = acked & set(range(sv["minLast"] + 1, final_q + 1))
            if tail:
                noisy = noisy_from is not None and t >= noisy_from
                if not (noisy and tail_short_class == "noisy-catchup"):
                    fails.append(f"stream {t}: {len(tail)} acked records short at freeze")
    return fails


def verify(manifest_path, sha, repo, results, binary, canary_log):
    fails = []
    m = json.load(open(manifest_path))
    full = git(repo, "rev-parse", f"{sha}^{{commit}}").strip()
    if m.get("source_sha") != full:
        fails.append(f"source_sha {m.get('source_sha')} != tag sha {full}")

    prof = m.get("profile") or {}
    try:
        content = subprocess.check_output(
            ["git", "-C", repo, "show", f"{full}:{prof.get('file', '')}"],
            stderr=subprocess.DEVNULL)
        if hashlib.sha256(content).hexdigest() != prof.get("sha256"):
            fails.append("profile sha256 mismatch against the tag tree")
        lines = content.decode().splitlines()
        for pin in prof.get("pins") or []:
            if pin not in lines:
                fails.append(f"profile pin not in tag tree: {pin}")
    except subprocess.CalledProcessError:
        fails.append(f"profile file {prof.get('file')} unreadable at tag")

    for path, oid in (m.get("harness_trees") or {}).items():
        try:
            actual = git(repo, "rev-parse", f"{full}:{path}").strip()
        except subprocess.CalledProcessError:
            actual = "<missing>"
        if actual != oid:
            fails.append(f"harness tree {path}: {oid} != {actual}")

    for leg in m.get("field_legs") or []:
        d = os.path.join(results, leg["run_id"])
        stage = os.path.join(d, f"stage-{leg['stage']}-gen0.json")
        if not os.path.isdir(d):
            fails.append(f"{leg['leg']}: run dir {leg['run_id']} missing")
            continue
        if not os.path.exists(stage):
            fails.append(f"{leg['leg']}: stage manifest missing")
            continue
        if sha256_file(stage) != leg.get("stage_sha256"):
            fails.append(f"{leg['leg']}: stage manifest sha mismatch")
            continue
        fails += [f"{leg['leg']}: {f}" for f in
                  recompute_integrity(stage, leg.get("tail_short_class"))]
        if leg.get("gates_required"):
            ev = os.path.join(d, f"eval-{leg['stage']}.json")
            try:
                verdict = json.load(open(ev)).get("verdict")
            except Exception:
                verdict = "<no eval>"
            if verdict != "PASS":
                fails.append(f"{leg['leg']}: gates required but verdict {verdict}")
        binj = os.path.join(d, "binaries.json")
        if leg.get("commit") or leg.get("server_sha256"):
            try:
                bins = json.load(open(binj))
                srv = next(v for k, v in bins.items() if "streams-" in k)
            except Exception:
                fails.append(f"{leg['leg']}: binaries.json unreadable")
                srv = {}
            if leg.get("commit") and srv.get("gitCommit") != leg["commit"]:
                fails.append(f"{leg['leg']}: run built at {srv.get('gitCommit')}, "
                             f"manifest claims {leg['commit']}")
            if leg.get("server_sha256") and srv.get("sha256") != leg["server_sha256"]:
                fails.append(f"{leg['leg']}: run server sha mismatch")

    srv_sha = m.get("server_binary_sha256")
    if not binary or not os.path.exists(binary):
        fails.append("exact-artifact binary missing")
    elif sha256_file(binary) != srv_sha:
        fails.append("binary sha256 != server_binary_sha256")

    can = m.get("exact_artifact_canary") or {}
    if can.get("result") != "PASS":
        fails.append(f"canary result {can.get('result')}")
    if not canary_log or not os.path.exists(canary_log):
        fails.append("canary log missing")
    else:
        if sha256_file(canary_log) != can.get("log_sha256"):
            fails.append("canary log sha mismatch")
        text = open(canary_log, errors="replace").read()
        if f"LIVEFEED_CANARY_OK server=sha256:{srv_sha}" not in text:
            fails.append("canary log does not attest LIVEFEED_CANARY_OK for this binary")
    return fails


# ---- self-test -------------------------------------------------------
def build_world(tmp, repo):
    full = git(repo, "rev-parse", "HEAD").strip()
    results = os.path.join(tmp, "results")
    run_id = "selftest-run-1"
    d = os.path.join(results, run_id)
    os.makedirs(d)
    stage = [{
        "phase": "cert_done",
        "recon": {
            "dupsTotal": 0, "wrongStreamTotal": 0, "noisyFrom": 10,
            "writerStreams": {"0": {"acked": [[0, 4]], "ackedCount": 5,
                                    "unacked": [], "unackedCount": 0,
                                    "shed": [], "shedCount": 0}},
            "subStreams": {"0": {"subs": 1, "minLast": 4, "maxLast": 4,
                                 "holeCount": 0, "holeQsDistinct": 0,
                                 "holes": []}},
        },
    }]
    sp = os.path.join(d, "stage-ST-gen0.json")
    json.dump(stage, open(sp, "w"))
    json.dump({"verdict": "PASS"}, open(os.path.join(d, "eval-ST.json"), "w"))
    json.dump({"bin/streams-x": {"sha256": "aa" * 32, "gitCommit": full}},
              open(os.path.join(d, "binaries.json"), "w"))
    binary = os.path.join(tmp, "artifact.bin")
    open(binary, "wb").write(b"rc artifact bytes")
    srv_sha = sha256_file(binary)
    log = os.path.join(tmp, "canary.log")
    open(log, "w").write(f"...\nLIVEFEED_CANARY_OK server=sha256:{srv_sha}\n")
    prof_path = "deploy/profiles/compute-1g.env"
    prof = subprocess.check_output(["git", "-C", repo, "show", f"{full}:{prof_path}"])
    pins = [l for l in prof.decode().splitlines() if l.startswith("SSE_FEED_TOTAL")]
    manifest = {
        "source_sha": full,
        "server_binary_sha256": srv_sha,
        "profile": {"file": prof_path,
                    "sha256": hashlib.sha256(prof).hexdigest(), "pins": pins},
        "harness_trees": {"bench/soak": git(repo, "rev-parse", f"{full}:bench/soak").strip()},
        "field_legs": [{"leg": "st", "run_id": run_id, "stage": "ST",
                        "stage_sha256": sha256_file(sp), "gates_required": True,
                        "commit": full, "server_sha256": "aa" * 32}],
        "exact_artifact_canary": {"result": "PASS", "log_sha256": sha256_file(log)},
    }
    mp = os.path.join(tmp, "manifest.json")
    json.dump(manifest, open(mp, "w"))
    return mp, full, results, binary, log, manifest, sp, d


def self_test(repo):
    with tempfile.TemporaryDirectory() as tmp:
        mp, full, results, binary, log, manifest, sp, d = build_world(tmp, repo)
        ok = verify(mp, full, repo, results, binary, log)
        assert not ok, f"clean world must verify: {ok}"

        def mutated(mut, name):
            m2 = json.loads(json.dumps(manifest))
            mut(m2)
            p2 = os.path.join(tmp, "mut.json")
            json.dump(m2, open(p2, "w"))
            fails = verify(p2, full, repo, results, binary, log)
            assert fails, f"mutation NOT caught: {name}"

        mutated(lambda m: m.update(source_sha="0" * 40), "source_sha")
        mutated(lambda m: m["profile"].update(sha256="0" * 64), "profile sha")
        mutated(lambda m: m["profile"]["pins"].append("SSE_FEED_TOTAL_BYTES=1"), "profile pin")
        mutated(lambda m: m["harness_trees"].update({"bench/soak": "0" * 40}), "harness tree")
        mutated(lambda m: m["field_legs"][0].update(stage_sha256="0" * 64), "stage sha")
        mutated(lambda m: m["field_legs"][0].update(run_id="missing-run"), "run dir")
        mutated(lambda m: m["field_legs"][0].update(commit="0" * 40), "leg commit")
        mutated(lambda m: m["field_legs"][0].update(server_sha256="bb" * 32), "leg server sha")
        mutated(lambda m: m.update(server_binary_sha256="0" * 64), "binary sha")
        mutated(lambda m: m["exact_artifact_canary"].update(result="FAIL"), "canary result")
        mutated(lambda m: m["exact_artifact_canary"].update(log_sha256="0" * 64), "canary log sha")

        # Evidence-side mutations: loss in the stage JSON, red eval.
        stage = json.load(open(sp))
        stage[0]["recon"]["subStreams"]["0"]["minLast"] = 2  # acked 3,4 undelivered
        json.dump(stage, open(sp, "w"))
        m2 = json.loads(json.dumps(manifest))
        m2["field_legs"][0]["stage_sha256"] = sha256_file(sp)
        p2 = os.path.join(tmp, "mut.json")
        json.dump(m2, open(p2, "w"))
        assert verify(p2, full, repo, results, binary, log), "acked tail loss NOT caught"
        stage[0]["recon"]["subStreams"]["0"]["minLast"] = 4
        json.dump(stage, open(sp, "w"))

        json.dump({"verdict": "FAIL"}, open(os.path.join(d, "eval-ST.json"), "w"))
        m2["field_legs"][0]["stage_sha256"] = sha256_file(sp)
        json.dump(m2, open(p2, "w"))
        assert verify(p2, full, repo, results, binary, log), "red eval verdict NOT caught"
        json.dump({"verdict": "PASS"}, open(os.path.join(d, "eval-ST.json"), "w"))

        open(binary, "ab").write(b"x")
        assert verify(mp, full, repo, results, binary, log), "binary tamper NOT caught"
        open(binary, "wb").write(b"rc artifact bytes")

        open(log, "a").write("x")
        assert verify(mp, full, repo, results, binary, log), "canary log tamper NOT caught"
    print("VERIFY_RC_EVIDENCE_SELFTEST_OK (15 mutations all refused)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manifest", nargs="?")
    ap.add_argument("--sha")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--results")
    ap.add_argument("--binary")
    ap.add_argument("--canary-log")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        self_test(a.repo)
        return
    if not (a.manifest and a.sha and a.results):
        ap.error("manifest, --sha and --results required")
    fails = verify(a.manifest, a.sha, a.repo, a.results, a.binary, a.canary_log)
    for f in fails:
        print(f"EVIDENCE_FAIL: {f}")
    if fails:
        sys.exit(1)
    print("EVIDENCE_VERIFIED")


if __name__ == "__main__":
    main()
