#!/usr/bin/env python3
"""Exhaust every reachable state; fail closed on invariant/coverage/mutant errors."""
import argparse
from collections import deque
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import history
import ownership

ROOT = Path(__file__).resolve().parents[3]
SOURCE_ANCHORS = {
    "Cargo.toml": ['git = "https://github.com/slatedb/slatedb.git"'],
    "src/history.rs": ["fn history_settings(", "garbage_collector_options: Some(gc)"],
    "src/shard/transaction/append.rs": ["ProducerDecision::Reply(reply)", "self.effects.acks.push((req.resp, reply))"],
    "src/crypto.rs": ["pub(crate) fn for_stream(", "pub(crate) fn for_segment("],
    "src/tenant.rs": ["fn storage_hash_input(", "stream_epoch.as_bytes()"],
    "src/application/creation/anchor.rs": ["next.fork_children.push(fork_id.clone())"],
    "src/application/creation/deletion.rs": ["!current.fork_children.is_empty()", "next.soft_deleted = true"],
    "src/shard/commit_handoff.rs": ["fn publication(", "fn attach(", "fn take_durable(", "fn retire("],
    "src/shard/transaction/finalize.rs": ["async fn join_prior_barrier(", "async fn write("],
    "src/shard/transaction/publish.rs": ["handoff.publication()"],
    "src/shard.rs": ["async fn dispatch_durable(", "take_durable(durable_seq)"],
    "src/history/gather.rs": ["async fn commit(", "part.flush().await?", "submit_absorbed_batch_v2"],
    "src/shard/transaction/maintenance.rs": ["trim_safe_to.max(prev_absorbed)", "fn trim("],
    "src/application/creation/fork.rs": ["f.source_epoch == d.stream_epoch", "d.fork_children.contains(&f.fork_id)"],
}


def source_receipt():
    files = {}
    for path, anchors in SOURCE_ANCHORS.items():
        content = (ROOT / path).read_bytes()
        missing = [anchor for anchor in anchors if anchor.encode() not in content]
        if missing:
            raise RuntimeError(f"implementation mapping drift in {path}: {missing}")
        files[path] = hashlib.sha256(content).hexdigest()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    return {"revision": revision, "source_sha256": files}


def trace(parents, state):
    labels = []
    while parents[state] is not None:
        state, label = parents[state]
        labels.append(label)
    return list(reversed(labels))


def explore(model, mutation=None):
    initial_violation = model.invariant(model.INITIAL)
    if initial_violation:
        raise RuntimeError(f"invalid initial state: {initial_violation}")
    parents = {model.INITIAL: None}
    queue = deque([model.INITIAL])
    edges = 0
    coverage = {}
    labels = set()
    while queue:
        before = queue.popleft()
        for label, after in model.transitions(before, mutation):
            edges += 1
            labels.add(label)
            violation = model.edge_invariant(before, label, after) or model.invariant(after)
            if violation:
                return {
                    "states_discovered": len(parents), "transitions_checked": edges,
                    "violation": violation, "trace": trace(parents, before) + [label],
                    "state": asdict(after),
                }
            for witness in sorted(model.witnesses(before, label, after)):
                if witness not in coverage:
                    coverage[witness] = trace(parents, before) + [label]
            if after not in parents:
                parents[after] = (before, label)
                queue.append(after)
    return {
        "states_exhausted": len(parents), "transitions_checked": edges,
        "transition_labels": sorted(labels), "reachability_witnesses": coverage,
    }


def replay_counterexamples(model):
    fixtures = json.loads(Path(__file__).with_name("counterexamples.json").read_text())[model.__name__]
    if fixtures.keys() != model.MUTATIONS.keys():
        raise RuntimeError(f"{model.__name__} counterexample inventory mismatch")
    for mutation, fixture in fixtures.items():
        state = model.INITIAL
        steps = fixture["trace"]
        for index, label in enumerate(steps):
            candidates = [after for action, after in model.transitions(state, mutation) if action == label]
            if len(candidates) != 1:
                raise RuntimeError(f"{model.__name__}/{mutation} cannot replay step {label!r}")
            after = candidates[0]
            violation = model.edge_invariant(state, label, after) or model.invariant(after)
            expected = model.MUTATIONS[mutation] if index == len(steps) - 1 else None
            if violation != expected or fixture["violation"] != model.MUTATIONS[mutation]:
                raise RuntimeError(f"{model.__name__}/{mutation} regression mechanism changed: {violation}")
            state = after
        if not steps:
            raise RuntimeError(f"{model.__name__}/{mutation} empty counterexample")


def run(model):
    replay_counterexamples(model)
    baseline = explore(model)
    if "violation" in baseline:
        raise RuntimeError(f"{model.__name__} baseline failed: {json.dumps(baseline)}")
    missing = model.REQUIRED - baseline["reachability_witnesses"].keys()
    if missing:
        raise RuntimeError(f"{model.__name__} unreachable required cases: {sorted(missing)}")
    mutations = {}
    for mutation, expected in model.MUTATIONS.items():
        result = explore(model, mutation)
        if result.get("violation") != expected:
            raise RuntimeError(f"{model.__name__}/{mutation} expected {expected}: {json.dumps(result)}")
        mutations[mutation] = result
    return {"bounds": model.BOUNDS, "baseline": baseline, "semantic_mutations": mutations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write deterministic detailed JSON receipt")
    args = parser.parse_args()
    result = {"schema": 1, "scope": "exhaustive finite abstract models; not a whole-service proof"}
    result["implementation"] = source_receipt()
    result["model_sha256"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path(__file__).parent.iterdir())
        if path.suffix in {".py", ".json"}
    }
    result["models"] = {}
    for model in (ownership, history):
        result["models"][model.__name__] = run(model)
        checked = result["models"][model.__name__]
        baseline = checked["baseline"]
        print(f"{model.__name__}: {baseline['states_exhausted']} states, "
              f"{baseline['transitions_checked']} transitions, "
              f"{len(baseline['reachability_witnesses'])} required witnesses, "
              f"{len(checked['semantic_mutations'])} executable semantic mutations detected", flush=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print(f"protocol models FAILED: {error}", file=sys.stderr)
        sys.exit(1)
