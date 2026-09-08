#!/usr/bin/env python3
"""Real sibling-module privacy and typed Clippy fixtures in a disposable copy.

The module is inside the actual crate so private root modules do not make the
negative cases fail vacuously. Positive controls must compile first. Production
visibility and implementations are never rewritten for the fixture.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from common import ROOT, write_json

PREFIX = '#![allow(dead_code, reason = "compiler fixture; compile both positive and negative cases; functions are type-checked without execution")]\n'
IMPORTS = '''use crate::postings::{ValidatedRuns as Runs, RunWindow as Window};
use crate::shard::record::CheckedFrame as Frame;
use crate::application::read_batch::{PlainBatch as Batch, PlainPayload as Payload};
use crate::history::span_cache::CipherSpan as Span;
'''
POSITIVE = '''
fn controls(frame: &Frame, batch: &Batch, payload: &Payload, span: &Span) {
    let _view = frame.view(); let _records = batch.iter();
    let _bytes = payload.as_ref(); let _frames = span.frames();
    let owner = Runs::new(Vec::new()).unwrap();
    let window = Window::new(owner, 0, 0); assert_eq!(window.iter().count(), 0);
}
fn released_before_await(window: Window) -> impl std::future::Future<Output=()> { drop(window); async { tokio::task::yield_now().await; } }
'''
CASES = [
 ('frame_construct', 'E0451', 'fn frame_construct() { let _x = Frame { raw:bytes::Bytes::new(), offset:0, timestamp:0, key_version:0, routing_end:0, header_len:0, version:0 }; }'),
 ('frame_mutate', 'E0616', 'fn frame_mutate(x: &mut Frame) { x.offset = 0; }'),
 ('runs_construct', 'E0423', 'fn runs_construct() { let _x = Runs(std::sync::Arc::from([])); }'),
 ('runs_mutate', 'E0616', 'fn runs_mutate(x: &mut Runs) { x.0 = std::sync::Arc::from([]); }'),
 ('window_construct', 'E0451', 'fn window_construct() { let _x = Window { owner:Runs::empty(), indices:0..0, from:0, upto:0 }; }'),
 ('window_mutate', 'E0616', 'fn window_mutate(x: &mut Window) { x.indices = 0..0; }'),
 ('batch_construct', 'E0451', 'fn batch_construct(x: Batch) { let _x = Batch { records:Vec::new(), ..x }; }'),
 ('batch_mutate', 'E0616', 'fn batch_mutate(x: &mut Batch) { x.records.clear(); }'),
 ('payload_construct', 'E0451', 'fn payload_construct() { let _x = Payload { owner:bytes::Bytes::new(), range:0..0 }; }'),
 ('payload_mutate', 'E0616', 'fn payload_mutate(x: &mut Payload) { x.range = 0..0; }'),
 ('span_construct', 'E0451', 'fn span_construct(x: Span) { let _x = Span { frames:Vec::new().into_boxed_slice(), ..x }; }'),
 ('span_mutate', 'E0616', 'fn span_mutate(x: &mut Span) { x.frames = Vec::new().into_boxed_slice(); }'),
]
TYPED = [
 ('hold_window', 'clippy::await_holding_invalid_type', 'async fn hold_window(window: Window) { tokio::task::yield_now().await; drop(window); }'),
 ('hold_lock', 'clippy::await_holding_lock', 'async fn hold_lock(lock: &std::sync::Mutex<()>) { let guard=lock.lock().unwrap(); tokio::task::yield_now().await; drop(guard); }'),
 ('spawn_alias', 'clippy::disallowed_methods', 'fn spawn_alias() { use tokio::spawn as launch; let _task = launch(async {}); }'),
 ('blocking_alias', 'clippy::disallowed_methods', 'fn blocking_alias() { use tokio::task::{spawn_blocking as launch}; let _task = launch(|| ()); }'),
 ('join_set', 'clippy::disallowed_methods', 'fn join_set() { let mut tasks=tokio::task::JoinSet::new(); let _task=tasks.spawn(async {}); }'),
 ('builder', 'clippy::disallowed_methods', 'fn builder() { let _thread=std::thread::Builder::new().spawn(|| ()); }'),
 ('environment_alias', 'clippy::disallowed_methods', 'fn environment_alias() { use std::env::var as read; let _value=read("QUALITY_FIXTURE"); }'),
 ('forget_alias', 'clippy::disallowed_methods', 'fn forget_alias(value: String) { use std::mem::forget as discard; discard(value); }'),
]


def copy_source(destination):
    destination.mkdir(parents=True, exist_ok=False)
    for name in ('src', 'tools', 'fuzz'):
        shutil.copytree(ROOT / name, destination / name, ignore=shutil.ignore_patterns('target', '__pycache__'))
    for name in ('Cargo.toml', 'Cargo.lock', 'build.rs', 'RUNBOOK.md', 'clippy.toml', 'rust-toolchain.toml'):
        if (ROOT / name).exists():
            shutil.copy2(ROOT / name, destination / name)
    with (destination / 'src/lib.rs').open('a') as root:
        root.write('\nmod quality_boundary_fixture;\n')


def run(copy, out, name, content, mode, cases=()):
    fixture = copy / 'src/quality_boundary_fixture.rs'
    fixture.write_text(content)
    command = ['cargo', mode, '--locked', '--lib', '--message-format=json']
    with (out / f'{name}.jsonl').open('w') as stdout, (out / f'{name}.stderr').open('w') as stderr:
        result = subprocess.run(command, cwd=copy, stdout=stdout, stderr=stderr)
    diagnostics = [entry['message'] for entry in map(json.loads, (out / f'{name}.jsonl').read_text().splitlines())
                   if entry.get('reason') == 'compiler-message']
    errors = [d for d in diagnostics if d['level'] == 'error']
    if not cases:
        if result.returncode or errors:
            raise RuntimeError(f'{name}: legitimate control failed; see {out}')
        return {'name': name, 'exit_code': 0}
    if result.returncode != 101 or not errors:
        raise RuntimeError(f'{name}: negative fixture did not fail as a Rust diagnostic')
    expected_lines = {}
    for case, code, source in cases:
        line = content.splitlines().index(source) + 1
        expected_lines[line] = (case, code)
    observed = set()
    for error in errors:
        code = (error.get('code') or {}).get('code')
        matches = [(s['line_start'], code) for s in error['spans'] if s['is_primary']
                   and s['file_name'].endswith('quality_boundary_fixture.rs')]
        found = False
        for line, actual in matches:
            if line in expected_lines and expected_lines[line][1] == actual:
                observed.add(expected_lines[line][0]); found = True
        if not found:
            raise RuntimeError(f'{name}: unrelated compiler error: {code}: {error["message"]}')
    missing = {case for case, _, _ in cases} - observed
    if missing:
        raise RuntimeError(f'{name}: missing required diagnostics: {sorted(missing)}')
    return {'name': name, 'exit_code': result.returncode, 'verified_cases': sorted(observed)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    out = Path(args.out).resolve(); out.mkdir(parents=True, exist_ok=True)
    copy = out / 'source'
    copy_source(copy)
    results = [run(copy, out, 'positive', PREFIX + IMPORTS + POSITIVE, 'clippy')]
    for phase, cases in [('private-mutation', [c for c in CASES if c[1] != 'E0451']), ('private-construction', [c for c in CASES if c[1] == 'E0451'])]:
        results.append(run(copy, out, phase, PREFIX + IMPORTS + '\n'.join(c[2] for c in cases), 'check', cases))
    typed = PREFIX + '#![deny(clippy::disallowed_methods)]\n' + IMPORTS
    results.append(run(copy, out, 'typed-effects', typed + '\n'.join(c[2] for c in TYPED), 'clippy', TYPED))
    # Positive control after negatives: catches a stale/cached failing artifact.
    results.append(run(copy, out, 'positive-after', PREFIX + IMPORTS + POSITIVE, 'clippy'))
    write_json(out / 'summary.json', results)
    print(f'compiler fixtures: OK ({len(CASES)} privacy + {len(TYPED)} typed violations; two legitimate controls)')


if __name__ == '__main__':
    main()
