#!/usr/bin/env python3
"""Run a push's whole mutation deal on this machine before its proof PR (Addendum 66 item 1).

CI deals a push's in-diff mutants over jobs (scripts/quality/mutants_width.py) and each job runs
its group through scripts/quality/mutations.sh. This plays the same deal here: it computes the
plan job's width and digest for the committed HEAD against `--base` (default origin/slate), then
runs every group with exactly CI's driver, commands, tests and timeouts, `--parallel` groups at a
time, each under its own `--out` directory (its own tree copy and target directory). Each
survivor is printed as its group's log shows it; at the end, every group's verdict.

Standing rule (Addendum 66): no proof PR goes up with a local MISSED, TIMEOUT or incomplete.
Timing differs from CI's runners, so a local TIMEOUT is a survivor all the same, and a clean
local deal does not stand in for CI's verdict.

Run it alone on the machine (under ~/.streams-k2/bin/locked.sh), on a clean committed tree, in
the background, and edit nothing in the checkout while it runs (the driver lists from it):

    python3 scripts/dev/local_mutants.py --out ~/.streams-k2/mut/<name> --parallel 4
"""
import sys

import os  # noqa: E402
import shutil  # noqa: E402

if sys.version_info < (3, 11):
    # macOS's /usr/bin/python3 is 3.9: re-run under the newest 3.11+ installed.
    if not os.environ.get('STREAMS_PYTHON_REEXEC'):
        os.environ['STREAMS_PYTHON_REEXEC'] = '1'
        for candidate in ('python3.14', 'python3.13', 'python3.12', 'python3.11',
                          '/opt/homebrew/bin/python3.13', '/opt/homebrew/bin/python3.12',
                          '/opt/homebrew/bin/python3.11'):
            found = shutil.which(candidate)
            if found:
                os.execv(found, [found, *sys.argv])
    sys.exit('local_mutants.py: Python >= 3.11 is required (the gate modules import tomllib); '
             'install one (brew install python@3.12) or `. scripts/dev/env.sh` first')

import argparse  # noqa: E402
from concurrent.futures import ThreadPoolExecutor  # noqa: E402
from pathlib import Path  # noqa: E402
import re  # noqa: E402
import subprocess  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SURVIVOR = re.compile(r'^(MISSED|TIMEOUT|INCOMPLETE)\b.*$', re.M)


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def push_env(base):
    """CI's push environment for HEAD against `base` (AGENTS.md section 7)."""
    return {**os.environ, 'QUALITY_EVENT_NAME': 'push', 'QUALITY_HEAD_SHA': git('rev-parse', 'HEAD'),
            'QUALITY_BEFORE_SHA': git('rev-parse', base), 'QUALITY_BASE_REF': base}


def parse_outputs(text):
    """The plan job's GITHUB_OUTPUT lines as a dict."""
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


def plan(out, env):
    """(width, digest) of the push's deal, as the mutants-plan job computes it."""
    outputs = out / 'plan.outputs'
    outputs.unlink(missing_ok=True)
    subprocess.run([sys.executable, 'scripts/quality/mutants_width.py', '--out', str(out / 'plan')],
                   cwd=ROOT, env={**env, 'GITHUB_OUTPUT': str(outputs)}, check=True)
    values = parse_outputs(outputs.read_text())
    return int(values['width']), values['digest']


def survivors(log):
    """Each MISSED, TIMEOUT or INCOMPLETE line of a group's log, in order."""
    return [match[0] for match in SURVIVOR.finditer(log)]


def watch(logs, stop):
    """Print each survivor line as a group's log shows it."""
    seen = set()
    while not stop.wait(10):
        for log in logs:
            text = log.read_text(errors='replace') if log.is_file() else ''
            for line in survivors(text):
                if (log, line) not in seen:
                    seen.add((log, line))
                    print(f'{log.stem}: {line}', flush=True)


def run_group(index, width, digest, out, env):
    log = out / f'g{index}.log'
    group_env = {**env, 'QUALITY_MUTANT_GROUP': f'{index}/{width}', 'QUALITY_MUTANT_DIGEST': digest,
                 'QUALITY_MUTANTS_OUT': str(out / f'g{index}')}
    started = time.monotonic()
    with log.open('w') as sink:
        code = subprocess.run(['scripts/quality/mutations.sh'], cwd=ROOT, env=group_env,
                              stdout=sink, stderr=subprocess.STDOUT).returncode
    text = log.read_text(errors='replace')
    found = survivors(text)
    verdict = 'clean' if code == 0 else 'survivors' if 'Mutation survivors' in text else f'ERROR exit {code}'
    tested = re.findall(r'^\d+ mutants? tested in [^\n]*', text, re.M)
    print(f'g{index}: {verdict} in {(time.monotonic() - started) / 60:.0f} min; '
          f'{"; ".join(tested) or "nothing tested"} [{log}]', flush=True)
    return index, verdict, found


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--out', required=True, type=Path, help='a directory outside the checkout')
    parser.add_argument('--parallel', type=int, default=4)
    parser.add_argument('--base', default='origin/slate')
    args = parser.parse_args()
    out = args.out.expanduser().resolve()
    if out.is_relative_to(ROOT.resolve()):
        sys.exit('--out must be outside the checkout (the source ratchet parses every .rs in it)')
    if git('status', '--porcelain', '--untracked-files=no'):
        sys.exit('commit first: the driver lists and copies the live checkout')
    out.mkdir(parents=True, exist_ok=True)
    env = push_env(args.base)
    width, digest = plan(out, env)
    if not digest:
        print('LOCAL_MUTANTS_OK: the push selects no mutants')
        return
    print(f'local deal: {width} group(s), digest {digest}, {args.parallel} at a time, HEAD '
          f'{env["QUALITY_HEAD_SHA"]} against {args.base} {env["QUALITY_BEFORE_SHA"]}', flush=True)
    logs = [out / f'g{index}.log' for index in range(width)]
    stop = threading.Event()
    watcher = threading.Thread(target=watch, args=(logs, stop), daemon=True)
    watcher.start()
    with ThreadPoolExecutor(max_workers=max(1, args.parallel)) as pool:
        results = list(pool.map(lambda i: run_group(i, width, digest, out, env), range(width)))
    stop.set()
    bad = [(index, verdict, found) for index, verdict, found in results if verdict != 'clean']
    if not bad:
        print(f'LOCAL_MUTANTS_OK: {width} group(s) clean')
        return
    for index, verdict, found in bad:
        print(f'g{index}: {verdict}')
        for line in found:
            print(f'  {line}')
    print(f'LOCAL_MUTANTS_FAIL: {len(bad)} of {width} group(s)')
    sys.exit(1)


if __name__ == '__main__':
    main()
