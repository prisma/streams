"""A `gh` stub that answers scripts/required-checks.sh's two Actions API reads.

The tests of required-checks.sh, promote-rc.sh and publish-sdk.sh install
`GH_STUB` as `gh` on PATH. It answers `gh api` for the repository's
workflow runs and a run's jobs from the JSON file `GH_STUB_RUNS` names (a
list of `run(...)`), as the API does: the runs list honours `head_sha` and
`event`; the jobs list answers only the run's latest attempt unless
`filter=all`; both cut pages of `per_page` (default 30, at most 100), and
gh prints the first page alone unless `--paginate` (`--slurp` wraps every
page in one array). Every call is logged to `STUB_LOG` when it is set; any
other call, flag or endpoint fails.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

GH_STUB = r'''#!/usr/bin/env python3
import json, os, sys
from urllib.parse import parse_qs, urlsplit
args = sys.argv[1:]
if os.environ.get('STUB_LOG'):
    with open(os.environ['STUB_LOG'], 'a') as log:
        log.write('gh ' + ' '.join(args) + '\n')
def refuse(why):
    sys.exit(f'stub gh: {why}: ' + ' '.join(args))
if args[:1] != ['api'] or len(args) < 2:
    refuse('not a gh api call')
flags = args[2:]
if any(flag not in ('--paginate', '--slurp') for flag in flags) or len(set(flags)) != len(flags):
    refuse('unexpected flags')
if '--slurp' in flags and '--paginate' not in flags:
    refuse('--slurp without --paginate')
if os.environ.get('GH_STUB_FAIL'):
    sys.exit('HTTP 502: Server Error')
url = urlsplit(args[1])
query = {key: values[-1] for key, values in parse_qs(url.query).items()}
runs = json.loads(open(os.environ['GH_STUB_RUNS']).read())
parts = url.path.split('/')
prefix = ['repos', '{owner}', '{repo}', 'actions', 'runs']
if parts == prefix:
    key = 'workflow_runs'
    items = [{k: v for k, v in run.items() if k != 'jobs'} for run in runs
             if all(run[field] == query[field] for field in ('head_sha', 'event') if field in query)]
elif parts[:5] == prefix and len(parts) == 7 and parts[6] == 'jobs':
    run = next((run for run in runs if str(run['id']) == parts[5]), None)
    if run is None:
        refuse('no such run')
    key = 'jobs'
    every = query.get('filter', 'latest') == 'all'
    items = [{**job, 'run_id': run['id']} for job in run['jobs']
             if every or job['run_attempt'] == run['run_attempt']]
else:
    refuse('unexpected endpoint')
per_page = min(int(query.get('per_page', 30)), 100)
pages = [items[i:i + per_page] for i in range(0, len(items), per_page)] or [[]]
bodies = [{'total_count': len(items), key: page} for page in pages]
if '--paginate' not in flags:
    print(json.dumps(bodies[0]))
elif '--slurp' in flags:
    print(json.dumps(bodies))
else:
    print(''.join(json.dumps(body) for body in bodies))
'''


def required_checks():
    script = (ROOT / 'scripts' / 'required-checks.sh').read_text()
    return re.search(r'REQUIRED_CHECKS=\(([^)]*)\)', script).group(1).split()


def job(name, conclusion='success', attempt=1, status='completed'):
    return {'name': name, 'run_attempt': attempt, 'status': status,
            'conclusion': conclusion if status == 'completed' else None}


def run(run_id, workflow, sha, jobs, event='push'):
    return {'id': run_id, 'path': f'.github/workflows/{workflow}', 'event': event,
            'head_sha': sha, 'run_attempt': max(j['run_attempt'] for j in jobs), 'jobs': jobs}


def ci_jobs(attempt=1, **conclusions):
    """One attempt of ci.yml's jobs: every required one but workflow-lint's
    `actionlint`, and two that no release candidate requires. Each is
    `success` unless named (`product_field_gate='failure'`), but
    `noisy-campaign`, which runs on the schedule only, is `skipped`."""
    names = [n for n in required_checks() if n != 'actionlint'] + ['architecture-report', 'noisy-campaign']
    default = {'noisy-campaign': 'skipped'}
    return [job(n, conclusions.get(n.replace('-', '_'), default.get(n, 'success')), attempt) for n in names]


def push_runs(sha, ci=None):
    """What a push runs at `sha`: ci.yml (`ci`, default every job green),
    rust-quality.yml and workflow-lint.yml, each green."""
    return [run(1001, 'ci.yml', sha, ci or ci_jobs()),
            run(1002, 'rust-quality.yml', sha, [job('quality'), job('mutants (0)')]),
            run(1003, 'workflow-lint.yml', sha, [job('actionlint')])]
