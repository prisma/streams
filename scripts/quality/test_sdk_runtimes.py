"""CI's sdk-package job smokes the SDK on every runtime its README names.

The README's Runtimes section says which runtimes are gated rather than
asserted. This file reads which runtimes the `sdk-package` job of
`.github/workflows/ci.yml` actually runs `sdk/smoke.mjs` under, and holds
the two lists equal, with the engines floor of `sdk/package.json` and the
current Node LTS among them (owner question X1 (c), 2026-10-12). Move
CURRENT_NODE_LTS, the job's step and the README together when the next
even Node release enters LTS.
"""
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
CURRENT_NODE_LTS = '24'
STEP = re.compile(r'^      - ', re.M)


def sdk_package_steps(workflow):
    lines = workflow.splitlines(keepends=True)
    start = lines.index('  sdk-package:\n')
    end = next((i for i in range(start + 1, len(lines))
                if re.match(r'^  [a-z][a-z0-9-]*:\s*$', lines[i])), len(lines))
    job = ''.join(lines[start:end])
    return [step for step in STEP.split(job)[1:]]


def smoked_runtimes(workflow):
    """The runtime each smoke step runs under, in order, and each server's port."""
    runtimes, ports, node = [], [], None
    for step in sdk_package_steps(workflow):
        if 'actions/setup-node' in step:
            node = re.search(r'node-version:\s*(\S+)', step).group(1)
        ports += re.findall(r'streams-slate --listen 127\.0\.0\.1:(\d+)', step)
        if 'sdk/smoke.mjs' not in step:
            continue
        if re.search(r'\bdeno run\b', step):
            runtimes.append('Deno')
        elif re.search(r'\bbun "\$GITHUB_WORKSPACE"/sdk/smoke\.mjs', step):
            runtimes.append('Bun')
        elif re.search(r'\bnode "\$GITHUB_WORKSPACE"/sdk/smoke\.mjs', step):
            runtimes.append(f'Node {node}')
        else:
            raise AssertionError(f'a smoke step under no known runtime:\n{step}')
    return runtimes, ports


def readme_runtimes(readme):
    section = readme.split('\n## Runtimes\n', 1)[1].split('\n## ', 1)[0]
    named = ' '.join(re.search(r'\*\*([^*]+)\*\*', section).group(1).split())
    return re.split(r', | and ', named)


class SdkRuntimes(unittest.TestCase):
    def setUp(self):
        self.smoked, self.ports = smoked_runtimes((ROOT / '.github/workflows/ci.yml').read_text())

    def test_the_readme_names_exactly_the_runtimes_ci_smokes(self):
        named = readme_runtimes((ROOT / 'sdk/README.md').read_text())
        self.assertEqual(sorted(named), sorted(self.smoked))

    def test_the_smoke_covers_the_engines_floor_and_the_current_node_lts(self):
        engines = json.loads((ROOT / 'sdk/package.json').read_text())['engines']['node']
        floor = re.fullmatch(r'>=(\d+)', engines).group(1)
        self.assertIn(f'Node {floor}', self.smoked)
        self.assertIn(f'Node {CURRENT_NODE_LTS}', self.smoked)

    def test_every_smoke_server_listens_on_its_own_port(self):
        self.assertEqual(len(self.ports), len(set(self.ports)), self.ports)
        self.assertEqual(len(self.ports), len(self.smoked))

    def test_a_smoke_step_is_attributed_to_the_runtime_that_runs_it(self):
        workflow = '''jobs:
  sdk-package:
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - name: smoke
        run: |
          ./target/release/streams-slate --listen 127.0.0.1:1 &
          node "$GITHUB_WORKSPACE"/sdk/smoke.mjs http://127.0.0.1:1
      - uses: oven-sh/setup-bun@v2
      - name: bun smoke
        run: |
          ./target/release/streams-slate --listen 127.0.0.1:2 &
          bun "$GITHUB_WORKSPACE"/sdk/smoke.mjs http://127.0.0.1:2
      - name: deno smoke
        run: |
          ./target/release/streams-slate --listen 127.0.0.1:3 &
          deno run -A --node-modules-dir \\
            "$GITHUB_WORKSPACE"/sdk/smoke.mjs http://127.0.0.1:3
  next-job:
    steps:
      - run: node "$GITHUB_WORKSPACE"/sdk/smoke.mjs
'''
        self.assertEqual(smoked_runtimes(workflow), (['Node 20', 'Bun', 'Deno'], ['1', '2', '3']))
        self.assertEqual(readme_runtimes('x\n## Runtimes\n\nCI runs on **Node 18, Bun and\nDeno** only.\n'),
                         ['Node 18', 'Bun', 'Deno'])


if __name__ == '__main__':
    unittest.main()
