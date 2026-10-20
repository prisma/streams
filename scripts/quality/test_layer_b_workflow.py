"""The layer-b workflow runs only when someone dispatches it, is never CI's
evidence, and publishes no raw evidence.

The lead's decision of 2026-10-20 13:00 (under the owner's delegation of
2026-10-17) gets native x86_64 Layer B evidence from a workflow_dispatch
job on GitHub's ubuntu-24.04 runners: run-shared.sh with the committed
layer-b.json at time scale 1 on a release cut-release.sh stages locally,
printing the verdict and the legs' numbers only. It is not a gate: it must
never run on a push, a pull request or a schedule, and no job of it may
carry a name the release candidate's evidence requires
(scripts/required-checks.sh) or sit among the workflows
scripts/dev/ci-status.sh reads. The owner's raw-evidence hold keeps the
manifest and the logs on the runner: no artifact, no cache and no secret.
"""
import re
import unittest

from common import ROOT

WORKFLOW = ROOT / '.github/workflows/layer-b.yml'


def top_level_block(text, key):
    """The lines of a top-level `key:` block, the key line excluded."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.split('#', 1)[0].rstrip() == f'{key}:')
    block = []
    for line in lines[start + 1:]:
        if line and not line[0].isspace() and not line.startswith('#'):
            break
        block.append(line)
    return block


def child_keys(block, indent):
    return [m.group(1) for m in (re.match(rf'^ {{{indent}}}([A-Za-z_][\w-]*):', line) for line in block) if m]


def run_commands(text):
    """Every shell line of every `run:` step, stripped, comments dropped."""
    out, in_run, run_indent = [], False, 0
    for line in text.splitlines():
        m = re.match(r'^(\s*)(?:- )?run: ?(\|)?(.*)$', line)
        if m:
            in_run, run_indent = bool(m.group(2)), len(m.group(1))
            if not m.group(2) and m.group(3).strip():
                out.append(m.group(3).strip())
            continue
        if in_run:
            if line.strip() and len(line) - len(line.lstrip()) <= run_indent:
                in_run = False
            elif line.strip() and not line.strip().startswith('#'):
                out.append(line.strip())
    return out


class LayerBWorkflow(unittest.TestCase):
    def setUp(self):
        self.text = WORKFLOW.read_text()

    def test_it_runs_only_when_dispatched(self):
        self.assertEqual(child_keys(top_level_block(self.text, 'on'), 2), ['workflow_dispatch'])

    def test_its_inputs_are_an_optional_ref_and_the_manifest_whose_default_is_layer_b(self):
        block = top_level_block(self.text, 'on')
        self.assertEqual(child_keys(block, 6), ['ref', 'manifest'])
        manifest = '\n'.join(block[next(i for i, line in enumerate(block) if line.strip() == 'manifest:'):])
        self.assertIn('default: layer-b.json', manifest)
        self.assertIn('options: [layer-b.json, layer-b-tiny.json]', manifest)

    def test_no_job_is_one_the_release_evidence_or_ci_status_reads(self):
        jobs = child_keys(top_level_block(self.text, 'jobs'), 2)
        self.assertEqual(jobs, ['layer-b'])
        required = re.search(r'REQUIRED_CHECKS=\(([^)]*)\)', (ROOT / 'scripts/required-checks.sh').read_text()).group(1).split()
        self.assertTrue(required)
        self.assertEqual([job for job in jobs if job in required], [])
        workflows = re.search(r'^workflows=\(([^)]*)\)', (ROOT / 'scripts/dev/ci-status.sh').read_text(), re.M).group(1).split()
        self.assertEqual(workflows, ['ci.yml', 'rust-quality.yml', 'workflow-lint.yml'])

    def test_it_publishes_no_raw_evidence_and_reads_no_secret(self):
        for word in ('upload-artifact', 'actions/cache', 'rust-cache', 'secrets.', 'GITHUB_STEP_SUMMARY'):
            self.assertNotIn(word, self.text)

    def test_it_stages_the_release_locally_and_runs_the_committed_manifest_at_time_scale_1(self):
        commands = run_commands(self.text)
        cut = [c for c in commands if 'scripts/cut-release.sh ' in c]
        self.assertEqual(len(cut), 1, commands)
        self.assertIn('--local "$RUNNER_TEMP/release"', cut[0])
        rig = [c for c in commands if c.startswith('bench/k2cost/run-shared.sh ')]
        self.assertEqual(rig, ['bench/k2cost/run-shared.sh "${args[@]}" > "$RUNNER_TEMP/rig.log" 2>&1'])
        self.assertIn('args=(--release "$RUNNER_TEMP/release")', commands)
        for flag in ('--time-scale', '--binary', '--bun-baseline-sha256'):
            self.assertNotIn(flag, self.text)
        self.assertEqual(commands[-1],
                         'python3 bench/k2cost/layer_b_summary.py --results "$K2_HOME/results" --rig-log "$RUNNER_TEMP/rig.log"')

    def test_the_driver_has_the_ports_and_descriptors_it_needs(self):
        commands = run_commands(self.text)
        self.assertIn('sudo sysctl -q -w net.ipv4.ip_local_port_range="1024 65535"', commands)
        self.assertIn('ulimit -n "$(ulimit -Hn)"', commands)

    def test_its_timeout_fits_the_run_inside_the_runners_six_hours(self):
        job = int(re.search(r'^    timeout-minutes: (\d+)', self.text, re.M).group(1))
        self.assertTrue(180 <= job < 360, job)


if __name__ == '__main__':
    unittest.main()
