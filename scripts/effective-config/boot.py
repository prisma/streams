"""Leg C: boot the REAL rc.4 and HEAD server binaries for every family.

rc.4 has no single validation function and runs most of its refusals after
store I/O, so the only faithful rc.4 refusal oracle is booting it. Each boot
gets a fresh in-memory s3lite (HEAD build; it ignores Authorization), runs
under `env -i` with the family's placeholder env, and is classified:

  BOOTED           connected, and still alive 3 s later
  REFUSED          exited before binding (exit code + last 40 log lines)
  DIED_AFTER_BIND  connected, then exited within 3 s
  TIMEOUT          neither within 120 s: a tool failure, not a verdict

Declared substitutions (recorded per boot): --listen -> 127.0.0.1:<free>
(appended when absent); --s3-endpoint and SLATE_S3_ENDPOINT -> the s3lite URL.

While a family is BOOTED, GET /v1/debug/absorb and /v1/debug/load with the
placeholder AUTH_TOKEN (C11: derived values, symmetric on both sides). After
SIGTERM (SIGKILL 10 s later) the boot log up to the stop is read for the
validation notices (C3): every WARN/ERROR line, HEAD's typed ConfigNotice
lines, and rc.4's `memory profile certified` line.

What a fresh s3lite on darwin cannot show (C10): persisted-state refusals
(the stored MAX_REQUEST_BODY_BYTES, stored topology vs INITIAL_SHARDS), and
Compute's x86_64-musl descriptor limits (the nofile-derived notices and the
SSE clamp differ).
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import effective_config as ec

BUDGET_SECS = 120
ALIVE_SECS = 3.0
STOP_SECS = 10
BOOT_WORKERS = 4
SIDES = {'rc4': ('rc4', 'streams-slate'), 'head': ('head-bin', 'streams-slate')}
LEFTOVER = 'probe-region-server+rc4-era-ABSORB_PASS_BYTES'
ANSI = re.compile(r'\x1b\[[0-9;]*m')
LOG_LINE = re.compile(r'^\S+\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+(\S+?):\s(.*)$')
# HEAD ConfigNotice Display prefixes (src/config/notice.rs); the one INFO
# notice is MemoryProfileCertified. rc.4 logs the same certification line.
NOTICE_PREFIXES = ('memory profile certified', 'FLEET_AUTH_MODE=static', 'SSE_FEED_TOTAL_BYTES=',
                   'INITIAL_SHARDS=', 'nofile_hard=', 'SSE_MAX_CONNECTIONS=', 'the platform reported no descriptor',
                   'ABSORB_PASS_BYTES', 'ABSORB_CONCURRENCY', 'ABSORB_SMALL_BYTES')
ADVISORY_PREFIXES = ('CERTIFICATION MODE',)
# R7: the boot-time budget summary (tokio workers, descriptors + feed
# retention, the memory-budget line with caches, absorb budget and shed line).
BUDGET_PREFIXES = ('tokio runtime:', 'nofile soft=', 'memory budget:')


def classify_boot(connected_at, exited_at, alive_secs=ALIVE_SECS):
    """Seconds since spawn (or None) of the first TCP connect and of the exit."""
    if connected_at is None:
        return 'REFUSED' if exited_at is not None else 'TIMEOUT'
    if exited_at is not None and exited_at <= connected_at + alive_secs:
        return 'DIED_AFTER_BIND'
    return 'BOOTED'


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def can_connect(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.2):
            return True
    except OSError:
        return False


def substitute(argv, env, port, s3_url):
    argv, env, subs = list(argv), dict(env), []
    for flag, value in (('--listen', f'127.0.0.1:{port}'), ('--s3-endpoint', s3_url)):
        if flag in argv:
            i = argv.index(flag)
            subs.append(f'{flag} {argv[i + 1]} -> {value}')
            argv[i + 1] = value
        elif flag == '--listen':
            argv += [flag, value]
            subs.append(f'{flag} (absent) -> {value}')
    if 'SLATE_S3_ENDPOINT' in env:
        subs.append(f"SLATE_S3_ENDPOINT {env['SLATE_S3_ENDPOINT']} -> {s3_url}")
        env['SLATE_S3_ENDPOINT'] = s3_url
    return argv, env, subs


def stop(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(STOP_SECS)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()


def fetch(port, path, token):
    request = urllib.request.Request(f'http://127.0.0.1:{port}{path}')
    if token:
        request.add_header('authorization', f'Bearer {token}')
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode()
            return {'status': response.status, 'json': json.loads(body)}
    except urllib.error.HTTPError as error:
        return {'status': error.code, 'body': error.read().decode()[:400]}
    except (OSError, ValueError) as error:
        return {'status': 0, 'error': str(error)}


def start_s3lite(binary):
    port = free_port()
    proc = subprocess.Popen([str(binary), '--listen', f'127.0.0.1:{port}', '--latency-ms', '2'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.monotonic() + 30
    while not can_connect(port):
        if proc.poll() is not None or time.monotonic() > deadline:
            raise SystemExit('s3lite did not start')
        time.sleep(0.05)
    return proc, f'http://127.0.0.1:{port}'


def boot_once(binary, s3lite, fam, side, logs):
    s3, s3_url = start_s3lite(s3lite)
    port = free_port()
    argv, env, subs = substitute(fam.argv, fam.env, port, s3_url)
    log_path = logs / f'{fam.name}.{side}.log'
    started = time.monotonic()
    with open(log_path, 'wb') as log:
        proc = subprocess.Popen([*ec.env_argv(env), str(binary), *argv], stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
    connected_at = exited_at = None
    try:
        while time.monotonic() - started < BUDGET_SECS:
            if proc.poll() is not None:
                exited_at = time.monotonic() - started
                break
            if can_connect(port):
                connected_at = time.monotonic() - started
                break
            time.sleep(0.25)
        while connected_at is not None and exited_at is None and time.monotonic() - started < connected_at + ALIVE_SECS:
            if proc.poll() is not None:
                exited_at = time.monotonic() - started
            time.sleep(0.1)
        verdict = classify_boot(connected_at, exited_at)
        debug = {}
        if verdict == 'BOOTED':
            token = env.get('AUTH_TOKEN')
            debug = {path: fetch(port, path, token) for path in ('/v1/debug/absorb', '/v1/debug/load')}
        stop_offset = log_path.stat().st_size
        exit_code = proc.returncode
    finally:
        stop(proc)
        stop(s3)
    return {'family': fam.name, 'side': side, 'verdict': verdict, 'exit_code': exit_code,
            'connected_after_s': None if connected_at is None else round(connected_at, 2),
            'substitutions': subs, 'log': log_path.name, 'stop_offset': stop_offset, 'debug': debug}


def parse_log(text):
    rows = []
    for raw in text.splitlines():
        line = ANSI.sub('', raw)
        match = LOG_LINE.match(line)
        if match:
            rows.append((match.group(1), match.group(2), match.group(3)))
        elif line.strip():
            rows.append(('RAW', '', line))
    return rows


def normalize(message):
    message = re.sub(r'127\.0\.0\.1:\d+', '127.0.0.1:<port>', message)
    message = re.sub(r'\b(?=[0-9a-f]*[a-f])[0-9a-f]{16,}\b', '<hex>', message)
    message = re.sub(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '<uuid>', message)
    message = re.sub(r'boot_id=\S+', 'boot_id=<id>', message)
    message = re.sub(r'(\d+(\.\d+)?)(ms|µs|s)\b', '<dur>', message)
    return message


def notices(text, config_names=frozenset()):
    """Configuration notices (C3, reviewed rows): the typed notices at any
    level, and every WARN/ERROR line that names a configuration variable or
    is a known advisory. Other WARN/ERROR lines are runtime behaviour of a
    fresh single process (listed, not reviewed); budget lines are R7."""
    names = re.compile(r'\b(' + '|'.join(sorted(config_names, key=len, reverse=True)) + r')\b') \
        if config_names else None
    out = {'notices': [], 'runtime': [], 'budget': [], 'refusal': []}
    for level, target, message in parse_log(text):
        if level == 'RAW':
            if message.startswith('Error:') or (out['refusal'] and message.startswith('  ')):
                out['refusal'].append(message)
            continue
        row = {'level': level, 'target': target, 'message': normalize(message)}
        warning = level in ('WARN', 'ERROR')
        if message.startswith(NOTICE_PREFIXES) or (warning and (message.startswith(ADVISORY_PREFIXES)
                                                                or (names and names.search(message)))):
            out['notices'].append(row)
        elif warning:
            out['runtime'].append(row)
        elif level == 'INFO' and message.startswith(BUDGET_PREFIXES):
            out['budget'].append({'target': target, 'message': normalize(message)})
    return out


def evidence(result, logs, config_names=frozenset()):
    text = (logs / result['log']).read_bytes()[:result['stop_offset']].decode(errors='replace')
    parsed = notices(text, config_names)
    result.update(parsed)
    if result['verdict'] != 'BOOTED':
        result['log_tail'] = [ANSI.sub('', line) for line in text.splitlines()[-40:]]
    return result


def flatten_json(value, prefix=''):
    if isinstance(value, dict):
        out = {}
        for key, inner in value.items():
            out.update(flatten_json(inner, f'{prefix}.{key}' if prefix else key))
        return out
    return {prefix: json.dumps(value)}


LOAD_CONFIG_KEYS = ('compactor_profile', 'shed_line_mb', 'sse_max_connections', 'sse_configured_max_connections',
                    'sse_effective_max_connections', 'project_pressure_model', 'binary_sha256')


def derived(result):
    """The resolved values both binaries serve (C11): /v1/debug/absorb
    `config` (the memory knobs oom-acceptance verifies) and the
    configuration-shaped keys of /v1/debug/load. Counters and clocks are not
    configuration and are left out."""
    debug = result.get('debug', {})
    absorb = debug.get('/v1/debug/absorb', {}).get('json')
    load = debug.get('/v1/debug/load', {}).get('json')
    if not isinstance(absorb, dict) or not isinstance(load, dict):
        return None
    values = flatten_json(absorb.get('config', {}), 'absorb.config')
    for key in LOAD_CONFIG_KEYS:
        if key in load:
            values.update(flatten_json(load[key], f'load.{key}'))
    return values


def debug_diff(rc4, head):
    old, new = derived(rc4), derived(head)
    if old is None or new is None:
        return None
    return [{'key': k, 'rc4': old.get(k, '<absent>'), 'head': new.get(k, '<absent>')}
            for k in sorted(set(old) | set(new)) if old.get(k) != new.get(k)]


def cmd_boot(args):
    work, out = Path(args.work), Path(args.out)
    logs = out / 'boot-logs'
    logs.mkdir(parents=True, exist_ok=True)
    families = ec.load_families(Path(args.families), args.only)
    s3lite = ec.artifact(work, 'head-bin', 's3lite')
    bins = {side: ec.artifact(work, *spec) for side, spec in SIDES.items()}
    defaults = next((f for f in families if f.name == 'defaults'), None) or \
        ec.load_family(Path(args.families) / 'defaults.family')
    extra = [ec.derive(defaults, 'k10-sweep-resident-0', env={'SWEEP_MAINT_RESIDENT': '0'})]
    region = next((f for f in families if f.name == 'region-server'), None)
    if region is not None:
        # The merge-trap probe (plan §1.5): ABSORB_PASS_BYTES left 13 scripts
        # after rc.4, but a Compute project keeps every name ever set, so an
        # rc.4-era project very likely still carries it. Not a family: what a
        # project really holds needs the platform export (D2).
        extra.append(ec.derive(region, LEFTOVER, env={'ABSORB_PASS_BYTES': '67108864'}))
    jobs = [(fam, side) for fam in [*families, *extra] for side in bins]
    started = time.monotonic()
    config_names = frozenset().union(*ec.static_names())
    results = []
    with concurrent.futures.ThreadPoolExecutor(BOOT_WORKERS) as pool:
        futures = [pool.submit(boot_once, bins[side], s3lite, fam, side, logs) for fam, side in jobs]
        for future in concurrent.futures.as_completed(futures):
            result = evidence(future.result(), logs, config_names)
            results.append(result)
            print(f"{result['family']} {result['side']}: {result['verdict']}"
                  f"{'' if result['exit_code'] is None else ' exit ' + str(result['exit_code'])}", flush=True)
    results.sort(key=lambda r: (r['family'], r['side']))
    by = {(r['family'], r['side']): r for r in results}
    for fam in [*families, *extra]:
        rc4, head = by[(fam.name, 'rc4')], by[(fam.name, 'head')]
        rc4['derived'], head['derived'] = derived(rc4), derived(head)
        head['derived_diff_vs_rc4'] = debug_diff(rc4, head)
    lines = [f"{fam.name}: rc.4={by[(fam.name, 'rc4')]['verdict']} head={by[(fam.name, 'head')]['verdict']}"
             for fam in [*families, *extra]]
    controls = boot_controls(by, families, out)
    (out / 'boot.json').write_text(json.dumps({'results': results, 'controls': controls,
                                               'seconds': round(time.monotonic() - started)}, indent=2) + '\n')
    for line in lines + controls:
        print(line)
    (out / 'boot.txt').write_text('\n'.join(lines + controls) + '\n')
    return 0 if all(c.startswith('PASS') for c in controls) else 1


def boot_controls(by, families, out):
    controls = []
    rc4, head = by[('k10-sweep-resident-0', 'rc4')], by[('k10-sweep-resident-0', 'head')]
    want_rc4 = ['Error: SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; set >= 1 or unset (default 2)']
    want_head = ['Error: configuration invalid (1 problem(s)):',
                 '  - SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; set >= 1 or unset (default 2)']
    ok = (rc4['verdict'] == 'REFUSED' and rc4['exit_code'] == 1 and rc4['refusal'] == want_rc4
          and head['verdict'] == 'REFUSED' and head['exit_code'] == 1 and head['refusal'] == want_head)
    controls.append(f"{'PASS' if ok else 'FAIL'} K10 boot refusal: defaults+SWEEP_MAINT_RESIDENT=0 -> rc.4 "
                    f"{rc4['verdict']} exit {rc4['exit_code']} {' / '.join(rc4['refusal'])!r}; head {head['verdict']} "
                    f"exit {head['exit_code']} {' / '.join(head['refusal'])!r}")
    compare = out / 'families'
    disagree = []
    for fam in families:
        path = compare / f'{fam.name}.json'
        if not path.exists():
            disagree.append(f'{fam.name}: no compare output')
            continue
        accepted = json.loads(path.read_text())['new']['verdict'].split('\n', 1)[0] == 'accepted'
        booted = by[(fam.name, 'head')]['verdict'] == 'BOOTED'
        if accepted != booted:
            disagree.append(f"{fam.name}: validate {'accepted' if accepted else 'refused'}, boot "
                            f"{by[(fam.name, 'head')]['verdict']}")
    controls.append(f"{'PASS' if not disagree else 'FAIL'} K11 agreement: every family's head boot verdict is "
                    f"BOOTED iff its head validate verdict is accepted{'' if not disagree else ': ' + '; '.join(disagree)}")
    d_rc4, d_head = by.get(('defaults', 'rc4')), by.get(('defaults', 'head'))
    ok = d_rc4 and d_head and d_rc4['verdict'] == d_head['verdict'] == 'BOOTED'
    controls.append(f"{'PASS' if ok else 'FAIL'} K12 positive: defaults BOOTED on rc.4 and head within "
                    f"{BUDGET_SECS} s (rc.4 {d_rc4 and d_rc4['connected_after_s']} s, head "
                    f"{d_head and d_head['connected_after_s']} s)")
    return controls
