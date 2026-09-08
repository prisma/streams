#!/usr/bin/env python3
"""Expose actual block pairing without republishing individual timed samples.

extract reads local raw logs, run receipts, binary/source identities; summarize
recomputes paired percentile ratios/CIs from the resulting reviewable block data.
Raw inputs remain separately permissioned. Their hashes bind, not independently
verify, provenance for a reader who has not received those inputs.
"""
import argparse, hashlib, json, math, pathlib, random, statistics

def quantile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def extract(root):
    identities = json.loads((root / 'source-identities.json').read_text())
    campaigns = ['system/final-system-screen', 'mimalloc/final-mimalloc-screen',
                 'system/final-system-load', 'mimalloc/final-mimalloc-load', 'ring/final-ring-screen']
    rows = []
    for campaign in campaigns:
        folder = root / campaign
        runs = [json.loads(line) for line in (folder / 'runs.jsonl').read_text().splitlines()]
        for log in sorted(folder.glob('*.log')):
            if '-seed' in log.name:
                continue
            version, test = log.stem.split('-', 1)
            test, block = test.rsplit('-', 1)
            block = int(block)
            receipts = [run for run in runs if run.get('revision') == version and run.get('block') == block and run.get('test') == test]
            # Ring's runner uses the same filename pairing but its receipt may
            # describe the all-cases test under its actual test symbol.
            if campaign.startswith('ring/') and not receipts:
                receipts = [run for run in runs if run.get('revision') == version and run.get('block') == block]
            assert len(receipts) == 1 and receipts[0]['exit_code'] == 0, (log, receipts)
            identity_name = 'probe' if version in ('on', 'off') else version
            identity = identities[campaign.split('/')[0] + '/' + identity_name]
            binary = root / campaign.split('/')[0] / 'binaries' / version
            for line in log.read_text().splitlines():
                marker = 'LOCAL_RING ' if 'LOCAL_RING {' in line else 'LOCAL_PERF '
                if marker + '{' not in line:
                    continue
                record = json.loads(line.split(marker, 1)[1])
                unit = 'ns' if marker == 'LOCAL_RING ' else 'us'
                values = record['latencies_' + unit]
                kind = record.get('kind', 'ring')
                case = kind + ':' + record['name']
                rows.append({
                    'pair_id': f'{campaign}/{test}/{case}/block-{block}',
                    'block_id': block, 'campaign': campaign, 'case': case, 'version': version,
                    'revision': identity['base_revision'], 'tree': identity['base_tree'],
                    'instrumented_source': identity['snapshot'], 'allocator': identity['allocator'],
                    'binary_sha256': sha(binary), 'raw_log': str(log.relative_to(root)),
                    'raw_log_sha256': sha(log), 'run_receipt': receipts[0],
                    'unit': unit, 'requests': len(values), 'p50': quantile(values, .5), 'p99': quantile(values, .99),
                })
    return {'schema': 1, 'contract': 'Pairs use explicit original execution block IDs, never array position.',
            'bootstrap': {'seed': 20260907, 'resamples': 20000, 'quantile': 'nearest rank'},
            'source_identity_sha256': sha(root / 'source-identities.json'), 'blocks': rows}

def summarize(packet):
    groups = {}
    for block in packet['blocks']:
        versions = groups.setdefault((block['campaign'], block['case']), {})
        pairs = versions.setdefault(block['version'], {})
        assert block['pair_id'] not in pairs, block['pair_id']
        pairs[block['pair_id']] = block
    output = []
    for (campaign, case), versions in sorted(groups.items()):
        comparisons = [(version, 'control' if campaign.startswith('ring/') else 'original')
                       for version in versions if version not in ('original', 'control')]
        if 'on' in versions:
            comparisons.append(('on', 'off'))
        for candidate, control in comparisons:
            assert versions[candidate].keys() == versions[control].keys(), (campaign, case)
            pair_ids = sorted(versions[candidate], key=lambda p: versions[candidate][p]['block_id'])
            result = {'campaign': campaign, 'case': case, 'candidate': candidate,
                      'control': control, 'pair_ids': pair_ids}
            for percentile in ('p50', 'p99'):
                ratios = [math.log(versions[candidate][pair][percentile] / versions[control][pair][percentile]) for pair in pair_ids]
                rng = random.Random(packet['bootstrap']['seed'])
                samples = sorted(math.exp(statistics.mean(rng.choices(ratios, k=len(ratios)))) for _ in range(packet['bootstrap']['resamples']))
                result[percentile] = {'ratio': math.exp(statistics.mean(ratios)), 'ci95': [quantile(samples, .025), quantile(samples, .975)]}
            output.append(result)
    return output

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['extract', 'summarize'])
    parser.add_argument('input', type=pathlib.Path)
    parser.add_argument('output', type=pathlib.Path)
    args = parser.parse_args()
    result = extract(args.input) if args.mode == 'extract' else summarize(json.loads(args.input.read_text()))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
if __name__ == '__main__':
    main()
