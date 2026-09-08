#!/usr/bin/env python3
"""Allocation-only diagnostic. The request latency acceptance screen is separate."""
import argparse,hashlib,json,pathlib,subprocess
p=argparse.ArgumentParser();p.add_argument('root',type=pathlib.Path);a=p.parse_args()
for allocator,versions in [('system',['control','candidate']),('mimalloc',['candidate','control'])]:
    for version in versions:
        folder=a.root/allocator; binary=folder/(version+'-binary')
        receipt=json.loads((folder/(version+'-receipt.json')).read_text())
        assert hashlib.sha256(binary.read_bytes()).hexdigest()==receipt['binary_sha256']
        command=['/usr/bin/time','-l',str(binary),'o2_allocation_experiment','--ignored','--nocapture','--test-threads=1']
        log=folder/(version+'-allocation.log')
        with log.open('w') as output:result=subprocess.run(command,stdout=output,stderr=subprocess.STDOUT)
        if result.returncode:raise SystemExit(f'allocation oracle failed: {log}')
        rows=[]
        for line in log.read_text().splitlines():
            if 'O2_ALLOC {' in line:rows.append(json.loads(line.split('O2_ALLOC ',1)[1])|{'allocator':allocator,'version':version,'revision':receipt['base_revision'],'binary_sha256':receipt['binary_sha256']})
        assert len(rows)==90,len(rows)
        (folder/(version+'-allocation.json')).write_text(json.dumps(rows,indent=2)+'\n')
        print(f'measured {allocator}/{version}: {len(rows)} allocation cases',flush=True)
