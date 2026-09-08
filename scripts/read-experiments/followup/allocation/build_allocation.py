#!/usr/bin/env python3
"""Build all diagnostics before any timed/allocator runs; preserve exact receipts."""
import argparse,hashlib,json,os,pathlib,shutil,subprocess
p=argparse.ArgumentParser();p.add_argument('root',type=pathlib.Path);p.add_argument('--versions',nargs='+',default=['control','candidate']);a=p.parse_args()
for allocator in ['system','mimalloc']:
    for version in a.versions:
        folder=a.root/allocator/version
        identity=json.loads((folder/'ALLOCATION-SOURCE.json').read_text())
        for path, hashes in identity['changes'].items():
            assert hashlib.sha256((folder/path).read_bytes()).hexdigest()==hashes['after_sha256'],path
        logdir=a.root/allocator;binary=logdir/(version+'-binary')
        command=['cargo','test','--release','--locked','--lib','--no-run','--message-format=json']
        with (logdir/(version+'-build.jsonl')).open('w') as output,(logdir/(version+'-build.stderr')).open('w') as error:
            result=subprocess.run(command,cwd=folder,env=os.environ|{'STREAMS_GIT_COMMIT':identity['base_revision'],'SOURCE_DATE_EPOCH':'1788825600'},stdout=output,stderr=error)
        if result.returncode:raise SystemExit(f'build failed: {folder}')
        artifacts=[]
        for line in (logdir/(version+'-build.jsonl')).read_text().splitlines():
            try:row=json.loads(line)
            except ValueError:continue
            if row.get('reason')=='compiler-artifact' and row.get('executable') and row.get('target',{}).get('kind')==['lib']:artifacts.append(row['executable'])
        assert len(artifacts)==1,artifacts
        shutil.copy2(artifacts[0],binary)
        receipt=identity|{'command':command,'exit_code':result.returncode,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()}
        (logdir/(version+'-receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n')
        print(f'built {allocator}/{version}',flush=True)
