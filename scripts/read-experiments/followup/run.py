#!/usr/bin/env python3
"""Matched AB/BA blocks; immutable receipt checks; no builds during measurements."""
import argparse,hashlib,json,os,pathlib,subprocess,time
p=argparse.ArgumentParser();p.add_argument('root',type=pathlib.Path);p.add_argument('--blocks',type=int,default=5);p.add_argument('--reads',type=int,default=128);p.add_argument('--campaign',default='matched-screen');p.add_argument('--tests',nargs='+',default=['append','read_history','transport']);a=p.parse_args()
out=a.root/a.campaign;out.mkdir(exist_ok=False)
variants=[('control',False),('candidate',False),('candidate',True)]
receipts={}
for alloc in ['system','mimalloc']:
 for version in ['control','candidate']:
  f=a.root/alloc;receipt=json.loads((f/(version+'-receipt.json')).read_text());assert hashlib.sha256((f/(version+'-binary')).read_bytes()).hexdigest()==receipt['binary_sha256'];receipts[alloc,version]=receipt
configuration={'blocks':a.blocks,'reads':a.reads,'tests':a.tests,'variants':variants,'order':'AB/BA reversed on even blocks','allocators':['system','mimalloc'],'started':time.time(),'revisions':{alloc+'/'+version:{k:v for k,v in r.items() if k in ['base_revision','base_tree','binary_sha256','allocator']} for (alloc,version),r in receipts.items()}}
(out/'configuration.json').write_text(json.dumps(configuration,indent=2)+'\n')
def run(alloc,version,on,test,fmt,block,seed=False):
 label=version+('-on' if on else '-off');pair=f'{a.campaign}/{alloc}/{test}/{fmt}/block-{block}'
 data=a.root/'data'/alloc/version/fmt;data.mkdir(parents=True,exist_ok=True)
 name=f'{alloc}-{label}-{test}-{fmt}-{block}';log=out/(name+'.log');receipt=receipts[alloc,version]
 command=['/usr/bin/time','-l',str(a.root/alloc/(version+'-binary')),'local_perf_'+test,'--ignored','--nocapture','--test-threads=1']
 env=os.environ|{'LOCAL_PERF_DATA':str(data),'LOCAL_PERF_READS':str(a.reads),'FOLLOWUP_CACHE':str(int(on)),'FOLLOWUP_FORMAT':fmt}
 started=time.time()
 with log.open('w') as output:result=subprocess.run(command,env=env,stdout=output,stderr=subprocess.STDOUT)
 row={'pair_id':pair,'block_id':block,'allocator':alloc,'version':version,'cache_on':on,'test':test,'format':fmt,'seed':seed,'base_revision':receipt['base_revision'],'base_tree':receipt['base_tree'],'binary_sha256':receipt['binary_sha256'],'command':command,'started':started,'elapsed':time.time()-started,'exit_code':result.returncode,'log_file':log.name,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
 with (out/'runs.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
 if result.returncode:raise SystemExit('FAILED '+str(log))
 print('completed '+name,flush=True)
for alloc in ['system','mimalloc']:
 for version in ['control','candidate']:
  for fmt in ['plain','compressed','mixed']:
   data=a.root/'data'/alloc/version/fmt
   if not data.exists():run(alloc,version,False,'seed_history',fmt,0,True)
for block in range(1,a.blocks+1):
 for alloc in (['system','mimalloc'] if block%2 else ['mimalloc','system']):
  for version,on in (variants if block%2 else variants[::-1]):
   for test in a.tests:
    for fmt in (['plain','compressed','mixed'] if test=='read_history' else ['plain']):run(alloc,version,on,test,fmt,block)
