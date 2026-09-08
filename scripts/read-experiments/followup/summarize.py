#!/usr/bin/env python3
"""Publish block summaries, stable pairs, and correlated-stage aggregates only."""
import argparse,hashlib,json,math,pathlib,random,statistics
p=argparse.ArgumentParser();p.add_argument('campaign',type=pathlib.Path);a=p.parse_args()
blocks=[];stages=[]
def pct(xs,p):
 xs=sorted(xs);return xs[int((len(xs)-1)*p)]
run_receipts=(a.campaign/'runs.jsonl').read_text().splitlines()
for line in run_receipts:
 receipt=json.loads(line)
 if receipt['seed']:continue
 assert receipt['exit_code']==0
 log=a.campaign/receipt['log_file'];assert hashlib.sha256(log.read_bytes()).hexdigest()==receipt['log_sha256']
 server={};clients=[];rss=None
 for line in log.read_text().splitlines():
  if 'LOCAL_PERF {' in line:
   row=json.loads(line.split('LOCAL_PERF ',1)[1]);samples=row.pop('latencies_us');assert len(samples)==row['requests'];assert row['p50_us']==pct(samples,.5);assert row['p99_us']==pct(samples,.99)
   row.update({k:receipt[k] for k in ['pair_id','block_id','allocator','version','cache_on','test','format','base_revision','base_tree','binary_sha256','log_file','log_sha256']});row['pair_id']+='/'+row['kind']+'/'+row['name']
   if row['name']=='process-cold':
    assert len(samples)==1
    row['single_cold_observation_us']=samples[0];row.pop('p99_us');row.pop('p95_us');row['cold_tail_status']='one observation in this process; no cold p99 claim'
   blocks.append(row)
  elif 'SERVER_STAGES {' in line:
   row=json.loads(line.split('SERVER_STAGES ',1)[1]);assert row['request_id'] not in server;server[row['request_id']]=row
  elif 'CLIENT_STAGES {' in line:clients.append(json.loads(line.split('CLIENT_STAGES ',1)[1]))
  elif 'maximum resident set size' in line:rss=int(line.split()[0])
 for row in blocks:
  if row['log_file']==log.name:row['process_peak_rss_bytes']=rss
 if clients:
  groups={}
  for row in clients:
   request_id=row['request_id'];assert request_id in server,request_id
   assert row.get('ok',True);row['application_response_us']=server[request_id]['application_response_us']
   row['nested_physical_page_us']=sum(t for name,t in server[request_id]['nested_stages'] if name=='physical_page')
   row['nested_page_auth_decode_us']=sum(t for name,t in server[request_id]['nested_stages'] if name=='page_auth_decode')
   assert row['completion_us']>=row['body_complete_us']>=row['headers_received_us']>=row['request_flushed_us']
   row['after_body_us']=row['completion_us']-row['body_complete_us']
   row['flushed_to_headers_us']=row['headers_received_us']-row['request_flushed_us']
   row['headers_to_body_us']=row['body_complete_us']-row['headers_received_us']
   row['connect_duration_us']=row['connected_us']-row.get('admitted_us',0)
   if 'admitted_us' in row:
    row['semaphore_queue_us']=row['admitted_us']-row['wake_us']
    row['wake_fraction']=row['wake_us']/row['completion_us']
    row['semaphore_queue_fraction']=row['semaphore_queue_us']/row['completion_us']
   groups.setdefault(request_id.rsplit('-',1)[0],[]).append(row)
  for name,rows in groups.items():
   slow=sorted(rows,key=lambda r:r['completion_us'])[-max(1,math.ceil(len(rows)*.01)):]
   metrics={}
   for k in ['completion_us','wake_us','admitted_us','semaphore_queue_us','connect_duration_us','headers_received_us','body_complete_us','after_body_us','flushed_to_headers_us','headers_to_body_us','wake_fraction','semaphore_queue_fraction','application_response_us','nested_physical_page_us','nested_page_auth_decode_us']:
    if k in rows[0]:metrics[k]={'median':statistics.median([r[k] for r in rows]),'p99':pct([r[k] for r in rows],.99),'max':max(r[k] for r in rows),'slowest_1pct_requests_median':statistics.median([r[k] for r in slow])}
   stages.append({k:receipt[k] for k in ['pair_id','allocator','version','cache_on','base_revision','binary_sha256']}|{'case':name,'joined_requests':len(rows),'unmatched_clients':0,'metrics':metrics,'interpretation':'Client deltas computed within each request; server durations nested and nonadditive. Diagnostic instrumentation can perturb these runs.'})
def comparison(control,candidate,metric):
 byid={r['pair_id']:r for r in control};pairs=[(byid[r['pair_id']],r) for r in candidate if r['pair_id'] in byid]
 assert len(pairs)==len(control)==len(candidate)
 if any(x[metric]<=0 or y[metric]<=0 for x,y in pairs):return {'status':'zero baseline or result; compare counts/absolute values, no ratio CI'}
 logs=[math.log(y[metric]/x[metric]) for x,y in pairs];rng=random.Random(20260907)
 ratios=sorted(math.exp(sum(rng.choices(logs,k=len(logs)))/len(logs)) for _ in range(20000))
 return {'paired_blocks':len(pairs),'pair_ids':[x['pair_id'] for x,y in pairs],'geometric_mean_ratio':math.exp(statistics.mean(logs)),'ci95':[ratios[499],ratios[19499]],'control_median_of_blocks':statistics.median(x[metric] for x,y in pairs),'candidate_median_of_blocks':statistics.median(y[metric] for x,y in pairs)}
comparisons=[]
cases=sorted({(r['allocator'],r['format'],r['kind'],r['name']) for r in blocks})
for alloc,fmt,kind,name in cases:
 selected=[r for r in blocks if (r['allocator'],r['format'],r['kind'],r['name'])==(alloc,fmt,kind,name)]
 for lhs,rhs in [(('control',False),('candidate',False)),(('control',False),('candidate',True)),(('candidate',False),('candidate',True))]:
  control=[r for r in selected if (r['version'],r['cache_on'])==lhs];candidate=[r for r in selected if (r['version'],r['cache_on'])==rhs]
  if not control or not candidate:continue
  metrics=['p50_us','p99_us','process_peak_rss_bytes','object_get_attempts','successful_object_gets','object_get_range_bytes']
  if name=='process-cold':metrics=['p50_us','process_peak_rss_bytes','object_get_attempts','successful_object_gets','object_get_range_bytes']
  comparisons.append({'allocator':alloc,'format':fmt,'kind':kind,'name':name,'control':list(lhs),'candidate':list(rhs),'metrics':{m:comparison(control,candidate,m) for m in metrics if m in control[0] and m in candidate[0]},'cold_tail_status':'not enough independent cold observations for a p99 claim' if name=='process-cold' else None})
for name,value in [('identified-blocks',blocks),('comparisons',comparisons),('correlated-stage-summary',stages)]:
 (a.campaign/(name+'.json')).write_text(json.dumps(value,indent=2)+'\n')
print(json.dumps({'blocks':len(blocks),'comparisons':len(comparisons),'correlated_groups':len(stages)}))
