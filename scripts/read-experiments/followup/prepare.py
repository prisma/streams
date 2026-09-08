#!/usr/bin/env python3
"""Minimal instrumented screen archive; never label this a clean-source binary."""
import argparse,hashlib,io,json,pathlib,re,subprocess,tarfile
p=argparse.ArgumentParser();p.add_argument('revision');p.add_argument('destination',type=pathlib.Path);p.add_argument('--allocator',choices=['system','mimalloc'],required=True);a=p.parse_args()
base=pathlib.Path(__file__).resolve().parent
revision=subprocess.check_output(['git','rev-parse',a.revision],text=True).strip();tree=subprocess.check_output(['git','rev-parse',revision+'^{tree}'],text=True).strip();archive=subprocess.check_output(['git','archive',revision])
a.destination.mkdir(parents=True,exist_ok=False)
with tarfile.open(fileobj=io.BytesIO(archive)) as t:t.extractall(a.destination,filter='data')
originals={}
def change(path,fn):
 f=a.destination/path;s=f.read_text() if f.exists() else '';originals.setdefault(path,hashlib.sha256(s.encode()).hexdigest() if s else None);f.write_text(fn(s))
def replace(path,old,new):
 def f(s):
  assert s.count(old)==1,(path,old,s.count(old));return s.replace(old,new)
 change(path,f)
def function(s,name):
 start=re.search(r'(?:pub\([^)]*\) )?(?:async )?fn '+name+r'\(',s).start();opening=s.index('{',start);level=1;end=opening+1
 while level:
  if s[end]=='{':level+=1
  if s[end]=='}':level-=1
  end+=1
 return s[start:end]
modern=(a.destination/'src/application/read_batch.rs').exists()
if modern and not (a.destination/'src/history/span_cache.rs').exists():
 raise SystemExit('Historical O5 harness only: use the recorded runtime eb5ab8ad7a1459b6c679b72bf342a3af73e0bede; current production has no experimental cache or switch.')
imports='''use super::fixture_http::{engine_shutdown,http_rig_build,HttpRigOptions};
use super::fixture_runtime::RigRuntime;
use super::fixture_requests::{PRISMA_KEY,preq};
use super::fixture_storage::{mem,skey,open_engine_cfg,wait_all_absorbed};
use crate::dst::{FaultStore,FaultPlan};
use std::sync::Arc;''' if modern else 'use super::*;'
imports+='\nuse crate::registry::StreamDesc;\nuse crate::tenant::ProjectId;'
workload=(base/'legacy_workload.rs').read_text().replace('__IMPORTS__',imports)
desc=(base/'descriptor.rs').read_text()
if not modern:desc=desc.replace('crate::registry::PersistedDescriptor','crate::registry::StreamDesc')
source=(a.destination/('src/dst/tests/fixture_storage.rs' if modern else 'src/dst/dst_tests.rs')).read_text()
append=function(source,'append_sized').replace('pub(super) ','').replace('append_sized(', 'append_fixture(')
append=append.replace('    payload_bytes: usize,','    payload_bytes: usize, epoch: &[u8;16], route:[u8;16], incompressible:bool,').replace('derive_subkey(key, &hash, rk, 0)','derive_subkey(key, epoch, rk, 0)').replace('route: hash,','route,').replace('vec![0x5au8; payload_bytes]','payload(incompressible)').replace('vec![0x5a; payload_bytes]','payload(incompressible)')
assert 'payload(incompressible)' in append
history=(base/'history.rs').read_text().replace('__DESCRIPTOR__',desc).replace('__APPEND__',append).replace('.epoch()', '.epoch_bytes().unwrap()')
if not modern:history=history.replace('.segment_route_by_id(0).unwrap()', '.segment_route_by_id(0)')
resources='''cfg.shared_history=Some(Arc::new(crate::history::HistoryResources::new(&crate::config::HistoryConfig {canonical_span_cache:cache_enabled(),..Default::default()},8*1024*1024)));''' if modern else ''
read='''crate::application::read::ReadPlan::segment(&key,&desc.epoch_bytes().unwrap(),handle,&engine,crate::application::read::ReadRange::open(from),Some(&lane),8*1024*1024,crate::shard::Deliver::Durable).for_descriptor(desc).execute().await.unwrap()''' if modern else '''crate::http::read_merged(&key,&desc.epoch_bytes().unwrap(),handle,&engine,from,Some(&lane),8*1024*1024,crate::shard::Deliver::Durable).await.unwrap()'''
history=history.replace('__RESOURCES__',resources).replace('__READ__',read).replace('__PAUSE__','engine.history_resources.paused' if modern else 'crate::history::absorb_pause_flag()').replace('__RESERVED__','engine.history_resources.spans.reserved()' if modern else '0usize')
if modern:
 workload+='\nasync fn http_rig(store:Arc<dyn object_store::ObjectStore>)->(Arc<crate::http::AppState>,std::net::SocketAddr) {http_rig_build(store,RigRuntime::first(),HttpRigOptions {shard:config(),..Default::default()}).await.parts()}\n'
workload+='\n'+history+'\n'+(base/'transport.rs').read_text()
change('src/dst/local_followup.rs',lambda _:workload)
change('src/dst/dst_tests.rs',lambda s:s+'\n#[path="local_followup.rs"]\nmod local_followup;\n')
change('src/lib.rs',lambda s:s+'\n#[cfg(test)] mod local_perf_meter;\n#[cfg(test)] mod local_stages;\n')
meter=(base/'meter.rs').read_text().replace('static ALLOCATOR: CountedSystem = CountedSystem;','static ALLOCATOR: '+('System = System;' if a.allocator=='system' else 'mimalloc::MiMalloc = mimalloc::MiMalloc;'))
meter=meter.replace('"allocation_calls": CALLS.load(Relaxed),','"allocator_counters_enabled": false, "allocation_calls": CALLS.load(Relaxed),')
# Keep the actual physical scan starts distinct from SlateDB GET attempts.
meter+='\npub(crate) static SCANS:AtomicU64=AtomicU64::new(0);\n'
meter=meter.replace('    ACTIVE.store(true, Relaxed);','    SCANS.store(0,Relaxed);\n    ACTIVE.store(true, Relaxed);').replace('"allocation_calls": CALLS.load(Relaxed),','"canonical_scan_starts": SCANS.load(Relaxed), "allocation_calls": CALLS.load(Relaxed),')
change('src/local_perf_meter.rs',lambda _:meter)
change('src/local_stages.rs',lambda _:(base/'stages.rs').read_text())
replace('src/dst/fault_store.rs','        let res = self.inner.get_opts(location, options).await;','''        crate::local_perf_meter::get_attempt();
        let measurement_head=options.head;
        let res = self.inner.get_opts(location, options).await;
        if let Ok(result)=&res {crate::local_perf_meter::get(if measurement_head {0} else {result.range.end-result.range.start});}''')
path='src/history/canonical_span.rs' if modern else 'src/history.rs'
needle='let mut iter = part.scan_with_options(range, &opts).await?;'
replace(path,needle,'crate::local_perf_meter::SCANS.fetch_add(1,std::sync::atomic::Ordering::Relaxed);\n'+needle)
# The supplied body owner boundary, measured only for one matching-size append.
needle='    let result = submit_product_append(' if modern else '    let raw = crate::http::append('
replace('src/product.rs',needle,'    crate::local_perf_meter::bridge_start(wire_body.as_ptr() as usize,wire_body.len());\n'+needle)
path='src/application/append.rs' if modern else 'src/http.rs'
needle='    let body = command.body.clone();' if modern else '    let close_only = close && body.is_empty();'
hook='    crate::local_perf_meter::bridge_end(body.as_ptr() as usize,body.len());'
replace(path,needle,needle+'\n'+hook if modern else hook+'\n'+needle)
# Optional diagnostics only when the client supplies a request correlation id.
path='src/product.rs';s=(a.destination/path).read_text();body=function(s,'product_read');signature=body[:body.index('{')+1]
wrapper=signature+'''
    let id=headers.get("x-followup-request").and_then(|v|v.to_str().ok()).map(str::to_owned);
    crate::local_stages::scope(id,product_read_instrumented(state,tenant,name,headers,query,live,lease)).await
}
'''+body.replace('fn product_read(', 'fn product_read_instrumented(',1)
replace(path,body,wrapper)
for path,name,label in [('src/application/read.rs' if modern else 'src/http.rs','execute_segment' if modern else 'read_merged','physical_page'),('src/application/read_decode.rs' if modern else 'src/http.rs','decode_frames_into','page_auth_decode')]:
 def hook(s,name=name,label=label):
  pattern=r'(\b(?:async )?fn '+name+r'(?:<[^>]*>)?\([\s\S]*?\)\s*->[^\{]+\{)'
  s,n=re.subn(pattern,r'\1\n    let _stage=crate::local_stages::enter("'+label+r'");',s,count=1);assert n==1,(path,name);return s
 change(path,hook)
# Production plain owner (test-only capacity probes are independently executed).
if modern:
 def owner(s):
  return s.replace('''        #[cfg(test)]
        let charge = super::read_retention_probe::charge(exact.len());
        #[cfg(not(test))]
        let charge = ();''','''        let charge = ();''')
 change('src/application/read_batch.rs',owner)
changes={p:{'before_sha256':h,'after_sha256':hashlib.sha256((a.destination/p).read_bytes()).hexdigest()} for p,h in originals.items()}
manifest={'base_revision':revision,'base_tree':tree,'archive_sha256':hashlib.sha256(archive).hexdigest(),'allocator':a.allocator,'purpose':'instrumented engineering screen; source-adapted fixture and optional correlated diagnostics, not clean-source acceptance binary','changes':changes}
(a.destination/'SCREEN-SOURCE.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({'destination':str(a.destination),'revision':revision,'allocator':a.allocator}))
