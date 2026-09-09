#!/usr/bin/env python3
"""Prepare an explicitly instrumented, immutable allocation-only source archive."""
import argparse, hashlib, io, json, pathlib, subprocess, tarfile
p=argparse.ArgumentParser();p.add_argument('revision');p.add_argument('destination',type=pathlib.Path);p.add_argument('--allocator',choices=['system','mimalloc'],required=True);a=p.parse_args()
base=pathlib.Path(__file__).resolve().parent
revision=subprocess.check_output(['git','rev-parse',a.revision],text=True).strip()
tree=subprocess.check_output(['git','rev-parse',revision+'^{tree}'],text=True).strip()
archive=subprocess.check_output(['git','archive',revision])
a.destination.mkdir(parents=True,exist_ok=False)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(a.destination,filter='data')
originals={}
def change(relative, fn):
 path=a.destination/relative
 old=path.read_text() if path.exists() else ''
 originals[relative]=hashlib.sha256(old.encode()).hexdigest() if old else None
 path.write_text(fn(old))
new=(a.destination/'src/application/read_batch.rs').exists()
workload=(base/'o2_alloc.rs').read_text().replace('BATCH_DEFAULT','PlainBatch::default()' if new else 'Vec::new()').replace('LEGACY_OWNER','' if new else 'contiguous: None,')
change('src/application/o2_alloc.rs',lambda _:workload)
change('src/application/read.rs',lambda s:s+'\n#[cfg(test)]\n#[path = "o2_alloc.rs"]\nmod allocation_experiment;\n')
change('src/lib.rs',lambda s:s+'\n#[cfg(test)]\nmod allocation_meter;\n')
meter=(base/'allocation_meter.rs').read_text()
if a.allocator=='mimalloc':meter=meter.replace('const BACKEND: std::alloc::System = std::alloc::System;','const BACKEND: mimalloc::MiMalloc = mimalloc::MiMalloc;')
change('src/allocation_meter.rs',lambda _:meter)
if new:
 # The normal cfg(test) wrapper observes Bytes lifetimes and has a larger owner
 # header. Use the exact existing production owner branch in this experiment.
 def production_owner(s):
  start=s.index('        #[cfg(test)]\n        let owner = super::read_retention_probe::track(')
  end=s.index('        let owner = Bytes::from_owner(exact);',start)+len('        let owner = Bytes::from_owner(exact);')
  return s[:start]+'        let owner = Bytes::from_owner(exact);'+s[end:]
 change('src/application/read_batch.rs',production_owner)
else:
 def copy_hook(s):
  needle='                    plaintext.extend_from_slice(&pt);'
  assert s.count(needle)==1
  return s.replace(needle,'                    crate::allocation_meter::aggregate_copy(pt.len());\n'+needle)
 change('src/crypto/decrypt.rs',copy_hook)
changes={relative:{'before_sha256':old,'after_sha256':hashlib.sha256((a.destination/relative).read_bytes()).hexdigest()} for relative,old in originals.items()}
manifest={'base_revision':revision,'base_tree':tree,'archive_sha256':hashlib.sha256(archive).hexdigest(),'allocator':a.allocator,'purpose':'synchronous allocation diagnostic; not a clean-source acceptance binary','uses_production_plaintext_owner':new,'changes':changes}
(a.destination/'ALLOCATION-SOURCE.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({'destination':str(a.destination),'base_revision':revision,'allocator':a.allocator}))
