// Fully durable deterministic data from the real append/absorb path. Both
// sources use the same project-qualified descriptor, payload and query oracle.
__DESCRIPTOR__
fn cache_enabled() -> bool { std::env::var("FOLLOWUP_CACHE").as_deref()==Ok("1") }
fn format_name() -> String { std::env::var("FOLLOWUP_FORMAT").unwrap_or_else(|_|"plain".into()) }
fn persistent_store() -> Arc<FaultStore> {
    let root=std::env::var("LOCAL_PERF_DATA").unwrap();
    FaultStore::uniform(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap()),107,FaultPlan::CLEAN)
}
fn config() -> crate::shard::ShardConfig {
    let mut cfg=crate::shard::ShardConfig { tail_ring_bytes:0,
        frame_compression: if format_name()=="plain" {crate::crypto::FrameCompression::Disabled} else {crate::crypto::FrameCompression::ZstdLevel1}, ..Default::default() };
    __RESOURCES__
    cfg
}
__APPEND__
#[ignore="matched follow-up engineering screen"]
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn local_perf_seed_history() {
    let store=persistent_store(); let db=Arc::new(slatedb::Db::builder("followup-history",store.clone())
        .with_settings(slatedb::config::Settings {flush_interval:Some(Duration::from_millis(5)),..Default::default()}).build().await.unwrap());
    let (tx,rx)=crate::history::absorber_channel();
    let engine=crate::shard::ShardEngine::start("followup-history".into(),db,store.clone(),config(),tx,None,Default::default());
    let keys=Arc::new(crate::history::KeyCache::default());
    let absorber=crate::history::Absorber::start(store,engine.clone(),keys.clone(),crate::history::AbsorberConfig {
        threshold_bytes:1,threshold_age:Duration::from_millis(1),tick:Duration::from_millis(20),batch_puts:256,pass_bytes:8*1024*1024,..Default::default()
    },rx);
    __PAUSE__.store(true,Relaxed);
    let mut hashes=Vec::new();
    for project in 0..32 {
        let desc=descriptor(&format!("project-{project}"));let hash=desc.storage_hash();hashes.push(hash);
        keys.put(hash,skey(),desc.epoch());
        for n in 0..256 {
            let lane=if n%16==0 {"hot".into()} else if project==31 {format!("key-{n}")} else {"other".into()};
            append_fixture(&engine,hash,&skey(),&lane,1024,&desc.epoch(),desc.segment_route_by_id(0).unwrap(),format_name()=="mixed" && n%2==1).await;
        }
    }
    engine.db.flush().await.unwrap(); __PAUSE__.store(false,Relaxed);
    wait_all_absorbed(&engine,&hashes).await;
    absorber.abort();let _=absorber.await;
    engine.db.flush().await.unwrap();engine.history_partition().await.unwrap().flush().await.unwrap();
    engine.begin_close();tokio::time::sleep(Duration::from_millis(250)).await;
    println!("SEEDED 32 projects, 256 records/project, durable absorbed=256");
}
// Mixed records alternate compressible bytes with deterministic high-entropy
// bytes. Compression falls back to version 1 when compression is unprofitable.
fn payload(incompressible:bool) -> Vec<u8> {
    if !incompressible {return vec![0x5a;1024]}
    let mut x=0x9e3779b97f4a7c15u64;
    (0..1024).map(|_|{x^=x<<13;x^=x>>7;x^=x<<17;x as u8}).collect()
}
#[ignore="matched follow-up engineering screen"]
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn local_perf_read_history() {
    let engine=open_engine_cfg(persistent_store(),"followup-history",config()).await;
    let key=skey();
    let descs:Vec<_>=(0..32).map(|n|descriptor(&format!("project-{n}"))).collect();
    let mut handles=Vec::new(); for d in &descs {handles.push(engine.stream_handle(d.storage_hash()).await.unwrap());}
    for case in ["process-cold","hot","rotating","high-cardinality","retained-reader","tenant-pressure"] {
        let count=if case=="process-cold" {1} else {samples()};
        let mut times=Vec::new(); let mut retained=Vec::new();
        crate::local_perf_meter::begin(0);
        for n in 0..count {
            let project=match case {"high-cardinality"=>31,"tenant-pressure"=>n%32,_=>0};
            let desc=&descs[project];let handle=&handles[project];
            let lane=if case=="high-cardinality" {format!("key-{}",1+n%254)} else {"hot".into()};
            let start=if case=="rotating" {n as u64%240} else {0};
            let begun=Instant::now();let mut from=start;let mut pages=Vec::new();
            for _ in 0..32 {
                let page=__READ__;
                let done=page.completed;let next=page.last.map(|l|l+1).unwrap_or(from);
                assert!(done || next>from);from=next;pages.push(page);if done{break}
            }
            times.push(begun.elapsed().as_micros() as u64);
            assert!(pages.last().unwrap().completed); assert_eq!(from,256);
            let rows:Vec<_>=pages.iter().flat_map(|p|p.recs.iter()).collect();
            let expected:Vec<_>=(start..256).filter(|i| if case=="high-cardinality" {format!("key-{i}")==lane && i%16!=0} else {i%16==0}).collect();
            assert_eq!(rows.iter().map(|r|r.off).collect::<Vec<_>>(),expected);
            for row in rows {let actual:&[u8]=row.payload.as_ref();assert_eq!(actual,payload(format_name()=="mixed" && row.off%2==1));}
            if case=="retained-reader" {retained.push(pages);if retained.len()>16 {retained.remove(0);}}
        }
        let mut metrics=crate::local_perf_meter::finish();
        metrics["cache_enabled"]=cache_enabled().into();metrics["format"]=format_name().into();
        metrics["span_reserved_bytes"]=__RESERVED__.into();
        emit("history",case,times,metrics);
        drop(retained);
    }
    engine.begin_close();
}
