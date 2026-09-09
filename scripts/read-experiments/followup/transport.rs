// Client milestones are timestamps from the same scheduled arrival, correlated
// to server spans by request_id. Primary close completion remains separate.
struct Wire {stream:tokio::net::TcpStream, buffer:Vec<u8>}
impl Wire {
    async fn connect(addr:std::net::SocketAddr)->Self { Self {stream:tokio::net::TcpStream::connect(addr).await.unwrap(),buffer:Vec::new()} }
    async fn read(&mut self,addr:std::net::SocketAddr,path:&str,id:&str,close:bool,arrival:Instant)->(u16,std::collections::HashMap<String,String>,Vec<u8>,serde_json::Value) {
        use tokio::io::{AsyncReadExt,AsyncWriteExt};
        assert!(self.buffer.is_empty());
        let request=format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: {}\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\nx-followup-request: {id}\r\n\r\n",if close{"close"}else{"keep-alive"});
        self.stream.write_all(request.as_bytes()).await.unwrap();
        let flushed=arrival.elapsed().as_micros() as u64;
        let split=loop {
            if let Some(n)=self.buffer.windows(4).position(|w|w==b"\r\n\r\n") {break n}
            assert!(self.stream.read_buf(&mut self.buffer).await.unwrap()>0);
        };
        let headers_at=arrival.elapsed().as_micros() as u64;
        let head=std::str::from_utf8(&self.buffer[..split]).unwrap();let mut lines=head.split("\r\n");
        let status=lines.next().unwrap().split_whitespace().nth(1).unwrap().parse().unwrap();
        let headers:std::collections::HashMap<_,_>=lines.filter_map(|s|s.split_once(':')).map(|(k,v)|(k.to_ascii_lowercase(),v.trim().to_owned())).collect();
        // These fixtures return one exact binary Body::from(Bytes); require its
        // HTTP length instead of interpreting an early close as body completion.
        let len:usize=headers.get("content-length").expect("known-length binary response").parse().unwrap();
        let end=split+4+len;
        while self.buffer.len()<end {assert!(self.stream.read_buf(&mut self.buffer).await.unwrap()>0)}
        let body_at=arrival.elapsed().as_micros() as u64;
        assert_eq!(self.buffer.len(),end);
        let body=self.buffer[split+4..end].to_vec();self.buffer.clear();
        if close {
            let mut trailing=Vec::new();
            if let Err(e)=self.stream.read_to_end(&mut trailing).await {assert_eq!(e.kind(),std::io::ErrorKind::ConnectionReset)}
            assert!(trailing.is_empty());
        }
        let completed=arrival.elapsed().as_micros() as u64;
        (status,headers,body,serde_json::json!({"request_id":id,"request_flushed_us":flushed,"headers_received_us":headers_at,"body_complete_us":body_at,"completion_us":completed,"connection_close_included":close}))
    }
}
#[ignore="correlated scheduled transport control; not primary serial acceptance"]
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn local_perf_transport() {
    let (state,addr)=http_rig(FaultStore::uniform(mem(),107,FaultPlan::CLEAN)).await;
    let headers=[("prisma-encryption-key",PRISMA_KEY)];
    assert_eq!(preq(addr,"PUT","/v1/streams/transport-background",&headers,br#"{"format":{"kind":"bytes"}}"#).await.0,201);
    for size in [1024usize,65536] {
        let name=format!("transport-{size}");let create=format!("/v1/streams/{name}");let path=format!("{create}/records?maxBytes=131072");
        assert_eq!(preq(addr,"PUT",&create,&headers,br#"{"format":{"kind":"bytes"}}"#).await.0,201);
        for _ in 0..65536/size {assert!(matches!(preq(addr,"POST",&format!("{create}/records"),&headers,&vec![0x5a;size]).await.0,200|204));}
        let desc=state.registry.get(&state.deployment.raw_adapter_sref(&name)).await.unwrap().unwrap();
        let expected=crate::product_cursor::KeyCursor{epoch:desc.epoch_bytes().unwrap(),key_hash:crate::crypto::stream_hash(""),seg_id:0,offset:(65536/size) as u64}.encode(&desc.project_id,&skey());
        for close in [true,false] {
            let mut wire=if close {None} else {Some(Wire::connect(addr).await)};let mut times=Vec::new();
            for n in 0..(16+samples()) {
                let arrival=Instant::now();let id=format!("serial-{size}-{close}-{n}");
                if close {wire=Some(Wire::connect(addr).await)}
                let connected=arrival.elapsed().as_micros() as u64;
                let (status,h,b,mut stages)=wire.as_mut().unwrap().read(addr,&path,&id,close,arrival).await;
                let elapsed=arrival.elapsed().as_micros() as u64;
                assert_eq!(status,200);assert_eq!(b,vec![0x5a;65536]);assert_eq!(h.get("prisma-next-cursor"),Some(&expected));assert_eq!(h.get("prisma-up-to-date").map(String::as_str),Some("true"));
                stages["connected_us"]=connected.into();
                if n>=16 {times.push(elapsed);println!("CLIENT_STAGES {stages}");}
            }
            emit("transport-serial",&format!("{size}-{}",if close{"close"}else{"persistent"}),times,serde_json::json!({"errors":0,"cache_enabled":cache_enabled()}));
        }
        for period in [2000u64,250] {
            let start=Instant::now()+Duration::from_millis(50);let sem=Arc::new(tokio::sync::Semaphore::new(16));let mut jobs=Vec::new();
            let producer=tokio::spawn(async move {
                let mut errors=0;
                for n in 0..40u64 {
                    let arrival=start+Duration::from_millis(n*10);tokio::time::sleep_until(arrival.into()).await;
                    let result=tokio::time::timeout_at((arrival+Duration::from_secs(10)).into(),preq(addr,"POST","/v1/streams/transport-background/records",&headers,&[0x5a;1024])).await;
                    if !result.is_ok_and(|r|matches!(r.0,200|204)) {errors+=1;}
                }
                errors
            });
            for n in 0..1000u64 {
                let arrival=start+Duration::from_micros(n*period);let sem=sem.clone();let path=path.clone();let expected=expected.clone();
                jobs.push(tokio::spawn(async move {
                    let id=format!("scheduled-{size}-{period}-{n}");
                    tokio::time::sleep_until(arrival.into()).await;let wake=arrival.elapsed().as_micros() as u64;
                    let result=tokio::time::timeout_at((arrival+Duration::from_secs(10)).into(),async {
                        let _permit=sem.acquire().await.unwrap();let admitted=arrival.elapsed().as_micros() as u64;
                        let mut wire=Wire::connect(addr).await;let connected=arrival.elapsed().as_micros() as u64;
                        let (status,h,b,mut stages)=wire.read(addr,&path,&id,true,arrival).await;
                        let ok=status==200 && b==[0x5a;65536] && h.get("prisma-next-cursor")==Some(&expected) && h.get("prisma-up-to-date").is_some_and(|v|v=="true");
                        stages["wake_us"]=wake.into();stages["admitted_us"]=admitted.into();stages["connected_us"]=connected.into();stages["ok"]=ok.into();
                        println!("CLIENT_STAGES {stages}");ok
                    }).await;
                    let ok=result==Ok(true);if !ok {println!("CLIENT_ERROR {}",serde_json::json!({"request_id":id,"timeout":result.is_err()}));}
                    (arrival.elapsed().as_micros() as u64,ok)
                }));
            }
            let mut times=Vec::new();let mut errors=0;
            for job in jobs {let (t,ok)=job.await.unwrap();times.push(t);if !ok{errors+=1;}}
            let producer_errors=producer.await.unwrap();assert_eq!(producer_errors,0);
            emit("transport-scheduled",&format!("{size}-{period}us-close"),times,serde_json::json!({"errors":errors,"scheduled_period_us":period,"inflight_cap":16,"producer_errors":producer_errors,"background_appends":40}));assert_eq!(errors,0);
        }
    }
    engine_shutdown(&state).await;
}
