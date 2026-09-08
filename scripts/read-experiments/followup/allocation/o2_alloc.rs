use super::*;
#[test]
#[ignore = "explicit instrumented allocation experiment"]
fn o2_allocation_experiment() {
    let key = crate::crypto::StreamKey([7;32]); let epoch = [8;16]; let hash = [9;16];
    let subkey = crate::crypto::derive_subkey(&key,&epoch,"",0);
    let plain = crate::crypto::FrameCipher::new(&subkey,&hash,crate::crypto::FrameCompression::Disabled);
    let compressed = crate::crypto::FrameCipher::new(&subkey,&hash,crate::crypto::FrameCompression::ZstdLevel1);
    for (count, size) in [(64usize,16384usize),(1024,1024)] {
        for format in ["plain","compressed","mixed"] {
            let originals: Vec<_> = (0..count).map(|off| {
                let cipher = if format=="compressed" || (format=="mixed" && off%2==1) { &compressed } else { &plain };
                Bytes::from(cipher.encrypt(&hash, off as u64,123,0,"",&vec![off as u8;size]))
            }).collect();
            for boundary in ["complete","withheld","bad-auth"] {
                let frames: Vec<_> = originals.iter().enumerate().map(|(index, raw)| {
                    let raw = if boundary=="bad-auth" && index+1==count { let mut bytes=raw.to_vec();*bytes.last_mut().unwrap()^=1;Bytes::from(bytes) } else { raw.clone() };
                    crate::shard::record::CheckedFrame::from_ring(&raw,index as u64,None).unwrap().unwrap()
                }).collect();
                for trial in 0..6 {
                    let max = if boundary=="withheld" { size*3/2 } else { size*count };
                    let ((result,page), allocations) = crate::allocation_meter::measure(|| {
                        let mut page=ReadPage { watermarks: Watermarks { durable: count as u64, applied: count as u64 }, recs: BATCH_DEFAULT, LEGACY_OWNER last: None,end: count as u64,completed: true };
                        let result = decode_frames_into(&frames,&mut ReadKeys::new(&key,&epoch,hash),&mut page,&mut PageBudget::new(max));
                        (result,page)
                    });
                    if boundary=="bad-auth" { assert!(result.is_err());assert!(page.recs.is_empty()); }
                    else {
                        assert_eq!(result.unwrap(),boundary=="complete");
                        assert_eq!(page.recs.len(),if boundary=="complete" { count } else { 1 });
                        for record in &page.recs { assert_eq!(record.payload.len(),size);assert!(record.payload.iter().all(|byte| *byte==record.off as u8)); }
                    }
                    if trial>0 { println!("O2_ALLOC {}",serde_json::json!({"pair_id": format!("alloc-{count}-{size}-{format}-{boundary}-{}",trial-1),"count":count,"record_bytes":size,"format":format,"boundary":boundary,"allocations":allocations})); }
                }
            }
        }
    }
}
