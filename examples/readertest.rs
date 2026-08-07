// Standalone DbReader probe against a history db written by the absorber.
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use std::sync::Arc;

fn streams_slate_key(b64: &str) -> [u8; 32] {
    use base64::Engine;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(b64.trim())
        .unwrap();
    raw.try_into().unwrap()
}

struct TestTransformer {
    cipher: Aes256Gcm,
}
impl TestTransformer {
    fn new(k: [u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new((&k).into()),
        }
    }
}
#[async_trait::async_trait]
impl slatedb::BlockTransformer for TestTransformer {
    async fn encode(&self, data: bytes::Bytes) -> Result<bytes::Bytes, slatedb::Error> {
        Ok(data)
    }
    async fn decode(&self, data: bytes::Bytes) -> Result<bytes::Bytes, slatedb::Error> {
        if data.len() < 12 {
            return Err(slatedb::Error::data("short".to_string()));
        }
        let (nonce, ct) = data.split_at(12);
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad: b"" })
            .map_err(|_| slatedb::Error::data("decrypt".to_string()))?;
        Ok(bytes::Bytes::from(pt))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: readertest <db-path>");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(
                std::env::var("RT_EP").unwrap_or_else(|_| "http://127.0.0.1:9503".into()),
            )
            .with_bucket_name("scale5")
            .with_region("local")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()?,
    );
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(store, "e2e5"));
    eprintln!("opening DbReader on {path}...");
    let key_b64 = std::env::args().nth(2);
    let t0 = std::time::Instant::now();
    let mut b = slatedb::DbReader::builder(path.as_str(), store);
    if let Some(k) = key_b64 {
        let sk = streams_slate_key(&k);
        b = b.with_block_transformer(Arc::new(TestTransformer::new(sk)));
        eprintln!("(with block transformer)");
    }
    let reader = tokio::time::timeout(std::time::Duration::from_secs(20), b.build()).await;
    match reader {
        Err(_) => eprintln!("TIMEOUT opening DbReader after {:?}", t0.elapsed()),
        Ok(Err(e)) => eprintln!("ERROR: {e}"),
        Ok(Ok(r)) => {
            eprintln!("OPENED in {:?}", t0.elapsed());
            let t1 = std::time::Instant::now();
            let scan = tokio::time::timeout(std::time::Duration::from_secs(20), async {
                let lo: Vec<u8> = b"r!".to_vec();
                let hi: Vec<u8> = b"r\xff".to_vec();
                let opts = slatedb::config::ScanOptions {
                    read_ahead_bytes: 2 * 1024 * 1024,
                    max_fetch_tasks: 2,
                    cache_blocks: true,
                    ..Default::default()
                };
                let mut it = r.scan_with_options(lo..hi, &opts).await?;
                let mut n = 0usize;
                let mut bytes = 0usize;
                while let Some(kv) = it.next().await? {
                    n += 1;
                    bytes += kv.value.len();
                    if n >= 20000 {
                        break;
                    }
                }
                Ok::<_, slatedb::Error>((n, bytes))
            })
            .await;
            match scan {
                Err(_) => eprintln!("SCAN TIMEOUT after {:?}", t1.elapsed()),
                Ok(Err(e)) => eprintln!("SCAN ERROR: {e}"),
                Ok(Ok((n, b))) => eprintln!("SCANNED {n} kvs / {b} bytes in {:?}", t1.elapsed()),
            }
        }
    }
    Ok(())
}
