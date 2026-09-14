//! Technical verification harness (README §8, V-items) against a live
//! S3-compatible store. Credentials via standard AWS_* env vars; endpoint,
//! bucket, region via flags. No credentials are stored in this file.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use object_store::{CopyOptions, ObjectStore, ObjectStoreExt, PutOptions, PutPayload};
use slatedb::admin::AdminBuilder;
use slatedb::config::{Settings, WriteOptions};
use slatedb::{CloneSourceSpec, Db};

#[path = "verify/contracts.rs"]
mod contracts;

#[derive(Parser, Debug)]
struct Args {
    /// contract | cas | fence | waloff | clone | idle | latency
    test: String,
    #[arg(
        long,
        env = "SLATE_S3_ENDPOINT",
        default_value = "https://t3.storage.dev"
    )]
    s3_endpoint: String,
    #[arg(long, env = "SLATE_S3_BUCKET", default_value = "slate-sin")]
    bucket: String,
    #[arg(long, env = "AWS_REGION", default_value = "auto")]
    region: String,
    /// Unique prefix inside the bucket for this run.
    #[arg(long, default_value = "verify")]
    prefix: String,
    #[arg(long, default_value_t = 8)]
    n: usize,
    #[arg(long, default_value_t = 60)]
    seconds: u64,
    /// idle test: use the D23 tuned profile (slow manifest poll, no
    /// embedded compactor, no GC) instead of defaults.
    #[arg(long, default_value_t = false)]
    tuned: bool,
}

fn store(args: &Args) -> anyhow::Result<Arc<dyn ObjectStore>> {
    // Credentials from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env.
    let s3 = AmazonS3Builder::from_env()
        .with_endpoint(&args.s3_endpoint)
        .with_bucket_name(&args.bucket)
        .with_region(&args.region)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_allow_http(args.s3_endpoint.starts_with("http://127.0.0.1:"))
        .build()?;
    Ok(Arc::new(s3))
}

/// Counting decorator for idle-overhead measurement.
#[derive(Debug)]
struct Counting {
    inner: Arc<dyn ObjectStore>,
    ops: AtomicU64,
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures_core::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_core::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_core::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.test.as_str() {
        "latency" => latency(&args).await,
        "cas" => cas(&args).await,
        "contract" => {
            anyhow::ensure!(
                args.prefix != "verify",
                "contract requires an explicit disposable --prefix"
            );
            anyhow::ensure!(
                (2..=64).contains(&args.n),
                "contract concurrency --n must be 2..=64"
            );
            contracts::run(store(&args)?, &args.prefix, args.n).await
        }
        "fence" => fence(&args).await,
        "waloff" => waloff(&args).await,
        "clone" => clone_split(&args).await,
        "idle" => idle(&args).await,
        other => anyhow::bail!("unknown test: {other}"),
    }
}

async fn latency(args: &Args) -> anyhow::Result<()> {
    let s = store(args)?;
    let key = ObjPath::from(format!("{}/latency/obj", args.prefix));
    let payload = Bytes::from(vec![b'x'; 10 * 1024]);
    let mut put_ms = Vec::new();
    let mut get_ms = Vec::new();
    for _ in 0..20 {
        let t = Instant::now();
        s.put(&key, PutPayload::from_bytes(payload.clone())).await?;
        put_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let t = Instant::now();
        let _ = s.get(&key).await?.bytes().await?;
        get_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    put_ms.sort_by(f64::total_cmp);
    get_ms.sort_by(f64::total_cmp);
    println!(
        "10KiB PUT ms: min={:.0} p50={:.0} max={:.0}",
        put_ms[0],
        put_ms[put_ms.len() / 2],
        put_ms[put_ms.len() - 1]
    );
    println!(
        "10KiB GET ms: min={:.0} p50={:.0} max={:.0}",
        get_ms[0],
        get_ms[get_ms.len() / 2],
        get_ms[get_ms.len() - 1]
    );
    Ok(())
}

async fn cas(args: &Args) -> anyhow::Result<()> {
    let prefix = contracts::conditional_writes(store(args)?, &args.prefix, args.n).await?;
    println!("CAS_OK {prefix}");
    Ok(())
}

async fn fence(args: &Args) -> anyhow::Result<()> {
    let s = store(args)?;
    let path = format!("{}/fence-db-{}", args.prefix, std::process::id());

    let db_a = Db::builder(path.as_str(), s.clone())
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        })
        .build()
        .await?;
    db_a.put(b"k1", b"from-a").await?.await_durable().await?;
    println!("writer A: wrote k1 (durable)");

    let db_b = Db::builder(path.as_str(), s.clone())
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        })
        .build()
        .await?;
    let v = db_b.get(b"k1").await?;
    anyhow::ensure!(
        v.as_deref() == Some(b"from-a".as_slice()),
        "new owner lost acknowledged bytes"
    );
    println!("writer B: opened same path, reads k1 = {:?}", v);
    db_b.put(b"k2", b"from-b").await?.await_durable().await?;
    println!("writer B: wrote k2 (durable)");

    // A must now be fenced: its next durable write must fail.
    let t = Instant::now();
    let res = match db_a.put(b"k3", b"zombie").await {
        Ok(handle) => handle.await_durable().await,
        Err(error) => Err(error),
    };
    println!(
        "writer A (fenced) put -> {:?} after {:.0}ms",
        res.as_ref().map(|_| "OK(!!)").map_err(|e| e.to_string()),
        t.elapsed().as_secs_f64() * 1000.0
    );
    match res {
        Err(error) if error.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) => {
            println!("fencing: PASS (old writer rejected as fenced)");
        }
        Err(error) => anyhow::bail!("old writer failed without the fencing witness: {error}"),
        Ok(_) => anyhow::bail!("fencing: zombie write accepted"),
    }
    let v2 = db_b.get(b"k3").await?;
    anyhow::ensure!(v2.is_none(), "fenced writer changed durable state");
    println!("writer B: k3 = {:?} (want None)", v2);
    db_b.close().await?;
    let recovered = Db::builder(path.as_str(), s).build().await?;
    anyhow::ensure!(
        recovered.get(b"k1").await?.as_deref() == Some(b"from-a".as_slice()),
        "first owner bytes missing after cold reopen"
    );
    anyhow::ensure!(
        recovered.get(b"k2").await?.as_deref() == Some(b"from-b".as_slice()),
        "second owner bytes missing after cold reopen"
    );
    anyhow::ensure!(
        recovered.get(b"k3").await?.is_none(),
        "zombie bytes found after cold reopen"
    );
    recovered.close().await?;
    Ok(())
}

async fn waloff(args: &Args) -> anyhow::Result<()> {
    let s = store(args)?;
    let path = format!("{}/waloff-db-{}", args.prefix, std::process::id());
    let db = Db::builder(path.as_str(), s.clone())
        .with_settings(Settings {
            wal_enabled: false,
            l0_sst_size_bytes: 1024 * 1024,
            flush_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        })
        .build()
        .await?;

    // Non-durable write should be fast; durable write must wait for L0 flush.
    let t = Instant::now();
    db.put_with_options(b"fast", b"v", &Default::default(), &WriteOptions::default())
        .await?;
    println!(
        "wal-off non-durable put: {:.1}ms",
        t.elapsed().as_secs_f64() * 1000.0
    );

    // Finding: with WAL off, await_durable waits for a memtable→L0 flush,
    // which never happens for small memtables without an explicit flush().
    // The absorber pattern is therefore: non-durable writes + explicit flush.
    db.put_with_options(
        b"durable",
        b"v",
        &Default::default(),
        &WriteOptions::default(),
    )
    .await?;
    let t = Instant::now();
    let r = db.flush().await; // wal off => FlushType::MemTable
    println!(
        "wal-off explicit memtable flush: {:?} in {:.0}ms",
        r.as_ref().map(|_| "OK").map_err(|e| e.to_string()),
        t.elapsed().as_secs_f64() * 1000.0
    );
    r?;
    db.close().await?;

    // Reopen: durable data must survive.
    let db2 = Db::builder(path.as_str(), s.clone())
        .with_settings(Settings {
            wal_enabled: false,
            ..Default::default()
        })
        .build()
        .await?;
    println!(
        "after reopen: fast={:?} durable={:?}",
        db2.get(b"fast").await?,
        db2.get(b"durable").await?
    );
    anyhow::ensure!(
        db2.get(b"fast").await?.as_deref() == Some(b"v".as_slice()),
        "flushed first value missing after reopen"
    );
    anyhow::ensure!(
        db2.get(b"durable").await?.as_deref() == Some(b"v".as_slice()),
        "flushed second value missing after reopen"
    );
    db2.close().await?;
    println!("WALOFF_OK");
    Ok(())
}

async fn clone_split(args: &Args) -> anyhow::Result<()> {
    let s = store(args)?;
    let run = std::process::id();
    let parent = format!("{}/clone-parent-{run}", args.prefix);
    // Projection/union requires WAL-free sources in the pinned SlateDB API,
    // matching the service's history DBs. Explicit flushes publish their SSTs.
    let settings = Settings {
        wal_enabled: false,
        ..Default::default()
    };

    // Parent with keys in two hash halves: a* (low) and q* (high).
    let db = Db::builder(parent.as_str(), s.clone())
        .with_settings(settings.clone())
        .build()
        .await?;
    for i in 0..500u32 {
        db.put(format!("a{:04}", i).as_bytes(), vec![b'x'; 256])
            .await?;
        db.put(format!("q{:04}", i).as_bytes(), vec![b'y'; 256])
            .await?;
    }
    db.flush().await?;
    db.close().await?;

    // Split: two children with disjoint projection ranges.
    let child_low = format!("{}/clone-low-{run}", args.prefix);
    let child_high = format!("{}/clone-high-{run}", args.prefix);
    let mut src_low = CloneSourceSpec::new(parent.as_str());
    src_low.projection_range = Some((
        std::ops::Bound::Unbounded,
        std::ops::Bound::Excluded(Bytes::from_static(b"m")),
    ));
    AdminBuilder::new(child_low.as_str(), s.clone())
        .build()
        .create_clone_builder_from_source(src_low)
        .build()
        .await?;
    let mut src_high = CloneSourceSpec::new(parent.as_str());
    src_high.projection_range = Some((
        std::ops::Bound::Included(Bytes::from_static(b"m")),
        std::ops::Bound::Unbounded,
    ));
    AdminBuilder::new(child_high.as_str(), s.clone())
        .build()
        .create_clone_builder_from_source(src_high)
        .build()
        .await?;

    // Verify contents + independence.
    let dbl = Db::builder(child_low.as_str(), s.clone())
        .with_settings(settings.clone())
        .build()
        .await?;
    check_clone(&dbl, &[(b'a', b'x')], false).await?;
    dbl.put(b"a-new", b"child-write").await?;
    dbl.flush().await?;
    dbl.close().await?;

    let dbh = Db::builder(child_high.as_str(), s.clone())
        .with_settings(settings.clone())
        .build()
        .await?;
    check_clone(&dbh, &[(b'q', b'y')], false).await?;
    dbh.close().await?;

    // Union the two children back into one.
    let merged = format!("{}/clone-merged-{run}", args.prefix);
    let mut m_low = CloneSourceSpec::new(child_low.as_str());
    m_low.projection_range = Some((
        std::ops::Bound::Unbounded,
        std::ops::Bound::Excluded(Bytes::from_static(b"m")),
    ));
    let mut m_high = CloneSourceSpec::new(child_high.as_str());
    m_high.projection_range = Some((
        std::ops::Bound::Included(Bytes::from_static(b"m")),
        std::ops::Bound::Unbounded,
    ));
    AdminBuilder::new(merged.as_str(), s.clone())
        .build()
        .create_clone_builder_from_source(m_low)
        .with_source(m_high)
        .build()
        .await?;
    let dbm = Db::builder(merged.as_str(), s.clone())
        .with_settings(settings)
        .build()
        .await?;
    check_clone(&dbm, &[(b'a', b'x'), (b'q', b'y')], true).await?;
    dbm.close().await?;
    println!("CLONE_OK");
    Ok(())
}

/// Drain the full projection against client-generated bytes, including absence
/// of extra keys. A few successful point reads cannot prove clone completeness.
async fn check_clone(db: &Db, prefixes: &[(u8, u8)], child_write: bool) -> anyhow::Result<()> {
    let mut expected = std::collections::BTreeMap::new();
    for &(prefix, value) in prefixes {
        for index in 0..500 {
            expected.insert(
                format!("{}{:04}", char::from(prefix), index),
                vec![value; 256],
            );
        }
    }
    if child_write {
        expected.insert("a-new".into(), b"child-write".to_vec());
    }
    let mut scan = db.scan(..).await?;
    for (key, value) in expected {
        let row = scan
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("clone omitted {key}"))?;
        anyhow::ensure!(
            row.key == key.as_bytes() && row.value == value,
            "clone projection differs at {key}"
        );
    }
    anyhow::ensure!(
        scan.next().await?.is_none(),
        "clone contains unexpected records"
    );
    Ok(())
}

async fn idle(args: &Args) -> anyhow::Result<()> {
    let inner = store(args)?;
    let counting = Arc::new(Counting {
        inner,
        ops: AtomicU64::new(0),
    });
    let mut dbs = Vec::new();
    for i in 0..args.n {
        let path = format!("{}/idle-db-{}-{i}", args.prefix, std::process::id());
        let settings = if args.tuned {
            Settings {
                flush_interval: Some(Duration::from_millis(5)),
                manifest_poll_interval: Duration::from_secs(60),
                compactor_options: None,
                garbage_collector_options: None,
                ..Default::default()
            }
        } else {
            Settings {
                flush_interval: Some(Duration::from_millis(5)),
                ..Default::default()
            }
        };
        let db = Db::builder(path.as_str(), counting.clone() as Arc<dyn ObjectStore>)
            .with_settings(settings)
            .build()
            .await?;
        db.put(b"seed", b"v").await?;
        dbs.push(db);
    }
    let start_ops = counting.ops.load(Ordering::Relaxed);
    let t = Instant::now();
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    let ops = counting.ops.load(Ordering::Relaxed) - start_ops;
    let secs = t.elapsed().as_secs_f64();
    println!(
        "{} idle DBs, {:.0}s: {} object-store ops total = {:.2} ops/s/db",
        args.n,
        secs,
        ops,
        ops as f64 / secs / args.n as f64
    );
    for db in dbs {
        db.close().await.ok();
    }
    Ok(())
}
