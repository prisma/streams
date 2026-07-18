//! Destructive, prefix-scoped conformance probe for a recovery provider.
//!
//! The failover path depends on more than "speaks S3": it requires immediate
//! read/list visibility, ETag-fenced create/update, ranged reads, multipart
//! upload, server-side copy, and delete. This binary exercises those semantics
//! under a unique temporary prefix and emits machine-readable timing evidence.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::Bytes;
use clap::Parser;
use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::Serialize;

const MULTIPART_PART_BYTES: usize = 6 * 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "streams-provider-check",
    about = "Verify the S3 semantics required by Prisma Streams recovery"
)]
struct Args {
    /// Stable non-secret label written to the evidence report.
    #[arg(long, env = "PROVIDER_CHECK_ID")]
    provider_id: String,
    #[arg(long, env = "PROVIDER_S3_ENDPOINT")]
    endpoint: String,
    #[arg(long, env = "PROVIDER_S3_BUCKET")]
    bucket: String,
    #[arg(long, env = "PROVIDER_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "PROVIDER_S3_ACCESS_KEY_ID")]
    access_key_id: String,
    #[arg(long, env = "PROVIDER_S3_SECRET_ACCESS_KEY")]
    secret_access_key: String,
    /// Existing namespace in which a unique disposable probe prefix is made.
    #[arg(
        long,
        env = "PROVIDER_CHECK_PREFIX",
        default_value = "streams-provider-check"
    )]
    prefix: String,
    /// Required only for a plain-HTTP emulator or explicitly approved endpoint.
    #[arg(long, env = "PROVIDER_S3_ALLOW_HTTP", default_value_t = false)]
    allow_http: bool,
}

#[derive(Serialize)]
struct ProviderEvidence {
    format_version: u32,
    provider_id: String,
    run_id: String,
    create_ms: u64,
    immediate_read_ms: u64,
    conditional_update_ms: u64,
    immediate_list_ms: u64,
    multipart_ms: u64,
    copy_ms: u64,
    delete_ms: u64,
    total_ms: u64,
    multipart_bytes: u64,
    checks: Vec<&'static str>,
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn build_store(args: &Args) -> anyhow::Result<Arc<dyn ObjectStore>> {
    anyhow::ensure!(
        !args.provider_id.is_empty()
            && args.provider_id.len() <= 64
            && args
                .provider_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "provider id must be 1..64 safe label characters"
    );
    ObjPath::parse(&args.prefix).context("invalid provider-check prefix")?;
    let store = AmazonS3Builder::new()
        .with_endpoint(&args.endpoint)
        .with_bucket_name(&args.bucket)
        .with_region(&args.region)
        .with_access_key_id(&args.access_key_id)
        .with_secret_access_key(&args.secret_access_key)
        .with_allow_http(args.allow_http)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(args.allow_http)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()
        .context("build provider object store")?;
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store,
        args.prefix.as_str(),
    )))
}

async fn run_probe(
    store: &Arc<dyn ObjectStore>,
    provider_id: String,
    run_id: String,
    object: &ObjPath,
    multipart: &ObjPath,
    copied: &ObjPath,
    cursor_objects: &[ObjPath; 3],
) -> anyhow::Result<ProviderEvidence> {
    let total_started = Instant::now();
    let v1 = Bytes::from_static(b"prisma-streams-provider-v1");
    let v2 = Bytes::from_static(b"prisma-streams-provider-v2");

    let started = Instant::now();
    let first = store
        .put_opts(
            object,
            PutPayload::from(v1.clone()),
            PutOptions::from(PutMode::Create),
        )
        .await
        .context("conditional create")?;
    anyhow::ensure!(
        first.e_tag.is_some() || first.version.is_some(),
        "provider returned no version identity for a conditional create"
    );
    let create_ms = elapsed_ms(started);

    let duplicate = store
        .put_opts(
            object,
            PutPayload::from_static(b"must-not-win"),
            PutOptions::from(PutMode::Create),
        )
        .await;
    anyhow::ensure!(
        matches!(
            duplicate,
            Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. })
        ),
        "duplicate conditional create did not fail closed"
    );

    let started = Instant::now();
    let observed = store.get(object).await?.bytes().await?;
    anyhow::ensure!(observed == v1, "read-after-create returned stale content");
    let range = store.get_range(object, 7..14).await?;
    anyhow::ensure!(
        range.as_ref() == b"streams",
        "ranged GET returned wrong bytes"
    );
    let immediate_read_ms = elapsed_ms(started);

    let stale_version = UpdateVersion {
        e_tag: first.e_tag.clone(),
        version: first.version.clone(),
    };
    let started = Instant::now();
    store
        .put_opts(
            object,
            PutPayload::from(v2.clone()),
            PutOptions::from(PutMode::Update(stale_version.clone())),
        )
        .await
        .context("conditional update")?;
    let stale_update = store
        .put_opts(
            object,
            PutPayload::from_static(b"stale-writer"),
            PutOptions::from(PutMode::Update(stale_version)),
        )
        .await;
    anyhow::ensure!(
        matches!(stale_update, Err(object_store::Error::Precondition { .. })),
        "stale conditional update was not fenced"
    );
    anyhow::ensure!(
        store.get(object).await?.bytes().await? == v2,
        "read-after-update returned stale content"
    );
    let conditional_update_ms = elapsed_ms(started);

    for (index, path) in cursor_objects.iter().enumerate() {
        store
            .put_opts(
                path,
                PutPayload::from(Bytes::from(format!("cursor-{index}"))),
                PutOptions::from(PutMode::Create),
            )
            .await
            .context("create ordered-list cursor probe")?;
    }
    let started = Instant::now();
    let prefix = ObjPath::from(format!("runs/{run_id}"));
    let listed = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    anyhow::ensure!(
        listed.iter().any(|meta| meta.location == *object),
        "list-after-write did not expose the created object"
    );
    anyhow::ensure!(
        listed
            .windows(2)
            .all(|pair| pair[0].location < pair[1].location),
        "provider listing is not strictly lexicographically ordered"
    );
    let after_first = store
        .list_with_offset(Some(&prefix), &cursor_objects[0])
        .try_collect::<Vec<_>>()
        .await?;
    anyhow::ensure!(
        after_first
            .iter()
            .all(|meta| meta.location > cursor_objects[0])
            && after_first
                .iter()
                .any(|meta| meta.location == cursor_objects[1])
            && after_first
                .iter()
                .any(|meta| meta.location == cursor_objects[2]),
        "provider listing does not implement an exclusive lexicographic offset"
    );
    let immediate_list_ms = elapsed_ms(started);

    let started = Instant::now();
    let mut upload = store.put_multipart(multipart).await?;
    if let Err(error) = upload
        .put_part(PutPayload::from(Bytes::from(vec![
            b'm';
            MULTIPART_PART_BYTES
        ])))
        .await
    {
        let _ = upload.abort().await;
        return Err(error).context("multipart first part");
    }
    if let Err(error) = upload
        .put_part(PutPayload::from(Bytes::from(vec![b'n'; 1024])))
        .await
    {
        let _ = upload.abort().await;
        return Err(error).context("multipart final part");
    }
    if let Err(error) = upload.complete().await {
        let _ = upload.abort().await;
        return Err(error).context("multipart completion");
    }
    let boundary = store
        .get_range(
            multipart,
            MULTIPART_PART_BYTES as u64 - 2..MULTIPART_PART_BYTES as u64 + 2,
        )
        .await?;
    anyhow::ensure!(
        boundary.as_ref() == b"mmnn",
        "multipart object was not atomically assembled"
    );
    let multipart_bytes = store.head(multipart).await?.size;
    anyhow::ensure!(
        multipart_bytes == (MULTIPART_PART_BYTES + 1024) as u64,
        "multipart object has the wrong size"
    );
    let multipart_ms = elapsed_ms(started);

    let started = Instant::now();
    store.copy(object, copied).await?;
    anyhow::ensure!(
        store.get(copied).await?.bytes().await? == v2,
        "server-side copy returned wrong content"
    );
    let copy_ms = elapsed_ms(started);

    let started = Instant::now();
    for path in [object, multipart, copied]
        .into_iter()
        .chain(cursor_objects.iter())
    {
        store.delete(path).await?;
    }
    for path in [object, multipart, copied]
        .into_iter()
        .chain(cursor_objects.iter())
    {
        anyhow::ensure!(
            matches!(
                store.head(path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "delete was not immediately visible: {path}"
        );
    }
    let delete_ms = elapsed_ms(started);

    Ok(ProviderEvidence {
        format_version: 1,
        provider_id,
        run_id,
        create_ms,
        immediate_read_ms,
        conditional_update_ms,
        immediate_list_ms,
        multipart_ms,
        copy_ms,
        delete_ms,
        total_ms: elapsed_ms(total_started),
        multipart_bytes,
        checks: vec![
            "conditional_create",
            "strong_read_after_write",
            "range_get",
            "conditional_update_fencing",
            "strong_list_after_write",
            "ordered_exclusive_offset_list",
            "multipart_upload",
            "server_side_copy",
            "strong_delete_visibility",
        ],
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let store = build_store(&args)?;
    let run_id = format!("{:032x}", rand::random::<u128>());
    let root = format!("runs/{run_id}");
    let object = ObjPath::from(format!("{root}/object"));
    let multipart = ObjPath::from(format!("{root}/multipart"));
    let copied = ObjPath::from(format!("{root}/copied"));
    let cursor_objects = [
        ObjPath::from(format!("{root}/cursor-a")),
        ObjPath::from(format!("{root}/cursor-b")),
        ObjPath::from(format!("{root}/cursor-c")),
    ];
    let result = run_probe(
        &store,
        args.provider_id,
        run_id,
        &object,
        &multipart,
        &copied,
        &cursor_objects,
    )
    .await;

    // Best-effort cleanup also runs after a failed assertion. A failed
    // multipart completion is explicitly aborted inside `run_probe`.
    for path in [&object, &multipart, &copied]
        .into_iter()
        .chain(cursor_objects.iter())
    {
        let _ = store.delete(path).await;
    }
    println!("{}", serde_json::to_string(&result?)?);
    Ok(())
}
