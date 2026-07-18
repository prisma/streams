//! Destructive, bounded conformance probe for managed-cell IAM boundaries.
//!
//! Three independently identified principals prove full access inside their
//! own disposable namespace and an actual provider `PermissionDenied` for
//! every cross-registry/cross-cell data-plane operation. The emitted JSON is
//! secret-free release evidence; failed runs still best-effort remove probes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use futures_util::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::Serialize;

const PROBE_BODY: &[u8] = b"prisma-streams-cell-iam-boundary-v1";

#[derive(Parser)]
#[command(
    name = "streams-cell-iam-check",
    about = "Prove registry and managed-cell S3 IAM boundaries"
)]
struct Args {
    /// Immutable build/release identifier qualified by this policy probe.
    #[arg(long, env = "CELL_IAM_RELEASE_ID")]
    release_id: String,
    /// Stable non-secret provider/account label written to the evidence.
    #[arg(long, env = "CELL_IAM_PROVIDER_ID")]
    provider_id: String,
    #[arg(long, env = "CELL_IAM_S3_ENDPOINT")]
    endpoint: String,
    #[arg(long, env = "CELL_IAM_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "CELL_IAM_S3_ALLOW_HTTP", default_value_t = false)]
    allow_http: bool,

    #[arg(long, env = "CELL_IAM_REGISTRY_BUCKET")]
    registry_bucket: String,
    #[arg(long, env = "CELL_IAM_REGISTRY_PREFIX")]
    registry_prefix: String,
    #[arg(long, env = "CELL_IAM_REGISTRY_ACCESS_KEY_ID")]
    registry_access_key_id: String,
    #[arg(long, env = "CELL_IAM_REGISTRY_SECRET_ACCESS_KEY")]
    registry_secret_access_key: String,

    #[arg(long, env = "CELL_IAM_CELL_A_ID")]
    cell_a_id: String,
    #[arg(long, env = "CELL_IAM_CELL_A_BUCKET")]
    cell_a_bucket: String,
    #[arg(long, env = "CELL_IAM_CELL_A_ACCESS_KEY_ID")]
    cell_a_access_key_id: String,
    #[arg(long, env = "CELL_IAM_CELL_A_SECRET_ACCESS_KEY")]
    cell_a_secret_access_key: String,

    #[arg(long, env = "CELL_IAM_CELL_B_ID")]
    cell_b_id: String,
    #[arg(long, env = "CELL_IAM_CELL_B_BUCKET")]
    cell_b_bucket: String,
    #[arg(long, env = "CELL_IAM_CELL_B_ACCESS_KEY_ID")]
    cell_b_access_key_id: String,
    #[arg(long, env = "CELL_IAM_CELL_B_SECRET_ACCESS_KEY")]
    cell_b_secret_access_key: String,

    /// Hermetic-emulator-only escape hatch. Real evidence requires three
    /// distinct access-key IDs so one credential cannot masquerade as roles.
    #[arg(long, default_value_t = false)]
    allow_shared_test_credentials: bool,
}

struct Credential<'a> {
    access_key_id: &'a str,
    secret_access_key: &'a str,
}

struct Namespace {
    role: &'static str,
    bucket: String,
    prefix: ObjPath,
    run_prefix: ObjPath,
    owner: Arc<dyn ObjectStore>,
    sentinel: ObjPath,
}

#[derive(Serialize)]
struct BoundaryEvidence {
    attacker: &'static str,
    target: &'static str,
    permission_denials: u32,
}

#[derive(Serialize)]
struct IamEvidence {
    format_version: u32,
    status: &'static str,
    release_id: String,
    provider_id: String,
    run_id: String,
    checked_at_ms: i64,
    principals: Vec<&'static str>,
    positive_checks: u32,
    permission_denials: u32,
    operations: Vec<&'static str>,
    boundaries: Vec<BoundaryEvidence>,
    probes_cleaned: bool,
}

fn build_store(
    args: &Args,
    credential: &Credential<'_>,
    bucket: &str,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&args.endpoint)
        .with_bucket_name(bucket)
        .with_region(&args.region)
        .with_access_key_id(credential.access_key_id)
        .with_secret_access_key(credential.secret_access_key)
        .with_allow_http(args.allow_http)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(args.allow_http)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()
        .context("build IAM-check object store")?;
    Ok(Arc::new(store))
}

fn validate_args(args: &Args) -> anyhow::Result<(ObjPath, ObjPath, ObjPath)> {
    anyhow::ensure!(
        !args.release_id.is_empty()
            && args.release_id.len() <= 64
            && args
                .release_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "release id must be 1..64 safe label characters"
    );
    anyhow::ensure!(
        !args.provider_id.is_empty()
            && args.provider_id.len() <= 64
            && args
                .provider_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "provider id must be 1..64 safe label characters"
    );
    anyhow::ensure!(
        streams_slate::cells::valid_cell_id(&args.cell_a_id)
            && streams_slate::cells::valid_cell_id(&args.cell_b_id)
            && args.cell_a_id != args.cell_b_id,
        "cell IDs must be distinct canonical IDs"
    );
    let registry = ObjPath::parse(&args.registry_prefix).context("invalid registry prefix")?;
    let cell_a = ObjPath::from(streams_slate::cells::cell_prefix(&args.cell_a_id));
    let cell_b = ObjPath::from(streams_slate::cells::cell_prefix(&args.cell_b_id));
    anyhow::ensure!(
        !args.registry_prefix.is_empty()
            && !args.registry_prefix.starts_with("cells/")
            && registry != cell_a
            && registry != cell_b,
        "registry prefix must be non-empty and outside cells/"
    );
    anyhow::ensure!(
        !args.registry_bucket.is_empty()
            && !args.cell_a_bucket.is_empty()
            && !args.cell_b_bucket.is_empty(),
        "all IAM-check buckets must be non-empty"
    );
    if !args.allow_shared_test_credentials {
        let ids = [
            args.registry_access_key_id.as_str(),
            args.cell_a_access_key_id.as_str(),
            args.cell_b_access_key_id.as_str(),
        ];
        anyhow::ensure!(
            ids[0] != ids[1] && ids[0] != ids[2] && ids[1] != ids[2],
            "real IAM evidence requires three distinct access-key IDs"
        );
    }
    Ok((registry, cell_a, cell_b))
}

fn expect_permission_denied<T>(
    result: Result<T, object_store::Error>,
    attacker: &str,
    target: &str,
    operation: &str,
) -> anyhow::Result<()> {
    match result {
        Err(object_store::Error::PermissionDenied { .. }) => Ok(()),
        // The AWS client's streaming LIST and batch-delete paths currently
        // wrap S3 status failures in Generic instead of applying the normal
        // retry classifier. Accept only an explicit HTTP 403 carrying the S3
        // AccessDenied code; a timeout, 404, 5xx, or bad credential is not
        // evidence of an IAM boundary.
        Err(error @ object_store::Error::Generic { .. })
            if error.to_string().contains("403 Forbidden")
                && error.to_string().contains("AccessDenied") =>
        {
            Ok(())
        }
        Err(error) => anyhow::bail!(
            "{attacker} -> {target} {operation} failed with a non-permission error: {error}"
        ),
        Ok(_) => anyhow::bail!("{attacker} -> {target} {operation} was not denied"),
    }
}

async fn prove_owner(namespace: &Namespace, run_id: &str) -> anyhow::Result<u32> {
    namespace
        .owner
        .put_opts(
            &namespace.sentinel,
            PutPayload::from_static(PROBE_BODY),
            PutOptions::from(PutMode::Create),
        )
        .await
        .with_context(|| format!("{} conditional create", namespace.role))?;
    anyhow::ensure!(
        namespace
            .owner
            .get(&namespace.sentinel)
            .await?
            .bytes()
            .await?
            .as_ref()
            == PROBE_BODY,
        "{} read returned wrong sentinel",
        namespace.role
    );
    anyhow::ensure!(
        namespace.owner.head(&namespace.sentinel).await?.size == PROBE_BODY.len() as u64,
        "{} HEAD returned wrong sentinel size",
        namespace.role
    );
    let listed = namespace
        .owner
        .list(Some(&namespace.prefix))
        .try_collect::<Vec<_>>()
        .await?;
    anyhow::ensure!(
        listed
            .iter()
            .any(|meta| meta.location == namespace.sentinel),
        "{} LIST omitted its sentinel",
        namespace.role
    );

    let delete_probe = ObjPath::from(format!(
        "{}/_iam_check/{run_id}/owner-delete",
        namespace.prefix
    ));
    namespace
        .owner
        .put(&delete_probe, PutPayload::from_static(b"delete"))
        .await?;
    namespace.owner.delete(&delete_probe).await?;
    anyhow::ensure!(
        matches!(
            namespace.owner.head(&delete_probe).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{} DELETE was not immediately visible",
        namespace.role
    );

    let batch_probe = ObjPath::from(format!(
        "{}/_iam_check/{run_id}/owner-batch-delete",
        namespace.prefix
    ));
    namespace
        .owner
        .put(&batch_probe, PutPayload::from_static(b"batch-delete"))
        .await?;
    namespace
        .owner
        .delete_stream(futures_util::stream::iter(vec![Ok(batch_probe.clone())]).boxed())
        .try_collect::<Vec<_>>()
        .await?;
    anyhow::ensure!(
        matches!(
            namespace.owner.head(&batch_probe).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{} batch DELETE was not immediately visible",
        namespace.role
    );

    let multipart_probe = ObjPath::from(format!(
        "{}/_iam_check/{run_id}/owner-multipart",
        namespace.prefix
    ));
    let mut upload = namespace.owner.put_multipart(&multipart_probe).await?;
    if let Err(error) = upload.put_part(PutPayload::from_static(b"multipart")).await {
        let _ = upload.abort().await;
        return Err(error.into());
    }
    upload.complete().await?;
    anyhow::ensure!(
        namespace
            .owner
            .get(&multipart_probe)
            .await?
            .bytes()
            .await?
            .as_ref()
            == b"multipart",
        "{} multipart write returned wrong content",
        namespace.role
    );
    namespace.owner.delete(&multipart_probe).await?;
    Ok(7)
}

async fn prove_boundary(
    args: &Args,
    attacker_role: &'static str,
    attacker: &Credential<'_>,
    target: &Namespace,
    run_id: &str,
) -> anyhow::Result<BoundaryEvidence> {
    let store = build_store(args, attacker, &target.bucket)?;
    expect_permission_denied(
        store.get(&target.sentinel).await.map(|_| ()),
        attacker_role,
        target.role,
        "get",
    )?;
    expect_permission_denied(
        store.head(&target.sentinel).await.map(|_| ()),
        attacker_role,
        target.role,
        "head",
    )?;
    expect_permission_denied(
        store
            .list(Some(&target.prefix))
            .try_next()
            .await
            .map(|_| ()),
        attacker_role,
        target.role,
        "list",
    )?;

    let write_probe = ObjPath::from(format!(
        "{}/_iam_check/{run_id}/cross-{attacker_role}",
        target.prefix
    ));
    let write = store
        .put_opts(
            &write_probe,
            PutPayload::from_static(b"must-not-write"),
            PutOptions::from(PutMode::Create),
        )
        .await;
    if write.is_ok() {
        let _ = target.owner.delete(&write_probe).await;
    }
    expect_permission_denied(write.map(|_| ()), attacker_role, target.role, "put")?;

    let multipart_probe = ObjPath::from(format!(
        "{}/_iam_check/{run_id}/cross-{attacker_role}-multipart",
        target.prefix
    ));
    match store.put_multipart(&multipart_probe).await {
        Err(error) => {
            expect_permission_denied(Err::<(), _>(error), attacker_role, target.role, "multipart")?
        }
        Ok(mut upload) => {
            let _ = upload.abort().await;
            let _ = target.owner.delete(&multipart_probe).await;
            anyhow::bail!(
                "{attacker_role} -> {} multipart initiation was not denied",
                target.role
            );
        }
    }

    expect_permission_denied(
        store.delete(&target.sentinel).await,
        attacker_role,
        target.role,
        "delete",
    )?;
    let mut batch =
        store.delete_stream(futures_util::stream::iter(vec![Ok(target.sentinel.clone())]).boxed());
    expect_permission_denied(
        batch.try_next().await.map(|_| ()),
        attacker_role,
        target.role,
        "batch_delete",
    )?;
    anyhow::ensure!(
        target
            .owner
            .get(&target.sentinel)
            .await?
            .bytes()
            .await?
            .as_ref()
            == PROBE_BODY,
        "{} sentinel changed after denied cross-role operations",
        target.role
    );
    Ok(BoundaryEvidence {
        attacker: attacker_role,
        target: target.role,
        permission_denials: 7,
    })
}

async fn cleanup(namespaces: &[Namespace]) -> anyhow::Result<()> {
    for namespace in namespaces {
        let objects = namespace
            .owner
            .list(Some(&namespace.run_prefix))
            .try_collect::<Vec<_>>()
            .await?;
        anyhow::ensure!(
            objects.len() <= 16,
            "{} IAM run prefix exceeds its cleanup bound",
            namespace.role
        );
        for meta in objects {
            namespace.owner.delete(&meta.location).await?;
        }
        anyhow::ensure!(
            namespace
                .owner
                .list(Some(&namespace.run_prefix))
                .try_next()
                .await?
                .is_none(),
            "{} IAM run-prefix cleanup was not immediately visible",
            namespace.role
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (registry_prefix, cell_a_prefix, cell_b_prefix) = validate_args(&args)?;
    let registry_credential = Credential {
        access_key_id: &args.registry_access_key_id,
        secret_access_key: &args.registry_secret_access_key,
    };
    let cell_a_credential = Credential {
        access_key_id: &args.cell_a_access_key_id,
        secret_access_key: &args.cell_a_secret_access_key,
    };
    let cell_b_credential = Credential {
        access_key_id: &args.cell_b_access_key_id,
        secret_access_key: &args.cell_b_secret_access_key,
    };
    let run_id = format!("{:032x}", rand::random::<u128>());
    let namespace = |role, bucket: &str, prefix: ObjPath, credential: &Credential<'_>| {
        let run_prefix = ObjPath::from(format!("{prefix}/_iam_check/{run_id}"));
        let sentinel = ObjPath::from(format!("{run_prefix}/sentinel"));
        Ok::<_, anyhow::Error>(Namespace {
            role,
            bucket: bucket.to_string(),
            owner: build_store(&args, credential, bucket)?,
            prefix,
            run_prefix,
            sentinel,
        })
    };
    let namespaces = vec![
        namespace(
            "registry",
            &args.registry_bucket,
            registry_prefix,
            &registry_credential,
        )?,
        namespace(
            "cell-a",
            &args.cell_a_bucket,
            cell_a_prefix,
            &cell_a_credential,
        )?,
        namespace(
            "cell-b",
            &args.cell_b_bucket,
            cell_b_prefix,
            &cell_b_credential,
        )?,
    ];

    let result = async {
        let mut positive_checks = 0u32;
        for namespace in &namespaces {
            positive_checks =
                positive_checks.saturating_add(prove_owner(namespace, &run_id).await?);
        }
        let credentials = [
            ("registry", &registry_credential),
            ("cell-a", &cell_a_credential),
            ("cell-b", &cell_b_credential),
        ];
        let mut boundaries = Vec::new();
        for (attacker_role, credential) in credentials {
            for target in &namespaces {
                if attacker_role != target.role {
                    boundaries.push(
                        prove_boundary(&args, attacker_role, credential, target, &run_id).await?,
                    );
                }
            }
        }
        cleanup(&namespaces).await?;
        let permission_denials = boundaries
            .iter()
            .map(|boundary| boundary.permission_denials)
            .sum();
        Ok::<_, anyhow::Error>(IamEvidence {
            format_version: 1,
            status: "pass",
            release_id: args.release_id.clone(),
            provider_id: args.provider_id.clone(),
            run_id,
            checked_at_ms: chrono::Utc::now().timestamp_millis(),
            principals: vec!["registry", "cell-a", "cell-b"],
            positive_checks,
            permission_denials,
            operations: vec![
                "get",
                "head",
                "list",
                "put",
                "multipart",
                "delete",
                "batch_delete",
            ],
            boundaries,
            probes_cleaned: true,
        })
    }
    .await;
    if result.is_err() {
        let _ = cleanup(&namespaces).await;
    }
    println!("{}", serde_json::to_string(&result?)?);
    Ok(())
}
