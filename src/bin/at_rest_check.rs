//! Stable-corpus inspection for forbidden plaintext and key material.
//!
//! The service is stopped (or the inspected recovery point is immutable), a
//! bounded exact object inventory is taken, every body is read with If-Match,
//! and the inventory is repeated afterward. The report contains labels and
//! aggregate evidence only; forbidden bytes are read from a local file and are
//! never printed.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::Engine;
use clap::Parser;
use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, ObjectStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

const MAX_SPEC_BYTES: u64 = 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "streams-at-rest-check",
    about = "Inspect a stable object-store corpus for forbidden plaintext/key bytes"
)]
struct Args {
    /// Immutable build/release identifier whose corpus is being inspected.
    #[arg(long, env = "AT_REST_RELEASE_ID")]
    release_id: String,
    /// Stable non-secret label included in the evidence report.
    #[arg(long, env = "AT_REST_PROVIDER_ID")]
    provider_id: String,
    #[arg(long, env = "AT_REST_S3_ENDPOINT")]
    endpoint: String,
    #[arg(long, env = "AT_REST_S3_BUCKET")]
    bucket: String,
    #[arg(long, env = "AT_REST_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "AT_REST_S3_ACCESS_KEY_ID")]
    access_key_id: String,
    #[arg(long, env = "AT_REST_S3_SECRET_ACCESS_KEY")]
    secret_access_key: String,
    #[arg(long, env = "AT_REST_PATH_PREFIX")]
    prefix: Option<String>,
    /// Local JSON specification; see `ForbiddenSpec` below.
    #[arg(long, env = "AT_REST_FORBIDDEN_FILE")]
    forbidden_file: PathBuf,
    #[arg(long, env = "AT_REST_S3_ALLOW_HTTP", default_value_t = false)]
    allow_http: bool,
    #[arg(long, default_value_t = 1_000_000)]
    max_objects: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    max_object_bytes: u64,
    #[arg(long, default_value_t = 1024 * 1024 * 1024 * 1024)]
    max_total_bytes: u64,
}

/// File format:
/// `{ "forbidden": [{"label":"payload", "base64":"..."}] }`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForbiddenSpec {
    forbidden: Vec<ForbiddenInput>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForbiddenInput {
    label: String,
    base64: String,
}

struct ForbiddenPattern {
    label: String,
    bytes: Zeroizing<Vec<u8>>,
}

#[derive(Clone, Eq, PartialEq)]
struct StableObject {
    size: u64,
    etag: String,
}

#[derive(Serialize)]
struct InspectionEvidence {
    format_version: u32,
    release_id: String,
    provider_id: String,
    objects: usize,
    bytes: u64,
    inventory_sha256: String,
    forbidden_labels: Vec<String>,
    stable_inventory_verified: bool,
    status: &'static str,
}

fn safe_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 64
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn load_patterns(path: &PathBuf) -> anyhow::Result<Vec<ForbiddenPattern>> {
    let metadata = std::fs::metadata(path).context("stat forbidden specification")?;
    anyhow::ensure!(
        metadata.len() > 0 && metadata.len() <= MAX_SPEC_BYTES,
        "forbidden specification size is outside the safety bound"
    );
    let encoded = Zeroizing::new(std::fs::read(path).context("read forbidden specification")?);
    let spec: ForbiddenSpec = serde_json::from_slice(&encoded)?;
    anyhow::ensure!(
        !spec.forbidden.is_empty() && spec.forbidden.len() <= 64,
        "forbidden specification must contain 1..64 patterns"
    );
    let mut patterns = Vec::with_capacity(spec.forbidden.len());
    for input in spec.forbidden {
        anyhow::ensure!(safe_label(&input.label), "invalid forbidden-pattern label");
        let base64 = Zeroizing::new(input.base64);
        let bytes = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(base64.as_bytes())
                .context("decode forbidden pattern")?,
        );
        anyhow::ensure!(
            (8..=4096).contains(&bytes.len()),
            "forbidden patterns must contain 8..4096 bytes"
        );
        anyhow::ensure!(
            !patterns
                .iter()
                .any(|pattern: &ForbiddenPattern| pattern.label == input.label),
            "duplicate forbidden-pattern label"
        );
        patterns.push(ForbiddenPattern {
            label: input.label,
            bytes,
        });
    }
    Ok(patterns)
}

fn build_store(args: &Args) -> anyhow::Result<Arc<dyn ObjectStore>> {
    anyhow::ensure!(safe_label(&args.release_id), "invalid release id");
    anyhow::ensure!(safe_label(&args.provider_id), "invalid provider id");
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
        .build()?;
    let store: Arc<dyn ObjectStore> = match &args.prefix {
        Some(prefix) => {
            ObjPath::parse(prefix).context("invalid at-rest prefix")?;
            Arc::new(object_store::prefix::PrefixStore::new(
                store,
                prefix.as_str(),
            ))
        }
        None => Arc::new(store),
    };
    Ok(store)
}

async fn inventory(
    store: &Arc<dyn ObjectStore>,
    max_objects: usize,
    max_object_bytes: u64,
    max_total_bytes: u64,
) -> anyhow::Result<(BTreeMap<ObjPath, StableObject>, u64)> {
    let mut objects = BTreeMap::new();
    let mut bytes = 0u64;
    let mut listing = store.list(None);
    while let Some(meta) = listing.try_next().await? {
        anyhow::ensure!(
            meta.size <= max_object_bytes,
            "object exceeds the inspection size bound: {}",
            meta.location
        );
        let etag = meta
            .e_tag
            .ok_or_else(|| anyhow::anyhow!("object has no ETag: {}", meta.location))?;
        anyhow::ensure!(
            objects
                .insert(
                    meta.location.clone(),
                    StableObject {
                        size: meta.size,
                        etag,
                    },
                )
                .is_none(),
            "duplicate object in provider listing: {}",
            meta.location
        );
        anyhow::ensure!(
            objects.len() <= max_objects,
            "object count exceeds inspection bound"
        );
        bytes = bytes
            .checked_add(meta.size)
            .ok_or_else(|| anyhow::anyhow!("inspection byte count overflow"))?;
        anyhow::ensure!(
            bytes <= max_total_bytes,
            "corpus exceeds inspection byte bound"
        );
    }
    Ok((objects, bytes))
}

fn contains_pattern(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

async fn inspect_object(
    store: &Arc<dyn ObjectStore>,
    path: &ObjPath,
    expected: &StableObject,
    patterns: &[ForbiddenPattern],
) -> anyhow::Result<()> {
    let result = store
        .get_opts(
            path,
            GetOptions {
                if_match: Some(expected.etag.clone()),
                ..Default::default()
            },
        )
        .await?;
    anyhow::ensure!(
        result.meta.size == expected.size,
        "object size changed: {path}"
    );
    let max_pattern = patterns
        .iter()
        .map(|pattern| pattern.bytes.len())
        .max()
        .unwrap_or(1);
    let mut tail = Zeroizing::new(Vec::new());
    let mut observed = 0u64;
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.try_next().await? {
        observed = observed
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("object read byte count overflow"))?;
        let mut window = Zeroizing::new(Vec::with_capacity(tail.len() + chunk.len()));
        window.extend_from_slice(&tail);
        window.extend_from_slice(&chunk);
        for pattern in patterns {
            anyhow::ensure!(
                !contains_pattern(&window, &pattern.bytes),
                "forbidden pattern '{}' found in object {path}",
                pattern.label
            );
        }
        let keep = max_pattern.saturating_sub(1).min(window.len());
        tail.clear();
        tail.extend_from_slice(&window[window.len() - keep..]);
    }
    anyhow::ensure!(
        observed == expected.size,
        "object body length changed: {path}"
    );
    Ok(())
}

fn inventory_digest(objects: &BTreeMap<ObjPath, StableObject>) -> String {
    let mut digest = Sha256::new();
    for (path, object) in objects {
        digest.update((path.as_ref().len() as u64).to_be_bytes());
        digest.update(path.as_ref().as_bytes());
        digest.update(object.size.to_be_bytes());
        digest.update((object.etag.len() as u64).to_be_bytes());
        digest.update(object.etag.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.max_objects > 0, "max-objects must be positive");
    anyhow::ensure!(
        args.max_object_bytes > 0,
        "max-object-bytes must be positive"
    );
    anyhow::ensure!(args.max_total_bytes > 0, "max-total-bytes must be positive");
    let patterns = load_patterns(&args.forbidden_file)?;
    let store = build_store(&args)?;
    let (before, bytes) = inventory(
        &store,
        args.max_objects,
        args.max_object_bytes,
        args.max_total_bytes,
    )
    .await?;
    anyhow::ensure!(!before.is_empty(), "refusing to inspect an empty corpus");
    for (path, object) in &before {
        inspect_object(&store, path, object, &patterns).await?;
    }
    let (after, after_bytes) = inventory(
        &store,
        args.max_objects,
        args.max_object_bytes,
        args.max_total_bytes,
    )
    .await?;
    anyhow::ensure!(
        before == after && bytes == after_bytes,
        "object corpus changed during at-rest inspection"
    );
    let evidence = InspectionEvidence {
        format_version: 1,
        release_id: args.release_id,
        provider_id: args.provider_id,
        objects: before.len(),
        bytes,
        inventory_sha256: inventory_digest(&before),
        forbidden_labels: patterns
            .iter()
            .map(|pattern| pattern.label.clone())
            .collect(),
        stable_inventory_verified: true,
        status: "pass",
    };
    println!("{}", serde_json::to_string(&evidence)?);
    Ok(())
}
