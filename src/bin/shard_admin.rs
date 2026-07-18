//! Offline, fail-closed shard topology administration.
//!
//! The split uses SlateDB metadata-only projection clones and publishes the
//! new trie with one CAS only after both children exist. The explicit
//! quiescence acknowledgement is intentional: this tool is a recovery-safe
//! primitive for drills and controlled maintenance, not the automatic online
//! split actor (which must first land a distributed split intent + barrier).

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjPath;
use slatedb::CloneSourceSpec;
use slatedb::admin::AdminBuilder;
use streams_slate::registry::{cas_publish_topology_split, load_topology, shard_projection_bounds};

#[derive(Parser, Debug)]
#[command(name = "streams-shard-admin")]
struct Args {
    /// The live parent prefix (`root` denotes the empty root prefix).
    #[arg(long)]
    parent: String,
    /// Refuse to act unless the operator explicitly confirms serving is
    /// stopped/quiesced for this entire cell.
    #[arg(long, default_value_t = false)]
    confirm_serving_quiesced: bool,
    #[arg(long, env = "SLATE_S3_ENDPOINT")]
    s3_endpoint: String,
    #[arg(long, env = "SLATE_S3_BUCKET", default_value = "streams")]
    bucket: String,
    #[arg(long)]
    ops_bucket: Option<String>,
    #[arg(long)]
    shard_bucket: Option<String>,
    #[arg(long, env = "SLATE_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "SLATE_S3_ACCESS_KEY_ID", default_value = "test")]
    access_key_id: String,
    #[arg(long, env = "SLATE_S3_SECRET_ACCESS_KEY", default_value = "test")]
    secret_access_key: String,
    #[arg(long, env = "PATH_PREFIX")]
    path_prefix: Option<String>,
}

fn store(args: &Args, bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let raw: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(&args.s3_endpoint)
            .with_bucket_name(bucket)
            .with_region(&args.region)
            .with_access_key_id(&args.access_key_id)
            .with_secret_access_key(&args.secret_access_key)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?,
    );
    Ok(match &args.path_prefix {
        Some(prefix) => Arc::new(object_store::prefix::PrefixStore::new(raw, prefix.as_str())),
        None => raw,
    })
}

fn db_path(prefix: &str) -> String {
    if prefix.is_empty() {
        "shards/root".to_string()
    } else {
        format!("shards/{prefix}")
    }
}

async fn prefix_is_empty(store: &Arc<dyn ObjectStore>, prefix: &str) -> bool {
    store
        .list(Some(&ObjPath::from(prefix)))
        .next()
        .await
        .is_none()
}

async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.confirm_serving_quiesced,
        "--confirm-serving-quiesced is required; an online clone can omit concurrent writes"
    );
    let parent = if args.parent == "root" {
        String::new()
    } else {
        args.parent.clone()
    };
    anyhow::ensure!(
        parent.len() < 128 && parent.bytes().all(|bit| bit == b'0' || bit == b'1'),
        "parent must be `root` or a binary prefix shorter than 128 bits"
    );
    let ops = store(&args, args.ops_bucket.as_deref().unwrap_or(&args.bucket))?;
    let shards = store(&args, args.shard_bucket.as_deref().unwrap_or(&args.bucket))?;
    let topology = load_topology(&ops).await.context("load topology")?;
    anyhow::ensure!(
        topology.shards.iter().any(|candidate| candidate == &parent),
        "parent is not live in topology version {}",
        topology.version
    );

    let parent_path = db_path(&parent);
    for child in [format!("{parent}0"), format!("{parent}1")] {
        let child_path = db_path(&child);
        anyhow::ensure!(
            prefix_is_empty(&shards, &child_path).await,
            "child path {child_path} is not empty; refusing to reuse a possibly stale clone"
        );
        let mut source = CloneSourceSpec::new(parent_path.as_str());
        source.projection_range = Some(shard_projection_bounds(&child)?);
        AdminBuilder::new(child_path.as_str(), shards.clone())
            .build()
            .create_clone_builder_from_source(source)
            .build()
            .await
            .with_context(|| format!("create projected child {child}"))?;
    }

    let published = cas_publish_topology_split(&ops, &parent, topology.version)
        .await
        .context("publish topology CAS (children remain safe but orphaned on failure)")?;
    println!(
        "published topology version {}: {} -> {},{}",
        published.version,
        if parent.is_empty() { "root" } else { &parent },
        format_args!("{parent}0"),
        format_args!("{parent}1")
    );
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(Args::parse()))
}
