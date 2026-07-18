//! Durable per-shard exclusion and post-durability acknowledgement fences.
//!
//! The object path predates merge support and is therefore named
//! `split-intents`, but it is the single CAS namespace for every operation
//! that replaces a live shard projection. Keeping one object per shard is
//! what makes split-vs-merge exclusion atomic.

use object_store::path::Path as ObjPath;
use serde::{Deserialize, Serialize};

const FENCE_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MergeFence {
    pub version: u32,
    pub kind: String,
    pub operation_id: String,
    pub parent: String,
    pub child: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReleasedFence {
    pub version: u32,
    pub kind: String,
    pub operation_id: String,
    pub parent: String,
    pub child: String,
    pub released_ms: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReleasedSplitFence {
    pub version: u32,
    pub status: String,
    pub operation_id: String,
    pub parent: String,
}

pub enum FenceDocument {
    Split,
    ReleasedSplit(ReleasedSplitFence),
    Merge(MergeFence),
    Released(ReleasedFence),
}

pub fn fence_path(prefix: &str) -> ObjPath {
    ObjPath::from(format!(
        "split-intents/{}.json",
        if prefix.is_empty() { "root" } else { prefix }
    ))
}

pub fn prefix_from_fence_path(path: &ObjPath) -> Result<String, String> {
    let raw = path
        .as_ref()
        .strip_prefix("split-intents/")
        .and_then(|name| name.strip_suffix(".json"))
        .ok_or_else(|| "malformed reconfiguration fence path".to_string())?;
    let prefix = if raw == "root" { "" } else { raw };
    if prefix.len() > 128 || !prefix.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err("malformed reconfiguration fence path".to_string());
    }
    Ok(prefix.to_string())
}

pub fn merge_fence(operation_id: &str, parent: &str, child: &str) -> MergeFence {
    MergeFence {
        version: FENCE_VERSION,
        kind: "merge".to_string(),
        operation_id: operation_id.to_ascii_lowercase(),
        parent: parent.to_string(),
        child: child.to_string(),
    }
}

pub fn validate_merge_fence(fence: &MergeFence) -> Result<(), String> {
    if fence.version != FENCE_VERSION
        || fence.kind != "merge"
        || fence.operation_id.len() != 32
        || !fence
            .operation_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || fence.parent.len() >= 128
        || !fence.parent.bytes().all(|bit| bit == b'0' || bit == b'1')
        || (fence.child != format!("{}0", fence.parent)
            && fence.child != format!("{}1", fence.parent))
    {
        return Err("malformed merge fence".to_string());
    }
    Ok(())
}

pub fn released_fence(fence: &MergeFence, released_ms: i64) -> ReleasedFence {
    ReleasedFence {
        version: FENCE_VERSION,
        kind: "released".to_string(),
        operation_id: fence.operation_id.clone(),
        parent: fence.parent.clone(),
        child: fence.child.clone(),
        released_ms,
    }
}

fn validate_released_fence(fence: &ReleasedFence) -> Result<(), String> {
    let active = MergeFence {
        version: fence.version,
        kind: "merge".to_string(),
        operation_id: fence.operation_id.clone(),
        parent: fence.parent.clone(),
        child: fence.child.clone(),
    };
    if fence.kind != "released" || fence.released_ms <= 0 {
        return Err("malformed released reconfiguration fence".to_string());
    }
    validate_merge_fence(&active)
}

/// A document without `kind` is a legacy/current split intent unless its
/// status is the released split tombstone. Any explicit kind must be
/// understood and fully validated; unknown/corrupt bodies fail closed.
pub fn decode_fence(raw: &[u8]) -> Result<FenceDocument, String> {
    let value: serde_json::Value =
        serde_json::from_slice(raw).map_err(|_| "malformed reconfiguration fence".to_string())?;
    match value.get("kind").and_then(serde_json::Value::as_str) {
        None => {
            if value.get("status").and_then(serde_json::Value::as_str) != Some("released") {
                return Ok(FenceDocument::Split);
            }
            let fence: ReleasedSplitFence = serde_json::from_value(value)
                .map_err(|_| "malformed released split fence".to_string())?;
            if fence.version != FENCE_VERSION
                || fence.status != "released"
                || fence.operation_id.len() != 32
                || !fence
                    .operation_id
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || fence.parent.len() >= 128
                || !fence.parent.bytes().all(|bit| bit == b'0' || bit == b'1')
            {
                return Err("malformed released split fence".to_string());
            }
            Ok(FenceDocument::ReleasedSplit(fence))
        }
        Some("merge") => {
            let fence: MergeFence =
                serde_json::from_value(value).map_err(|_| "malformed merge fence".to_string())?;
            validate_merge_fence(&fence)?;
            Ok(FenceDocument::Merge(fence))
        }
        Some("released") => {
            let fence: ReleasedFence = serde_json::from_value(value)
                .map_err(|_| "malformed released reconfiguration fence".to_string())?;
            validate_released_fence(&fence)?;
            Ok(FenceDocument::Released(fence))
        }
        Some(_) => Err("unknown reconfiguration fence kind".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_namespace_round_trips_root_and_binary_prefixes() {
        assert_eq!(prefix_from_fence_path(&fence_path("")).unwrap(), "");
        assert_eq!(prefix_from_fence_path(&fence_path("0101")).unwrap(), "0101");
        assert!(prefix_from_fence_path(&ObjPath::from("split-intents/nope.json")).is_err());
    }

    #[test]
    fn merge_fences_are_strict_and_legacy_split_documents_are_distinct() {
        let fence = merge_fence("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "01", "010");
        let raw = serde_json::to_vec(&fence).unwrap();
        assert!(matches!(
            decode_fence(&raw).unwrap(),
            FenceDocument::Merge(_)
        ));
        let released = released_fence(&fence, 1);
        assert!(matches!(
            decode_fence(&serde_json::to_vec(&released).unwrap()).unwrap(),
            FenceDocument::Released(_)
        ));
        assert!(matches!(
            decode_fence(
                br#"{"version":1,"status":"released","operation_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","parent":"01"}"#
            )
            .unwrap(),
            FenceDocument::ReleasedSplit(_)
        ));
        assert!(matches!(
            decode_fence(br#"{"version":1}"#).unwrap(),
            FenceDocument::Split
        ));
        assert!(decode_fence(br#"{"kind":"future"}"#).is_err());
    }
}
