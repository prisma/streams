//! Stable source-shard fence encoding shared by the service and move tool.

use serde::{Deserialize, Serialize};

use crate::registry::StorageHash;

/// Serving/fleet protocol required for online cross-cell moves. A zero or
/// absent value is deliberately incompatible: N-1 binaries that predate the
/// move fence deserialize new heartbeats but cannot advertise support.
pub const PROTOCOL_VERSION: u32 = 1;

pub fn key(hash: &StorageHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.extend_from_slice(hash);
    key.push(b'm');
    key
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellMoveFence {
    pub version: u32,
    pub operation_id: String,
    pub target_cell: String,
}

pub fn encode(operation_id: &str, target_cell: &str) -> Result<Vec<u8>, String> {
    if operation_id.len() != 32
        || !operation_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !crate::cells::valid_cell_id(target_cell)
    {
        return Err("invalid cell move fence identity".to_string());
    }
    serde_json::to_vec(&CellMoveFence {
        version: 1,
        operation_id: operation_id.to_string(),
        target_cell: target_cell.to_string(),
    })
    .map_err(|error| error.to_string())
}

pub fn decode(value: &[u8]) -> Result<CellMoveFence, String> {
    if value.len() > 1024 {
        return Err("cell move fence is too large".to_string());
    }
    let fence: CellMoveFence =
        serde_json::from_slice(value).map_err(|_| "corrupt cell move fence".to_string())?;
    if fence.version != 1 || encode(&fence.operation_id, &fence.target_cell).is_err() {
        return Err("invalid cell move fence".to_string());
    }
    Ok(fence)
}
