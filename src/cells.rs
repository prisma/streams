//! Bounded global cell directory and deterministic stream placement.
//!
//! `cells.json` is operator-rate control state, never a per-request lookup.
//! Stream descriptors persist the winning cell; this directory is consulted
//! only before the descriptor's create CAS. A separate durable customer
//! affinity document bounds one tenant to at most four cells.

use std::collections::HashSet;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CELLS_PATH: &str = "cells.json";
pub const MAX_CELLS: usize = 1_024;
pub const MAX_CELLS_PER_CUSTOMER: usize = 4;
const MAX_DIRECTORY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CellState {
    Active,
    Draining,
    Frozen,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub cell_id: String,
    pub region: String,
    pub ops_prefix: String,
    pub weight: u32,
    pub state: CellState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellDirectory {
    pub version: u32,
    pub generation: u64,
    pub cells: Vec<Cell>,
}

pub fn valid_cell_id(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub fn cell_prefix(cell_id: &str) -> String {
    format!("cells/{cell_id}")
}

impl CellDirectory {
    pub fn decode(raw: &[u8]) -> Result<Self, String> {
        if raw.is_empty() || raw.len() > MAX_DIRECTORY_BYTES {
            return Err("cell directory size is out of bounds".to_string());
        }
        let directory: Self =
            serde_json::from_slice(raw).map_err(|_| "malformed cell directory".to_string())?;
        directory.validate()?;
        Ok(directory)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 || self.generation == 0 || self.cells.is_empty() {
            return Err("cell directory version, generation, or membership is invalid".to_string());
        }
        if self.cells.len() > MAX_CELLS {
            return Err("cell directory exceeds the cell bound".to_string());
        }
        let mut ids = HashSet::with_capacity(self.cells.len());
        let mut prefixes = HashSet::with_capacity(self.cells.len());
        for cell in &self.cells {
            if !valid_cell_id(&cell.cell_id)
                || cell.region.is_empty()
                || cell.region.len() > 64
                || !cell
                    .region
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                || cell.ops_prefix != cell_prefix(&cell.cell_id)
                || cell.weight > 1_000_000
                || !ids.insert(cell.cell_id.as_str())
                || !prefixes.insert(cell.ops_prefix.as_str())
            {
                return Err("cell directory contains an invalid or duplicate cell".to_string());
            }
            if cell.state != CellState::Active && cell.weight != 0 {
                return Err("non-active cells must have zero placement weight".to_string());
            }
        }
        if !self
            .cells
            .iter()
            .any(|cell| cell.state == CellState::Active && cell.weight > 0)
        {
            return Err("cell directory has no placement-eligible cell".to_string());
        }
        Ok(())
    }

    pub fn get(&self, cell_id: &str) -> Option<&Cell> {
        self.cells.iter().find(|cell| cell.cell_id == cell_id)
    }

    /// Weighted rendezvous is used only for create placement. Reads route by
    /// the authoritative cell persisted in the stream descriptor.
    pub fn select<'a>(
        &'a self,
        customer_id: &str,
        stream_name: &str,
        affinity: &[String],
    ) -> Result<&'a Cell, String> {
        if customer_id.is_empty() || customer_id.len() > 256 || stream_name.len() > 1024 {
            return Err("placement identity is out of bounds".to_string());
        }
        validate_customer_affinity(affinity)?;
        let allowed: HashSet<&str> = affinity.iter().map(String::as_str).collect();
        self.cells
            .iter()
            .filter(|cell| {
                cell.state == CellState::Active
                    && cell.weight > 0
                    && (allowed.is_empty() || allowed.contains(cell.cell_id.as_str()))
            })
            .min_by(|left, right| {
                placement_score(customer_id, stream_name, left)
                    .total_cmp(&placement_score(customer_id, stream_name, right))
                    .then_with(|| left.cell_id.cmp(&right.cell_id))
            })
            .ok_or_else(|| "customer affinity has no placement-eligible cell".to_string())
    }
}

pub fn validate_customer_affinity(cells: &[String]) -> Result<(), String> {
    if cells.len() > MAX_CELLS_PER_CUSTOMER {
        return Err("customer cell affinity exceeds four cells".to_string());
    }
    let mut unique = HashSet::with_capacity(cells.len());
    if cells
        .iter()
        .any(|cell| !valid_cell_id(cell) || !unique.insert(cell.as_str()))
    {
        return Err("customer cell affinity is invalid or duplicated".to_string());
    }
    Ok(())
}

fn placement_score(customer_id: &str, stream_name: &str, cell: &Cell) -> f64 {
    let mut digest = Sha256::new();
    digest.update(b"prisma-streams-cell-placement-v1\0");
    digest.update((customer_id.len() as u64).to_be_bytes());
    digest.update(customer_id.as_bytes());
    digest.update((stream_name.len() as u64).to_be_bytes());
    digest.update(stream_name.as_bytes());
    digest.update(cell.cell_id.as_bytes());
    let digest = digest.finalize();
    let sample = u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix"));
    // Exponential-race weighted rendezvous: lowest -ln(U)/weight wins.
    // Adding 1 keeps U in (0,1], including for an all-zero hash prefix.
    let unit = (sample as f64 + 1.0) / (u64::MAX as f64 + 1.0);
    -unit.ln() / f64::from(cell.weight)
}

pub async fn load(store: &Arc<dyn ObjectStore>) -> Result<CellDirectory, object_store::Error> {
    let result = store.get(&ObjPath::from(CELLS_PATH)).await?;
    if result.meta.size > MAX_DIRECTORY_BYTES as u64 {
        return Err(cell_error("cell directory size is out of bounds"));
    }
    let raw = result.bytes().await?;
    CellDirectory::decode(&raw).map_err(|message| cell_error(&message))
}

fn cell_error(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "cells",
        source: message.to_string().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> CellDirectory {
        CellDirectory {
            version: 1,
            generation: 7,
            cells: vec![
                Cell {
                    cell_id: "c-a".to_string(),
                    region: "ap-southeast-1".to_string(),
                    ops_prefix: "cells/c-a".to_string(),
                    weight: 1,
                    state: CellState::Active,
                },
                Cell {
                    cell_id: "c-b".to_string(),
                    region: "ap-southeast-2".to_string(),
                    ops_prefix: "cells/c-b".to_string(),
                    weight: 2,
                    state: CellState::Active,
                },
            ],
        }
    }

    #[test]
    fn directory_is_bounded_strict_and_requires_safe_prefixes() {
        let directory = directory();
        let encoded = serde_json::to_vec(&directory).unwrap();
        assert_eq!(CellDirectory::decode(&encoded).unwrap(), directory);

        let mut invalid = directory.clone();
        invalid.cells[1].ops_prefix = "cells/c-a".to_string();
        assert!(invalid.validate().is_err());
        let mut invalid = directory;
        invalid.cells[1].state = CellState::Frozen;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn placement_is_stable_and_never_escapes_customer_affinity() {
        let directory = directory();
        let first = directory.select("customer", "stream", &[]).unwrap();
        assert_eq!(directory.select("customer", "stream", &[]).unwrap(), first);
        assert_eq!(
            directory
                .select("customer", "stream", &["c-a".to_string()])
                .unwrap()
                .cell_id,
            "c-a"
        );
        assert!(
            directory
                .select("customer", "stream", &["c-missing".to_string()])
                .is_err()
        );
        assert!(
            validate_customer_affinity(&[
                "c-a".to_string(),
                "c-b".to_string(),
                "c-c".to_string(),
                "c-d".to_string(),
                "c-e".to_string(),
            ])
            .is_err()
        );
    }
}
