//! The cell's shard topology, `topology.json` (S4-DESIGN §3.1).
//!
//! - `version` changes only when routing changes: a published split or merge. A move is an
//!   override, not a topology change.
//! - `homes` names every home: its path, its prefix, its parent homes and the version it was
//!   born at. `leaves` maps each live prefix to its current home. `shards` keeps `leaves`'
//!   prefixes in order for the readers that match a hash against them
//!   (`registry::shard_for_hash`).
//! - `next_home` is the next home an operation allocates; its bump is a CAS that leaves
//!   `version` alone.
//! - `last_op` names the operation that produced `version`, so a retried publish recognises
//!   its own.
//!
//! Initial homes are `shards/<prefix>` (the root `shards/root`), each the prefix read as a
//! number (`keyspace::home_of`); every home an operation allocates is
//! `shards/<prefix>~<home>`.
//!
//! A topology that does not decode, or whose maps disagree, fails closed: boot aborts rather
//! than route by a broken map (layout 8 resets every cell, so there is no older form to read).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Unchecked")]
pub(crate) struct Topology {
    pub version: u64,
    /// The shards and their homes, stored flat beside `version`.
    #[serde(flatten)]
    pub shards: ShardSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_op: Option<LastOp>,
    /// The request-body ceiling this namespace was created with.
    ///
    /// R23-2: the absorber sizes its worst-frame reservation from the CURRENT process
    /// setting, so a deployment that lowers MAX_REQUEST_BODY_BYTES on a namespace already
    /// holding a large unabsorbed record would under-reserve for it. Recording it here lets
    /// startup refuse the mismatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_body_bytes: Option<usize>,
}

/// The shards and their homes: what the shard directory routes and opens by. It reads as
/// its ordered prefixes, the list `registry::shard_for_hash` matches a hash against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShardSet {
    /// The topology's version, carried for the directory's snapshot. It is stored once, as
    /// `Topology::version`, and filled in as the topology decodes.
    #[serde(skip)]
    pub version: u64,
    /// Complete binary prefix code over the stream-hash bit space, in order. "" = one shard
    /// covering everything.
    pub shards: Vec<String>,
    pub homes: BTreeMap<u32, Home>,
    pub leaves: BTreeMap<String, u32>,
    pub next_home: u32,
}

impl std::ops::Deref for ShardSet {
    type Target = [String];
    fn deref(&self) -> &[String] {
        &self.shards
    }
}

/// The shard directory's view of the topology (S4-DESIGN §3.2): a shard set, the ETag of the
/// stored object it came from (`None` until a revalidation read one), and when this process
/// first installed its version. It reads as its ordered prefixes.
#[derive(Debug)]
pub(crate) struct Snapshot {
    pub shards: ShardSet,
    pub etag: Option<String>,
    /// This process's monotonic instant of the first install of `shards.version` (`None` for
    /// the set it booted with, until an install). A re-read of the same version keeps it; a
    /// newer version sets it. S6's lineage sweeper waits a grace from it (S6 §4 step 1); no
    /// time from another node ever measures that grace.
    pub installed_at: Option<crate::runtime::MonotonicNow>,
}

impl std::ops::Deref for Snapshot {
    type Target = [String];
    fn deref(&self) -> &[String] {
        &self.shards
    }
}

impl ShardSet {
    /// A static shard set: each prefix its own home, in order (the prefix read as a number for
    /// a complete code of `n`-bit prefixes), at `shard_db_path`, born at version 1.
    fn of_prefixes(shards: Vec<String>) -> Self {
        let mut homes = BTreeMap::new();
        let mut leaves = BTreeMap::new();
        for (home, prefix) in (0u32..).zip(&shards) {
            homes.insert(
                home,
                Home {
                    path: crate::sharddir::shard_db_path(prefix),
                    prefix: prefix.clone(),
                    parents: Vec::new(),
                    born_version: 1,
                    retired_version: None,
                },
            );
            leaves.insert(prefix.clone(), home);
        }
        Self {
            version: 1,
            next_home: u32::try_from(shards.len()).unwrap_or(u32::MAX),
            shards,
            homes,
            leaves,
        }
    }
}

/// Tests: a static shard set of `prefixes`, labels included.
#[cfg(test)]
impl From<Vec<String>> for ShardSet {
    fn from(shards: Vec<String>) -> Self {
        Self::of_prefixes(shards)
    }
}

/// One home: where its shard log lives, the prefix it serves, the homes it was split or
/// merged from, and the versions it was born and retired at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Home {
    pub path: String,
    pub prefix: String,
    pub parents: Vec<u32>,
    pub born_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_version: Option<u64>,
}

/// The operation that produced a version: its id, its kind, the source incarnations
/// `(prefix, home)` and the child homes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LastOp {
    pub op_id: String,
    pub kind: String,
    pub sources: Vec<(String, u32)>,
    pub children: Vec<u32>,
}

/// `Topology` as stored, before its maps are checked against each other.
/// Its fields are plain, not flattened: a flattened map with numeric keys does not decode.
#[derive(Deserialize)]
struct Unchecked {
    version: u64,
    shards: Vec<String>,
    homes: BTreeMap<u32, Home>,
    leaves: BTreeMap<String, u32>,
    next_home: u32,
    #[serde(default)]
    last_op: Option<LastOp>,
    #[serde(default)]
    max_request_body_bytes: Option<usize>,
}

impl TryFrom<Unchecked> for Topology {
    type Error = String;

    /// The maps agree: `shards` lists exactly `leaves`' prefixes, every leaf's home exists and
    /// serves that prefix, and `next_home` is past every home.
    fn try_from(t: Unchecked) -> Result<Self, String> {
        let set = ShardSet {
            version: t.version,
            shards: t.shards,
            homes: t.homes,
            leaves: t.leaves,
            next_home: t.next_home,
        };
        let leaves: Vec<&String> = set.leaves.keys().collect();
        let mut listed: Vec<&String> = set.shards.iter().collect();
        listed.sort();
        if listed != leaves || listed.windows(2).any(|w| w[0] == w[1]) {
            return Err(format!(
                "shards {:?} are not exactly the leaves {leaves:?}",
                set.shards
            ));
        }
        for (prefix, home) in &set.leaves {
            match set.homes.get(home) {
                Some(h) if &h.prefix == prefix && h.retired_version.is_none() => {}
                _ => {
                    return Err(format!(
                        "leaf {prefix:?} names home {home}, which does not serve it"
                    ));
                }
            }
        }
        if set.homes.keys().any(|&h| h >= set.next_home) {
            return Err(format!(
                "next_home {} is not past every home",
                set.next_home
            ));
        }
        Ok(Self {
            version: t.version,
            shards: set,
            last_op: t.last_op,
            max_request_body_bytes: t.max_request_body_bytes,
        })
    }
}

/// Where an operation-born home's shard log lives: `shards/<prefix>~<home>` (S4-DESIGN §3.1).
#[cfg(test)]
pub(crate) fn born_home_path(prefix: &str, home: u32) -> String {
    format!("shards/{prefix}~{home}")
}

/// Tests until the split calls them (S4 step 11): the topology's two writes of a split.
#[cfg(test)]
impl Topology {
    /// Two fresh homes for a split's children: `next_home` and the one after, with `next_home`
    /// past them. The version is unchanged (a reservation routes nothing).
    pub(crate) fn reserve_homes(&self) -> (Self, [u32; 2]) {
        let mut next = self.clone();
        let first = self.shards.next_home;
        next.shards.next_home = first.saturating_add(2);
        (next, [first, first.saturating_add(1)])
    }

    /// The topology once `op_id` split leaf `prefix` into its halves at `children` (S4-DESIGN §6.2
    /// step 4.1): the next version; the halves are leaves at their homes, born at it, whose parent
    /// is the prefix's home, retired at it; and `last_op` names the operation. Refused for a
    /// prefix that is no leaf or is at the routing width, or children that are not fresh homes
    /// below `next_home`.
    pub(crate) fn split(
        &self,
        prefix: &str,
        children: [u32; 2],
        op_id: &str,
    ) -> Result<Self, String> {
        let parent = *self
            .shards
            .leaves
            .get(prefix)
            .ok_or_else(|| format!("{prefix:?} is no leaf"))?;
        if prefix.len() >= crate::keyspace::ROUTE_PREFIX_BITS {
            return Err(format!("{prefix:?} is at the routing width"));
        }
        let fresh = |h: u32| h < self.shards.next_home && !self.shards.homes.contains_key(&h);
        if children[0] == children[1] || !children.into_iter().all(fresh) {
            return Err(format!("{children:?} are not two fresh reserved homes"));
        }
        let mut next = self.clone();
        let version = self.version + 1;
        next.version = version;
        next.shards.version = version;
        next.shards.leaves.remove(prefix);
        if let Some(home) = next.shards.homes.get_mut(&parent) {
            home.retired_version = Some(version);
        }
        for (half, home) in ["0", "1"].into_iter().zip(children) {
            let child = format!("{prefix}{half}");
            next.shards.leaves.insert(child.clone(), home);
            let born = Home {
                path: born_home_path(&child, home),
                prefix: child,
                parents: vec![parent],
                born_version: version,
                retired_version: None,
            };
            next.shards.homes.insert(home, born);
        }
        next.shards.shards = next.shards.leaves.keys().cloned().collect();
        next.last_op = Some(LastOp {
            op_id: op_id.to_string(),
            kind: "split".to_string(),
            sources: vec![(prefix.to_string(), parent)],
            children: children.to_vec(),
        });
        Ok(next)
    }
}

impl crate::config::validation::InitialShards {
    /// A new cell's topology (version 1): `2^n` shards of `n`-bit prefixes in numeric order,
    /// each its own home (the prefix read as a number) at `shards/<prefix>`, and the body
    /// ceiling the cell is created with.
    pub(crate) fn topology(self, body_ceiling: usize) -> Topology {
        let count = self.get();
        let bits = usize::try_from(count.trailing_zeros()).unwrap_or(0);
        let shards: Vec<String> = if bits == 0 {
            vec![String::new()]
        } else {
            (0..count).map(|i| format!("{i:0bits$b}")).collect()
        };
        Topology {
            version: 1,
            shards: ShardSet::of_prefixes(shards),
            last_op: None,
            max_request_body_bytes: Some(body_ceiling),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Topology;

    fn initial(n: usize) -> Topology {
        crate::config::validation::InitialShards::new(n)
            .unwrap()
            .topology(1 << 20)
    }

    /// A new cell's homes are its prefixes read as numbers at `shards/<prefix>`, each prefix
    /// its own leaf, and the next home is past them: the homes S3a's static layout already
    /// names its objects by (`keyspace::home_of`).
    #[test]
    fn a_new_cells_homes_are_its_prefixes_read_as_numbers() {
        let t = initial(4);
        assert_eq!(*t.shards, ["00", "01", "10", "11"]);
        let homes: Vec<(u32, &str, &str)> = t
            .shards
            .homes
            .iter()
            .map(|(h, home)| (*h, home.path.as_str(), home.prefix.as_str()))
            .collect();
        assert_eq!(
            homes,
            [
                (0, "shards/00", "00"),
                (1, "shards/01", "01"),
                (2, "shards/10", "10"),
                (3, "shards/11", "11"),
            ]
        );
        for (prefix, home) in &t.shards.leaves {
            assert_eq!(crate::keyspace::home_of(prefix), Some(*home), "{prefix}");
        }
        assert_eq!((t.version, t.shards.next_home, t.last_op), (1, 4, None));
        let root = initial(1);
        assert_eq!(*root.shards, [""]);
        assert_eq!(root.shards.homes[&0].path, "shards/root");
        assert_eq!((root.shards.leaves[""], root.shards.next_home), (0, 1));
    }

    /// A stored topology reads back as written, and one whose maps disagree is refused: a
    /// leaf missing from `shards`, a leaf naming another prefix's or a retired home, and a
    /// `next_home` that is not past every home.
    #[test]
    fn a_topology_whose_maps_disagree_is_refused() {
        let t = initial(2);
        let raw = serde_json::to_value(&t).unwrap();
        let back: Topology = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(back, t);
        let refused = |edit: fn(&mut serde_json::Value)| {
            let mut v = raw.clone();
            edit(&mut v);
            serde_json::from_value::<Topology>(v)
                .unwrap_err()
                .to_string()
        };
        assert_eq!(
            refused(|v| v["shards"] = serde_json::json!(["0"])),
            "shards [\"0\"] are not exactly the leaves [\"0\", \"1\"]"
        );
        assert_eq!(
            refused(|v| v["leaves"]["1"] = serde_json::json!(0)),
            "leaf \"1\" names home 0, which does not serve it"
        );
        assert_eq!(
            refused(|v| v["homes"]["1"]["retired_version"] = serde_json::json!(2)),
            "leaf \"1\" names home 1, which does not serve it"
        );
        assert_eq!(
            refused(|v| v["next_home"] = serde_json::json!(1)),
            "next_home 1 is not past every home"
        );
    }

    /// A new cell's `topology.json` names each shard's home: its path and prefix, born at
    /// version 1, each prefix its own leaf, and the next home past them (S4-DESIGN §3.1), so a
    /// split can allocate a fresh home and every reader resolves a prefix to its home's path.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_new_cells_topology_names_each_shards_home() {
        use object_store::ObjectStoreExt;
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        crate::registry::load_or_init_topology(
            &store,
            crate::config::validation::InitialShards::new(2).unwrap(),
            crate::protocol_pin::MAX_BODY_BYTES,
        )
        .await
        .unwrap();
        let raw = store
            .get(&object_store::path::Path::from("topology.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let stored: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let home = |path: &str, prefix: &str| serde_json::json!({"path": path, "prefix": prefix, "parents": [], "born_version": 1});
        assert_eq!(
            (&stored["homes"], &stored["leaves"], &stored["next_home"]),
            (
                &serde_json::json!({"0": home("shards/0", "0"), "1": home("shards/1", "1")}),
                &serde_json::json!({"0": 0, "1": 1}),
                &serde_json::json!(2)
            )
        );
    }

    /// **A split reserves two homes, then publishes its halves.** On a two-shard cell, the
    /// reservation takes homes 2 and 3 and leaves the version; the split of `"1"` makes version
    /// 2 with leaves `0`, `10`, `11` at homes 0, 2, 3, the children at `shards/1x~h` with parent
    /// home 1, home 1 retired at version 2, and `last_op` naming it. The result decodes through
    /// the checked form unchanged.
    #[test]
    fn a_split_reserves_two_homes_then_publishes_its_halves() {
        let t = initial(2);
        let (reserved, homes) = t.reserve_homes();
        assert_eq!(
            (homes, reserved.version, reserved.shards.next_home),
            ([2, 3], 1, 4)
        );
        let split = reserved.split("1", homes, "op-1").unwrap();
        assert_eq!(split.version, 2);
        assert_eq!(*split.shards, ["0", "10", "11"]);
        let leaves: Vec<(&str, u32)> = split
            .shards
            .leaves
            .iter()
            .map(|(p, h)| (p.as_str(), *h))
            .collect();
        assert_eq!(leaves, [("0", 0), ("10", 2), ("11", 3)]);
        let child = &split.shards.homes[&3];
        assert_eq!(
            (
                child.path.as_str(),
                child.parents.as_slice(),
                child.born_version
            ),
            ("shards/11~3", [1].as_slice(), 2)
        );
        assert_eq!(split.shards.homes[&1].retired_version, Some(2));
        let op = split.last_op.as_ref().unwrap();
        assert_eq!(
            (
                op.op_id.as_str(),
                op.sources.as_slice(),
                op.children.as_slice()
            ),
            ("op-1", [("1".to_string(), 1)].as_slice(), [2, 3].as_slice())
        );
        let round: Topology = serde_json::from_slice(&serde_json::to_vec(&split).unwrap()).unwrap();
        assert_eq!(round, split);
    }

    /// **A split of what is no splittable leaf, or into homes not reserved, is refused.**
    #[test]
    fn a_split_of_no_leaf_or_into_unreserved_homes_is_refused() {
        let (t, homes) = initial(2).reserve_homes();
        assert!(t.split("10", homes, "op").is_err(), "no leaf");
        assert!(t.split("1", [1, 3], "op").is_err(), "a live home");
        assert!(t.split("1", [2, 2], "op").is_err(), "one home twice");
        assert!(t.split("1", [4, 5], "op").is_err(), "not reserved");
        let mut deep = t;
        let width = "1".repeat(crate::keyspace::ROUTE_PREFIX_BITS);
        deep.shards.leaves.insert(width.clone(), 1);
        assert!(
            deep.split(&width, homes, "op").is_err(),
            "at the routing width"
        );
    }
}
