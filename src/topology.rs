//! The cell's versioned shard topology (S4-DESIGN §3): the stored map, and the split that
//! changes it.
pub(crate) mod fence;
pub(crate) mod gate;
// The intent's read is production (the open-path fence, and S6's lineage sweeper, read the live
// intent); its writes and lease build only for their tests until their first caller (S4 step 11).
pub(crate) mod intent;
pub(crate) mod model;
pub(crate) mod revalidate;
// The split's sentinel helper builds for its tests until the split calls it (S4 step 11).
#[cfg(test)]
pub(crate) mod sentinel;
