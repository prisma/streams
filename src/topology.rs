//! The cell's versioned shard topology (S4-DESIGN §3): the stored map, and the split that
//! changes it.
pub(crate) mod fence;
pub(crate) mod gate;
// The intent's read, its CAS and the lapse are production (the open-path fence's claimant, and
// S6's lineage sweeper, read the live intent); its create and the actor's lease build only for
// their tests until the split's route calls them (S4 step 13).
pub(crate) mod intent;
pub(crate) mod model;
pub(crate) mod ops;
pub(crate) mod recover;
pub(crate) mod revalidate;
// The split's sentinel helper.
pub(crate) mod sentinel;
// The topology's CAS writes: the claimant's roll forward publishes through them.
pub(crate) mod store;
// The split: its route and the scaler start it through `ops`.
pub(crate) mod split;
