//! The cell's versioned shard topology (S4-DESIGN §3): the stored map, and the split that
//! changes it.
// Until its first caller (the open-path fence, S4 step 9) the intent's store protocol builds
// only for its tests.
#[cfg(test)]
pub(crate) mod intent;
pub(crate) mod model;
pub(crate) mod revalidate;
