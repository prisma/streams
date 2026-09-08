//! Physical range carried unchanged through local and peer page execution.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadRange {
    pub(crate) from: u64,
    pub(crate) end: u64,
}
impl ReadRange {
    pub(crate) fn open(from: u64) -> Self {
        Self {
            from,
            end: u64::MAX,
        }
    }
    pub(crate) fn bounded(from: u64, end: u64) -> Self {
        Self { from, end }
    }
}
