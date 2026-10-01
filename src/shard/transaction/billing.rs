//! A staged billing op (a close or a retention flag) answers its submitter
//! through its group's replies: `Ok` once the group is durable, or the
//! group's refusal. An op dropped before it was staged answers its own
//! refusal (`BillingReply`).
use super::{BillingReply, CommitTransaction, StreamOverlay};

impl CommitTransaction<'_> {
    /// Stage the close (`billing_close`), then hand its reply to the group.
    pub(super) fn billing_close_answered(
        &mut self,
        local: &mut StreamOverlay,
        close_ms: i64,
        resp: BillingReply,
    ) {
        let hash = local.handle.hash;
        self.billing_close(local, hash, close_ms);
        resp.applied(&mut self.effects, &local.fields);
    }

    /// Stage the retention flag (`billing_retained`), then hand its reply to
    /// the group.
    pub(super) fn billing_retained_answered(
        &mut self,
        local: &mut StreamOverlay,
        retained: bool,
        resp: BillingReply,
    ) {
        let hash = local.handle.hash;
        self.billing_retained(local, hash, retained);
        resp.applied(&mut self.effects, &local.fields);
    }
}
