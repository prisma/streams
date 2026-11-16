//! Operator actions and views (docs/QUEUES-DESIGN.md §6.7, §7.2-§7.3,
//! §8.2 "Queue delete", §9.2): pause, cancel, redrive, discard, the failed
//! and pending views, one message, configuration updates, wake-up
//! subscriptions and deletion.
use serde::Serialize;

use super::delivery::RESEAL_RETRIES;
use super::envelope::{self, Shown};
use super::{
    FailureClass, Message, QueueFailure, QueueKey, QueueService, QueueTarget, fail, unexpected,
};
use crate::native_queue::{DeliveryState, DeliveryView, NqOp, NqOut, PrepareFrom, QueueConfig};

/// The longest page of the failed and pending views.
const MAX_PAGE: usize = 1_000;
/// The untaken sealed pages one page of the pending view reads at most.
const PEEK_PREPARE_BYTES: usize = 1 << 20;

/// One page of messages and the id to continue after.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessagePage {
    messages: Vec<Message>,
    next: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeliveryOut {
    state: DeliveryState,
    attempts: u32,
    cycle: u32,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageWithDelivery {
    message: Message,
    delivery: DeliveryOut,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Redriven {
    redriven: u64,
    remaining: u64,
    stopped_at: Option<&'static str>,
}

/// A wake-up subscription as set: its secret only when one was set.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Wakeup {
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret: Option<String>,
    version: u32,
}

/// Where a page starts: after the message `cursor` names, at most `limit`
/// messages (100 by default, at most 1,000).
#[derive(Default)]
pub(crate) struct PageAt {
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<usize>,
}

/// Which view a page lists.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum View {
    Failed,
    Pending,
}

impl QueueService {
    pub(crate) async fn pause(&self, t: &QueueTarget, paused: bool) -> Result<bool, QueueFailure> {
        match self.op(t, NqOp::Pause { paused }).await? {
            NqOut::Paused(p) => Ok(p),
            other => Err(unexpected(&other)),
        }
    }

    /// Cancel (queue-wide), or discard failed deliveries: the count acted on.
    pub(crate) async fn count_op(
        &self,
        t: &QueueTarget,
        ids: &[String],
        cancel: bool,
    ) -> Result<u64, QueueFailure> {
        let offsets = t.offsets(ids);
        let op = if cancel {
            NqOp::Cancel { offsets }
        } else {
            NqOp::Discard { offsets }
        };
        match self.op(t, op).await? {
            NqOut::Count(n) => Ok(n),
            other => Err(unexpected(&other)),
        }
    }

    pub(crate) async fn redrive(
        &self,
        t: &QueueTarget,
        input: super::RedriveInput,
    ) -> Result<Redriven, QueueFailure> {
        let op = NqOp::Redrive {
            offsets: t.offsets(&input.ids),
            all: input.all,
            limit: input.limit.unwrap_or(MAX_PAGE).clamp(1, MAX_PAGE),
        };
        let out = self.op(t, op).await?;
        crate::native_queue::wake(&t.hash);
        match out {
            NqOut::Redriven {
                redriven,
                remaining,
                capacity,
            } => Ok(Redriven {
                redriven,
                remaining,
                stopped_at: capacity.then_some("capacity"),
            }),
            other => Err(unexpected(&other)),
        }
    }

    /// A page of the failed (§7.2) or pending (§6.7) view, after the
    /// message the cursor names, with each envelope opened by the key.
    pub(crate) async fn page(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        view: View,
        at: PageAt,
    ) -> Result<MessagePage, QueueFailure> {
        let after = match at.cursor {
            Some(c) => Some(t.offsets(&[c]).pop().ok_or_else(|| {
                fail(
                    FailureClass::Invalid,
                    "invalid_cursor",
                    "cursor names no message of this queue",
                )
            })?),
            None => None,
        };
        let limit = at.limit.unwrap_or(100).clamp(1, MAX_PAGE);
        let (items, next) = match view {
            View::Failed => self.views(t, NqOp::ListFailed { after, limit }).await?,
            View::Pending => self.pending(t, after, limit).await?,
        };
        let shown = Shown {
            failure: view == View::Failed,
            state: view == View::Pending,
            ..Shown::default()
        };
        let mut messages = Vec::with_capacity(items.len());
        for item in &items {
            if let Some(env) = read(t, key, item).await? {
                messages.push(envelope::message(t, item, env, shown));
            }
        }
        Ok(MessagePage {
            messages,
            next: next.map(|o| t.id(o)),
        })
    }

    /// One message and its delivery: 404 `message_not_found` for an id of
    /// no message here, 404 `reclaimed` once it is gone.
    pub(crate) async fn message(
        &self,
        t: &QueueTarget,
        key: &QueueKey,
        id: &str,
    ) -> Result<MessageWithDelivery, QueueFailure> {
        let Some(offset) = t.offsets(&[id.to_string()]).pop() else {
            return Err(super::message_not_found());
        };
        let view = self.message_view(t, offset).await?;
        let Some(env) = read(t, key, &view).await? else {
            return Err(QueueFailure::from(
                crate::native_queue::NqRefusal::Reclaimed,
            ));
        };
        let delivery = DeliveryOut {
            state: view.state,
            attempts: view.attempt,
            cycle: view.cycle,
        };
        let shown = Shown {
            failure: true,
            ..Shown::default()
        };
        Ok(MessageWithDelivery {
            message: envelope::message(t, &view, env, shown),
            delivery,
        })
    }

    /// The deliveries a view's operation answers, and its cursor.
    async fn views(
        &self,
        t: &QueueTarget,
        op: NqOp,
    ) -> Result<(Vec<DeliveryView>, Option<u64>), QueueFailure> {
        match self.op(t, op).await? {
            NqOut::Deliveries { items, next } => Ok((items, next)),
            other => Err(unexpected(&other)),
        }
    }

    /// The pending view's page after `after` (S3b-DESIGN §6.2): its untaken
    /// sealed pages are read here, at most `PEEK_PREPARE_BYTES` of them. The
    /// page is exact only up to the offset that read reached, so it ends
    /// there and its cursor continues from there; a read a seal raced is
    /// read again.
    async fn pending(
        &self,
        t: &QueueTarget,
        after: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<DeliveryView>, Option<u64>), QueueFailure> {
        let from = after.map_or(0, |a| a.saturating_add(1));
        let mut retries = 0;
        loop {
            let (prepared, through) = t
                .engine
                .prepare_queue_pages(t.hash, PrepareFrom::From(from), PEEK_PREPARE_BYTES)
                .await
                .map_err(read_failed)?;
            let covered = prepared.last().map_or(through, |(o, _)| o + 1);
            let op = NqOp::Peek {
                after,
                limit,
                prepared,
            };
            let (items, next) = self.views(t, op).await?;
            let raced = sealed_boundary(t).await? > through;
            if raced && retries < RESEAL_RETRIES {
                retries += 1;
                continue;
            }
            if !raced && covered >= through {
                return Ok((items, next));
            }
            return cut_at(items, next, (from, covered));
        }
    }

    /// One message's view (S3b-DESIGN §6.2): its page is read here when it
    /// is sealed and untaken, and read again when a seal took it after a
    /// read that found it unsealed; a seal that does so each of four times
    /// answers 503 `read_failed`, never a message's absence it cannot tell.
    async fn message_view(
        &self,
        t: &QueueTarget,
        offset: u64,
    ) -> Result<DeliveryView, QueueFailure> {
        let mut retries = 0;
        loop {
            let (prepared, through) = t
                .engine
                .prepare_queue_pages(t.hash, PrepareFrom::At(offset), 1)
                .await
                .map_err(read_failed)?;
            let unread = prepared.is_empty() && offset >= through;
            let failure = match self.op(t, NqOp::Message { offset, prepared }).await {
                Ok(NqOut::Message(view)) => return Ok(view),
                Ok(other) => return Err(unexpected(&other)),
                Err(failure) => failure,
            };
            let raced = unread && sealed_boundary(t).await? > offset;
            if raced && retries < RESEAL_RETRIES {
                retries += 1;
                continue;
            }
            if raced {
                // A seal took the page each time it was read: whether the
                // message is still there is not known.
                return Err(read_failed(
                    "a seal moved the message's page while it was read".to_owned(),
                ));
            }
            return Err(failure);
        }
    }

    /// Update the default group's configuration and the admission budget
    /// (§11): `patch` merges into the stored configuration; any other
    /// field is fixed at creation (400 `immutable_field`).
    pub(crate) async fn update(
        &self,
        t: &QueueTarget,
        patch: serde_json::Value,
    ) -> Result<QueueConfig, QueueFailure> {
        let invalid = |code: &'static str, m: String| fail(FailureClass::Invalid, code, m);
        let serde_json::Value::Object(patch) = patch else {
            return Err(invalid(
                "invalid_config",
                "the update is a JSON object".into(),
            ));
        };
        let current = self.state(t).await?.cfg;
        let mut doc = serde_json::to_value(&current)
            .map_err(|e| fail(FailureClass::Internal, "internal", e.to_string()))?;
        for (field, value) in patch {
            if !matches!(
                field.as_str(),
                "group" | "maxQueuedBytes" | "maxQueuedMessages"
            ) {
                return Err(invalid(
                    "immutable_field",
                    format!("{field} is fixed at creation or unknown"),
                ));
            }
            merge(&mut doc[field.as_str()], value);
        }
        let cfg: QueueConfig = serde_json::from_value(doc)
            .map_err(|e| invalid("invalid_config", format!("configuration: {e}")))?;
        let op = NqOp::Update {
            group: cfg.group,
            max_queued_bytes: cfg.max_queued_bytes,
            max_queued_messages: cfg.max_queued_messages,
        };
        match self.op(t, op).await? {
            NqOut::Config { cfg, .. } => Ok(cfg),
            other => Err(unexpected(&other)),
        }
    }

    /// Set (`Some`) or remove the default group's wake-up subscription
    /// (§9.2). A set answers the subscription's secret, once: base64url of
    /// `wakeup_secret(root, hash, version)`.
    pub(crate) async fn wakeup(
        &self,
        t: &QueueTarget,
        url: Option<String>,
    ) -> Result<Wakeup, QueueFailure> {
        let root = match (&url, self.wake_root) {
            (Some(u), Some(root)) => {
                check_wakeup_url(u)?;
                Some(root)
            }
            (Some(_), None) => {
                let mut f = fail(
                    FailureClass::Unavailable,
                    "wakeup_unavailable",
                    "this deployment holds no wake-up key (STREAMS_CURSOR_KEY)",
                );
                f.retryable = false;
                return Err(f);
            }
            (None, _) => None,
        };
        let (url, version) = match self
            .op(
                t,
                NqOp::SetWakeup {
                    url,
                    name: t.desc.name.clone(),
                },
            )
            .await?
        {
            NqOut::Wakeup { url, version } => (url, version),
            other => return Err(unexpected(&other)),
        };
        let secret = root.map(|root| {
            use base64::Engine;
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                crate::native_queue::wakeup::wakeup_secret(&root, &t.hash, version),
            )
        });
        Ok(Wakeup {
            url,
            secret,
            version,
        })
    }

    /// Begin deleting the queue (§8.2): sends, claims and redrives are
    /// refused from here on, and the cleanup runs as request maintenance.
    /// A queue stream whose queue was never created (a create that failed
    /// after its stream) is cleaned up the same way.
    pub(crate) async fn delete(&self, t: &QueueTarget) -> Result<(), QueueFailure> {
        match t
            .engine
            .try_submit_native_queue(t.hash, NqOp::BeginDelete)
            .await
        {
            Ok(NqOut::Deleting) | Err(crate::native_queue::NqRefusal::NotFound) => {}
            Ok(other) => return Err(unexpected(&other)),
            Err(refusal) => return Err(refusal.into()),
        }
        self.resume_cleanup(t);
        Ok(())
    }

    /// Submit (or join) the deleting queue's cleanup. A full maintenance
    /// queue is not an error: the next GET or DELETE of the queue resumes it.
    pub(crate) fn resume_cleanup(&self, t: &QueueTarget) {
        let job = super::QueueDeletion::new(self.clone(), &t.desc);
        let _ticket = self.work.submit(job.key(), job.into_action());
    }
}

/// The envelope a listing or a read by id shows: a message whose envelope
/// will not open is shown without a body (S3b-DESIGN §6.5).
/// A queue's pages could not be read.
fn read_failed(error: String) -> QueueFailure {
    fail(FailureClass::Unavailable, "read_failed", error)
}

/// The queue stream's sealed boundary now, as a view checks it after the
/// committer answered.
async fn sealed_boundary(t: &QueueTarget) -> Result<u64, QueueFailure> {
    let sealed = t
        .engine
        .queue_sealed_boundary(t.hash)
        .await
        .map_err(read_failed)?;
    #[cfg(test)]
    let sealed = sealed + t.engine.queue_seal_race_for_tests();
    Ok(sealed)
}

/// A pending page cut at `covered`, the offset its read of sealed pages
/// reached: its items before it, and a cursor that continues there. A read
/// that reached nothing past `from` is a failure the client retries.
fn cut_at(
    mut items: Vec<DeliveryView>,
    next: Option<u64>,
    (from, covered): (u64, u64),
) -> Result<(Vec<DeliveryView>, Option<u64>), QueueFailure> {
    items.retain(|v| v.offset < covered);
    if let Some(n) = next.filter(|n| *n < covered) {
        return Ok((items, Some(n)));
    }
    match covered.checked_sub(1).filter(|c| *c >= from) {
        Some(cursor) => Ok((items, Some(cursor))),
        None => Err(read_failed(
            "a seal moved the queue's pages while the view read them".to_owned(),
        )),
    }
}

async fn read(
    t: &QueueTarget,
    key: &QueueKey,
    view: &DeliveryView,
) -> Result<Option<envelope::Envelope>, QueueFailure> {
    match envelope::read(t, key, view.offset).await {
        Ok(env) => Ok(env),
        Err(envelope::ReadFailure::Unreadable(_)) => Ok(Some(envelope::Envelope::unreadable())),
        Err(e @ envelope::ReadFailure::Store(_)) => Err(fail(
            FailureClass::Unavailable,
            "read_failed",
            e.to_string(),
        )),
    }
}

/// Merge `patch` into `target`: objects field by field, anything else
/// replaced.
fn merge(target: &mut serde_json::Value, patch: serde_json::Value) {
    match (target, patch) {
        (serde_json::Value::Object(t), serde_json::Value::Object(p)) => {
            for (k, v) in p {
                merge(t.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (t, p) => *t = p,
    }
}

/// A wake-up URL is `https` on the default port, at most 2 KiB. Address
/// policy (public addresses only) is the notifier's, at connect time.
fn check_wakeup_url(url: &str) -> Result<(), QueueFailure> {
    let parsed = reqwest::Url::parse(url).ok();
    let ok = url.len() <= 2_048
        && parsed.is_some_and(|u| {
            u.scheme() == "https" && u.host_str().is_some() && u.port().is_none_or(|p| p == 443)
        });
    if ok {
        Ok(())
    } else {
        Err(fail(
            FailureClass::Invalid,
            "invalid_wakeup_url",
            "a wake-up URL is https on port 443, at most 2 KiB",
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_wakeup_secret_is_the_hmac_of_the_domain_hash_and_version() {
        use hmac::{Hmac, Mac};
        let (root, hash) = ([7u8; 32], [3u8; 16]);
        let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(&root).unwrap();
        mac.update(b"queue-wakeup/v1");
        mac.update(&hash);
        mac.update(&[0, 0, 0, 2]);
        let expected: [u8; 32] = mac.finalize().into_bytes().into();
        assert_eq!(
            crate::native_queue::wakeup::wakeup_secret(&root, &hash, 2),
            expected
        );
        assert_ne!(
            crate::native_queue::wakeup::wakeup_secret(&root, &hash, 3),
            expected
        );
        assert_ne!(
            crate::native_queue::wakeup::wakeup_secret(&root, &[4u8; 16], 2),
            expected
        );
    }

    #[test]
    fn a_wakeup_url_is_https_on_the_default_port() {
        assert!(super::check_wakeup_url("https://example.com/hook").is_ok());
        assert!(super::check_wakeup_url("https://example.com:443/hook").is_ok());
        assert!(super::check_wakeup_url("http://example.com/hook").is_err());
        assert!(super::check_wakeup_url("https://example.com:8443/hook").is_err());
        assert!(super::check_wakeup_url("not a url").is_err());
    }
}

#[cfg(test)]
mod cut_tests {
    use super::{DeliveryView, cut_at};
    use crate::native_queue::DeliveryState;

    fn views(offsets: &[u64]) -> Vec<DeliveryView> {
        offsets
            .iter()
            .map(|&offset| DeliveryView {
                offset,
                state: DeliveryState::Pending,
                lease_gen: 0,
                attempt: 0,
                cycle: 0,
                enqueued_at: 0,
                available_at: 0,
                lease_deadline: 0,
                failure: None,
                page: None,
                bytes: 0,
            })
            .collect()
    }

    fn offsets(cut: &(Vec<DeliveryView>, Option<u64>)) -> (Vec<u64>, Option<u64>) {
        (cut.0.iter().map(|v| v.offset).collect(), cut.1)
    }

    /// A page cut where its read of sealed pages reached keeps what lies
    /// before it and continues there; a cursor before the cut stands; a read
    /// that reached nothing past the page's start is a retryable failure.
    #[test]
    fn a_pending_page_is_cut_where_its_read_reached_and_continues_there() {
        let cut = cut_at(views(&[3, 4, 9, 12]), None, (3, 10)).unwrap();
        assert_eq!(offsets(&cut), (vec![3, 4, 9], Some(9)));
        let cut = cut_at(views(&[3, 4]), Some(4), (3, 10)).unwrap();
        assert_eq!(offsets(&cut), (vec![3, 4], Some(4)), "a full page's cursor");
        let cut = cut_at(views(&[3, 11]), Some(11), (3, 6)).unwrap();
        assert_eq!(
            offsets(&cut),
            (vec![3], Some(5)),
            "a cursor past the cut moves back"
        );
        let cut = cut_at(views(&[12]), None, (3, 6)).unwrap();
        assert_eq!(
            offsets(&cut),
            (vec![], Some(5)),
            "nothing before the cut: continue there"
        );
        let failed = cut_at(views(&[7]), None, (5, 5)).unwrap_err();
        assert_eq!(failed.code, "read_failed");
    }
}
