//! The request documents of the queue routes (docs/QUEUES-DESIGN.md §11,
//! the first release's HTTP contract): decoded here, checked by the
//! workflows that take them. Unknown fields are refused.
use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::value::RawValue;

/// One message to send: exactly one of `body` (JSON) and `bodyBase64`;
/// at most one of `deliverAt` and `delayMs`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SendInput {
    #[serde(default)]
    pub(crate) body: Option<Box<RawValue>>,
    #[serde(default)]
    pub(crate) body_base64: Option<String>,
    #[serde(default)]
    pub(crate) content_type: Option<String>,
    #[serde(default)]
    // mt-lint: allow(name-keyed-map): message headers, keyed by header name; no stream identity
    pub(crate) headers: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub(crate) key: Option<String>,
    #[serde(default)]
    pub(crate) dedup_key: Option<String>,
    #[serde(default)]
    pub(crate) deliver_at: Option<i64>,
    #[serde(default)]
    pub(crate) delay_ms: Option<i64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SendBatchInput {
    pub(crate) messages: Vec<SendInput>,
}

/// A claim of the default group; every field optional (§6.1).
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaimInput {
    #[serde(default)]
    pub(crate) max: Option<i64>,
    #[serde(default)]
    pub(crate) max_bytes: Option<i64>,
    #[serde(default)]
    pub(crate) lease_ms: Option<i64>,
    #[serde(default)]
    pub(crate) wait_ms: Option<i64>,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SettleVerb {
    Ack,
    Retry,
    Extend,
    Fail,
    Discard,
    Release,
}

/// One settle operation by lease token (§6.2). `error` text is accepted
/// and not stored: the first release stores the `code` only.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SettleOp {
    pub(crate) token: String,
    pub(crate) op: SettleVerb,
    #[serde(default)]
    pub(crate) delay_ms: Option<i64>,
    #[serde(default)]
    pub(crate) lease_ms: Option<i64>,
    #[serde(default)]
    pub(crate) code: Option<String>,
    #[serde(default)]
    pub(crate) error: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SettleInput {
    pub(crate) ops: Vec<SettleOp>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct IdsInput {
    #[serde(default)]
    pub(crate) ids: Vec<String>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RedriveInput {
    #[serde(default)]
    pub(crate) ids: Vec<String>,
    #[serde(default)]
    pub(crate) all: bool,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WakeupInput {
    pub(crate) url: String,
}
