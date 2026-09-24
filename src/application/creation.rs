//! Creation, fork anchoring and deletion own their persisted initialization/debt transitions.
use crate::crypto::{StreamKey, derive_subkey, hex};
use crate::registry::{Mutation, MutationResult, Registry, StreamDesc};
use crate::shard::{AppendReq, ShardEngine, now_ms};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::oneshot;

const APPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreationFailure {
    Invalid,
    Conflict,
    Missing,
    Gone,
    WrongKey,
    Storage,
    TooLarge,
    Overloaded,
    Ambiguous,
    Opening,
}
#[derive(Debug)]
pub(crate) struct CreationError {
    pub kind: CreationFailure,
    pub code: &'static str,
    pub message: String,
    pub owner: Option<String>,
    pub retry_after: Option<u64>,
}
impl CreationError {
    fn new(kind: CreationFailure, code: &'static str, message: &str) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            owner: None,
            retry_after: None,
        }
    }
    fn gone(desc: Option<&StreamDesc>) -> Self {
        if desc.is_some_and(|d| retained_for_forks(d, now_ms())) {
            Self::new(
                CreationFailure::Gone,
                "gone",
                "stream deleted; live forks remain",
            )
        } else {
            Self::new(CreationFailure::Missing, "not_found", "stream not found")
        }
    }
}
#[derive(Clone)]
pub(crate) struct ForkCommand {
    pub source: crate::tenant::TenantStreamRef,
    pub offset: Option<u64>,
    pub sub_offset: Option<u64>,
}
pub(crate) struct CreateCommand {
    pub sref: crate::tenant::TenantStreamRef,
    pub key: StreamKey,
    pub content_type: Option<String>,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub close: bool,
    pub body: Bytes,
    pub fork: Option<ForkCommand>,
}
pub(crate) struct CreateOutcome {
    pub created: bool,
    pub desc: StreamDesc,
    pub next: u64,
    pub closed: bool,
}
#[derive(Clone)]
pub(crate) struct CreationService {
    pub registry: Arc<Registry>,
    pub shards: crate::shard_directory::ShardDirectory,
    pub ownership: crate::ownership::OwnershipService,
    pub reads: Arc<crate::application::read::ReadService>,
    pub keys: Arc<crate::history::KeyCache>,
    pub runtime: crate::runtime::RuntimeCaps,
    pub deployment: crate::deployment::DeploymentIdentity,
    pub auth: Arc<crate::auth::AuthService>,
    pub quotas: crate::quota::QuotaRegistry,
    pub admission: crate::admission::AdmissionController,
}
mod anchor;
mod claim;
mod deletion;
mod fork;
mod initialization;
mod product;
mod raw;
mod reconcile;
pub(crate) use product::{ProductCreateConfig, ProductCreateError};
pub(crate) use reconcile::{ForkDebtStatus, spawn_fork_debt_reconciler};

impl CreationService {
    pub(crate) async fn resolve(
        &self,
        route: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, CreationError> {
        use crate::shard_directory::ResolveError;
        self.shards
            .resolve(route, crate::shard_directory::Adoption::External)
            .await
            .map_err(|e| match e {
                ResolveError::NotOwner { prefix, owner } => CreationError {
                    owner: Some(owner.clone()),
                    ..CreationError::new(
                        CreationFailure::Conflict,
                        "not_ring_owner",
                        &format!("shard {prefix} belongs to {owner}"),
                    )
                },
                ResolveError::Opening {
                    code,
                    retry_after_secs,
                    ..
                } => CreationError {
                    retry_after: Some(retry_after_secs),
                    ..CreationError::new(
                        CreationFailure::Opening,
                        code,
                        "shard not currently serving here; retry",
                    )
                },
                ResolveError::OpenFailed { prefix, error } => CreationError::new(
                    CreationFailure::Storage,
                    "shard_open",
                    &format!("open shard {prefix}: {error}"),
                ),
            })
    }
}
pub(crate) fn desc_alive(desc: &crate::registry::PersistedDescriptor) -> bool {
    alive_at(desc, now_ms())
}
/// Liveness at the caller's instant: the recreate CAS and the classification
/// of the winner it declines on must judge one instant, or a winner expiring
/// between the two would be neither live nor retained.
fn alive_at(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !desc.deleted && !desc.soft_deleted && desc.expires_at_ms.is_none_or(|expires| at_ms < expires)
}
/// A dead incarnation whose epoch and records its live forks still read
/// through (pinned fork lifecycle): soft-deleted, or expired with children.
/// Creation, append and delete answer it as gone and nothing may replace it
/// until the last fork releases it, so this is the one place that decides it.
pub(crate) fn retained_for_forks(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    desc.soft_deleted
        || (!desc.deleted
            && !desc.fork_children.is_empty()
            && desc.expires_at_ms.is_some_and(|expires| at_ms >= expires))
}
/// The recreate CAS predicate. It judges the STORED descriptor, because a
/// cached snapshot can predate a fork another instance anchored, and replacing
/// a retained source strands every fork reading through it.
pub(crate) fn recreatable(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !alive_at(desc, at_ms) && !retained_for_forks(desc, at_ms)
}
fn init_claim_stale(desc: &crate::registry::PersistedDescriptor) -> bool {
    desc.init
        .as_ref()
        .is_some_and(|init| now_ms() - init.claimed_ms > crate::registry::INIT_CLAIM_MS)
}
#[expect(
    clippy::too_many_arguments,
    reason = "create_request_hash; the request hash covers every field a client can vary, taken separately as the handler parsed them; a request struct would exist only to be hashed"
)]
pub(crate) fn create_request_hash(
    content_type: &str,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    close: bool,
    body: &[u8],
    fork: Option<&crate::registry::ForkRef>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content_type.as_bytes());
    h.update([0u8]);
    h.update(ttl_secs.unwrap_or(0).to_le_bytes());
    h.update(expires_at_ms.unwrap_or(0).to_le_bytes());
    h.update([u8::from(close)]);
    h.update((body.len() as u64).to_le_bytes());
    h.update(body);
    if let Some(fr) = fork {
        h.update(fr.source.as_bytes());
        h.update([0u8]);
        // The source INCARNATION is part of the identity. Without it, a
        // retry against a recreated source hashed the same as the
        // original, so it resumed an initialization whose stored
        // forked_from still pointed at the previous epoch — reference
        // installed on incarnation B, child recorded against A, and
        // stitched reads later failing the epoch check.
        h.update(fr.source_epoch.as_bytes());
        h.update([0u8]);
        h.update(fr.fork_offset.to_le_bytes());
        h.update(fr.fork_sub.to_le_bytes());
    }
    hex(&h.finalize()[..16])
}
#[expect(
    clippy::too_many_arguments,
    reason = "fresh_desc; a fresh descriptor is built from the resolved name, epoch, policy and fork parts separately as creation decided them; a builder would restate the descriptor's own fields"
)]
pub(crate) fn fresh_desc(
    service: &CreationService,
    sref: &crate::tenant::TenantStreamRef,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
) -> crate::registry::PersistedDescriptor {
    let epoch = service.runtime.epoch();
    crate::registry::PersistedDescriptor {
        name: sref.name().as_str().to_string(),
        account_id: Some(service.deployment.account_id().to_string()),
        project_id: sref.project_id().clone(),
        stream_epoch: hex(&epoch),
        seal_gen_counter: 0,
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs.map(ttl::expiry_from_now).or(expires_at_ms),
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type,
        ttl_secs,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
}

/// The records a JSON body holds, as a JSON collection stores them: each
/// top-level element of an array body, or the body itself when it is not an
/// array, as the client's own text with the whitespace outside strings
/// removed. Number literals, key order, duplicate keys and string escapes
/// are kept byte for byte, so every read serves what the client wrote and
/// storing a stored record again is a no-op. The body is refused unless
/// every record is JSON under the correctly rounded parser: finite numbers
/// and no lone surrogate escapes.
pub(crate) fn json_entries(body: &[u8], allow_empty_array: bool) -> Result<Vec<Bytes>, String> {
    let first = body.iter().find(|b| !is_json_whitespace(**b));
    if first != Some(&b'[') {
        return Ok(vec![json_record(body)?]);
    }
    let elements: Vec<&serde_json::value::RawValue> =
        serde_json::from_slice(body).map_err(|_| "invalid JSON body".to_string())?;
    if elements.is_empty() && !allow_empty_array {
        return Err("empty JSON array".to_string());
    }
    elements
        .iter()
        .map(|element| json_record(element.get().as_bytes()))
        .collect()
}
/// One JSON value's stored text: validated without building a DOM, then
/// with the whitespace outside its strings removed. A valid string holds no
/// raw control character, so the stored text holds no raw CR or LF: SSE
/// frames each record on one `data:` line and relies on that.
pub(crate) fn json_record(text: &[u8]) -> Result<Bytes, String> {
    serde_json::from_slice::<ValidJson>(text).map_err(|_| "invalid JSON body".to_string())?;
    let mut stored = Vec::with_capacity(text.len());
    let (mut in_string, mut escaped) = (false, false);
    for &byte in text {
        if escaped {
            escaped = false;
        } else if in_string {
            escaped = byte == b'\\';
            in_string = byte != b'"';
        } else if is_json_whitespace(byte) {
            continue;
        } else {
            in_string = byte == b'"';
        }
        stored.push(byte);
    }
    Ok(Bytes::from(stored))
}
fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
/// Any JSON value, parsed into nothing through `deserialize_any`: serde_json
/// still range-checks every number (correctly rounded under
/// `float_roundtrip`) and decodes every string escape, but builds no DOM.
struct ValidJson;
impl<'de> serde::Deserialize<'de> for ValidJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(ValidJson)
    }
}
impl<'de> serde::de::Visitor<'de> for ValidJson {
    type Value = ValidJson;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_str<E>(self, _: &str) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_unit<E>(self) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        while seq.next_element::<ValidJson>()?.is_some() {}
        Ok(self)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self, A::Error> {
        while map.next_entry::<ValidJson, ValidJson>()?.is_some() {}
        Ok(self)
    }
}
pub(crate) fn over_record_ceiling(cap: usize, entries: &[Bytes]) -> Option<usize> {
    if cap == 0 {
        return None;
    }
    entries.iter().map(|e| e.len()).find(|l| *l > cap)
}
mod ttl;
pub(crate) use ttl::{TtlMutation, admit_ttl};

#[cfg(test)]
mod tests {
    use crate::application::request_work::{Action, Key, Kind, RequestWork, WorkError};
    use std::{sync::Arc, time::Duration};

    #[expect(
        clippy::excessive_nesting,
        reason = "r05_cancelled_ttl_attempt_releases_only_its_owned_slot; the fixture nests the drain wait inside the timeout that bounds it inside the test; flattening it would separate the wait from the bound it must respect"
    )]
    #[tokio::test]
    async fn r05_cancelled_ttl_attempt_releases_only_its_owned_slot() {
        let project = crate::tenant::ProjectId::new("creation-test").unwrap();
        let source = Key {
            stream: project.stream_ref("source"),
            epoch: "first".into(),
            kind: Kind::Ttl,
        };
        let other = Key {
            stream: project.stream_ref("other"),
            epoch: "first".into(),
            kind: Kind::Ttl,
        };
        let work = Arc::new(RequestWork::default());
        let tasks = crate::tasks::TaskSupervisor::new();
        work.start(&tasks).unwrap();
        let first = work
            .test_admit(
                source.clone(),
                Action::Held(Box::pin(std::future::pending())),
                tokio::time::Instant::now() + Duration::from_millis(10),
            )
            .unwrap();
        let _other = work
            .submit(
                other.clone(),
                Action::Held(Box::pin(std::future::pending())),
            )
            .unwrap();
        assert_eq!(first.wait().await, Err(WorkError::TimedOut));
        tokio::time::timeout(Duration::from_secs(1), async {
            while work.test_keys().contains(&source) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            work.test_keys(),
            std::collections::HashSet::from([other.clone()])
        );
        let retry = work
            .submit(source, Action::Held(Box::pin(async { Ok(()) })))
            .expect("retry can claim the released slot");
        retry.wait().await.unwrap();
        assert_eq!(work.test_keys(), std::collections::HashSet::from([other]));
        tasks.shutdown(Duration::from_secs(1)).await;
        assert!(work.test_keys().is_empty());
    }

    /// The recreate CAS and the classification of the winner it declines on
    /// share one instant, so the three name verdicts must partition every
    /// descriptor at that instant, the expiry instant itself included: live,
    /// retained for forks, or recreatable, never two and never none.
    #[test]
    fn a_name_is_live_retained_or_recreatable_at_one_instant() {
        use super::{alive_at, recreatable, retained_for_forks};
        let at = 1_000;
        let base = crate::registry::PersistedDescriptor {
            seal_gen_counter: 0,
            account_id: None,
            project_id: crate::tenant::ProjectId::new("creation-test").unwrap(),
            name: "verdict".into(),
            stream_epoch: "0123456789abcdef0123456789abcdef".into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            watch_sig_key: None,
            parent_ref_pending: false,
            soft_deleted: false,
            logical_close_ms: None,
            forked_from: None,
            fork_children: Vec::new(),
            init: None,
            sealing: None,
            seal_op: None,
            layout_version: crate::registry::LAYOUT_VERSION,
        };
        // (expiry, deleted, soft-deleted, children) -> (live, retained, recreatable)
        let cases = [
            (None, false, false, 1, (true, false, false)),
            (Some(at + 1), false, false, 0, (true, false, false)),
            (Some(at), false, false, 0, (false, false, true)),
            (Some(at), false, false, 1, (false, true, false)),
            (Some(at - 1), false, false, 1, (false, true, false)),
            (None, false, true, 1, (false, true, false)),
            (None, true, false, 0, (false, false, true)),
        ];
        for (expiry, deleted, soft_deleted, children, want) in cases {
            let mut d = base.clone();
            d.expires_at_ms = expiry;
            d.deleted = deleted;
            d.soft_deleted = soft_deleted;
            d.fork_children = (0..children).map(|i| format!("child-{i}")).collect();
            let got = (
                alive_at(&d, at),
                retained_for_forks(&d, at),
                recreatable(&d, at),
            );
            assert_eq!(got, want, "{expiry:?} {deleted} {soft_deleted} {children}");
        }
    }
}

#[cfg(test)]
mod json_record_tests {
    use super::json_entries;
    use proptest::prelude::{Just, ProptestConfig, Strategy, any};
    use proptest::{collection::vec, prop_assert_eq, prop_oneof};

    /// Numeric fidelity corpus (FLOAT report §2.1): shortest, long, halfway,
    /// subnormal, exponent-form, big-integer and out-of-range literals.
    const CORPUS: &[&str] = &[
        "1.7802719962921167e-19",
        "-1.7802719962921167e-19",
        "0.1",
        "0.3",
        "0.30000000000000004",
        "0.8337463852537358",
        "3.141592653589793",
        "2.718281828459045",
        "1.0",
        "1",
        "1.50",
        "1e2",
        "1E+2",
        "100.0",
        "-0",
        "-0.0",
        "0e10",
        "-0e-5",
        "5e-324",
        "4.9406564584124654e-324",
        "2.4703282292062327e-324",
        "2.4703282292062328e-324",
        "1e-310",
        "3e-320",
        "4.35416759957975e-310",
        "2.225073858507201e-308",
        "2.2250738585072014e-308",
        "2.2250738585072011e-308",
        "1e-7",
        "1.2345678901234567e-19",
        "9.999999999999999e-20",
        "3.0000000000000004e-19",
        "1e16",
        "1e21",
        "1e22",
        "1e23",
        "9.999999999999999e22",
        "7.3177701707893310e+15",
        "8.98846567431158e307",
        "1e308",
        "1.7976931348623157e308",
        "1.7976931348623158e308",
        "1.7976931348623159e308",
        "1e309",
        "1e400",
        "9007199254740992",
        "9007199254740993",
        "9007199254740993.0",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775808",
        "-9223372036854775809",
        "123456789012345678901234567890",
        "0.1000000000000000055511151231257827021181583404541015625",
        "3.141592653589793238462643383279502884197",
        "1.00000000000000011102230246251565404236316680908203125",
        "1.00000000000000011102230246251565404236316680908203126",
        "123456789012345678901234567890.123",
        "1.23456789012345678901234567890e+30",
        "0.000000000000000000000000000001",
        "12345678901234567890e-10",
        "1234567.0",
    ];
    /// The corpus literals that round to infinity: refused, as today.
    const OVERFLOW: &[&str] = &["1.7976931348623159e308", "1e309", "1e400"];

    fn stored(body: &str) -> Result<Vec<String>, String> {
        let records = json_entries(body.as_bytes(), false)?;
        Ok(records
            .iter()
            .map(|record| String::from_utf8(record.to_vec()).unwrap())
            .collect())
    }

    #[test]
    fn json_records_store_the_validated_client_text() {
        for literal in CORPUS {
            let admitted = !OVERFLOW.contains(literal);
            let bodies = [
                (format!("[ {literal} ]"), (*literal).to_string()),
                (
                    format!("{{ \"v\" :\n\t{literal} }}"),
                    format!("{{\"v\":{literal}}}"),
                ),
            ];
            for (body, record) in bodies {
                let want = admitted
                    .then(|| vec![record])
                    .ok_or_else(|| "invalid JSON body".to_string());
                assert_eq!(stored(&body), want, "{body}");
            }
        }
        // serde_json's default parse refused 159 finite literals below
        // f64::MAX; the correctly rounded one admits exactly the finite ones.
        for i in 0..1000 {
            let literal = format!("1.797693134862315{i:03}e308");
            let finite = literal.parse::<f64>().unwrap().is_finite();
            assert_eq!(stored(&literal).is_ok(), finite, "{literal}");
        }
        let cases: &[(&str, &[&str])] = &[
            (r#"[ 1 , {"a" : 1} ,"x"]"#, &["1", r#"{"a":1}"#, r#""x""#]),
            (
                "{\n  \"b\" : 1,\n  \"a\" : 2,\r\n  \"a\" : 3\n}\n",
                &[r#"{"b":1,"a":2,"a":3}"#],
            ),
            (r#"[ "é\/" , 1.0 ]"#, &[r#""é\/""#, "1.0"]),
            (
                r#"[ {"k" : " a \" b \\" , "z":" x  y "} ]"#,
                &[r#"{"k":" a \" b \\","z":" x  y "}"#],
            ),
            (
                r#"[ "\\" , "\" " , [ 1 , 2 ] ]"#,
                &[r#""\\""#, r#""\" ""#, "[1,2]"],
            ),
            (r#" "\ud83d\ude00 \u00e9" "#, &[r#""\ud83d\ude00 \u00e9""#]),
            (" [[ ]] ", &["[]"]),
            (" null ", &["null"]),
        ];
        for (body, records) in cases {
            let want: Vec<String> = records.iter().map(|r| (*r).to_string()).collect();
            assert_eq!(stored(body), Ok(want), "{body}");
        }
        for refused in [
            r#"["\ud800"]"#,
            r#""\udc00 x""#,
            r#"{"\ud800":1}"#,
            "[1e400]",
            r#"{"a":[-1e400]}"#,
            "[1,]",
            "[",
            "",
            " ",
            r#"{"a" 1}"#,
            "[1] x",
            "[\"\t\"]",
        ] {
            assert!(stored(refused).is_err(), "{refused:?} admitted");
        }
        assert!(json_entries(b"[\"\xff\"]", false).is_err());
        assert_eq!(stored(" [ ] "), Err("empty JSON array".to_string()));
        assert_eq!(json_entries(b" [ ] ", true), Ok(Vec::new()));
    }

    /// Fails if `float_roundtrip` is dropped (the default parse moves this
    /// literal by one ulp) or if a dependency unifies `arbitrary_precision`
    /// in (it keeps `1.50` verbatim, and breaks the billing envelope's
    /// flatten decode).
    #[test]
    fn serde_json_parses_correctly_rounded_and_without_arbitrary_precision() {
        let literal = "1.7802719962921167e-19";
        let parsed: f64 = serde_json::from_str(literal).unwrap();
        assert_eq!(parsed.to_bits(), literal.parse::<f64>().unwrap().to_bits());
        let value: serde_json::Value = serde_json::from_str("1.50").unwrap();
        assert_eq!(value.to_string(), "1.5");
    }

    /// One generated JSON value: the client's text, with random whitespace
    /// between its tokens, the text it must be stored as, and its numbers.
    #[derive(Clone, Debug)]
    struct Doc {
        client: String,
        stored: String,
        numbers: Vec<String>,
    }

    fn whitespace() -> impl Strategy<Value = String> {
        vec(
            prop_oneof![Just(' '), Just('\t'), Just('\n'), Just('\r')],
            0..3,
        )
        .prop_map(String::from_iter)
    }

    fn number() -> impl Strategy<Value = String> {
        let finite = any::<u64>()
            .prop_map(f64::from_bits)
            .prop_filter("finite", |f| f.is_finite());
        prop_oneof![
            finite.clone().prop_map(|f| format!("{f:e}")),
            finite.prop_map(|f| format!("{f:.16e}")),
            (1u64..1 << 52).prop_map(|bits| format!("{:e}", f64::from_bits(bits))),
            (1u8..10, vec(0u8..10, 19..30), any::<bool>()).prop_map(|(lead, rest, negative)| {
                let digits: String = rest.iter().map(|d| char::from(b'0' + d)).collect();
                format!("{}{lead}{digits}", if negative { "-" } else { "" })
            }),
            any::<i64>().prop_map(|i| i.to_string()),
            (0u32..100_000, 0u32..100).prop_map(|(units, cents)| format!("{units}.{cents:02}")),
        ]
    }

    fn string() -> impl Strategy<Value = String> {
        let part = prop_oneof![
            Just(" "),
            Just("\\\""),
            Just("\\\\"),
            Just("\\n"),
            Just("\\u00e9"),
            Just("é"),
            Just("x"),
            Just("\\/"),
        ];
        vec(part, 0..6).prop_map(|parts| format!("\"{}\"", parts.concat()))
    }

    fn document() -> impl Strategy<Value = Doc> {
        let leaf = prop_oneof![
            number().prop_map(|n| Doc {
                client: n.clone(),
                stored: n.clone(),
                numbers: vec![n],
            }),
            prop_oneof![string(), Just("true".into()), Just("null".into())].prop_map(|s| Doc {
                client: s.clone(),
                stored: s,
                numbers: Vec::new(),
            }),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            let members = vec((whitespace(), string(), whitespace(), inner.clone()), 0..4);
            prop_oneof![
                (vec((whitespace(), inner), 0..4), whitespace()).prop_map(|(items, end)| {
                    let client: Vec<String> = items
                        .iter()
                        .map(|(ws, d)| format!("{ws}{}{ws}", d.client))
                        .collect();
                    let stored: Vec<&str> = items.iter().map(|(_, d)| d.stored.as_str()).collect();
                    Doc {
                        client: format!("[{end}{}]", client.join(",")),
                        stored: format!("[{}]", stored.join(",")),
                        numbers: items.into_iter().flat_map(|(_, d)| d.numbers).collect(),
                    }
                }),
                (members, whitespace()).prop_map(|(members, end)| {
                    let client: Vec<String> = members
                        .iter()
                        .map(|(a, key, b, d)| format!("{a}{key}{b}:{a}{}{b}", d.client))
                        .collect();
                    let stored: Vec<String> = members
                        .iter()
                        .map(|(_, key, _, d)| format!("{key}:{}", d.stored))
                        .collect();
                    Doc {
                        client: format!("{{{end}{}}}", client.join(",")),
                        stored: format!("{{{}}}", stored.join(",")),
                        numbers: members.into_iter().flat_map(|(.., d)| d.numbers).collect(),
                    }
                }),
            ]
        })
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig { cases: 1024, ..ProptestConfig::default() })]

        /// Every record is stored as its client text without whitespace;
        /// storing the stored records again reproduces them byte for byte;
        /// and the server's parse of every number is the correctly rounded
        /// f64 of the client's literal.
        #[test]
        fn stored_json_records_are_the_minified_client_text(
            elements in vec((whitespace(), document(), whitespace()), 1..4),
        ) {
            let client: Vec<String> =
                elements.iter().map(|(a, d, b)| format!("{a}{}{b}", d.client)).collect();
            let want: Vec<String> = elements.iter().map(|(_, d, _)| d.stored.clone()).collect();
            let records = stored(&format!("[{}]", client.join(","))).unwrap();
            prop_assert_eq!(&records, &want);
            prop_assert_eq!(stored(&format!("[{}]", records.join(","))).unwrap(), records);
            for literal in elements.iter().flat_map(|(_, d, _)| &d.numbers) {
                let server: f64 = serde_json::from_str(literal).unwrap();
                let correct: f64 = literal.parse().unwrap();
                prop_assert_eq!(server.to_bits(), correct.to_bits(), "{}", literal);
            }
        }
    }
}
