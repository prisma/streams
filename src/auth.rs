//! Request authentication and tenant-scoped authorization.
//!
//! Production mode verifies short-lived asymmetric JWTs locally from a
//! background-refreshed JWKS. The key service is never called on the request
//! path. A stale key set fails closed after `max_stale`, matching the GA
//! contract in OPERATIONS.md §3.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use futures_util::StreamExt;
use jsonwebtoken::jwk::{JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_JWKS_BYTES: usize = 1024 * 1024;
const MAX_REVOCATION_BYTES: usize = 1024 * 1024;
const MAX_REVOKED_TOKEN_IDS: usize = 100_000;
const MAX_TOKEN_LIFETIME_SECS: u64 = 24 * 60 * 60;
const CLOCK_SKEW_SECS: u64 = 30;
const MAX_PREFIXES: usize = 32;
const MAX_PREFIX_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Verb {
    Create,
    Append,
    Read,
    Delete,
    Queue,
    Touch,
    List,
}

impl Verb {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "create" => Some(Self::Create),
            "append" => Some(Self::Append),
            "read" => Some(Self::Read),
            "delete" => Some(Self::Delete),
            "queue" => Some(Self::Queue),
            "touch" => Some(Self::Touch),
            "list" => Some(Self::List),
            _ => None,
        }
    }

    fn all() -> HashSet<Self> {
        [
            Self::Create,
            Self::Append,
            Self::Read,
            Self::Delete,
            Self::Queue,
            Self::Touch,
            Self::List,
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub customer_id: String,
    pub token_id: String,
    verbs: HashSet<Verb>,
    prefixes: Vec<String>,
    pub operator: bool,
}

impl Principal {
    pub fn allows(&self, verb: Verb, stream_name: &str) -> bool {
        self.allows_verb(verb) && self.allows_name(stream_name)
    }

    pub fn allows_verb(&self, verb: Verb) -> bool {
        self.verbs.contains(&verb)
    }

    pub fn allows_name(&self, stream_name: &str) -> bool {
        self.prefixes
            .iter()
            .any(|prefix| stream_name.starts_with(prefix))
    }

    fn unrestricted(customer_id: &str, token_id: &str, operator: bool) -> Self {
        Self {
            customer_id: customer_id.to_string(),
            token_id: token_id.to_string(),
            verbs: Verb::all(),
            prefixes: vec![String::new()],
            operator,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AuthError {
    Missing,
    Invalid,
    Unavailable,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(f, "bearer token required"),
            Self::Invalid => write!(f, "invalid bearer token"),
            Self::Unavailable => write!(f, "authentication keys are stale"),
        }
    }
}

#[derive(Clone)]
pub enum Authenticator {
    /// Local development only. Requests become one unrestricted development
    /// tenant so the rest of the serving path still exercises tenant scope.
    Disabled,
    /// Pilot compatibility. This is deliberately one tenant and is not a GA
    /// mode; production must use JWKS.
    Legacy {
        token: Arc<String>,
    },
    Jwks(Arc<JwksAuthenticator>),
}

impl Authenticator {
    pub fn legacy(token: String) -> Self {
        Self::Legacy {
            token: Arc::new(token),
        }
    }

    pub async fn jwks(config: JwksConfig) -> anyhow::Result<Self> {
        let auth = Arc::new(JwksAuthenticator::new(config).await?);
        JwksAuthenticator::start_refresh(auth.clone());
        Ok(Self::Jwks(auth))
    }

    pub fn authenticate(&self, authorization: Option<&str>) -> Result<Principal, AuthError> {
        match self {
            Self::Disabled => Ok(Principal::unrestricted(
                "__development__",
                "development",
                true,
            )),
            Self::Legacy { token: expected } => {
                let supplied = bearer(authorization)?;
                if bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
                    Ok(Principal::unrestricted("__legacy__", "legacy", true))
                } else {
                    Err(AuthError::Invalid)
                }
            }
            Self::Jwks(auth) => auth.authenticate(bearer(authorization)?),
        }
    }

    pub fn production_ready(&self) -> bool {
        matches!(self, Self::Jwks(_))
    }

    pub fn ready(&self) -> bool {
        match self {
            Self::Disabled | Self::Legacy { .. } => true,
            Self::Jwks(auth) => {
                let state = auth.state.read().unwrap();
                state.last_success.elapsed() <= auth.config.max_stale
                    && state.last_revocation_success.elapsed() <= auth.config.revocation_max_stale
            }
        }
    }
}

fn bearer(raw: Option<&str>) -> Result<&str, AuthError> {
    raw.and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
        .ok_or(AuthError::Missing)
}

pub fn constant_time_bearer_matches(authorization: Option<&str>, expected: &str) -> bool {
    bearer(authorization)
        .map(|supplied| bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())))
        .unwrap_or(false)
}

#[derive(Clone)]
pub struct JwksConfig {
    pub url: String,
    pub revocation_url: String,
    pub issuer: String,
    pub audience: String,
    pub refresh_interval: Duration,
    pub max_stale: Duration,
    pub revocation_refresh_interval: Duration,
    pub revocation_max_stale: Duration,
}

struct VerificationKey {
    algorithm: Algorithm,
    key: DecodingKey,
}

struct KeyState {
    keys: HashMap<String, VerificationKey>,
    last_success: Instant,
    revoked_token_ids: HashSet<String>,
    revocation_version: u64,
    last_revocation_success: Instant,
}

pub struct JwksAuthenticator {
    config: JwksConfig,
    client: reqwest::Client,
    state: RwLock<KeyState>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Claims {
    sub: String,
    exp: u64,
    iat: u64,
    #[allow(dead_code)]
    iss: String,
    #[allow(dead_code)]
    aud: serde_json::Value,
    jti: String,
    #[serde(default)]
    stream_prefixes: Vec<String>,
    #[serde(default)]
    verbs: Vec<String>,
}

#[derive(Deserialize)]
struct RevocationDocument {
    version: u64,
    revoked_token_ids: Vec<String>,
}

impl JwksAuthenticator {
    async fn new(config: JwksConfig) -> anyhow::Result<Self> {
        if config.url.is_empty()
            || config.revocation_url.is_empty()
            || config.issuer.is_empty()
            || config.audience.is_empty()
        {
            anyhow::bail!("JWKS URL, revocation URL, issuer, and audience are all required");
        }
        if config.refresh_interval.is_zero() || config.max_stale < config.refresh_interval {
            anyhow::bail!("JWKS max-stale must be at least one non-zero refresh interval");
        }
        if config.revocation_refresh_interval.is_zero()
            || config.revocation_max_stale < config.revocation_refresh_interval
        {
            anyhow::bail!("revocation max-stale must be at least one non-zero refresh interval");
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(4))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let keys = fetch_jwks(&client, &config.url).await?;
        let revocations = fetch_revocations(&client, &config.revocation_url).await?;
        Ok(Self {
            config,
            client,
            state: RwLock::new(KeyState {
                keys,
                last_success: Instant::now(),
                revoked_token_ids: revocations.revoked_token_ids.into_iter().collect(),
                revocation_version: revocations.version,
                last_revocation_success: Instant::now(),
            }),
        })
    }

    fn start_refresh(auth: Arc<Self>) {
        let jwks_auth = auth.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(jwks_auth.config.refresh_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                match fetch_jwks(&jwks_auth.client, &jwks_auth.config.url).await {
                    Ok(keys) => {
                        let mut state = jwks_auth.state.write().unwrap();
                        state.keys = keys;
                        state.last_success = Instant::now();
                    }
                    Err(error) => {
                        tracing::error!("JWKS refresh failed: {error:#}");
                    }
                }
            }
        });
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(auth.config.revocation_refresh_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                match fetch_revocations(&auth.client, &auth.config.revocation_url).await {
                    Ok(document) => {
                        let mut state = auth.state.write().unwrap();
                        if document.version < state.revocation_version {
                            tracing::error!(
                                current = state.revocation_version,
                                received = document.version,
                                "revocation document rollback rejected"
                            );
                            continue;
                        }
                        state.revoked_token_ids = document.revoked_token_ids.into_iter().collect();
                        state.revocation_version = document.version;
                        state.last_revocation_success = Instant::now();
                    }
                    Err(error) => {
                        tracing::error!("revocation refresh failed: {error:#}");
                    }
                }
            }
        });
    }

    fn authenticate(&self, token: &str) -> Result<Principal, AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::Invalid);
        }
        let header = decode_header(token).map_err(|_| AuthError::Invalid)?;
        let kid = header
            .kid
            .as_deref()
            .filter(|kid| !kid.is_empty() && kid.len() <= 128)
            .ok_or(AuthError::Invalid)?;
        let state = self.state.read().unwrap();
        if state.last_success.elapsed() > self.config.max_stale
            || state.last_revocation_success.elapsed() > self.config.revocation_max_stale
        {
            return Err(AuthError::Unavailable);
        }
        let verification = state.keys.get(kid).ok_or(AuthError::Invalid)?;
        if header.alg != verification.algorithm {
            return Err(AuthError::Invalid);
        }
        let mut validation = Validation::new(verification.algorithm);
        validation.leeway = CLOCK_SKEW_SECS;
        validation.validate_nbf = true;
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<Claims>(token, &verification.key, &validation)
            .map_err(|_| AuthError::Invalid)?
            .claims;
        let principal = principal_from_claims(claims)?;
        if state.revoked_token_ids.contains(&principal.token_id) {
            return Err(AuthError::Invalid);
        }
        Ok(principal)
    }
}

fn principal_from_claims(claims: Claims) -> Result<Principal, AuthError> {
    let now = jsonwebtoken::get_current_timestamp();
    if claims.iat > now.saturating_add(CLOCK_SKEW_SECS)
        || claims.exp <= claims.iat
        || claims.exp - claims.iat > MAX_TOKEN_LIFETIME_SECS
        || !valid_customer_id(&claims.sub)
        || claims.jti.is_empty()
        || claims.jti.len() > 256
        || claims.stream_prefixes.is_empty()
        || claims.stream_prefixes.len() > MAX_PREFIXES
        || claims
            .stream_prefixes
            .iter()
            .any(|prefix| prefix.len() > MAX_PREFIX_BYTES)
    {
        return Err(AuthError::Invalid);
    }
    let mut verbs = HashSet::new();
    for raw in claims.verbs {
        let verb = Verb::parse(&raw).ok_or(AuthError::Invalid)?;
        verbs.insert(verb);
    }
    if verbs.is_empty() {
        return Err(AuthError::Invalid);
    }
    Ok(Principal {
        customer_id: claims.sub,
        token_id: claims.jti,
        verbs,
        prefixes: claims.stream_prefixes,
        operator: false,
    })
}

fn valid_customer_id(customer: &str) -> bool {
    !customer.is_empty()
        && customer.len() <= 128
        && customer
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

async fn fetch_jwks(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<HashMap<String, VerificationKey>> {
    let body = fetch_limited(client, url, MAX_JWKS_BYTES, "JWKS").await?;
    let set: JwkSet = serde_json::from_slice(&body)?;
    let mut keys = HashMap::new();
    for jwk in &set.keys {
        if jwk
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|usage| usage != &PublicKeyUse::Signature)
            || jwk
                .common
                .key_operations
                .as_ref()
                .is_some_and(|operations| {
                    !operations
                        .iter()
                        .any(|operation| operation == &KeyOperations::Verify)
                })
        {
            continue;
        }
        let Some(kid) = jwk
            .common
            .key_id
            .as_ref()
            .filter(|kid| !kid.is_empty() && kid.len() <= 128)
        else {
            continue;
        };
        let algorithm = match jwk.common.key_algorithm {
            Some(KeyAlgorithm::RS256) => Algorithm::RS256,
            Some(KeyAlgorithm::EdDSA) => Algorithm::EdDSA,
            _ => continue,
        };
        let key = DecodingKey::from_jwk(jwk)?;
        if keys
            .insert(kid.clone(), VerificationKey { algorithm, key })
            .is_some()
        {
            anyhow::bail!("JWKS contains duplicate kid {kid}");
        }
    }
    if keys.is_empty() {
        anyhow::bail!("JWKS contains no supported RS256 or EdDSA verification keys");
    }
    Ok(keys)
}

async fn fetch_limited(
    client: &reqwest::Client,
    url: &str,
    limit: usize,
    label: &str,
) -> anyhow::Result<BytesMut> {
    let response = client.get(url).send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        anyhow::bail!("{label} exceeds {limit} bytes");
    }
    let mut body = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > limit {
            anyhow::bail!("{label} exceeds {limit} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn fetch_revocations(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<RevocationDocument> {
    let body = fetch_limited(client, url, MAX_REVOCATION_BYTES, "revocation document").await?;
    let document: RevocationDocument = serde_json::from_slice(&body)?;
    if document.revoked_token_ids.len() > MAX_REVOKED_TOKEN_IDS {
        anyhow::bail!("revocation document has too many token IDs");
    }
    if document
        .revoked_token_ids
        .iter()
        .any(|token_id| token_id.is_empty() || token_id.len() > 256)
    {
        anyhow::bail!("revocation document contains an invalid token ID");
    }
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn claims() -> Claims {
        let now = jsonwebtoken::get_current_timestamp();
        Claims {
            sub: "customer-42".into(),
            exp: now + 300,
            iat: now,
            iss: "issuer".into(),
            aud: serde_json::json!("streams"),
            jti: "token-1".into(),
            stream_prefixes: vec!["prod/".into()],
            verbs: vec!["read".into(), "append".into()],
        }
    }

    #[test]
    fn principal_enforces_verbs_and_prefixes() {
        let principal = principal_from_claims(claims()).unwrap();
        assert!(principal.allows(Verb::Read, "prod/orders"));
        assert!(principal.allows(Verb::Append, "prod/orders"));
        assert!(!principal.allows(Verb::Delete, "prod/orders"));
        assert!(!principal.allows(Verb::Read, "staging/orders"));
    }

    #[test]
    fn tokens_are_short_lived_and_claims_are_bounded() {
        let mut value = claims();
        value.exp = value.iat + MAX_TOKEN_LIFETIME_SECS + 1;
        assert_eq!(
            principal_from_claims(value).unwrap_err(),
            AuthError::Invalid
        );

        let mut value = claims();
        value.stream_prefixes = vec!["x".into(); MAX_PREFIXES + 1];
        assert_eq!(
            principal_from_claims(value).unwrap_err(),
            AuthError::Invalid
        );
    }

    #[test]
    fn legacy_token_check_and_missing_token_fail_closed() {
        let auth = Authenticator::legacy("secret".into());
        assert!(auth.authenticate(Some("Bearer secret")).is_ok());
        assert_eq!(
            auth.authenticate(Some("Bearer wrong")).unwrap_err(),
            AuthError::Invalid
        );
        assert_eq!(auth.authenticate(None).unwrap_err(), AuthError::Missing);
    }

    #[test]
    fn signed_jwt_verifies_and_algorithm_confusion_fails() {
        let signing = SigningKey::from_bytes(&[11u8; 32]);
        let private_der = signing.to_pkcs8_der().unwrap();
        let public = signing.verifying_key().to_bytes();
        let mut keys = HashMap::new();
        keys.insert(
            "test-key".to_string(),
            VerificationKey {
                algorithm: Algorithm::EdDSA,
                key: DecodingKey::from_ed_der(&public),
            },
        );
        let auth = JwksAuthenticator {
            config: JwksConfig {
                url: "https://keys.invalid/jwks".into(),
                revocation_url: "https://keys.invalid/revocations".into(),
                issuer: "issuer".into(),
                audience: "streams".into(),
                refresh_interval: Duration::from_secs(600),
                max_stale: Duration::from_secs(3600),
                revocation_refresh_interval: Duration::from_secs(60),
                revocation_max_stale: Duration::from_secs(120),
            },
            client: reqwest::Client::new(),
            state: RwLock::new(KeyState {
                keys,
                last_success: Instant::now(),
                revoked_token_ids: HashSet::new(),
                revocation_version: 1,
                last_revocation_success: Instant::now(),
            }),
        };

        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("test-key".into());
        let token = encode(
            &header,
            &claims(),
            &EncodingKey::from_ed_der(private_der.as_bytes()),
        )
        .unwrap();
        let principal = auth.authenticate(&token).unwrap();
        assert_eq!(principal.customer_id, "customer-42");

        auth.state
            .write()
            .unwrap()
            .revoked_token_ids
            .insert("token-1".to_string());
        assert_eq!(auth.authenticate(&token).unwrap_err(), AuthError::Invalid);
        auth.state.write().unwrap().revoked_token_ids.clear();

        // A token that advertises HMAC and uses public key bytes as its HMAC
        // secret is rejected before verification (classic JWT alg confusion).
        let mut hmac_header = Header::new(Algorithm::HS256);
        hmac_header.kid = Some("test-key".into());
        let confused = encode(&hmac_header, &claims(), &EncodingKey::from_secret(&public)).unwrap();
        assert_eq!(
            auth.authenticate(&confused).unwrap_err(),
            AuthError::Invalid
        );
    }
}
