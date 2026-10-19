//! The customer tokens whose signatures this instance has verified (mem2
//! lane, Layer B's hostile phase). Every request verified its token's RS256
//! or EdDSA signature, refused requests included: 41% of the server's CPU
//! per refusal natively (a refusal flood of 42,800 requests a second on four
//! workers, sampled), more under emulation, so one tenant flooding at its
//! rate limit took the instance's CPU, and with it its neighbours' latency
//! and the memory their slowed requests hold.
//!
//! A signature check is a pure function of the token's bytes and the key
//! set it is checked under, so its verdict is kept for exactly that pair:
//! the claims a token carried when its signature verified, for as long as
//! the published key set is the same snapshot. Everything that depends on
//! the clock or on the policy and grant feeds (issuer, audience, expiry,
//! the project's placement and status, the credential's grant version)
//! still runs on every request, on the kept claims; a kept verdict is
//! refused while its key set is stale, and a miss runs `verify_signature`
//! unchanged, in its own order of refusals. A failed verification is
//! never kept. At most `MAX_TOKENS` tokens of at most `MAX_KEPT_TOKEN_BYTES`
//! bytes each are kept (about 5 MiB at the full count for typical tokens, 15-20
//! MiB at most, claim strings included; only tokens the issuer signed get in); the set is emptied
//! when it is full and whenever another key set is in force.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jsonwebtoken::{Validation, decode, decode_header};

use super::{
    ALLOWED_ALGS, AuthError, AuthService, JWKS_STALENESS_MAX_SECS, JwksSnapshot, KeyAudience,
    MAX_TOKEN_BYTES, RawClaims,
};

/// Tokens kept at most.
const MAX_TOKENS: usize = 4_096;
/// A longer token is verified every time and never kept.
const MAX_KEPT_TOKEN_BYTES: usize = 2_048;

/// The verified customer tokens, under the key set they verified with.
#[derive(Default)]
pub(super) struct VerifiedTokens {
    held: Mutex<Held>,
    /// Signature checks made for customer tokens (cache misses).
    checks: AtomicU64,
}

#[derive(Default)]
struct Held {
    /// The key set every kept verdict was reached under.
    keys: Option<Arc<JwksSnapshot>>,
    // mt-lint: allow(name-keyed-map): keyed by a whole bearer token, not stream identity
    claims: HashMap<Box<str>, Arc<RawClaims>>,
}

impl VerifiedTokens {
    /// The claims `token` carried when its signature verified under `keys`.
    fn get(&self, keys: &Arc<JwksSnapshot>, token: &str) -> Option<Arc<RawClaims>> {
        if token.len() > MAX_KEPT_TOKEN_BYTES {
            return None;
        }
        let held = self.held.lock().ok()?;
        let same = held.keys.as_ref().is_some_and(|k| Arc::ptr_eq(k, keys));
        same.then(|| held.claims.get(token).cloned()).flatten()
    }

    /// Keep `claims`, verified for `token` under `keys`: a key set other
    /// than the kept one, or a full set, empties it first.
    fn put(&self, keys: &Arc<JwksSnapshot>, token: &str, claims: &Arc<RawClaims>) {
        if token.len() > MAX_KEPT_TOKEN_BYTES {
            return;
        }
        let Ok(mut held) = self.held.lock() else {
            return;
        };
        let same = held.keys.as_ref().is_some_and(|k| Arc::ptr_eq(k, keys));
        if !same || held.claims.len() >= MAX_TOKENS {
            held.claims = HashMap::new();
            held.keys = Some(keys.clone());
        }
        held.claims.insert(Box::from(token), claims.clone());
    }

    /// Signature checks made for customer tokens.
    #[cfg(test)]
    pub(super) fn checks(&self) -> u64 {
        self.checks.load(Ordering::Relaxed)
    }
}

impl AuthService {
    /// A customer token's verified claims: kept from an earlier check under
    /// the key set in force (which must not be stale), or checked now by
    /// `verify_signature`, in its own order of refusals, and kept under the
    /// key set it was checked with.
    pub(super) fn verified_customer_claims(
        &self,
        token: &str,
        now: i64,
    ) -> Result<Arc<RawClaims>, AuthError> {
        let keys = self.jwks.load_full();
        if let Some(claims) = self.verified.get(&keys, token) {
            let window = JWKS_STALENESS_MAX_SECS;
            self.refuse_stale(keys.fetched_at_unix, window, now, AuthError::KeysStale)?;
            return Ok(claims);
        }
        self.verified.checks.fetch_add(1, Ordering::Relaxed);
        let (claims, keys) =
            self.verify_signature_under::<RawClaims>(token, now, KeyAudience::Customer)?;
        let claims = Arc::new(claims);
        self.verified.put(&keys, token, &claims);
        Ok(claims)
    }

    /// Signature + structural verification shared by all audiences
    /// (`verify_signature_under`), the claims alone.
    pub(super) fn verify_signature<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
        now: i64,
        audience: KeyAudience,
    ) -> Result<T, AuthError> {
        self.verify_signature_under(token, now, audience)
            .map(|(claims, _)| claims)
    }

    /// Signature + structural verification shared by all audiences, under
    /// a kid the feed pinned to `audience`. Returns the
    /// still-untrusted-but-authentic claim JSON and the key set it verified
    /// under, which the cache files the verdict under.
    fn verify_signature_under<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
        now: i64,
        audience: KeyAudience,
    ) -> Result<(T, Arc<JwksSnapshot>), AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::TokenTooLarge);
        }
        let header = decode_header(token).map_err(|_| AuthError::Malformed("header"))?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::AlgNotAllowed);
        }
        let kid = header.kid.ok_or(AuthError::KidMissing)?;
        // Bounded key-set staleness (review item 6): fail closed like
        // policies and grants — retryable, not a credential error.
        let jwks = self.fresh_jwks(now)?;
        let entry = match jwks.keys.get(&kid) {
            Some(k) => k,
            None => {
                self.unknown_kid_seen.fetch_add(1, Ordering::Relaxed);
                self.request_kid_refresh();
                return Err(AuthError::KidUnknown);
            }
        };
        // The kid pins one alg (review item 6) and one audience (H6).
        let key = entry.pinned(header.alg, audience)?;
        // jsonwebtoken verifies the signature with EXACTLY the header
        // alg (already allowlisted). Time and audience checks are done
        // by us against the injected clock, so tests are deterministic
        // and error kinds are precise.
        let mut v = Validation::new(header.alg);
        v.validate_exp = false;
        v.validate_nbf = false;
        v.validate_aud = false;
        v.required_spec_claims.clear();
        let data = decode::<T>(token, key, &v).map_err(|_| AuthError::BadSignature)?;
        Ok((data.claims, Arc::clone(&jwks)))
    }
}

#[cfg(test)]
mod tests;
