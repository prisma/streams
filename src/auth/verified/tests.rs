//! A customer token's signature is checked once per key set, and every other
//! check still runs on every request.
#![cfg(test)]

use super::super::tests::{FLEET_KID, FLEET_PUB, KID, NOW, claims, service, sign_with};
use super::super::{
    AuthError, AuthService, JwksKey, JwksSnapshot, KeyAudience, MAX_TOKEN_BYTES, RawClaims,
};
use super::{MAX_KEPT_TOKEN_BYTES, VerifiedTokens};
use jsonwebtoken::Algorithm;
use std::collections::HashMap;
use std::sync::Arc;

const PUB: &str = include_str!("../../dst/fixtures/mt-test-rsa.pub.pem");

fn token() -> String {
    sign_with(&claims(), KID, Algorithm::RS256)
}

/// Publish the test key set again at `version`, with the customer key or
/// without it (retiring it).
fn republish(svc: &AuthService, version: u64, customer: bool) {
    let mut keys = HashMap::from([(
        FLEET_KID.to_string(),
        JwksKey::rs256(FLEET_PUB, KeyAudience::Internal),
    )]);
    if customer {
        keys.insert(KID.to_string(), JwksKey::rs256(PUB, KeyAudience::Customer));
    }
    svc.publish_jwks(JwksSnapshot {
        keys,
        fetched_at_unix: NOW,
        feed_version: version,
    })
    .unwrap();
}

/// The same token verifies three times on one signature check, and once
/// more after another key set is published.
#[test]
fn a_tokens_signature_is_checked_once_per_key_set() {
    let svc = service();
    let token = token();
    for _ in 0..3 {
        assert!(svc.verify_customer(&token, NOW).is_ok());
    }
    assert_eq!(svc.verified.checks(), 1);
    republish(&svc, 2, true);
    assert!(svc.verify_customer(&token, NOW).is_ok());
    assert_eq!(svc.verified.checks(), 2);
}

/// A kept token is refused once its key is retired, once it has expired,
/// and while the key set is stale: the cache keeps a signature's verdict,
/// never a principal.
#[test]
fn a_kept_token_is_refused_when_its_key_retires_or_it_expires() {
    let svc = service();
    let token = token();
    assert!(svc.verify_customer(&token, NOW).is_ok());
    let later = NOW + 3_600;
    assert!(matches!(
        svc.verify_customer(&token, later),
        Err(AuthError::Expired)
    ));
    let stale = NOW + 86_400 * 30;
    assert!(matches!(
        svc.verify_customer(&token, stale),
        Err(AuthError::KeysStale)
    ));
    republish(&svc, 2, false);
    assert!(matches!(
        svc.verify_customer(&token, NOW),
        Err(AuthError::KidUnknown)
    ));
}

/// A token whose signature fails is checked every time: no failure is kept.
#[test]
fn a_failed_signature_is_never_kept() {
    let svc = service();
    let mut forged = token();
    forged.push('A');
    for _ in 0..2 {
        assert!(svc.verify_customer(&forged, NOW).is_err());
    }
    assert_eq!(svc.verified.checks(), 2);
}

/// While the key set is stale a token that could never verify keeps its
/// own refusal (a malformed token is a 401, not a retryable stale feed), and
/// a kept token is refused as stale: the cache moves no refusal ahead of
/// the checks `verify_signature` makes in its own order.
#[test]
fn a_stale_key_set_keeps_each_refusal_in_its_place() {
    let svc = service();
    let token = token();
    assert!(svc.verify_customer(&token, NOW).is_ok());
    let stale = NOW + 86_400 * 30;
    let malformed = svc.verify_customer("not.a.token", stale);
    assert!(
        matches!(malformed, Err(AuthError::Malformed(_))),
        "{:?}",
        malformed.err()
    );
    assert!(matches!(
        svc.verify_customer(&token, stale),
        Err(AuthError::KeysStale)
    ));
}

/// Claims to keep (their content is not looked at here).
fn kept() -> Arc<RawClaims> {
    let token = token();
    let (claims, _) = service()
        .verify_signature_under::<RawClaims>(&token, NOW, KeyAudience::Customer)
        .unwrap();
    Arc::new(claims)
}

/// A token of up to 2 KiB is kept and found again under its key set, and
/// two tokens are kept side by side; a longer one is neither kept nor
/// looked up, and another key set finds nothing.
#[test]
fn tokens_up_to_the_kept_length_are_kept_side_by_side_under_their_key_set() {
    let cache = VerifiedTokens::default();
    let (keys, other) = (
        Arc::new(JwksSnapshot::empty()),
        Arc::new(JwksSnapshot::empty()),
    );
    let (longest, longer) = (
        "t".repeat(MAX_KEPT_TOKEN_BYTES),
        "t".repeat(MAX_KEPT_TOKEN_BYTES + 1),
    );
    cache.put(&keys, &longest, &kept());
    cache.put(&keys, "short", &kept());
    assert!(cache.get(&keys, &longest).is_some() && cache.get(&keys, "short").is_some());
    assert!(
        cache.get(&other, "short").is_none(),
        "another key set finds nothing"
    );
    cache.put(&keys, &longer, &kept());
    assert_eq!(
        cache.held.lock().unwrap().claims.len(),
        2,
        "the longer token is not kept"
    );
    cache
        .held
        .lock()
        .unwrap()
        .claims
        .insert(Box::from(longer.as_str()), kept());
    assert!(
        cache.get(&keys, &longer).is_none(),
        "a longer token is not looked up"
    );
}

/// A token of exactly the largest accepted size reaches the header check;
/// one byte more is refused for its size.
#[test]
fn the_largest_accepted_token_reaches_the_header_check() {
    let svc = service();
    let at = svc.verify_customer(&"x".repeat(MAX_TOKEN_BYTES), NOW);
    assert!(matches!(at, Err(AuthError::Malformed(_))), "{:?}", at.err());
    let over = svc.verify_customer(&"x".repeat(MAX_TOKEN_BYTES + 1), NOW);
    assert!(matches!(over, Err(AuthError::TokenTooLarge)));
}
