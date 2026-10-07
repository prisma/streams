//! Signing keys, and the one audience each may sign for (§5, §14.1;
//! shared-cells H6).
//!
//! One keys feed carries both the customer issuer's keys and the fleet's
//! workload keys, and the feed pins every kid to ONE audience (`aud`). A
//! token verifies only under a kid pinned to the audience its verifier
//! serves. So the key that mints customer tokens cannot mint a workload
//! token for the internal surface, which closes, seal-fences, sweeps and
//! reads the segments of any project on the cell; and a fleet key cannot
//! mint a customer token for any project. A kid names its algorithm, its
//! key material and its audience forever: the fingerprint covers material
//! and audience, so a feed that re-pins a kid is refused exactly like one
//! that rebinds its material (`publish_jwks`, SR3-3).

use jsonwebtoken::{Algorithm, DecodingKey};

use super::{AUD_CUSTOMER, AUD_INTERNAL, AuthError};

/// The one audience a signing key may sign for: the keys feed's `aud`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyAudience {
    /// `prisma-streams-data`: customer access tokens.
    Customer,
    /// `prisma-streams-internal`: fleet workload tokens.
    Internal,
}

impl KeyAudience {
    /// The audience a keys feed names, when it is one this cell verifies.
    pub(crate) fn parse(aud: &str) -> Option<Self> {
        match aud {
            AUD_CUSTOMER => Some(Self::Customer),
            AUD_INTERNAL => Some(Self::Internal),
            _ => None,
        }
    }

    /// The token `aud` claim a key with this audience signs.
    pub(crate) fn claim(self) -> &'static str {
        match self {
            Self::Customer => AUD_CUSTOMER,
            Self::Internal => AUD_INTERNAL,
        }
    }
}

/// One verification key with its PINNED algorithm (review item 6: the
/// allowlist alone still permitted verifying an RSA key under any
/// allowlisted algorithm the header claimed) and its PINNED audience
/// (shared-cells H6). Built through `new`, so the fingerprint always
/// covers the audience.
pub(crate) struct JwksKey {
    pub alg: Algorithm,
    pub key: DecodingKey,
    pub aud: KeyAudience,
    /// SR3-3, H6: the canonical fingerprint of what the kid names besides
    /// its algorithm: its audience and its key MATERIAL (the PEM bytes),
    /// computed at parse. Same kid + different fp is a publisher defect,
    /// and the snapshot is refused.
    pub fp: [u8; 32],
}

impl JwksKey {
    pub(crate) fn new(alg: Algorithm, key: DecodingKey, pem: &[u8], aud: KeyAudience) -> Self {
        Self {
            alg,
            key,
            aud,
            fp: key_fp(pem, aud),
        }
    }

    /// The key material a token signed under this kid verifies against:
    /// only for the header alg the kid pins, and only for a verifier
    /// serving the audience the kid pins. Both run before the signature
    /// is checked.
    pub(super) fn pinned(
        &self,
        alg: Algorithm,
        audience: KeyAudience,
    ) -> Result<&DecodingKey, AuthError> {
        if alg != self.alg {
            return Err(AuthError::AlgNotAllowed);
        }
        if audience != self.aud {
            return Err(AuthError::WrongAudience);
        }
        Ok(&self.key)
    }

    /// TEST HOOK: an RS256 key from a PEM fixture, pinned to `aud`.
    #[cfg(test)]
    pub(crate) fn rs256(pem: &str, aud: KeyAudience) -> Self {
        let key = DecodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
        Self::new(Algorithm::RS256, key, pem.as_bytes(), aud)
    }
}

/// SR3-3, H6: the fingerprint stored per kid, over its audience and its
/// key material (the audience is one of two fixed claims, never empty).
pub(crate) fn key_fp(pem: &[u8], aud: KeyAudience) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(aud.claim().as_bytes());
    h.update([0u8]);
    h.update(pem);
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::tests::{FLEET_KID, KID, NOW, claims, service, sign_with};
    use super::super::{AUD_CUSTOMER, AUD_INTERNAL, AuthError, JwksSnapshot};
    use super::{JwksKey, KeyAudience};

    const PUB: &str = include_str!("../dst/fixtures/mt-test-rsa.pub.pem");

    #[test]
    fn a_feed_names_exactly_the_two_audiences() {
        assert_eq!(
            KeyAudience::parse(AUD_CUSTOMER),
            Some(KeyAudience::Customer)
        );
        assert_eq!(
            KeyAudience::parse(AUD_INTERNAL),
            Some(KeyAudience::Internal)
        );
        for other in [
            "",
            "prisma-streams-data ",
            "PRISMA-STREAMS-DATA",
            "prisma-streams",
        ] {
            assert_eq!(KeyAudience::parse(other), None, "{other:?}");
        }
        for aud in [KeyAudience::Customer, KeyAudience::Internal] {
            assert_eq!(KeyAudience::parse(aud.claim()), Some(aud));
        }
    }

    /// H6, the other direction: the fleet's key cannot mint a customer
    /// token, even one whose claims are otherwise exactly valid.
    #[test]
    fn a_fleet_key_cannot_sign_a_customer_token() {
        let svc = service();
        let signed = |kid: &str| sign_with(&claims(), kid, jsonwebtoken::Algorithm::RS256);
        assert!(svc.verify_customer(&signed(KID), NOW).is_ok());
        assert_eq!(
            svc.verify_customer(&signed(FLEET_KID), NOW).unwrap_err(),
            AuthError::WrongAudience
        );
    }

    /// A kid's audience is as permanent as its material: re-pinning the
    /// customer kid to the fleet audience is refused, the published key
    /// set stays, and the kid still verifies only customer tokens.
    #[test]
    fn a_kid_is_never_repinned_to_another_audience() {
        let svc = service();
        let repinned = JwksSnapshot {
            keys: HashMap::from([
                (KID.to_string(), JwksKey::rs256(PUB, KeyAudience::Internal)),
                (
                    FLEET_KID.to_string(),
                    JwksKey::rs256(PUB, KeyAudience::Internal),
                ),
            ]),
            fetched_at_unix: NOW,
            feed_version: 2,
        };
        assert_eq!(
            svc.publish_jwks(repinned),
            Err("kid rebound to different key material or audience")
        );
        let token = sign_with(&claims(), KID, jsonwebtoken::Algorithm::RS256);
        assert!(svc.verify_customer(&token, NOW).is_ok());
    }
}
