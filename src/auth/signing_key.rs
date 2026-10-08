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
//! that rebinds its material (`publish_jwks`, SR3-3). And one key MATERIAL
//! signs for one audience whatever kid names it (isolation review F2): a
//! key set that names one public key under both audiences is refused whole,
//! or the customer issuer's key would sign workload tokens under a second,
//! internal kid.

use std::collections::HashMap;

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
    /// F2: the fingerprint of the key material alone ([`material_fp`]),
    /// which one audience owns whatever kid names it.
    pub material: [u8; 32],
}

impl JwksKey {
    pub(crate) fn new(alg: Algorithm, key: DecodingKey, pem: &[u8], aud: KeyAudience) -> Self {
        Self {
            alg,
            key,
            aud,
            fp: key_fp(pem, aud),
            material: material_fp(pem),
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

/// F2: the fingerprint of a key's material whatever PEM armour, line
/// breaks or kid carry it: the SHA-256 of the key bytes jsonwebtoken
/// verifies with, an SPKI's subjectPublicKey (RSA and Ed25519 `PUBLIC
/// KEY`) or a PKCS#1 key's whole DER (`RSA PUBLIC KEY`).
pub(crate) fn material_fp(pem: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let der = pem_der(pem);
    let key = spki_public_key(&der).unwrap_or(&der);
    sha2::Sha256::digest(key).into()
}

/// F2: two kids of `keys` that name one key material under two audiences,
/// in kid order, or `None` when every material signs for one audience.
pub(crate) fn audience_shared(keys: &HashMap<String, JwksKey>) -> Option<(&str, &str)> {
    let mut kids: Vec<_> = keys.iter().collect();
    kids.sort_unstable_by_key(|(kid, _)| kid.as_str());
    let mut owner: HashMap<[u8; 32], (KeyAudience, &str)> = HashMap::new();
    for (kid, key) in kids {
        let &mut (aud, first) = owner.entry(key.material).or_insert((key.aud, kid.as_str()));
        if aud != key.aud {
            return Some((first, kid.as_str()));
        }
    }
    None
}

/// The DER a PEM armours: its base64 body without the armour lines and
/// whitespace (the PEM bytes themselves if the body is not base64, which a
/// key jsonwebtoken parsed never is).
fn pem_der(pem: &[u8]) -> Vec<u8> {
    use base64::Engine;
    let body: String = String::from_utf8_lossy(pem)
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .flat_map(|line| line.chars().filter(|c| !c.is_whitespace()))
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .unwrap_or_else(|_| pem.to_vec())
}

/// The subjectPublicKey of an SPKI, `SEQUENCE { SEQUENCE algorithm, BIT
/// STRING key }`, without the BIT STRING's unused-bits octet.
fn spki_public_key(der: &[u8]) -> Option<&[u8]> {
    let (0x30, spki, _) = der_element(der)? else {
        return None;
    };
    let (0x30, _, rest) = der_element(spki)? else {
        return None;
    };
    let (0x03, bits, _) = der_element(rest)? else {
        return None;
    };
    bits.split_first().map(|(_, key)| key)
}

/// A DER element's tag, its contents and the bytes that follow it.
type DerElement<'a> = (u8, &'a [u8], &'a [u8]);

/// The first DER element of `der` (definite lengths of at most four
/// octets, as every key here has).
fn der_element(der: &[u8]) -> Option<DerElement<'_>> {
    let (&tag, rest) = der.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let (octets, rest) = rest.split_at_checked(usize::from(first & 0x7f))?;
        if octets.is_empty() || octets.len() > 4 {
            return None;
        }
        let len = octets.iter().try_fold(0usize, |len, &octet| {
            len.checked_mul(256)?.checked_add(usize::from(octet))
        })?;
        (len, rest)
    };
    let (contents, rest) = rest.split_at_checked(len)?;
    Some((tag, contents, rest))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::tests::{FLEET_KID, FLEET_PUB, KID, NOW, claims, service, sign_with};
    use super::super::{AUD_CUSTOMER, AUD_INTERNAL, AuthError, JwksSnapshot};
    use super::{JwksKey, KeyAudience, der_element, material_fp};

    const PUB: &str = include_str!("../dst/fixtures/mt-test-rsa.pub.pem");
    /// The same public key as [`PUB`], armoured as PKCS#1.
    const PKCS1_PUB: &str = include_str!("../dst/fixtures/mt-test-rsa.pkcs1.pub.pem");

    /// F2: a key's material is the key, not its armour. The same public
    /// key rewrapped at 76 columns with CRLF line ends, or armoured as
    /// PKCS#1, names one material (each a key jsonwebtoken reads); the
    /// fleet's key names another.
    #[test]
    fn a_key_material_is_the_key_whatever_its_armour() {
        let body: String = PUB.lines().filter(|l| !l.starts_with("-----")).collect();
        let lines: Vec<_> = body
            .as_bytes()
            .chunks(76)
            .map(String::from_utf8_lossy)
            .collect();
        let rewrapped = format!(
            "-----BEGIN PUBLIC KEY-----\r\n{}\r\n-----END PUBLIC KEY-----\r\n",
            lines.join("\r\n")
        );
        let armours = [PUB, rewrapped.as_str(), PKCS1_PUB];
        for pem in armours {
            assert!(jsonwebtoken::DecodingKey::from_rsa_pem(pem.as_bytes()).is_ok());
        }
        let material = material_fp(PUB.as_bytes());
        assert_eq!(
            armours.map(|pem| material_fp(pem.as_bytes())),
            [material; 3]
        );
        assert_ne!(material_fp(FLEET_PUB.as_bytes()), material);
    }

    /// A DER length is one short octet below 0x80, or 0x81-0x84 and that
    /// many big-endian octets; the indefinite form 0x80 and five or more
    /// length octets are refused, whatever bytes follow.
    #[test]
    fn a_der_length_takes_one_to_four_octets() {
        let element = |head: &[u8], tail: usize| {
            let der: Vec<u8> = head
                .iter()
                .copied()
                .chain(std::iter::repeat_n(0, tail))
                .collect();
            der_element(&der).map(|(tag, contents, rest)| (tag, contents.len(), rest.len()))
        };
        assert_eq!(element(&[0x04, 0x7f], 130), Some((0x04, 127, 3)));
        assert_eq!(element(&[0x04, 0x80], 130), None);
        assert_eq!(element(&[0x04, 0x81, 0x80], 130), Some((0x04, 128, 2)));
        assert_eq!(
            element(&[0x04, 0x82, 0x01, 0x02], 300),
            Some((0x04, 258, 42))
        );
        assert_eq!(
            element(&[0x04, 0x84, 0, 0, 0x01, 0x02], 300),
            Some((0x04, 258, 42))
        );
        assert_eq!(element(&[0x04, 0x85, 0, 0, 0, 0x01, 0x02], 300), None);
        assert_eq!(element(&[0x04, 0x82, 0x01], 0), None);
        assert_eq!(element(&[0x04, 0x82, 0x01, 0x02], 257), None);
    }

    /// F2 at publication: the customer key armoured as PKCS#1 under an
    /// internal kid is the same material under a second audience, and is
    /// refused; the fleet's own key under that kid is published.
    #[test]
    fn a_rearmoured_customer_key_cannot_be_published_for_the_fleet() {
        let svc = service();
        let snapshot = |fleet_pem: &str, feed_version| JwksSnapshot {
            keys: HashMap::from([
                (KID.to_string(), JwksKey::rs256(PUB, KeyAudience::Customer)),
                (
                    "fleet-2".to_string(),
                    JwksKey::rs256(fleet_pem, KeyAudience::Internal),
                ),
            ]),
            fetched_at_unix: NOW,
            feed_version,
        };
        assert_eq!(
            svc.publish_jwks(snapshot(PKCS1_PUB, 2)),
            Err("one key material published under two audiences")
        );
        assert!(!svc.jwks.load().keys.contains_key("fleet-2"));
        assert_eq!(svc.publish_jwks(snapshot(FLEET_PUB, 2)), Ok(()));
        assert!(svc.jwks.load().keys.contains_key("fleet-2"));
    }

    /// Isolation review F2: the audience pin binds a kid, not the key it
    /// names. A key set that publishes ONE public key under a customer kid
    /// and under an internal kid makes the customer signing key a fleet
    /// key: whoever signs customer tokens signs a workload token under the
    /// internal kid, and the internal surface accepts it for every project
    /// on the cell (exactly what `rig_keys` and A10's positive control
    /// publish). A key material must sign for one audience, whatever kid
    /// names it: such a set is refused, and the internal kid verifies
    /// nothing.
    #[test]
    fn one_key_material_signs_for_one_audience_whatever_its_kid() {
        let svc = super::super::AuthService::new(
            super::super::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "fra-cell-07",
        )
        .unwrap();
        let both = JwksSnapshot {
            keys: HashMap::from([
                (KID.to_string(), JwksKey::rs256(PUB, KeyAudience::Customer)),
                (
                    FLEET_KID.to_string(),
                    JwksKey::rs256(PUB, KeyAudience::Internal),
                ),
            ]),
            fetched_at_unix: NOW,
            feed_version: 1,
        };
        let published = svc.publish_jwks(both);
        let internal = svc.jwks.load().keys.contains_key(FLEET_KID);
        assert_eq!(
            (published.is_err(), internal),
            (true, false),
            "one public key published for both audiences: {published:?}"
        );
    }

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
