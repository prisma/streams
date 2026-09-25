//! Refusal classes (§7.1/§8.1). The request path and the lease path
//! classify here, exhaustively and side by side, so a new reason cannot
//! fall into a transport's catch-all arm, and one reason cannot answer
//! differently on the two paths without these tables showing it.
//! Transports own the status codes; `auth` owns the class.

use super::{AuthError, LeaseInvalidReason};

/// Why authorization was refused, in the classes transports answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    /// §8.1 placement: this cell does not serve the project, and the
    /// credential itself is fine.
    WrongCell,
    /// This cell's own feed is stale: retryable, not the caller's fault.
    FeedStale,
    /// Verified, but the project or credential state or the grant denies it.
    Denied(Denial),
    /// Anything a fresh token could fix.
    Unverified,
}

/// What denied a verified caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Denial {
    Project,
    Credential,
    Scope,
    Prefix,
}

impl AuthError {
    pub(crate) fn refusal(&self) -> Refusal {
        match self {
            Self::WrongCell => Refusal::WrongCell,
            Self::PolicyStale | Self::GrantsStale | Self::KeysStale => Refusal::FeedStale,
            Self::ProjectNotActive(_) => Refusal::Denied(Denial::Project),
            Self::CredentialNotActive(_) => Refusal::Denied(Denial::Credential),
            Self::MissingScope(_) => Refusal::Denied(Denial::Scope),
            Self::PrefixDenied => Refusal::Denied(Denial::Prefix),
            Self::TokenTooLarge
            | Self::Malformed(_)
            | Self::KidMissing
            | Self::KidUnknown
            | Self::AlgNotAllowed
            | Self::BadSignature
            | Self::WrongIssuer
            | Self::WrongAudience
            | Self::Expired
            | Self::NotYetValid
            | Self::LifetimeTooLong
            | Self::ClaimInvalid(_)
            | Self::EmptyPrefixArray
            | Self::OwnershipVersionMismatch
            | Self::WorkspaceMismatch
            | Self::CredentialUnknown
            | Self::CredentialExpired
            | Self::CredentialProjectMismatch
            | Self::GrantVersionMismatch => Refusal::Unverified,
        }
    }
}

impl LeaseInvalidReason {
    /// Two classes differ from the request path's, and changing either is
    /// a wire decision: an inactive credential is `Unverified` here but
    /// `Denied` there, and a project this cell does not serve is `Denied`
    /// here but `WrongCell` there.
    pub(crate) fn refusal(self) -> Refusal {
        match self {
            Self::PolicyStale | Self::GrantsStale => Refusal::FeedStale,
            Self::ProjectMissing | Self::ProjectNotActive => Refusal::Denied(Denial::Project),
            Self::TokenExpired
            | Self::OwnershipChanged
            | Self::CredentialMissing
            | Self::CredentialInactive
            | Self::GrantChanged
            | Self::CredentialExpired => Refusal::Unverified,
        }
    }
}
