// SPDX-License-Identifier: Apache-2.0

//! Typed compensation hints for rejected sync deltas.
//!
//! When the Origin rejects a CRDT delta (constraint violation, RLS, rate limit),
//! it sends a `CompensationHint` back to the edge client. The edge uses this
//! to roll back optimistic local state and notify the application with a
//! typed, actionable error — not a generic string.

use serde::{Deserialize, Serialize};

/// Typed compensation hint sent from Origin to edge when a delta is rejected.
///
/// The edge's `CompensationHandler` receives this and can programmatically
/// decide how to react (prompt user, auto-retry with suffix, silently merge).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CompensationHint {
    /// UNIQUE constraint violated — another device wrote the same value first.
    #[serde(rename = "unique_violation")]
    UniqueViolation {
        /// The field that has the UNIQUE constraint (e.g., "username").
        field: String,
        /// The conflicting value that was already taken.
        conflicting_value: String,
    },

    /// Foreign key reference missing — the referenced entity doesn't exist.
    #[serde(rename = "foreign_key_missing")]
    ForeignKeyMissing {
        /// The ID that was referenced but not found.
        referenced_id: String,
    },

    /// Permission denied — the user doesn't have write access.
    /// No details are leaked (security: the edge is untrusted).
    #[serde(rename = "permission_denied")]
    PermissionDenied,

    /// Rate limit exceeded — try again later.
    #[serde(rename = "rate_limited")]
    RateLimited {
        /// Suggested delay before retrying (milliseconds).
        retry_after_ms: u64,
    },

    /// Schema violation — the delta doesn't conform to the collection schema.
    #[serde(rename = "schema_violation")]
    SchemaViolation {
        /// Which field failed validation.
        field: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Custom application-defined constraint violation.
    #[serde(rename = "custom")]
    Custom {
        /// Constraint name.
        constraint: String,
        /// Typed payload for the application to interpret.
        detail: String,
    },

    /// Data integrity violation — CRC32C checksum mismatch on delta payload.
    /// The client should re-send the delta.
    #[serde(rename = "integrity_violation")]
    IntegrityViolation,

    /// Transient rejection — the delta was admitted against a constraint
    /// version not yet installed on the accepting replica. Not a failure:
    /// re-push the delta as a new sequence after the suggested delay.
    #[serde(rename = "retry")]
    Retry {
        /// Suggested delay before re-pushing (milliseconds).
        retry_after_ms: u64,
    },
}

impl CompensationHint {
    /// Constraint name carried by [`Self::Custom`] when a delta's Loro peer id
    /// is already owned by another replica.
    ///
    /// Unlike every other refusal, nothing about the row is wrong and no
    /// conflict policy can resolve it: the producer's identity is unusable, and
    /// only the producer can replace it. Defined here so the server that emits
    /// the refusal and the client that acts on it cannot drift apart on the
    /// spelling of a string that is the difference between a replica healing
    /// itself and one that is permanently refused.
    pub const PEER_ID_COLLISION: &'static str = "peer_id_collision";

    /// Whether this refusal says the producer's Loro peer id is unusable.
    pub fn is_peer_id_collision(&self) -> bool {
        matches!(
            self,
            Self::Custom { constraint, .. } if constraint == Self::PEER_ID_COLLISION
        )
    }

    /// Returns a short, machine-readable code for the hint type.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UniqueViolation { .. } => "UNIQUE_VIOLATION",
            Self::ForeignKeyMissing { .. } => "FK_MISSING",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::RateLimited { .. } => "RATE_LIMITED",
            Self::SchemaViolation { .. } => "SCHEMA_VIOLATION",
            Self::Custom { .. } => "CUSTOM",
            Self::IntegrityViolation => "INTEGRITY_VIOLATION",
            Self::Retry { .. } => "RETRY",
        }
    }
}

impl std::fmt::Display for CompensationHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UniqueViolation {
                field,
                conflicting_value,
            } => write!(
                f,
                "UNIQUE({field}): value '{conflicting_value}' already exists"
            ),
            Self::ForeignKeyMissing { referenced_id } => {
                write!(f, "FK_MISSING: referenced ID '{referenced_id}' not found")
            }
            Self::PermissionDenied => write!(f, "PERMISSION_DENIED"),
            Self::RateLimited { retry_after_ms } => {
                write!(f, "RATE_LIMITED: retry after {retry_after_ms}ms")
            }
            Self::SchemaViolation { field, reason } => {
                write!(f, "SCHEMA({field}): {reason}")
            }
            Self::Custom {
                constraint, detail, ..
            } => write!(f, "CUSTOM({constraint}): {detail}"),
            Self::IntegrityViolation => write!(f, "INTEGRITY_VIOLATION: CRC32C mismatch"),
            Self::Retry { retry_after_ms } => {
                write!(f, "RETRY: retry after {retry_after_ms}ms")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compensation_codes() {
        assert_eq!(
            CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into()
            }
            .code(),
            "UNIQUE_VIOLATION"
        );
        assert_eq!(
            CompensationHint::PermissionDenied.code(),
            "PERMISSION_DENIED"
        );
        assert_eq!(
            CompensationHint::RateLimited {
                retry_after_ms: 5000
            }
            .code(),
            "RATE_LIMITED"
        );
    }

    #[test]
    fn compensation_display() {
        let hint = CompensationHint::UniqueViolation {
            field: "username".into(),
            conflicting_value: "alice".into(),
        };
        assert!(hint.to_string().contains("alice"));
        assert!(hint.to_string().contains("username"));
    }

    #[test]
    fn retry_code_and_display() {
        let hint = CompensationHint::Retry {
            retry_after_ms: 1000,
        };
        assert_eq!(hint.code(), "RETRY");
        assert!(hint.to_string().contains("1000"));
    }

    #[test]
    fn msgpack_roundtrip() {
        let hint = CompensationHint::ForeignKeyMissing {
            referenced_id: "user-42".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&hint).unwrap();
        let decoded: CompensationHint = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(hint, decoded);
    }
}
