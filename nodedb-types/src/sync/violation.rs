// SPDX-License-Identifier: Apache-2.0

//! Violation types for DLQ classification.
//!
//! When a sync delta is rejected, the `ViolationType` categorizes *why* it
//! was rejected. This is stored in the DLQ on the Origin for forensic review
//! and is separate from `CompensationHint` (which is what the edge sees).

use serde::{Deserialize, Serialize};

/// Why a sync delta was placed in the Dead-Letter Queue.
///
/// Used on the Origin side for audit/forensics. The edge never sees this
/// directly — it only receives `CompensationHint` (which may be generic
/// for security reasons).
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
pub enum ViolationType {
    /// RLS write policy rejected the delta.
    #[serde(rename = "rls_policy_violation")]
    RlsPolicyViolation { policy_name: String },
    /// UNIQUE constraint violation.
    #[serde(rename = "unique_violation")]
    UniqueViolation { field: String, value: String },
    /// Foreign key reference missing.
    #[serde(rename = "foreign_key_missing")]
    ForeignKeyMissing { referenced_id: String },
    /// Permission denied (no write access to target resource).
    #[serde(rename = "permission_denied")]
    PermissionDenied,
    /// Rate limit exceeded for this session.
    #[serde(rename = "rate_limited")]
    RateLimited,
    /// JWT token expired during active session.
    #[serde(rename = "token_expired")]
    TokenExpired,
    /// Schema validation failed.
    #[serde(rename = "schema_violation")]
    SchemaViolation { field: String, reason: String },
    /// Generic constraint violation (catch-all).
    #[serde(rename = "constraint_violation")]
    ConstraintViolation { detail: String },
    /// Delta admitted against a constraint-set version this replica has not
    /// installed yet (constraint install still propagating on the data Raft
    /// log). Transient: the client should retry after the install catches up.
    #[serde(rename = "constraint_version_pending")]
    ConstraintVersionPending {
        collection: String,
        required: u64,
        installed: u64,
    },
}

/// Suggested delay (milliseconds) before re-pushing a delta rejected by
/// `ConstraintVersionPending`. Matches the ~1s tick of the constraint
/// reconcile loop, so a single retry after this delay typically finds the
/// install has caught up.
pub const CONSTRAINT_RETRY_AFTER_MS: u64 = 1000;

impl std::fmt::Display for ViolationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RlsPolicyViolation { policy_name } => {
                write!(f, "rls_policy:{policy_name}")
            }
            Self::UniqueViolation { field, value } => {
                write!(f, "unique:{field}={value}")
            }
            Self::ForeignKeyMissing { referenced_id } => {
                write!(f, "fk_missing:{referenced_id}")
            }
            Self::PermissionDenied => write!(f, "permission_denied"),
            Self::RateLimited => write!(f, "rate_limited"),
            Self::TokenExpired => write!(f, "token_expired"),
            Self::SchemaViolation { field, reason } => {
                write!(f, "schema:{field}={reason}")
            }
            Self::ConstraintViolation { detail } => write!(f, "constraint:{detail}"),
            Self::ConstraintVersionPending {
                collection,
                required,
                installed,
            } => {
                write!(
                    f,
                    "constraint_version_pending:{collection} req={required} installed={installed}"
                )
            }
        }
    }
}

impl ViolationType {
    /// Convert a violation to the corresponding `CompensationHint` for the edge.
    ///
    /// Some violations map to a generic hint (e.g., RLS → PermissionDenied)
    /// to avoid leaking security-sensitive information to untrusted edges.
    pub fn to_compensation_hint(&self) -> super::compensation::CompensationHint {
        use super::compensation::CompensationHint;
        match self {
            Self::UniqueViolation { field, value } => CompensationHint::UniqueViolation {
                field: field.clone(),
                conflicting_value: value.clone(),
            },
            Self::ForeignKeyMissing { referenced_id } => CompensationHint::ForeignKeyMissing {
                referenced_id: referenced_id.clone(),
            },
            Self::RateLimited => CompensationHint::RateLimited {
                retry_after_ms: 5000,
            },
            // Security-sensitive violations all map to generic PermissionDenied.
            Self::RlsPolicyViolation { .. } | Self::PermissionDenied | Self::TokenExpired => {
                CompensationHint::PermissionDenied
            }
            Self::SchemaViolation { field, reason } => CompensationHint::SchemaViolation {
                field: field.clone(),
                reason: reason.clone(),
            },
            Self::ConstraintViolation { detail } => CompensationHint::Custom {
                constraint: "constraint".into(),
                detail: detail.clone(),
            },
            Self::ConstraintVersionPending { .. } => CompensationHint::Retry {
                retry_after_ms: CONSTRAINT_RETRY_AFTER_MS,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn violation_display() {
        assert_eq!(
            ViolationType::PermissionDenied.to_string(),
            "permission_denied"
        );
        assert_eq!(ViolationType::RateLimited.to_string(), "rate_limited");
        assert_eq!(
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x@y.com".into()
            }
            .to_string(),
            "unique:email=x@y.com"
        );
    }

    #[test]
    fn rls_violation_maps_to_permission_denied() {
        let v = ViolationType::RlsPolicyViolation {
            policy_name: "user_write_own".into(),
        };
        let hint = v.to_compensation_hint();
        // RLS details are NOT leaked to the edge.
        assert!(matches!(
            hint,
            super::super::compensation::CompensationHint::PermissionDenied
        ));
    }

    #[test]
    fn unique_violation_preserves_details() {
        let v = ViolationType::UniqueViolation {
            field: "username".into(),
            value: "alice".into(),
        };
        let hint = v.to_compensation_hint();
        match hint {
            super::super::compensation::CompensationHint::UniqueViolation {
                field,
                conflicting_value,
            } => {
                assert_eq!(field, "username");
                assert_eq!(conflicting_value, "alice");
            }
            _ => panic!("expected UniqueViolation hint"),
        }
    }

    #[test]
    fn token_expired_maps_to_permission_denied() {
        let hint = ViolationType::TokenExpired.to_compensation_hint();
        assert!(matches!(
            hint,
            super::super::compensation::CompensationHint::PermissionDenied
        ));
    }

    #[test]
    fn constraint_version_pending_maps_to_retry() {
        let v = ViolationType::ConstraintVersionPending {
            collection: "orders".into(),
            required: 3,
            installed: 2,
        };
        let hint = v.to_compensation_hint();
        assert!(matches!(
            hint,
            super::super::compensation::CompensationHint::Retry { .. }
        ));
    }
}
