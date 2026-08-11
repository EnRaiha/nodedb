// SPDX-License-Identifier: Apache-2.0

//! Reconstruction of a typed [`NodeDbError`] from the numeric code a peer put
//! on the wire.
//!
//! Every other constructor in this directory is used by the side that
//! *detects* a failure and therefore still holds the structured context
//! (which collection, which gate, which document). A client decoding a
//! response frame holds neither: it has a numeric code and the server's
//! rendered message. Rebuilding the [`ErrorDetails`] variant from the code is
//! what makes `is_constraint_violation()`, `is_not_found()`, `is_auth_denied()`
//! and friends answer correctly on the client — without it every remote
//! failure collapses into `internal`, and a duplicate key is indistinguishable
//! from a crashed server.
//!
//! The reverse SQLSTATE mapping is deliberately absent: SQLSTATE is
//! many-to-one (a unique violation and a duplicate idempotency key are both
//! `23505`; every unclassified failure is `XX000`), so it cannot recover a
//! classification. The numeric code is the authoritative one.

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// Rebuild a typed error from a wire-carried numeric `code` plus the
    /// message the originating side rendered.
    ///
    /// The message is preserved verbatim rather than re-derived: it is the
    /// only place the structured context survives (the offending value, the
    /// index name, the gate). String fields on the reconstructed
    /// [`ErrorDetails`] are therefore left empty — the code identifies *what
    /// kind* of failure occurred, which is what the category predicates match
    /// on, while the human-readable specifics stay in `message`. Populating
    /// them by parsing the message back apart would invent structure the wire
    /// never carried.
    ///
    /// Unrecognised codes fall through to [`ErrorDetails::Internal`] tagged
    /// `"remote"`, which is also where a `0` code lands: a peer older than the
    /// numeric-code field sends nothing, and guessing from SQLSTATE would be
    /// worse than admitting the classification is unavailable.
    pub fn from_wire(code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        let details = wire_details(code, &message);
        Self {
            code,
            message,
            details,
            cause: None,
        }
    }
}

/// Map a numeric code onto the details variant that carries its category.
fn wire_details(code: ErrorCode, message: &str) -> ErrorDetails {
    let empty = String::new;
    match code {
        // Write path.
        ErrorCode::CONSTRAINT_VIOLATION => ErrorDetails::ConstraintViolation {
            collection: empty(),
        },
        ErrorCode::WRITE_CONFLICT => ErrorDetails::WriteConflict {
            collection: empty(),
            document_id: empty(),
        },
        ErrorCode::DEADLINE_EXCEEDED => ErrorDetails::DeadlineExceeded,
        ErrorCode::PREVALIDATION_REJECTED => ErrorDetails::PrevalidationRejected {
            constraint: empty(),
        },
        ErrorCode::APPEND_ONLY_VIOLATION => ErrorDetails::AppendOnlyViolation {
            collection: empty(),
        },
        ErrorCode::BALANCE_VIOLATION => ErrorDetails::BalanceViolation {
            collection: empty(),
        },
        ErrorCode::PERIOD_LOCKED => ErrorDetails::PeriodLocked {
            collection: empty(),
        },
        ErrorCode::STATE_TRANSITION_VIOLATION => ErrorDetails::StateTransitionViolation {
            collection: empty(),
        },
        ErrorCode::TRANSITION_CHECK_VIOLATION => ErrorDetails::TransitionCheckViolation {
            collection: empty(),
        },
        ErrorCode::RETENTION_VIOLATION => ErrorDetails::RetentionViolation {
            collection: empty(),
        },
        ErrorCode::LEGAL_HOLD_ACTIVE => ErrorDetails::LegalHoldActive {
            collection: empty(),
        },
        ErrorCode::TYPE_MISMATCH => ErrorDetails::TypeMismatch {
            collection: empty(),
        },
        ErrorCode::OVERFLOW => ErrorDetails::Overflow {
            collection: empty(),
        },
        ErrorCode::INSUFFICIENT_BALANCE => ErrorDetails::InsufficientBalance {
            collection: empty(),
        },
        ErrorCode::RATE_EXCEEDED => ErrorDetails::RateExceeded { gate: empty() },
        ErrorCode::TYPE_GUARD_VIOLATION => ErrorDetails::TypeGuardViolation {
            collection: empty(),
        },

        // Read path.
        ErrorCode::COLLECTION_NOT_FOUND => ErrorDetails::CollectionNotFound {
            collection: empty(),
        },
        ErrorCode::DOCUMENT_NOT_FOUND => ErrorDetails::DocumentNotFound {
            collection: empty(),
            document_id: empty(),
        },
        ErrorCode::COLLECTION_DRAINING => ErrorDetails::CollectionDraining {
            collection: empty(),
        },

        // Query.
        ErrorCode::PLAN_ERROR => ErrorDetails::PlanError {
            phase: "remote".into(),
            detail: message.to_owned(),
        },
        ErrorCode::FAN_OUT_EXCEEDED => ErrorDetails::FanOutExceeded {
            shards_touched: 0,
            limit: 0,
        },
        ErrorCode::SQL_NOT_ENABLED => ErrorDetails::SqlNotEnabled,
        ErrorCode::UNDEFINED_FUNCTION => ErrorDetails::UndefinedFunction { name: empty() },
        ErrorCode::DIVISION_BY_ZERO => ErrorDetails::DivisionByZero,

        // Quota.
        ErrorCode::TENANT_QUOTA_EXCEEDED | ErrorCode::DATABASE_QUOTA_EXCEEDED => {
            ErrorDetails::QuotaExceeded { scope: empty() }
        }
        ErrorCode::SERVER_OVERLOAD => ErrorDetails::ServerOverload,

        // Auth / security.
        ErrorCode::AUTHORIZATION_DENIED => ErrorDetails::AuthorizationDenied { resource: empty() },
        ErrorCode::AUTH_EXPIRED => ErrorDetails::AuthExpired,

        // Storage / infrastructure.
        ErrorCode::STORAGE => ErrorDetails::Storage {
            component: "remote".into(),
            op: empty(),
            detail: message.to_owned(),
        },
        ErrorCode::WAL => ErrorDetails::Wal {
            stage: "remote".into(),
            detail: message.to_owned(),
        },

        // Config.
        ErrorCode::CONFIG => ErrorDetails::Config,
        ErrorCode::BAD_REQUEST => ErrorDetails::BadRequest,

        // Cluster.
        ErrorCode::NO_LEADER => ErrorDetails::NoLeader,
        ErrorCode::NOT_LEADER => ErrorDetails::NotLeader {
            leader_addr: empty(),
        },
        ErrorCode::MIGRATION_IN_PROGRESS => ErrorDetails::MigrationInProgress,
        ErrorCode::NODE_UNREACHABLE => ErrorDetails::NodeUnreachable,
        ErrorCode::CLUSTER => ErrorDetails::Cluster,

        // Memory.
        ErrorCode::MEMORY_EXHAUSTED => ErrorDetails::MemoryExhausted { engine: empty() },

        // Anything this build does not recognise, including the `0` a peer
        // older than the numeric-code field sends.
        _ => ErrorDetails::Internal {
            component: "remote".into(),
            detail: message.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_violation_survives_the_wire() {
        let e = NodeDbError::from_wire(
            ErrorCode::CONSTRAINT_VIOLATION,
            "unique index 'idx_users_email' violation on field '$.email'",
        );
        assert!(e.is_constraint_violation());
        assert_eq!(e.code(), ErrorCode::CONSTRAINT_VIOLATION);
        assert!(e.message().contains("idx_users_email"));
    }

    #[test]
    fn not_found_survives_the_wire() {
        assert!(NodeDbError::from_wire(ErrorCode::DOCUMENT_NOT_FOUND, "not found").is_not_found());
        assert!(
            NodeDbError::from_wire(ErrorCode::COLLECTION_NOT_FOUND, "not found").is_not_found()
        );
    }

    #[test]
    fn auth_and_rate_categories_survive_the_wire() {
        assert!(NodeDbError::from_wire(ErrorCode::AUTHORIZATION_DENIED, "denied").is_auth_denied());
        assert!(NodeDbError::from_wire(ErrorCode::RATE_EXCEEDED, "slow down").is_rate_exceeded());
    }

    #[test]
    fn absent_code_is_internal_not_a_guess() {
        let e = NodeDbError::from_wire(ErrorCode(0), "boom");
        assert!(e.is_internal());
        assert!(e.message().contains("boom"));
    }
}
