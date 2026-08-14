// SPDX-License-Identifier: BUSL-1.1

//! Numeric NodeDB codes for native error frames authored as a bare SQLSTATE.
//!
//! A native error frame carries both a SQLSTATE and the stable numeric NodeDB
//! code, and the client rebuilds its typed error from the number: a frame that
//! ships `ndb_code == 0` collapses on arrival into a generic internal failure,
//! so `is_not_found()`, `is_auth_denied()` and `is_retriable()` all answer
//! wrongly for it.
//!
//! Most frames get their number from [`crate::error_classify::classify`],
//! which is the one internal-`Error`-to-public mapping the crate owns. This
//! module exists for the frames that never held an `Error` to classify: a DDL
//! refusal ([`DdlError`](crate::control::server::shared::ddl::DdlError) is
//! authored as a SQLSTATE plus a message, in ~600 places, and has no numeric
//! code to carry), and the session/dispatch guards that reject a request with
//! a literal SQLSTATE and a static message. For those the SQLSTATE *is* the
//! only classification the server ever produced, so reading it back is a
//! lookup rather than a guess.
//!
//! This is the inverse of the client-side rule in
//! `NodeDbError::from_wire`, and deliberately so. There, every SQLSTATE the
//! server can emit arrives through one funnel, so a reverse mapping would have
//! to resolve `23505` into either a unique violation or a duplicate
//! idempotency key with no way to tell them apart. Here the lookup happens at
//! the site that chose the SQLSTATE, and the table only carries SQLSTATEs
//! whose NodeDB classification is unambiguous *whatever* site emitted them.
//!
//! Everything else maps to `0`, which is exactly the frame today's code ships,
//! so an unmapped SQLSTATE is never worse off than before this table existed.
//! Three groups stay unmapped on purpose:
//!
//! - **Overloaded SQLSTATEs.** `53400` is `QUOTA_OVERCOMMIT`,
//!   `TENANT_QUOTA_EXCEEDED`, `DATABASE_QUOTA_EXCEEDED` and `SERVER_OVERLOAD`;
//!   `0A000` is `SQL_NOT_ENABLED`, `CANNOT_DROP_DEFAULT_DATABASE` and
//!   `CANNOT_CLONE_MIRROR`. A caller that knows which one it is passes the
//!   code explicitly instead of routing through this table.
//! - **SQLSTATEs with no NodeDB variant.** `42P07` (duplicate table), `42704`
//!   (undefined object), `25P02` (aborted transaction), `3B001` (no such
//!   savepoint). These need new `ErrorCode`/`ErrorDetails` variants to type at
//!   all, which is a public-API change tracked separately.
//! - **SQLSTATEs that are deliberately undistinguished.** Every credential
//!   failure renders as `28P01` and every ILP auth failure as a single code
//!   with one message, precisely so a caller cannot tell a wrong password from
//!   an unknown user. Typing them would rebuild the oracle that collapsing
//!   removed.

use nodedb_types::error::{ErrorCode, sqlstate};
use nodedb_types::protocol::NativeResponse;

/// The numeric NodeDB code a bare `sqlstate` classifies to, or `0` when it
/// carries no unambiguous classification.
pub(crate) fn ndb_code_for_sqlstate(sqlstate_str: &str) -> u16 {
    let code = match sqlstate_str {
        sqlstate::UNDEFINED_TABLE => ErrorCode::COLLECTION_NOT_FOUND,
        sqlstate::INVALID_CATALOG_NAME => ErrorCode::DATABASE_NOT_FOUND,
        sqlstate::INSUFFICIENT_PRIVILEGE => ErrorCode::AUTHORIZATION_DENIED,
        sqlstate::UNDEFINED_FUNCTION => ErrorCode::UNDEFINED_FUNCTION,
        // Both a malformed request and a plan that cannot be built render as
        // `42601`, so this cannot say which. It does not have to: the two
        // differ in which side wrote the bad statement, not in how a client
        // must react, and both `BadRequest` and `PlanError` are client errors
        // that no caller should retry.
        sqlstate::SYNTAX_ERROR => ErrorCode::BAD_REQUEST,
        // A cross-shard OCC abort and a retryable refusal both mean "nothing
        // applied, retry the whole thing" — the same contract `WriteConflict`
        // states, and the classification a retry loop reads.
        sqlstate::SERIALIZATION_FAILURE => ErrorCode::WRITE_CONFLICT,
        sqlstate::QUERY_CANCELED => ErrorCode::DEADLINE_EXCEEDED,
        sqlstate::TOO_MANY_CONNECTIONS => ErrorCode::RATE_EXCEEDED,
        sqlstate::INTERNAL_ERROR => ErrorCode::INTERNAL,
        _ => return 0,
    };
    code.0
}

/// Build a native error frame from a bare SQLSTATE, classifying it through
/// [`ndb_code_for_sqlstate`].
///
/// Use this wherever a site rejects a request with a literal SQLSTATE and no
/// `Error` value. A site that holds an `Error` must use
/// `error_to_native` / `error_to_native_with_sqlstate` instead: those read the
/// classification the error already carries rather than inferring one.
pub(crate) fn sqlstate_error(
    seq: u64,
    sqlstate_str: impl Into<String>,
    message: impl Into<String>,
) -> NativeResponse {
    let sqlstate_str = sqlstate_str.into();
    let ndb_code = ndb_code_for_sqlstate(&sqlstate_str);
    NativeResponse::error_with_code(seq, sqlstate_str, message, ndb_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classified_sqlstates_carry_their_code() {
        assert_eq!(
            ndb_code_for_sqlstate("42P01"),
            ErrorCode::COLLECTION_NOT_FOUND.0
        );
        assert_eq!(
            ndb_code_for_sqlstate("42501"),
            ErrorCode::AUTHORIZATION_DENIED.0
        );
        assert_eq!(ndb_code_for_sqlstate("42601"), ErrorCode::BAD_REQUEST.0);
        assert_eq!(
            ndb_code_for_sqlstate("3D000"),
            ErrorCode::DATABASE_NOT_FOUND.0
        );
        assert_eq!(ndb_code_for_sqlstate("XX000"), ErrorCode::INTERNAL.0);
    }

    /// A retry loop reads the numeric code, so the SQLSTATE the server sends
    /// precisely to get a transaction retried must not arrive unclassified.
    #[test]
    fn serialization_failure_stays_retriable() {
        let frame = sqlstate_error(1, "40001", "OCC abort");
        let payload = frame.error.expect("error frames carry a payload");
        assert_eq!(payload.ndb_code, ErrorCode::WRITE_CONFLICT.0);
        assert!(
            nodedb_types::NodeDbError::from_wire(ErrorCode(payload.ndb_code), payload.message)
                .is_retriable()
        );
    }

    /// An overloaded or unmapped SQLSTATE must fall through to `0` rather than
    /// pick a side: `0` is what the frame ships today, so an unknown SQLSTATE
    /// is no worse off, while a wrong guess would misreport retriability.
    #[test]
    fn ambiguous_and_unknown_sqlstates_stay_unclassified() {
        // Overloaded across several NodeDB variants.
        assert_eq!(ndb_code_for_sqlstate("53400"), 0);
        assert_eq!(ndb_code_for_sqlstate("0A000"), 0);
        // No NodeDB variant exists to map onto.
        assert_eq!(ndb_code_for_sqlstate("42P07"), 0);
        assert_eq!(ndb_code_for_sqlstate("42704"), 0);
        // Deliberately undistinguished so credential failures stay opaque.
        assert_eq!(ndb_code_for_sqlstate("28P01"), 0);
        assert_eq!(ndb_code_for_sqlstate("28000"), 0);
        // Not a SQLSTATE this server emits.
        assert_eq!(ndb_code_for_sqlstate("99999"), 0);
    }

    /// An unclassified frame must still reach the client exactly as it does
    /// today — same SQLSTATE, same message, `ndb_code == 0` — so adding the
    /// table cannot regress a path it does not cover.
    #[test]
    fn unclassified_frame_is_unchanged() {
        let frame = sqlstate_error(7, "42P07", "table 'repro_t' already exists");
        let payload = frame.error.expect("error frames carry a payload");
        assert_eq!(payload.code, "42P07");
        assert_eq!(payload.message, "table 'repro_t' already exists");
        assert_eq!(payload.ndb_code, 0);
    }
}
