// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane errors a client acts on answer one class on every surface.

use nodedb_types::error::sqlstate;

use crate::control::server::native::dispatch::native_error_fields;
use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;
use crate::control::server::pgwire::types::error_to_sqlstate;

use super::super::GatewayErrorMap;
use super::support::class;

/// Control-Plane errors a client acts on. Each has a class of its own.
fn control_plane_samples() -> Vec<crate::Error> {
    vec![
        crate::Error::RetryableSchemaChanged {
            descriptor: "orders".into(),
        },
        crate::Error::SessionTokenExpired,
    ]
}

/// A Control-Plane error has one class on native and pgwire, and its native
/// numeric code renders in that class.
#[test]
fn control_plane_errors_have_one_class_on_native_and_pgwire() {
    for err in control_plane_samples() {
        let (_, pg_state, _) = error_to_sqlstate(&err);
        assert_ne!(pg_state, sqlstate::INTERNAL_ERROR, "{err:?} has no class");

        let native = native_error_fields(&err);
        assert_eq!(native.sqlstate, pg_state, "native SQLSTATE for {err:?}");
        let native_state = numeric_code_to_sqlstate(native.code);
        assert_eq!(
            class(native_state),
            class(pg_state),
            "{err:?}: pgwire sends {pg_state}, native code {} renders {native_state}",
            native.code
        );
    }
}

/// A schema change the server cannot absorb is the retryable
/// serialization class on every surface.
#[test]
fn schema_change_is_a_retryable_serialization_failure() {
    let err = crate::Error::RetryableSchemaChanged {
        descriptor: "orders".into(),
    };
    assert_eq!(error_to_sqlstate(&err).1, sqlstate::SERIALIZATION_FAILURE);
    let native = native_error_fields(&err);
    assert_eq!(native.sqlstate, sqlstate::SERIALIZATION_FAILURE);
    assert_eq!(native.code, nodedb_types::error::ErrorCode::WRITE_CONFLICT);
    assert!(crate::error_classify::classify(&err).is_retriable());
    let status = GatewayErrorMap::to_http(&err).0;
    assert_eq!(status, 409);
    assert_eq!(
        GatewayErrorMap::sqlstate_to_http(sqlstate::SERIALIZATION_FAILURE),
        status
    );
}

/// A committed write whose result is gone answers the internal class on every
/// surface, which no client retries, and says the write committed.
#[test]
fn committed_result_unavailable_is_never_retried() {
    let err = crate::Error::CommittedResultUnavailable {
        group_id: 1,
        log_index: 2,
    };
    let (_, pg_state, message) = error_to_sqlstate(&err);
    assert_eq!(pg_state, sqlstate::INTERNAL_ERROR);
    assert!(message.contains("committed"), "{message}");
    let native = native_error_fields(&err);
    assert_eq!(native.code, nodedb_types::error::ErrorCode::INTERNAL);
    assert_eq!(
        class(numeric_code_to_sqlstate(native.code)),
        class(pg_state)
    );
    assert!(!crate::error_classify::classify(&err).is_retriable());
    assert!(!crate::error_classify::is_unclassified_failure(&err));
}

/// A write whose outcome is unknown answers the same internal class, which
/// no client retries, and says the outcome must be checked first.
#[test]
fn proposal_outcome_unknown_is_never_retried() {
    let err = crate::Error::ProposalOutcomeUnknown {
        group_id: 1,
        log_index: 2,
    };
    let (_, pg_state, message) = error_to_sqlstate(&err);
    assert_eq!(pg_state, sqlstate::INTERNAL_ERROR);
    assert!(message.contains("unknown"), "{message}");
    let native = native_error_fields(&err);
    assert_eq!(native.code, nodedb_types::error::ErrorCode::INTERNAL);
    assert_eq!(
        class(numeric_code_to_sqlstate(native.code)),
        class(pg_state)
    );
    assert!(!crate::error_classify::classify(&err).is_retriable());
    assert!(!crate::error_classify::is_unclassified_failure(&err));
}

/// An expired session token is invalid authorization on every surface.
#[test]
fn expired_session_token_is_invalid_authorization_everywhere() {
    let err = crate::Error::SessionTokenExpired;
    assert_eq!(error_to_sqlstate(&err).1, sqlstate::AUTH_TOKEN_EXPIRED.0);
    let native = native_error_fields(&err);
    assert_eq!(native.sqlstate, sqlstate::AUTH_TOKEN_EXPIRED.0);
    assert_eq!(native.code, nodedb_types::error::ErrorCode::AUTH_EXPIRED);
    assert_eq!(
        numeric_code_to_sqlstate(native.code),
        sqlstate::AUTH_TOKEN_EXPIRED.0
    );
    let status = GatewayErrorMap::to_http(&err).0;
    assert_eq!(status, 401);
    assert_eq!(
        GatewayErrorMap::sqlstate_to_http(sqlstate::AUTH_TOKEN_EXPIRED.0),
        status
    );
}
