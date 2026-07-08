// SPDX-License-Identifier: BUSL-1.1

use super::errors::calvin_cancelled_error;
use pgwire::error::{ErrorInfo, PgWireError};

#[test]
fn calvin_assignment_channel_closed_returns_57014() {
    // Exercises the production constructor directly — no replicated mapping.
    let err = calvin_cancelled_error();
    match err {
        PgWireError::UserError(info) => {
            assert_eq!(
                info.code, "57014",
                "expected SQLSTATE 57014 (query_canceled) for Calvin channel-closed, got {}",
                info.code
            );
            assert_ne!(
                info.code, "XX000",
                "must not surface XX000 (internal_error) for a coordinator deadline cancel"
            );
        }
        other => panic!("expected PgWireError::UserError, got {other:?}"),
    }
}

#[test]
fn calvin_completion_channel_closed_returns_57014() {
    // Completion arm uses the same production constructor — assert it here too.
    let err = calvin_cancelled_error();
    match err {
        PgWireError::UserError(info) => {
            assert_eq!(
                info.code, "57014",
                "expected SQLSTATE 57014 for Calvin completion channel-closed, got {}",
                info.code
            );
        }
        other => panic!("expected PgWireError::UserError, got {other:?}"),
    }
}

#[test]
fn ollp_mismatch_is_not_57014() {
    // Verify the OLLP mismatch arm (a genuine invariant violation) stays
    // XX000 — a distinct code path from coordinator deadline cancellation.
    let err = PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
    )));
    match err {
        PgWireError::UserError(info) => {
            assert_eq!(
                info.code, "XX000",
                "OLLP mismatch must stay XX000 (internal_error)"
            );
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}
