// SPDX-License-Identifier: BUSL-1.1

//! The answer a handler gives when it refuses after part of its write landed.
//!
//! A definite refusal code claims nothing applied. On one, the Control Plane
//! writes an abort marker for the request's WAL records, and recovery skips
//! them. A handler whose refusal follows a landed write must answer with a
//! code that keeps the records, or recovery drops the part that landed.

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::control::server::dispatch_utils::write_definitely_not_applied;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// The refusal of a handler that committed the rows `landed` names before
    /// it refused. The rows stay in storage, so the refusal carries their
    /// write-set entries and the Control Plane journals them like the rows of
    /// an accepted write.
    pub(in crate::data::executor) fn refusal_with_landed_rows(
        &self,
        task: &ExecutionTask,
        code: ErrorCode,
        landed: Vec<WriteSetEntry>,
    ) -> Response {
        let mut response = self.response_error(task, code);
        response.write_set = landed;
        response
    }
}

/// `code` as a refusal that follows a landed write.
///
/// A definite code becomes `Internal`, which keeps the request's records
/// for replay. Any other code already keeps them and passes through.
pub(in crate::data::executor) fn refusal_after_partial_apply(code: ErrorCode) -> ErrorCode {
    if write_definitely_not_applied(&code) {
        ErrorCode::Internal {
            detail: format!("refused after part of the write applied, which stays: {code:?}"),
        }
    } else {
        code
    }
}

/// `code` from a handler that commits row by row, after `rows_landed` rows
/// committed. With no row landed, `code` passes through.
pub(in crate::data::executor) fn refusal_after_rows(
    rows_landed: u64,
    code: impl Into<ErrorCode>,
) -> ErrorCode {
    let code = code.into();
    if rows_landed > 0 {
        refusal_after_partial_apply(code)
    } else {
        code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_definite_refusal_after_a_landed_write_keeps_the_records() {
        let answered = refusal_after_partial_apply(ErrorCode::RejectedAuthz {
            resource: "RLS write policy on 'orders' rejected the row".into(),
        });
        assert!(matches!(answered, ErrorCode::Internal { .. }));
        assert!(!write_definitely_not_applied(&answered));
    }

    #[test]
    fn a_refusal_before_any_row_landed_stays_definite() {
        let code = ErrorCode::PeriodLocked {
            collection: "ledger".into(),
        };
        assert_eq!(refusal_after_rows(0, code.clone()), code);
        assert!(matches!(
            refusal_after_rows(1, code),
            ErrorCode::Internal { .. }
        ));
    }

    #[test]
    fn a_code_that_keeps_the_records_passes_through() {
        assert_eq!(
            refusal_after_partial_apply(ErrorCode::DeadlineExceeded),
            ErrorCode::DeadlineExceeded
        );
    }
}
