// SPDX-License-Identifier: BUSL-1.1

//! The typed error of an undo entry whose reverse write did not apply.

use crate::bridge::envelope::ErrorCode;

/// An undo entry whose reverse write did not apply.
///
/// The core's state is unknown after it. The caller answers
/// `ErrorCode::RollbackFailed`, and the core fail-stops when that response
/// leaves it.
#[derive(Debug, thiserror::Error)]
#[error(
    "undo entry {entry_index}: {action}{}",
    .cause.as_ref().map(|c| format!(": {c}")).unwrap_or_default()
)]
pub struct UndoError {
    /// Forward-order position of the entry in the undo log.
    pub entry_index: usize,
    /// The reverse write that failed, with the object it targets.
    pub action: String,
    /// The error the reverse write returned. `None` when the engine state
    /// does not match the entry, so no reverse write ran. Boxed to keep the
    /// `Result` of every undo function small.
    #[source]
    pub cause: Option<Box<crate::Error>>,
}

impl UndoError {
    /// A reverse write that returned `cause`.
    pub fn failed(
        entry_index: usize,
        action: impl Into<String>,
        cause: impl Into<crate::Error>,
    ) -> Self {
        Self {
            entry_index,
            action: action.into(),
            cause: Some(Box::new(cause.into())),
        }
    }

    /// An entry the engine state does not match. No reverse write ran.
    pub fn mismatch(entry_index: usize, action: impl Into<String>) -> Self {
        Self {
            entry_index,
            action: action.into(),
            cause: None,
        }
    }
}

impl From<UndoError> for ErrorCode {
    fn from(e: UndoError) -> Self {
        ErrorCode::RollbackFailed {
            entry_index: e.entry_index,
            detail: e.action,
            cause: e.cause.map(|cause| Box::new(ErrorCode::from(*cause))),
        }
    }
}

impl From<UndoError> for crate::Error {
    fn from(e: UndoError) -> Self {
        crate::Error::DataPlane(ErrorCode::from(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_reverse_write_keeps_its_typed_cause() {
        let cause = crate::Error::Storage {
            engine: "sparse".into(),
            detail: "commit".into(),
        };
        let code = ErrorCode::from(UndoError::failed(3, "restoring row r1", cause));
        match code {
            ErrorCode::RollbackFailed {
                entry_index,
                detail,
                cause: Some(cause),
            } => {
                assert_eq!(entry_index, 3);
                assert_eq!(detail, "restoring row r1");
                assert!(matches!(*cause, ErrorCode::Internal { .. }));
            }
            other => panic!("expected RollbackFailed with a cause, got {other:?}"),
        }
    }

    #[test]
    fn a_state_mismatch_carries_no_cause() {
        let code = ErrorCode::from(UndoError::mismatch(0, "vector index missing"));
        assert!(matches!(
            code,
            ErrorCode::RollbackFailed {
                entry_index: 0,
                cause: None,
                ..
            }
        ));
    }

    #[test]
    fn a_data_plane_cause_keeps_its_code() {
        let cause = crate::Error::DataPlane(ErrorCode::DivisionByZero);
        let code = ErrorCode::from(UndoError::failed(1, "x", cause));
        assert!(matches!(
            code,
            ErrorCode::RollbackFailed { cause: Some(c), .. } if *c == ErrorCode::DivisionByZero
        ));
    }
}
