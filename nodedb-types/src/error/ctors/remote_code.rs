// SPDX-License-Identifier: Apache-2.0

//! Cross-node `RemoteTyped` code handling for cluster RPC replies.

use super::super::code::ErrorCode;
use super::super::code_table::details_for_code;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// Preserve a numeric `ErrorCode` from a remote node verbatim, using
    /// `wire_details`'s pairing table to reconstruct typed `details`.
    /// `code` stays the wire code even for a secondary pairing (e.g.
    /// `DATABASE_QUOTA_EXCEEDED`), relaying the remote classification as-is.
    pub fn remote_typed(code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        let details = details_for_code(code, &message);
        Self {
            code,
            details,
            message,
            cause: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::details::ErrorDetails;
    use super::*;

    #[test]
    fn remote_typed_reconstructs_a_dedicated_variant() {
        let e = NodeDbError::remote_typed(ErrorCode::CONSTRAINT_VIOLATION, "dup email");
        assert!(e.is_constraint_violation());
        assert_eq!(e.code(), ErrorCode::CONSTRAINT_VIOLATION);
        assert!(e.message().contains("dup email"));
    }

    #[test]
    fn remote_typed_falls_back_to_internal_for_unmapped_code() {
        let e = NodeDbError::remote_typed(ErrorCode(65000), "mystery");
        assert!(e.is_internal());
        assert_eq!(e.code(), ErrorCode(65000));
    }

    #[test]
    fn remote_typed_keeps_the_wire_code_for_a_secondary_pairing() {
        let e = NodeDbError::remote_typed(ErrorCode::DATABASE_QUOTA_EXCEEDED, "over quota");
        assert!(matches!(e.details(), ErrorDetails::QuotaExceeded { .. }));
        assert_eq!(e.code(), ErrorCode::DATABASE_QUOTA_EXCEEDED);
    }
}
