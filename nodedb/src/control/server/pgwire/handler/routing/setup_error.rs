// SPDX-License-Identifier: BUSL-1.1

//! Error type for the retried statement-setup unit (plan → authorize → admit).
//!
//! Statement setup mixes typed engine failures with protocol failures that are
//! already rendered for the wire (session-handle resolution, authorization
//! denials). The retry loop must classify the former without inspecting the
//! rendered SQLSTATE of the latter, so setup keeps both shapes distinct until
//! the retry budget is settled and only then renders a `PgWireError`.

use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::retry::RetryableSchemaChange;

/// A failure escaping the retried statement-setup unit.
pub(in crate::control::server::pgwire::handler) enum StatementSetupError {
    /// Typed engine failure — still classifiable by the retry loop.
    Engine(crate::Error),
    /// Protocol failure already rendered for the client. Never retryable:
    /// a session-handle or authorization outcome does not change while a
    /// descriptor drain completes.
    Protocol(PgWireError),
}

impl StatementSetupError {
    /// Build a rendered protocol failure from its severity/SQLSTATE/message.
    pub(in crate::control::server::pgwire::handler) fn protocol(
        severity: &str,
        code: &str,
        message: impl Into<String>,
    ) -> Self {
        Self::Protocol(PgWireError::UserError(Box::new(ErrorInfo::new(
            severity.to_owned(),
            code.to_owned(),
            message.into(),
        ))))
    }
}

impl From<crate::Error> for StatementSetupError {
    fn from(error: crate::Error) -> Self {
        Self::Engine(error)
    }
}

impl From<PgWireError> for StatementSetupError {
    fn from(error: PgWireError) -> Self {
        Self::Protocol(error)
    }
}

impl From<StatementSetupError> for PgWireError {
    fn from(error: StatementSetupError) -> Self {
        match error {
            StatementSetupError::Engine(error) => {
                let (severity, code, message) = error_to_sqlstate(&error);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            }
            StatementSetupError::Protocol(error) => error,
        }
    }
}

impl RetryableSchemaChange for StatementSetupError {
    fn retryable_descriptor(&self) -> Option<&str> {
        match self {
            Self::Engine(error) => error.retryable_descriptor(),
            Self::Protocol(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_drain_error_is_retryable() {
        let error = StatementSetupError::Engine(crate::Error::RetryableSchemaChanged {
            descriptor: "orders at version 4".into(),
        });
        assert_eq!(error.retryable_descriptor(), Some("orders at version 4"));
    }

    #[test]
    fn engine_non_drain_error_is_not_retryable() {
        let error = StatementSetupError::Engine(crate::Error::Config {
            detail: "lease grant rejected".into(),
        });
        assert!(error.retryable_descriptor().is_none());
    }

    #[test]
    fn protocol_error_is_never_retryable() {
        let error = StatementSetupError::protocol("ERROR", "42501", "permission denied");
        assert!(error.retryable_descriptor().is_none());
    }
}
