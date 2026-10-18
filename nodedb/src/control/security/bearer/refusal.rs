// SPDX-License-Identifier: BUSL-1.1

//! A refused bearer login.

/// A refused bearer login: the reason the audit log records and the error the client sees.
///
/// The two differ on purpose.
/// The reason names the exact check that refused the token.
/// A token-stage refusal reaches the client as one generic error that names no check.
#[derive(Debug)]
pub struct BearerRefusal {
    /// The audit reason. It never holds the token.
    pub reason: String,
    /// The error the caller returns to the client, boxed to keep `Result` small.
    pub error: Box<crate::Error>,
}

impl BearerRefusal {
    /// A refusal that records `reason` and returns `error`.
    pub fn new(reason: impl Into<String>, error: crate::Error) -> Self {
        Self {
            reason: reason.into(),
            error: Box::new(error),
        }
    }

    /// A refusal that records and returns `error` itself.
    pub fn from_error(error: crate::Error) -> Self {
        Self {
            reason: error.to_string(),
            error: Box::new(error),
        }
    }
}
