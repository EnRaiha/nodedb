// SPDX-License-Identifier: Apache-2.0

//! `NodeDbError` constructors for sequence-accessor expression misuse.

use crate::error::ErrorCode;
use crate::error::details::ErrorDetails;
use crate::error::types::NodeDbError;

impl NodeDbError {
    /// A registered sequence accessor (`nextval`/`currval`/`setval`) was
    /// evaluated in a SQL expression context. Accessors are stateful and
    /// CP-side only — valid as column DEFAULTs, invalid elsewhere. Distinct
    /// from `plan_error` so clients match on the code (SQLSTATE `0A000`,
    /// `feature_not_supported`) rather than parsing the message.
    pub fn feature_not_supported(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            code: ErrorCode::FEATURE_NOT_SUPPORTED,
            message: "sequence accessors are supported as column DEFAULTs \
                 (DEFAULT nextval('s')); SELECT-time evaluation is not yet wired"
                .to_string(),
            details: ErrorDetails::FeatureNotSupported { name },
            cause: None,
        }
    }
}
