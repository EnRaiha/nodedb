// SPDX-License-Identifier: BUSL-1.1

//! Flattening a Data-Plane [`Response`] into the payload bytes a
//! payload-returning door hands back.

use crate::bridge::envelope::{Response, Status};

/// Return `response`'s payload, or its rejection as a typed error.
///
/// The typed [`crate::bridge::envelope::ErrorCode`] is preserved rather than
/// formatted into a message, so a caller classifies the rejection
/// (`is_retriable()`, `is_not_found()`, …) instead of substring-matching it. A
/// rejection carrying no code has only its payload to report, so that becomes
/// the detail.
///
/// Shared by every door that returns bytes rather than a `Response`: the
/// replicated sync write, the user DDL/DSL dispatch, and the version-history
/// read. One copy so a response a clone hook served and a response the Data
/// Plane returned cannot be reported differently.
pub(crate) fn payload_or_typed_error(response: Response) -> crate::Result<Vec<u8>> {
    if response.status != Status::Ok {
        return Err(match response.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Internal {
                detail: String::from_utf8_lossy(&response.payload).into_owned(),
            },
        });
    }
    Ok(response.payload.to_vec())
}
