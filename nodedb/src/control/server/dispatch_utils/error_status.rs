// SPDX-License-Identifier: BUSL-1.1

//! The one place a Data-Plane error `Response` becomes a typed `Err`.
//!
//! Every Control-Plane seam that collapses a shard `Response` into a
//! `crate::Result` goes through here, so an error status can never flatten
//! into an empty success payload.

use crate::bridge::envelope::{ErrorCode, Response, Status};

/// Reject a Data-Plane error response, keeping its typed code.
///
/// `NotFound` passes: it means the shard holds no slice of the collection,
/// which reads back as an empty — still validatable — observation. Every
/// other code crosses as `Error::DataPlane` so its SQLSTATE survives. An
/// error status carrying no code fails closed rather than reading as success.
pub(crate) fn reject_data_plane_error(resp: &Response) -> crate::Result<()> {
    if resp.status != Status::Error {
        return Ok(());
    }
    match resp.error_code.as_deref() {
        Some(ErrorCode::NotFound) => Ok(()),
        Some(code) => Err(crate::Error::DataPlane(code.clone())),
        None => Err(crate::Error::DataPlane(ErrorCode::Internal {
            detail: "data plane returned an error status with no error code".into(),
        })),
    }
}
