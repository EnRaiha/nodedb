// SPDX-License-Identifier: BUSL-1.1

//! Small response/error-shaping helpers shared by the Document and KV clone
//! write-interception paths.

use pgwire::error::{ErrorInfo, PgWireError};

use nodedb_types::{DatabaseId, Lsn};

use crate::bridge::envelope::{Response, Status};
use crate::types::RequestId;

/// Build a synthetic OK response with no payload (used for tombstone success).
pub(super) fn synthetic_ok_response(request_id: RequestId, watermark_lsn: Lsn) -> Response {
    Response {
        request_id,
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Vec::<u8>::new().into(),
        watermark_lsn,
        error_code: None,
    }
}

/// Strip the `"<db_id>/"` db-qualified prefix.
pub(super) fn strip_db_prefix(db_id: DatabaseId, qualified: &str) -> &str {
    if db_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", db_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}

/// Convert a clone write error to a PgWireError.
pub(super) fn write_err(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg.to_owned(),
    )))
}
