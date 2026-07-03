// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral bulk data import handler.
//!
//! Document bytes flow over the pgwire COPY framing — the client
//! reads the file under the operator's UID and streams content to
//! the server.
//!
//! Ported from the pgwire `ddl::bulk` handler; only the result
//! construction changed from a pgwire `Response` error to the
//! protocol-neutral [`DdlError`]. The SQLSTATE and message are preserved
//! verbatim.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// `COPY <collection> FROM STDIN [WITH (FORMAT csv|json|ndjson)]`
pub async fn copy_from(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    Err(DdlError {
        sqlstate: "0A000".to_string(),
        message: "use `COPY <collection> FROM STDIN [WITH (FORMAT csv|json|ndjson)]` \
         and stream the file from the client"
            .to_string(),
    })
}
