// SPDX-License-Identifier: BUSL-1.1

//! Surrogate assignment shared by the KV RESP handlers.

use crate::control::state::SharedState;

use super::super::codec::RespValue;
use super::super::session::RespSession;

/// Resolve the stable cross-engine surrogate for a KV atomic op on this
/// session's collection, content-addressed on `(collection, key)` — the same
/// binding a normal insert of that key allocated, so an atomic op on an
/// existing key keeps its identity.
pub(super) fn resp_kv_surrogate(
    state: &SharedState,
    session: &RespSession,
    key: &[u8],
) -> Result<nodedb_types::Surrogate, RespValue> {
    state
        .surrogate_assigner
        .assign(
            crate::types::DatabaseId::DEFAULT,
            session.tenant_id,
            &session.collection,
            key,
        )
        .map_err(|e| RespValue::err(format!("ERR {e}")))
}
