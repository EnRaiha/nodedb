// SPDX-License-Identifier: BUSL-1.1

//! Surrogate assignment shared by the KV RESP handlers.

use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::super::codec::RespValue;
use super::super::session::RespSession;

/// The stable cross-engine surrogates of `keys` in this session's
/// collection, in `keys` order, content-addressed on `(collection, key)`: the
/// binding a normal insert of each key made. An absent key is bound at the
/// collection's home. All keys resolve in one batch through the async routed
/// exchange.
pub(super) async fn resp_kv_surrogates(
    state: &SharedState,
    session: &RespSession,
    keys: &[&[u8]],
) -> Result<Vec<nodedb_types::Surrogate>, RespValue> {
    assign_surrogates_routed(
        state,
        nodedb_types::CollectionKey::from_bare(
            crate::types::DatabaseId::DEFAULT,
            &session.collection,
        ),
        session.tenant_id,
        keys,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| RespValue::err(format!("ERR {e}")))
}

/// [`resp_kv_surrogates`] for one key. A KV atomic op on an existing key keeps
/// that key's identity.
pub(super) async fn resp_kv_surrogate(
    state: &SharedState,
    session: &RespSession,
    key: &[u8],
) -> Result<nodedb_types::Surrogate, RespValue> {
    resp_kv_surrogates(state, session, &[key])
        .await?
        .pop()
        .ok_or_else(|| RespValue::err("ERR the key's home returned no surrogate"))
}
