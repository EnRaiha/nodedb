// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK naming for GraphRAG fusion responses.
//!
//! The fusion answers a row named by no graph node with its decimal and its
//! surrogate. This translator names that row by its primary key, through
//! [`super::pk_resolve::resolve_surrogate_pks`], the resolver every search
//! translator runs, and clears the surrogate. Naming runs after fusion, so
//! scores and ranks are the fusion's own. In a cluster it runs on the
//! coordinator, and the resolver asks the collection home leader for every
//! key this node's catalog lacks.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::control::state::SharedState;
use crate::data::executor::response_codec::GraphRagResponse;

use super::pk_resolve::resolve_surrogate_pks;

/// Decode a fusion payload, name its rows, and re-encode it. `collection` is
/// the plan's database-qualified name. A row no binding names keeps its
/// decimal. A payload that does not decode fails the call.
pub async fn translate_rag_fusion_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let mut response: GraphRagResponse =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("graph rag fusion response does not decode: {e}"),
        })?;
    let unnamed: Vec<Surrogate> = response
        .results
        .iter()
        .filter_map(|row| row.surrogate.map(Surrogate::new))
        .collect();
    if unnamed.is_empty() {
        return Ok(payload.to_vec());
    }
    let keys = resolve_surrogate_pks(state, database_id, tenant_id, collection, unnamed).await?;
    for row in &mut response.results {
        if let Some(raw) = row.surrogate.take()
            && let Some(key) = keys.get(Surrogate::new(raw))
        {
            row.node_id = key.to_owned();
        }
    }
    zerompk::to_msgpack_vec(&response).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("graph rag fusion response does not encode: {e}"),
    })
}
