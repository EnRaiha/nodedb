// SPDX-License-Identifier: BUSL-1.1

//! The vector writes of one inserted row for its numeric-array fields that
//! no vector index covers. The document write already indexed the covered
//! fields, and a second insert would append a second HNSW node for the row.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::{DatabaseId, Value};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use super::parse::{dispatch_plan, extract_vector_fields};

/// The statement-wide inputs of [`insert_row_vectors`].
pub(super) struct RowVectorCtx<'a, 't> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    pub coll_name: &'a str,
    /// Vector fields an index already covers.
    pub indexed: &'a HashSet<String>,
    pub txn_ctx: &'a DmlTxnCtx<'t>,
}

/// Write the uncovered vectors of the row keyed `doc_id`. Each vector write
/// joins the statement's transaction with the document write, so both
/// commit or neither does.
pub(super) async fn insert_row_vectors(
    ctx: &RowVectorCtx<'_, '_>,
    doc_id: &str,
    fields: &HashMap<String, Value>,
) -> Result<(), DdlError> {
    let state: &SharedState = ctx.state;
    let identity: &AuthenticatedIdentity = ctx.identity;
    let database_id = ctx.database_id;
    let coll_name: &str = ctx.coll_name;
    let indexed: &HashSet<String> = ctx.indexed;
    let txn_ctx: &DmlTxnCtx<'_> = ctx.txn_ctx;
    let tenant_id = identity.tenant_id;
    let key = nodedb_types::CollectionKey::from_bare(database_id, coll_name);
    for (field_name, vector) in extract_vector_fields(fields) {
        if indexed.contains(&field_name) {
            continue;
        }
        let dim = vector.len();
        let column = if field_name.is_empty() {
            "embedding"
        } else {
            field_name.as_str()
        };
        if let Ok(Some(entry)) = state.credentials.catalog().get_vector_model(
            database_id.as_u64(),
            tenant_id.as_u64(),
            coll_name,
            column,
        ) && entry.metadata.strict_dimensions
            && entry.metadata.dimensions != dim
        {
            return Err(DdlError::new(
                "23514",
                format!(
                    "strict_dimensions: vector has {} dimensions, model '{}' requires {}",
                    dim, entry.metadata.model, entry.metadata.dimensions
                ),
            ));
        }
        let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
            state,
            key,
            tenant_id,
            doc_id.as_bytes(),
            crate::types::TraceId::ZERO,
        )
        .await
        .map_err(|e| DdlError::from_error_in_context("surrogate assign", &e))?;
        let plan = crate::bridge::envelope::PhysicalPlan::Vector(VectorOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, coll_name),
            vector,
            dim,
            field_name: field_name.clone(),
            surrogate,
            pk_bytes: Some(doc_id.as_bytes().to_vec()),
            provenance: None,
        });
        if let Some(result) =
            dispatch_plan(state, identity, database_id, key.vshard(), plan, txn_ctx).await
        {
            result?;
        }
    }
    Ok(())
}
