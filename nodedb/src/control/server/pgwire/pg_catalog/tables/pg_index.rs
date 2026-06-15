// SPDX-License-Identifier: BUSL-1.1

//! `pg_index` materializer — one row per secondary index.

use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::oid::{stable_collection_oid, stable_index_oid};
use crate::control::server::pgwire::pg_catalog::vquery::VTable;
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType, VValue};
use crate::control::state::SharedState;

use super::collections::load_collections;

pub fn columns() -> Vec<VColumn> {
    vec![
        VColumn::new("indexrelid", VType::Int8),
        VColumn::new("indrelid", VType::Int8),
        VColumn::new("indisunique", VType::Bool),
        VColumn::new("indisprimary", VType::Bool),
    ]
}

pub fn pg_index(state: &SharedState, identity: &AuthenticatedIdentity) -> PgWireResult<VTable> {
    let mut t = VTable::new(columns());
    for coll in load_collections(state, identity) {
        let indrelid = stable_collection_oid(coll.tenant_id, &coll.name);
        for index in &coll.indexes {
            let indexrelid = stable_index_oid(coll.tenant_id, &coll.name, &index.name);
            t.push(vec![
                VValue::Int8(indexrelid),
                VValue::Int8(indrelid),
                VValue::Bool(index.unique),
                VValue::Bool(false),
            ]);
        }
    }
    Ok(t)
}
