// SPDX-License-Identifier: BUSL-1.1

//! `pg_index` row producer — one row per secondary index.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog::oid::{stable_collection_oid, stable_index_oid};
use crate::control::state::SharedState;

use super::collections::{encode_row, load_collections};

pub fn pg_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    for coll in load_collections(state, identity) {
        let indrelid = stable_collection_oid(coll.tenant_id, &coll.name);
        for index in &coll.indexes {
            let indexrelid = stable_index_oid(coll.tenant_id, &coll.name, &index.name);
            let mut r: HashMap<String, Value> = HashMap::with_capacity(4);
            r.insert("indexrelid".into(), Value::Integer(indexrelid));
            r.insert("indrelid".into(), Value::Integer(indrelid));
            r.insert("indisunique".into(), Value::Bool(index.unique));
            r.insert("indisprimary".into(), Value::Bool(false));
            rows.push(encode_row(r)?);
        }
    }
    Ok(rows)
}
