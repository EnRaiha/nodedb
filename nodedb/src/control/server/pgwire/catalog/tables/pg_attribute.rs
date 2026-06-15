// SPDX-License-Identifier: BUSL-1.1

//! `pg_attribute` row producer — one row per collection field.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog::oid::stable_collection_oid;
use crate::control::state::SharedState;

use super::collections::{encode_row, field_type_to_oid, load_collections};

pub fn pg_attribute(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    for coll in load_collections(state, identity) {
        let rel_oid = stable_collection_oid(coll.tenant_id, &coll.name);
        for (col_num, (field_name, field_type)) in coll.fields.iter().enumerate() {
            let mut r: HashMap<String, Value> = HashMap::with_capacity(20);
            r.insert("attrelid".into(), Value::Integer(rel_oid));
            r.insert("attname".into(), Value::String(field_name.clone()));
            r.insert(
                "atttypid".into(),
                Value::Integer(field_type_to_oid(field_type)),
            );
            r.insert("attstattarget".into(), Value::Integer(-1));
            r.insert("attlen".into(), Value::Integer(-1));
            r.insert("attnum".into(), Value::Integer((col_num + 1) as i64));
            r.insert("attndims".into(), Value::Integer(0));
            r.insert("attcacheoff".into(), Value::Integer(-1));
            r.insert("atttypmod".into(), Value::Integer(-1));
            r.insert("attbyval".into(), Value::Bool(false));
            r.insert("attstorage".into(), Value::String("p".into()));
            r.insert("attalign".into(), Value::String("i".into()));
            r.insert("attnotnull".into(), Value::Bool(false));
            r.insert("atthasdef".into(), Value::Bool(false));
            r.insert("attidentity".into(), Value::String(String::new()));
            r.insert("attgenerated".into(), Value::String(String::new()));
            r.insert("attisdropped".into(), Value::Bool(false));
            r.insert("attislocal".into(), Value::Bool(true));
            r.insert("attinhcount".into(), Value::Integer(0));
            r.insert("attcollation".into(), Value::Integer(0));
            rows.push(encode_row(r)?);
        }
    }
    Ok(rows)
}
