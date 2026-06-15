// SPDX-License-Identifier: BUSL-1.1

//! `pg_attribute` materializer — one row per collection field.

use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::oid::stable_collection_oid;
use crate::control::server::pgwire::pg_catalog::vquery::VTable;
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType, VValue};
use crate::control::state::SharedState;

use super::collections::{field_type_to_oid, load_collections};

pub fn columns() -> Vec<VColumn> {
    vec![
        VColumn::new("attrelid", VType::Int8),
        VColumn::new("attname", VType::Text),
        VColumn::new("atttypid", VType::Int8),
        VColumn::new("attstattarget", VType::Int4),
        VColumn::new("attlen", VType::Int4),
        VColumn::new("attnum", VType::Int4),
        VColumn::new("attndims", VType::Int4),
        VColumn::new("attcacheoff", VType::Int4),
        VColumn::new("atttypmod", VType::Int4),
        VColumn::new("attbyval", VType::Bool),
        VColumn::new("attstorage", VType::Text),
        VColumn::new("attalign", VType::Text),
        VColumn::new("attnotnull", VType::Bool),
        VColumn::new("atthasdef", VType::Bool),
        VColumn::new("attidentity", VType::Text),
        VColumn::new("attgenerated", VType::Text),
        VColumn::new("attisdropped", VType::Bool),
        VColumn::new("attislocal", VType::Bool),
        VColumn::new("attinhcount", VType::Int4),
        VColumn::new("attcollation", VType::Int8),
    ]
}

pub fn pg_attribute(state: &SharedState, identity: &AuthenticatedIdentity) -> PgWireResult<VTable> {
    let mut t = VTable::new(columns());
    for coll in load_collections(state, identity) {
        let rel_oid = stable_collection_oid(coll.tenant_id, &coll.name);
        for (col_num, (field_name, field_type)) in coll.fields.iter().enumerate() {
            t.push(vec![
                VValue::Int8(rel_oid),
                VValue::Text(field_name.clone()),
                VValue::Int8(field_type_to_oid(field_type)),
                VValue::Int4(-1),
                VValue::Int4(-1),
                VValue::Int4((col_num + 1) as i32),
                VValue::Int4(0),
                VValue::Int4(-1),
                VValue::Int4(-1),
                VValue::Bool(false),
                VValue::Text("p".into()),
                VValue::Text("i".into()),
                VValue::Bool(false),
                VValue::Bool(false),
                VValue::Text(String::new()),
                VValue::Text(String::new()),
                VValue::Bool(false),
                VValue::Bool(true),
                VValue::Int4(0),
                VValue::Int8(0),
            ]);
        }
    }
    Ok(t)
}
