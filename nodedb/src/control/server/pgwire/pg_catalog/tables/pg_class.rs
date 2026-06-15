// SPDX-License-Identifier: BUSL-1.1

//! `pg_class` materializer — one row per visible collection.

use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::oid::stable_collection_oid;
use crate::control::server::pgwire::pg_catalog::vquery::VTable;
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType, VValue};
use crate::control::state::SharedState;

use super::collections::{has_secondary_index, load_collections};

pub fn columns() -> Vec<VColumn> {
    vec![
        VColumn::new("oid", VType::Int8),
        VColumn::new("relname", VType::Text),
        VColumn::new("relnamespace", VType::Int8),
        VColumn::new("reltype", VType::Int8),
        VColumn::new("relam", VType::Int8),
        VColumn::new("relfilenode", VType::Int8),
        VColumn::new("relpages", VType::Int4),
        VColumn::new("relkind", VType::Text),
        VColumn::new("relnatts", VType::Int4),
        VColumn::new("relchecks", VType::Int4),
        VColumn::new("relhasindex", VType::Bool),
        VColumn::new("relisshared", VType::Bool),
        VColumn::new("relpersistence", VType::Text),
        VColumn::new("relhasrules", VType::Bool),
        VColumn::new("relhastriggers", VType::Bool),
        VColumn::new("relhassubclass", VType::Bool),
        VColumn::new("relrowsecurity", VType::Bool),
        VColumn::new("relispartition", VType::Bool),
        VColumn::new("relreplident", VType::Text),
        VColumn::new("relowner", VType::Int8),
    ]
}

pub fn pg_class(state: &SharedState, identity: &AuthenticatedIdentity) -> PgWireResult<VTable> {
    let mut t = VTable::new(columns());
    for coll in load_collections(state, identity) {
        let oid = stable_collection_oid(coll.tenant_id, &coll.name);
        let has_index = has_secondary_index(&coll);
        let has_triggers = !coll.event_defs.is_empty();
        t.push(vec![
            VValue::Int8(oid),
            VValue::Text(coll.name.clone()),
            VValue::Int8(2200),
            VValue::Int8(0),
            VValue::Int8(2),
            VValue::Int8(oid),
            VValue::Int4(0),
            VValue::Text("r".into()),
            VValue::Int4(coll.fields.len() as i32),
            VValue::Int4(0),
            VValue::Bool(has_index),
            VValue::Bool(false),
            VValue::Text("p".into()),
            VValue::Bool(false),
            VValue::Bool(has_triggers),
            VValue::Bool(false),
            VValue::Bool(false),
            VValue::Bool(false),
            VValue::Text("d".into()),
            VValue::Int8(10),
        ]);
    }
    Ok(t)
}
