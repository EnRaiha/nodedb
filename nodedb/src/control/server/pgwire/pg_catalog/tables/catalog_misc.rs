// SPDX-License-Identifier: BUSL-1.1

//! Small static catalog tables: `pg_namespace`, `pg_database`, `pg_authid`.

use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::pg_catalog::vquery::VTable;
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType, VValue};
use crate::control::state::SharedState;

pub fn pg_namespace_columns() -> Vec<VColumn> {
    vec![
        VColumn::new("oid", VType::Int8),
        VColumn::new("nspname", VType::Text),
        VColumn::new("nspowner", VType::Int8),
    ]
}

pub fn pg_namespace() -> PgWireResult<VTable> {
    let mut t = VTable::new(pg_namespace_columns());
    t.push(vec![
        VValue::Int8(11),
        VValue::Text("pg_catalog".into()),
        VValue::Int8(10),
    ]);
    t.push(vec![
        VValue::Int8(2200),
        VValue::Text("public".into()),
        VValue::Int8(10),
    ]);
    Ok(t)
}

pub fn pg_database_columns() -> Vec<VColumn> {
    vec![
        VColumn::new("oid", VType::Int8),
        VColumn::new("datname", VType::Text),
        VColumn::new("datdba", VType::Text),
        VColumn::new("encoding", VType::Text),
    ]
}

pub fn pg_database() -> PgWireResult<VTable> {
    let mut t = VTable::new(pg_database_columns());
    t.push(vec![
        VValue::Int8(1),
        VValue::Text("nodedb".into()),
        VValue::Text("nodedb".into()),
        VValue::Text("UTF8".into()),
    ]);
    Ok(t)
}

pub fn pg_authid_columns() -> Vec<VColumn> {
    vec![
        VColumn::new("oid", VType::Int8),
        VColumn::new("rolname", VType::Text),
        VColumn::new("rolsuper", VType::Bool),
        VColumn::new("rolcanlogin", VType::Bool),
    ]
}

pub fn pg_authid(state: &SharedState, identity: &AuthenticatedIdentity) -> PgWireResult<VTable> {
    let mut t = VTable::new(pg_authid_columns());
    let users = state.credentials.list_users();
    for (i, user) in users.iter().enumerate() {
        let oid = 10i64 + i as i64;
        let is_super = identity.is_superuser && user == &identity.username;
        t.push(vec![
            VValue::Int8(oid),
            VValue::Text(user.clone()),
            VValue::Bool(is_super),
            VValue::Bool(true),
        ]);
    }
    Ok(t)
}
