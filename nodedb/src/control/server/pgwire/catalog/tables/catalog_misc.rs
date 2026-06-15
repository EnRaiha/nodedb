// SPDX-License-Identifier: BUSL-1.1

//! Small catalog relations: `pg_namespace`, `pg_database`, `pg_authid`.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::collections::encode_row;

pub fn pg_namespace() -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::with_capacity(2);
    for (oid, name) in [(11i64, "pg_catalog"), (2200i64, "public")] {
        let mut r: HashMap<String, Value> = HashMap::with_capacity(3);
        r.insert("oid".into(), Value::Integer(oid));
        r.insert("nspname".into(), Value::String(name.into()));
        r.insert("nspowner".into(), Value::Integer(10));
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}

pub fn pg_database() -> crate::Result<Vec<Vec<u8>>> {
    let mut r: HashMap<String, Value> = HashMap::with_capacity(4);
    r.insert("oid".into(), Value::Integer(1));
    r.insert("datname".into(), Value::String("nodedb".into()));
    r.insert("datdba".into(), Value::String("nodedb".into()));
    r.insert("encoding".into(), Value::String("UTF8".into()));
    Ok(vec![encode_row(r)?])
}

pub fn pg_authid(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    let users = state.credentials.list_users();
    for (i, user) in users.iter().enumerate() {
        let oid = 10i64 + i as i64;
        let is_super = identity.is_superuser && user == &identity.username;
        let mut r: HashMap<String, Value> = HashMap::with_capacity(4);
        r.insert("oid".into(), Value::Integer(oid));
        r.insert("rolname".into(), Value::String(user.clone()));
        r.insert("rolsuper".into(), Value::Bool(is_super));
        r.insert("rolcanlogin".into(), Value::Bool(true));
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}
