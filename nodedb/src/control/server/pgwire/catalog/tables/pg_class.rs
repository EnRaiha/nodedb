// SPDX-License-Identifier: BUSL-1.1

//! `pg_class` row producer — one row per visible collection.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog::oid::stable_collection_oid;
use crate::control::state::SharedState;

use super::collections::{encode_row, has_secondary_index, load_collections};

pub fn pg_class(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    for coll in load_collections(state, identity) {
        let oid = stable_collection_oid(coll.tenant_id, &coll.name);
        let has_index = has_secondary_index(&coll);
        let has_triggers = !coll.event_defs.is_empty();
        let mut r: HashMap<String, Value> = HashMap::with_capacity(20);
        r.insert("oid".into(), Value::Integer(oid));
        r.insert("relname".into(), Value::String(coll.name.clone()));
        r.insert("relnamespace".into(), Value::Integer(2200));
        r.insert("reltype".into(), Value::Integer(0));
        r.insert("relam".into(), Value::Integer(2));
        r.insert("relfilenode".into(), Value::Integer(oid));
        r.insert("relpages".into(), Value::Integer(0));
        r.insert("relkind".into(), Value::String("r".into()));
        r.insert("relnatts".into(), Value::Integer(coll.fields.len() as i64));
        r.insert("relchecks".into(), Value::Integer(0));
        r.insert("relhasindex".into(), Value::Bool(has_index));
        r.insert("relisshared".into(), Value::Bool(false));
        r.insert("relpersistence".into(), Value::String("p".into()));
        r.insert("relhasrules".into(), Value::Bool(false));
        r.insert("relhastriggers".into(), Value::Bool(has_triggers));
        r.insert("relhassubclass".into(), Value::Bool(false));
        r.insert("relrowsecurity".into(), Value::Bool(false));
        r.insert("relispartition".into(), Value::Bool(false));
        r.insert("relreplident".into(), Value::String("d".into()));
        r.insert("relowner".into(), Value::Integer(10));
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}
