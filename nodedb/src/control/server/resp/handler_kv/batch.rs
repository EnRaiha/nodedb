// SPDX-License-Identifier: BUSL-1.1

//! Multi-key commands: MGET, MSET.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::{dispatch_kv, dispatch_kv_write};
use super::super::payload::payload_json;
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogate;

pub(in crate::control::server::resp) async fn handle_mget(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'mget' command");
    }

    let plan = PhysicalPlan::Kv(KvOp::BatchGet {
        collection: session.collection.clone(),
        keys: cmd.args.clone(),
        rls_filters: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let values = match payload_json(&resp.payload) {
                serde_json::Value::Array(values) => values,
                _ => Vec::new(),
            };
            let items: Vec<RespValue> = values
                .into_iter()
                .map(|v| match v {
                    serde_json::Value::String(b64) => {
                        match base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            &b64,
                        ) {
                            Ok(data) => RespValue::bulk(data),
                            Err(_) => RespValue::nil(),
                        }
                    }
                    _ => RespValue::nil(),
                })
                .collect();
            RespValue::array(items)
        }
        Ok(_) => RespValue::nil_array(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_mset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 || !cmd.argc().is_multiple_of(2) {
        return RespValue::err("ERR wrong number of arguments for 'mset' command");
    }

    let entries: Vec<(Vec<u8>, Vec<u8>)> = cmd
        .args
        .chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();

    // Assign each entry's stable cross-engine surrogate the same way SET
    // (`handle_set`) does per key -- otherwise MSET rows would land with
    // `Surrogate::ZERO` and be invisible to any surrogate-keyed cross-engine
    // read/join.
    let surrogates = match entries
        .iter()
        .map(|(key, _value)| resp_kv_surrogate(state, session, key))
        .collect::<Result<Vec<_>, RespValue>>()
    {
        Ok(s) => s,
        Err(e) => return e,
    };

    let plan = PhysicalPlan::Kv(KvOp::BatchPut {
        collection: session.collection.clone(),
        entries,
        ttl_ms: 0,
        surrogates,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::from_error(&e),
    }
}
