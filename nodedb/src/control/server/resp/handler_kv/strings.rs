// SPDX-License-Identifier: BUSL-1.1

//! Single-key string commands: GET, SET, DEL, EXISTS, GETSET.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;

use super::super::codec::RespValue;
use super::super::command::RespCommand;
use super::super::handler::{dispatch_kv, dispatch_kv_write};
use super::super::payload::{payload_field_i64, payload_json};
use super::super::session::RespSession;
use super::surrogate::resp_kv_surrogate;

pub(in crate::control::server::resp) async fn handle_get(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'get' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: session.collection.clone(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => {
            RespValue::bulk(resp.payload.to_vec())
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_set(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'set' command");
    }

    let key = cmd.args[0].clone();
    let value = cmd.args[1].clone();

    // Parse optional flags: EX, PX, NX, XX.
    let mut ttl_ms: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < cmd.argc() {
        match cmd.arg_str(i).map(|s| s.to_uppercase()) {
            Some(ref flag) if flag == "EX" => {
                if let Some(secs) = cmd.arg_i64(i + 1) {
                    ttl_ms = (secs as u64) * 1000;
                    i += 2;
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
            }
            Some(ref flag) if flag == "PX" => {
                if let Some(ms) = cmd.arg_i64(i + 1) {
                    ttl_ms = ms as u64;
                    i += 2;
                } else {
                    return RespValue::err("ERR value is not an integer or out of range");
                }
            }
            Some(ref flag) if flag == "NX" => {
                nx = true;
                i += 1;
            }
            Some(ref flag) if flag == "XX" => {
                xx = true;
                i += 1;
            }
            _ => {
                return RespValue::err(format!(
                    "ERR syntax error at '{}'",
                    cmd.arg_str(i).unwrap_or("?")
                ));
            }
        }
    }

    // NX/XX conditional write: check existence first.
    if nx || xx {
        let check = PhysicalPlan::Kv(KvOp::Get {
            collection: session.collection.clone(),
            key: key.clone(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        match dispatch_kv(state, session, check).await {
            Ok(resp) => {
                let exists = resp.status == Status::Ok && !resp.payload.is_empty();
                if nx && exists {
                    return RespValue::nil(); // NX: key already exists.
                }
                if xx && !exists {
                    return RespValue::nil(); // XX: key doesn't exist.
                }
            }
            Err(e) => return RespValue::from_error(&e),
        }
    }

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: session.collection.clone(),
        key,
        value,
        ttl_ms,
        surrogate,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_del(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'del' command");
    }

    let keys: Vec<Vec<u8>> = cmd.args.clone();
    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: session.collection.clone(),
        keys,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let count = payload_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::from_error(&e),
    }
}

pub(in crate::control::server::resp) async fn handle_exists(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'exists' command");
    }

    let mut count = 0i64;
    for key in &cmd.args {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: session.collection.clone(),
            key: key.clone(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        match dispatch_kv(state, session, plan).await {
            Ok(resp) if resp.status == Status::Ok && !resp.payload.is_empty() => count += 1,
            Ok(_) => {}
            // A policy refusal is not an absent key: reporting it as one would
            // let EXISTS answer a question the caller is not allowed to ask.
            Err(e) => return RespValue::from_error(&e),
        }
    }

    RespValue::integer(count)
}

/// GETSET key value — atomically set new value and return old.
pub(in crate::control::server::resp) async fn handle_getset(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'getset' command");
    }

    let key = cmd.args[0].clone();
    let new_value = cmd.args[1].clone();

    let surrogate = match resp_kv_surrogate(state, session, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection: session.collection.clone(),
        key,
        new_value,
        surrogate,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            if let Some(serde_json::Value::String(b64)) =
                payload_json(&resp.payload).get("old_value")
                && let Ok(data) =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
            {
                return RespValue::bulk(data);
            }
            RespValue::nil()
        }
        Err(e) => RespValue::from_error(&e),
    }
}
