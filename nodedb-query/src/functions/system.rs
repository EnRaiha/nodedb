// SPDX-License-Identifier: Apache-2.0

//! System/metadata scalar functions (version, ...).

use nodedb_types::Value;

/// Evaluate a system scalar function; returns `None` if `name` is not handled.
pub(crate) fn try_eval(name: &str, _args: &[Value]) -> Option<Value> {
    match name {
        "version" => Some(Value::String(nodedb_types::pg_compat::version_string())),
        _ => None,
    }
}
