// SPDX-License-Identifier: BUSL-1.1

//! `CREATE FUNCTION ... LANGUAGE WASM AS <base64>` DDL handler.
//!
//! Parses WASM function DDL, decodes the base64 binary, validates it,
//! carries it in the replicated function proposal, and registers the function
//! through the catalog-entry path.

use crate::control::planner::wasm;
use crate::control::security::catalog::{
    FunctionLanguage, FunctionParam, FunctionSecurity, FunctionVolatility, StoredFunction,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::create::emit_function_put;
use super::parse::parse_function_header;

/// Handle `CREATE [OR REPLACE] FUNCTION ... LANGUAGE WASM AS '<base64>'`
pub fn create_wasm_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create WASM functions")?;

    let parsed = parse_wasm_create(sql)?;
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function_in_database(database_id, tenant_id, &parsed.name)
    {
        return Err(DdlError {
            sqlstate: "42723".to_string(),
            message: format!("function '{}' already exists", parsed.name),
        });
    }

    // Decode base64 binary.
    use base64::Engine;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(&parsed.base64_body)
        .map_err(|e| DdlError {
            sqlstate: "42601".to_string(),
            message: format!("invalid base64: {e}"),
        })?;

    // Validate before proposal; the applier stores the blob on every node.
    let config = wasm::WasmConfig::default();
    let hash =
        wasm::store::validate_wasm_binary(&wasm_bytes, config.max_binary_size).map_err(|e| {
            DdlError {
                sqlstate: "42601".to_string(),
                message: e.to_string(),
            }
        })?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock before UNIX epoch".to_string(),
        })?
        .as_secs();

    let stored = StoredFunction {
        tenant_id,
        database_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: String::new(), // WASM functions have no SQL body
        compiled_body_sql: None,
        volatility: FunctionVolatility::Volatile, // WASM = always volatile
        security: FunctionSecurity::Invoker,
        language: FunctionLanguage::Wasm,
        wasm_hash: Some(hash),
        wasm_module: Some(wasm_bytes),
        dependencies: vec![],
        wasm_fuel: parsed.fuel.unwrap_or(config.default_fuel),
        wasm_memory: parsed.memory.unwrap_or(config.default_memory_bytes),
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(stored.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        crate::control::catalog_entry::post_apply::function::put(stored.clone(), state);
    }
    emit_function_put(state, &stored);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE FUNCTION {} LANGUAGE WASM", stored.name),
    );

    Ok(status("CREATE FUNCTION"))
}

// ─── Parsing ────────────────────────────────────────────────────────────────

struct ParsedWasmCreate {
    or_replace: bool,
    name: String,
    parameters: Vec<FunctionParam>,
    return_type: String,
    base64_body: String,
    fuel: Option<u64>,
    memory: Option<usize>,
}

/// Parse: CREATE [OR REPLACE] FUNCTION name(params) RETURNS type LANGUAGE WASM
///        [WITH (FUEL = N, MEMORY = N)] AS '<base64>'
fn parse_wasm_create(sql: &str) -> Result<ParsedWasmCreate, DdlError> {
    let header = parse_function_header(sql, &[" LANGUAGE "])?;

    // rest starts with "LANGUAGE ..."
    let after_lang_kw = if header
        .rest
        .get(.."LANGUAGE".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("LANGUAGE"))
    {
        header.rest.get("LANGUAGE".len()..).unwrap_or_default()
    } else {
        &header.rest
    };
    let after_lang = after_lang_kw.trim();
    if !after_lang
        .get(.."WASM".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WASM"))
    {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected LANGUAGE WASM".to_string(),
        });
    }
    let after_wasm = after_lang.get("WASM".len()..).unwrap_or_default().trim();

    // Optional WITH (FUEL = N, MEMORY = N)
    let (fuel, memory, after_with) = parse_wasm_with(after_wasm)?;

    // AS '<base64>'
    if !after_with
        .get(.."AS".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"))
    {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected AS '<base64>'".to_string(),
        });
    }
    let body_part = after_with.get("AS".len()..).unwrap_or_default().trim();

    // Extract string literal.
    let base64_body =
        if body_part.starts_with('\'') && body_part.ends_with('\'') && body_part.len() >= 2 {
            body_part
                .get(1..body_part.len() - 1)
                .unwrap_or_default()
                .replace("''", "'")
        } else {
            body_part.to_string()
        };

    if base64_body.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "WASM binary body is empty".to_string(),
        });
    }

    Ok(ParsedWasmCreate {
        or_replace: header.or_replace,
        name: header.name,
        parameters: header.parameters,
        return_type: header.return_type,
        base64_body,
        fuel,
        memory,
    })
}

fn parse_wasm_with(s: &str) -> Result<(Option<u64>, Option<usize>, &str), DdlError> {
    if !s
        .get(.."WITH".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WITH"))
    {
        return Ok((None, None, s));
    }
    let after = s.get("WITH".len()..).unwrap_or_default().trim_start();
    if !after.starts_with('(') {
        return Ok((None, None, s));
    }
    let close = after.find(')').ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "unmatched '(' in WITH".to_string(),
    })?;
    let inner = after.get(1..close).unwrap_or_default();
    let rest = after.get(close + 1..).unwrap_or_default().trim();

    let mut fuel = None;
    let mut memory = None;
    for part in inner.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "FUEL" => fuel = kv[1].parse().ok(),
            "MEMORY" => memory = kv[1].parse().ok(),
            _ => {}
        }
    }

    Ok((fuel, memory, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_wasm_create() {
        let sql = "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert_eq!(parsed.name, "add");
        assert_eq!(parsed.parameters.len(), 2);
        assert_eq!(parsed.return_type, "INT");
        assert_eq!(parsed.base64_body, "AGFzbQ==");
        assert!(!parsed.or_replace);
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE FUNCTION f() RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert!(parsed.or_replace);
    }

    #[test]
    fn parse_with_limits() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM WITH (FUEL = 500000, MEMORY = 8388608) AS 'AGFzbQ=='";
        let parsed = parse_wasm_create(sql).unwrap();
        assert_eq!(parsed.fuel, Some(500000));
        assert_eq!(parsed.memory, Some(8388608));
    }

    #[test]
    fn parse_missing_language() {
        let sql = "CREATE FUNCTION f() RETURNS INT AS 'body'";
        assert!(parse_wasm_create(sql).is_err());
    }
}
