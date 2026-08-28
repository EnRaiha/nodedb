// SPDX-License-Identifier: BUSL-1.1

//! `CREATE AGGREGATE FUNCTION ... LANGUAGE WASM AS <base64>` DDL handler.
//!
//! The WASM aggregate-export validation is retained while the function
//! definition is committed through the replicated catalog-entry path.

use crate::control::planner::wasm;
use crate::control::security::catalog::FunctionParam;
use crate::control::security::catalog::function_types::*;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::create::emit_function_put;
use super::parse::{find_matching_paren, parse_parameters, validate_identifier};

/// Handle `CREATE [OR REPLACE] AGGREGATE FUNCTION <name>(<input_type>)
///         RETURNS <type> LANGUAGE WASM AS '<base64>'`
pub fn create_wasm_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create WASM aggregate functions")?;

    let parsed = parse_aggregate_create(sql)?;
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function_in_database(database_id, tenant_id, &parsed.name)
    {
        return Err(DdlError::new(
            "42723",
            format!("function '{}' already exists", parsed.name),
        ));
    }

    // Decode base64 binary.
    use base64::Engine;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(&parsed.base64_body)
        .map_err(|e| DdlError::new("42601", format!("invalid base64: {e}")))?;

    // Validate before proposal; the applier stores the blob on every node.
    let config = wasm::WasmConfig::default();
    let hash = wasm::store::validate_wasm_binary(&wasm_bytes, config.max_binary_size)
        .map_err(|e| DdlError::new("42601", e.to_string()))?;

    // Validate aggregate exports (init, accumulate, merge, finalize).
    let runtime =
        wasm::runtime::WasmRuntime::new().map_err(|e| DdlError::new("XX000", e.to_string()))?;
    let module = runtime
        .get_or_compile(&wasm_bytes)
        .map_err(|e| DdlError::new("XX000", e.to_string()))?;
    wasm::wit::validate_aggregate_exports(&module)
        .map_err(|e| DdlError::new("42601", e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError::new("XX000", "system clock"))?
        .as_secs();

    // Store as a function with language=WASM. The "aggregate" nature is
    // indicated by the name prefix "agg_" in the WASM exports and by
    // the fact that it will be registered as AggregateUDF, not ScalarUDF.
    let stored = StoredFunction {
        tenant_id,
        database_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: "AGGREGATE".into(), // Marker for aggregate functions
        compiled_body_sql: None,
        volatility: FunctionVolatility::Volatile,
        security: FunctionSecurity::Invoker,
        language: FunctionLanguage::Wasm,
        wasm_hash: Some(hash),
        wasm_module: Some(wasm_bytes),
        dependencies: vec![],
        wasm_fuel: config.default_fuel,
        wasm_memory: config.default_memory_bytes,
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(stored.clone()));
    let outcome = propose_and_apply(state, &entry)?;
    if outcome.needs_local_apply() {
        crate::control::catalog_entry::post_apply::function::put(stored.clone(), state);
    }
    emit_function_put(state, &stored);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE AGGREGATE FUNCTION {} LANGUAGE WASM", stored.name),
    );

    Ok(status("CREATE AGGREGATE FUNCTION"))
}

struct ParsedAggregateCreate {
    or_replace: bool,
    name: String,
    parameters: Vec<FunctionParam>,
    return_type: String,
    base64_body: String,
}

fn parse_aggregate_create(sql: &str) -> Result<ParsedAggregateCreate, DdlError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();

    let (or_replace, after) = if trimmed
        .get(.."CREATE OR REPLACE AGGREGATE FUNCTION ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE OR REPLACE AGGREGATE FUNCTION "))
    {
        (
            true,
            trimmed
                .get("CREATE OR REPLACE AGGREGATE FUNCTION ".len()..)
                .unwrap_or_default(),
        )
    } else if trimmed
        .get(.."CREATE AGGREGATE FUNCTION ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE AGGREGATE FUNCTION "))
    {
        (
            false,
            trimmed
                .get("CREATE AGGREGATE FUNCTION ".len()..)
                .unwrap_or_default(),
        )
    } else {
        return Err(DdlError::new("42601", "expected CREATE AGGREGATE FUNCTION"));
    };

    let paren_open = after
        .find('(')
        .ok_or_else(|| DdlError::new("42601", "expected '('"))?;
    let name = after
        .get(..paren_open)
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    validate_identifier(&name)?;

    let paren_close = find_matching_paren(after, paren_open)
        .ok_or_else(|| DdlError::new("42601", "unmatched '('"))?;
    let params_str = after.get(paren_open + 1..paren_close).unwrap_or_default();
    let parameters = parse_parameters(params_str)?;

    let rest = after.get(paren_close + 1..).unwrap_or_default().trim();

    if !rest
        .get(.."RETURNS ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("RETURNS "))
    {
        return Err(DdlError::new("42601", "expected RETURNS <type>"));
    }
    let after_returns = rest.get("RETURNS ".len()..).unwrap_or_default().trim();

    let lang_pos = find_ascii_case_insensitive(after_returns, "LANGUAGE")
        .ok_or_else(|| DdlError::new("42601", "expected LANGUAGE WASM"))?;
    let return_type = after_returns
        .get(..lang_pos)
        .unwrap_or_default()
        .trim()
        .to_uppercase();

    let after_lang = after_returns
        .get(lang_pos + "LANGUAGE".len()..)
        .unwrap_or_default()
        .trim();
    if !after_lang
        .get(.."WASM".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WASM"))
    {
        return Err(DdlError::new("42601", "expected LANGUAGE WASM"));
    }
    let after_wasm = after_lang.get("WASM".len()..).unwrap_or_default().trim();

    if !after_wasm
        .get(.."AS".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"))
    {
        return Err(DdlError::new("42601", "expected AS '<base64>'"));
    }
    let body = after_wasm.get("AS".len()..).unwrap_or_default().trim();
    let base64_body = if body.starts_with('\'') && body.ends_with('\'') && body.len() >= 2 {
        body.get(1..body.len() - 1)
            .unwrap_or_default()
            .replace("''", "'")
    } else {
        body.to_string()
    };

    Ok(ParsedAggregateCreate {
        or_replace,
        name,
        parameters,
        return_type,
        base64_body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql =
            "CREATE AGGREGATE FUNCTION my_sum(val INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        let parsed = parse_aggregate_create(sql).unwrap();
        assert_eq!(parsed.name, "my_sum");
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.return_type, "INT");
        assert!(!parsed.or_replace);
    }

    #[test]
    fn parse_or_replace() {
        let sql =
            "CREATE OR REPLACE AGGREGATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='";
        assert!(parse_aggregate_create(sql).unwrap().or_replace);
    }
}
