// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral sequence DDL — CREATE / ALTER / DROP / SHOW / DESCRIBE.
//!
//! Ported from the pgwire `ddl::sequence` handlers. All non-return logic
//! (StoredSequence build, validation, sequence-registry ops, catalog proposes,
//! `schema_version.bump()`, tenant scoping) is preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::catalog::propose_and_apply;
use super::super::result::{DdlError, DdlResult};

/// A `CREATE SEQUENCE` request. Fields mirror the typed
/// `CollectionStmt::CreateSequence` AST variant.
pub struct CreateSequenceRequest<'a> {
    pub name: &'a str,
    pub if_not_exists: bool,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
    pub cache: Option<i64>,
    pub format_template_raw: Option<&'a str>,
    pub reset_period_raw: Option<&'a str>,
    pub gap_free: bool,
    /// Parsed but not yet consumed by the handler (mirrors the pgwire
    /// handler's ignored `_scope` argument); retained for field fidelity.
    #[allow(dead_code)]
    pub scope: Option<&'a str>,
}

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Handle `CREATE [IF NOT EXISTS] SEQUENCE <name> [options…]`.
///
/// The `IF NOT EXISTS` existence short-circuit is folded in from the pgwire
/// guard: an already-existing sequence returns the tag before any option
/// parsing, preserving the guard's error-free early return. The `IF NOT
/// EXISTS` + non-existing case never reaches here — the neutral router returns
/// `None` for it so dispatch falls through to the planner, matching today.
pub fn create_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateSequenceRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // IF NOT EXISTS: swallow duplicate-creation (folded from the pgwire guard).
    // Placed before any building so an existing sequence returns the tag
    // without option parsing — matching the guard's behavior exactly.
    if req.if_not_exists && state.sequence_registry.exists(tenant_id, req.name) {
        return Ok(status("CREATE SEQUENCE"));
    }

    let mut def = StoredSequence::new(tenant_id, req.name.to_string(), identity.username.clone());

    if let Some(s) = req.start {
        def.start_value = s;
    }
    if let Some(inc) = req.increment {
        def.increment = inc;
    }
    if let Some(min) = req.min_value {
        def.min_value = min;
    }
    if let Some(max) = req.max_value {
        def.max_value = max;
    }
    def.cycle = req.cycle;
    if let Some(c) = req.cache {
        def.cache_size = c;
    }
    if let Some(fmt) = req.format_template_raw {
        let tokens =
            crate::control::sequence::format::parse_format_template(fmt).map_err(|e| DdlError {
                sqlstate: "42601".to_string(),
                message: format!("invalid FORMAT: {e}"),
            })?;
        def.format_template = Some(tokens);
    }
    if let Some(reset) = req.reset_period_raw {
        def.reset_scope =
            crate::control::sequence::format::ResetScope::parse(reset).map_err(|e| DdlError {
                sqlstate: "42601".to_string(),
                message: e.to_string(),
            })?;
    }
    def.gap_free = req.gap_free;

    // Apply defaults for descending sequences.
    if def.increment < 0 && def.min_value == 1 && def.max_value == i64::MAX {
        def.max_value = -1;
        def.min_value = i64::MIN;
        if def.start_value == 1 {
            def.start_value = -1;
        }
    }

    def.created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    def.validate().map_err(|e| DdlError {
        sqlstate: "42P17".to_string(),
        message: e.to_string(),
    })?;

    if state.sequence_registry.exists(tenant_id, &def.name) {
        return Err(DdlError {
            sqlstate: "42P07".to_string(),
            message: format!("sequence \"{}\" already exists", def.name),
        });
    }

    let entry = crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(def.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        state.sequence_registry.create(def).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;
    }

    state.schema_version.bump();

    Ok(status("CREATE SEQUENCE"))
}

/// Handle `ALTER SEQUENCE <name> RESTART [WITH <value>] | FORMAT '<template>'`.
pub fn alter_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
    with_value: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    if !state.sequence_registry.exists(tenant_id, name) {
        return Err(DdlError {
            sqlstate: "42P01".to_string(),
            message: format!("sequence \"{name}\" does not exist"),
        });
    }

    match action.to_uppercase().as_str() {
        "RESTART" => alter_restart(state, tenant_id, name, with_value),
        "FORMAT" => alter_format(state, tenant_id, name, with_value),
        _ => Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "ALTER SEQUENCE supports: RESTART [WITH value], FORMAT 'template'".to_string(),
        }),
    }
}

/// `ALTER SEQUENCE <name> RESTART [WITH <value>]`
fn alter_restart(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    with_value: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    let restart_value = if let Some(v) = with_value.and_then(|s| s.parse::<i64>().ok()) {
        v
    } else {
        state
            .sequence_registry
            .get_def(tenant_id, name)
            .map(|d| d.start_value)
            .unwrap_or(1)
    };

    // RESTART touches the sequence *state* (current counter), not the
    // definition. Propose a `PutSequenceState` entry so every node's in-memory
    // registry converges on the new counter value.
    let def = state
        .sequence_registry
        .get_def(tenant_id, name)
        .ok_or(DdlError {
            sqlstate: "42P01".to_string(),
            message: format!("sequence \"{name}\" does not exist"),
        })?;
    let new_state = crate::control::security::catalog::sequence_types::SequenceState {
        tenant_id,
        name: name.to_string(),
        current_value: restart_value,
        is_called: false,
        epoch: def.epoch,
        period_key: String::new(),
    };
    let entry = crate::control::catalog_entry::CatalogEntry::PutSequenceState(Box::new(new_state));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;
    if log_index == 0 {
        state
            .sequence_registry
            .restart(tenant_id, name, restart_value)
            .map_err(|e| DdlError {
                sqlstate: "22023".to_string(),
                message: e.to_string(),
            })?;
        {
            let catalog = state.credentials.catalog();
            state.sequence_registry.persist_all(catalog);
        }
    }

    Ok(status("ALTER SEQUENCE"))
}

/// `ALTER SEQUENCE <name> FORMAT '<template>'`
fn alter_format(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    with_value: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    let Some(raw) = with_value else {
        return Ok(status("ALTER SEQUENCE"));
    };
    let raw = raw.trim_matches('\'').trim_matches('"');
    let tokens =
        crate::control::sequence::format::parse_format_template(raw).map_err(|e| DdlError {
            sqlstate: "42601".to_string(),
            message: format!("invalid FORMAT: {e}"),
        })?;

    // FORMAT alters the stored *definition*, not the counter — ship the whole
    // updated `StoredSequence` through `PutSequence` and let every node's
    // applier replace it in redb + registry.
    if let Some(mut def) = state.sequence_registry.get_def(tenant_id, name) {
        def.format_template = Some(tokens);
        let entry = crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(def.clone()));
        let log_index = propose_and_apply(state, &entry)?;
        if log_index == 0 {
            let _ = state.sequence_registry.remove(tenant_id, name);
            let _ = state.sequence_registry.create(def);
        }
    }
    Ok(status("ALTER SEQUENCE"))
}

/// Handle `DROP [IF EXISTS] SEQUENCE <name>`.
///
/// Takes the typed `(name, if_exists)` from the `DropSequence` AST variant.
/// The pgwire guard's IF EXISTS short-circuit is folded in: a non-existing
/// sequence with `if_exists` returns the tag; without it, errors 42P01.
pub fn drop_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    if !state.sequence_registry.exists(tenant_id, name) {
        if if_exists {
            return Ok(status("DROP SEQUENCE"));
        }
        return Err(DdlError {
            sqlstate: "42P01".to_string(),
            message: format!("sequence \"{name}\" does not exist"),
        });
    }

    // Propose the delete through the metadata raft group. Every node's applier
    // removes the record from local redb and from its in-memory registry.
    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSequence {
        tenant_id,
        name: name.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;
    if log_index == 0 {
        // Single-node / no-cluster fallback.
        {
            let catalog = state.credentials.catalog();
            let _ = catalog.delete_sequence(tenant_id, name);
        }
        let _ = state.sequence_registry.remove(tenant_id, name);
    }

    state.schema_version.bump();

    Ok(status("DROP SEQUENCE"))
}

/// Handle `SHOW SEQUENCES`.
pub fn show_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let sequences = state.sequence_registry.list(tenant_id);

    let columns = vec![
        "name".to_string(),
        "current_value".to_string(),
        "called".to_string(),
    ];

    let mut rows = Vec::with_capacity(sequences.len());
    for (name, current_value, is_called) in &sequences {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(name.clone()));
        row.insert(
            "current_value".to_string(),
            JsonValue::String(current_value.to_string()),
        );
        row.insert(
            "called".to_string(),
            JsonValue::String(is_called.to_string()),
        );
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// Handle `DESCRIBE SEQUENCE <name>`.
pub fn describe_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = name.to_lowercase();

    let def = state
        .sequence_registry
        .get_def(tenant_id, &name)
        .ok_or(DdlError {
            sqlstate: "42P01".to_string(),
            message: format!("sequence \"{name}\" does not exist"),
        })?;

    let format_str = def
        .format_template
        .as_ref()
        .map(|_| "(defined)")
        .unwrap_or("(none)");

    let reset_str = match def.reset_scope {
        crate::control::sequence::ResetScope::Never => "NEVER",
        crate::control::sequence::ResetScope::Yearly => "YEARLY",
        crate::control::sequence::ResetScope::Monthly => "MONTHLY",
        crate::control::sequence::ResetScope::Quarterly => "QUARTERLY",
        crate::control::sequence::ResetScope::Daily => "DAILY",
    };

    let props = [
        ("name", def.name.clone()),
        ("start_value", def.start_value.to_string()),
        ("increment", def.increment.to_string()),
        ("min_value", def.min_value.to_string()),
        ("max_value", def.max_value.to_string()),
        ("cycle", def.cycle.to_string()),
        ("cache_size", def.cache_size.to_string()),
        ("format", format_str.to_string()),
        ("reset_scope", reset_str.to_string()),
        ("gap_free", def.gap_free.to_string()),
    ];

    let columns = vec!["property".to_string(), "value".to_string()];

    let mut rows = Vec::with_capacity(props.len());
    for (k, v) in &props {
        let mut row = Map::new();
        row.insert("property".to_string(), JsonValue::String(k.to_string()));
        row.insert("value".to_string(), JsonValue::String(v.clone()));
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
