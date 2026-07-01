// SPDX-License-Identifier: BUSL-1.1

//! Shared implementation behind `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! The two surface DDLs differ in only five places: the error label
//! ("collection" vs "table"), whether an empty column list is allowed,
//! the default `CollectionType` when no engine is named (schemaless
//! vs strict), the audit-log verb, and the pgwire response tag.
//! Everything in between — name validation, duplicate check, engine
//! validation, schema construction, vector-primary parsing, flag
//! validation, `StoredCollection` assembly, propose+apply, SERIAL
//! sequence auto-creation, vector-field auto-config — is identical.
//!
//! Inlining all of it in two sibling files gave us a 270-line file in
//! `handler.rs` and a 269-line near-copy in `table.rs` whose only real
//! difference was the five points above; any cross-cutting fix had to
//! be made twice and stayed in sync only by reviewer vigilance.
//! [`build_and_persist`] is the single body; [`Variant`] supplies the
//! five differences declaratively.

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;
use super::super::super::schema_validation::{
    extract_vector_fields, parse_fields_clause_from_pairs,
};
use super::enforcement::{parse_balanced_clause_from_raw, resolve_custom_type_columns};
use super::engine_option::validate_engine_name;
use super::request::CreateCollectionRequest;

/// Per-surface configuration. The fields are the entire surface-level
/// difference between `CREATE COLLECTION` and `CREATE TABLE`.
pub struct Variant {
    /// Object-class label used in the duplicate-name / empty-columns
    /// error messages and in the audit log entry.
    /// `"collection"` for CREATE COLLECTION, `"table"` for CREATE TABLE.
    pub label: &'static str,
    /// pgwire response tag returned on success.
    /// `"CREATE COLLECTION"` / `"CREATE TABLE"`.
    pub response_tag: &'static str,
    /// CREATE TABLE requires a column list by convention; CREATE
    /// COLLECTION accepts an empty one (schemaless documents).
    pub require_columns: bool,
    /// `default_strict` argument to `build_collection_type` when no
    /// engine is named in WITH: CREATE COLLECTION → schemaless,
    /// CREATE TABLE → strict.
    pub default_strict: bool,
}

/// Shared body. Validates the request, builds the
/// `StoredCollection`, replicates it through the metadata raft
/// group, and runs the post-create side effects (SERIAL sequence
/// auto-creation, vector-field logging, audit).
pub async fn build_and_persist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateCollectionRequest<'_>,
    database_id: DatabaseId,
    variant: &Variant,
) -> PgWireResult<Vec<Response>> {
    let CreateCollectionRequest {
        name,
        engine,
        columns,
        options,
        flags,
        balanced_raw,
    } = *req;

    validate_name(name, variant.label)?;
    if variant.require_columns && columns.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "CREATE TABLE requires a column list; for schemaless collections use CREATE COLLECTION",
        ));
    }

    let tenant_id = identity.tenant_id;

    // Check if the object already exists.
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(existing)) = catalog.get_collection(database_id, tenant_id.as_u64(), name)
    {
        if existing.is_active {
            return Err(sqlstate_error(
                "42P07",
                &format!("{} '{name}' already exists", variant.label),
            ));
        }

        // Soft-deleted collection with the same name. A re-CREATE is an
        // explicit request for a FRESH collection, distinct from UNDROP
        // recovery — so the old catalog row and its Data Plane storage
        // keys must be gone before the new collection registers over the
        // reused `{db}:{tenant}:{name}:` storage prefix. Otherwise the
        // stale rows resurrect until the retention GC runs (days later).
        //
        // Hard-purge synchronously through the SAME path DROP ... PURGE
        // uses: remove the catalog row + reclaim every engine's storage
        // on the Data Plane, awaiting completion before we proceed. The
        // WAL tombstone boundary is the current `next_lsn`: every pre-drop
        // row sits below it and is shadowed on replay, while every row the
        // new collection writes sits at or above it and survives.
        let purge_lsn = state.wal.next_lsn().as_u64();
        // Fail closed: if the hard-purge could not remove the old
        // catalog row, ABORT the CREATE rather than build a new
        // collection over un-purged data (which would resurrect the
        // stale rows). Surface as an internal error to the client.
        super::super::purge::hard_purge_collection(state, tenant_id.as_u64(), name, purge_lsn)
            .await
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let canonical_engine = validate_engine_name(engine, options)?;
    let bitemporal_flag = flags.iter().any(|f| f == "BITEMPORAL");

    // Resolve user-defined type names to TEXT for schema building.
    // Original names are preserved in `fields` for drop-protection.
    let resolved_columns: Vec<(String, String)> =
        resolve_custom_type_columns(columns, state, tenant_id.as_u64());

    let (collection_type, columnar_schema_columns) = nodedb_sql::ddl_ast::build_collection_type(
        canonical_engine,
        &resolved_columns,
        options,
        bitemporal_flag,
        variant.default_strict,
    )
    .map_err(|e| sqlstate_error("42601", &e.to_string()))?;

    let (mut fields, serial_fields) = parse_fields_clause_from_pairs(columns);
    if fields.is_empty() && !columnar_schema_columns.is_empty() {
        fields = columnar_schema_columns;
    }

    let schema_json = match &collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            sonic_rs::to_string(schema).ok()
        }
        nodedb_types::CollectionType::KeyValue(config) => sonic_rs::to_string(config).ok(),
        _ => None,
    };

    let (primary, vector_primary) =
        resolve_primary_engine(options, columns, &fields, &collection_type)?;

    let append_only = flags.iter().any(|f| f == "APPEND_ONLY");
    let hash_chain = flags.iter().any(|f| f == "HASH_CHAIN");
    let bitemporal = bitemporal_flag;
    if hash_chain && !append_only {
        return Err(sqlstate_error("42601", "HASH_CHAIN requires APPEND_ONLY"));
    }
    let balanced = parse_balanced_clause_from_raw(balanced_raw.unwrap_or(""))
        .map_err(|e| sqlstate_error("42601", &e))?;

    let partition_strategy =
        nodedb_types::PartitionStrategy::default_for_collection_type(&collection_type);

    // Extract the declared PRIMARY KEY column name (if any) from the raw
    // column list. Recorded on every engine so schemaless collections can
    // key their document id off it instead of the hardcoded `id` field;
    // harmless for strict/KV, which already track the PK on their schema.
    let declared_primary_key = columns.iter().find_map(|(col_name, type_str)| {
        let (_, is_pk, _, _) =
            nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full(type_str);
        is_pk.then(|| col_name.clone())
    });

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        constraint_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type,
        timeseries_config: schema_json,
        is_active: true,
        append_only,
        hash_chain,
        balanced,
        last_chain_hash: None,
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
        type_guards: Vec::new(),
        check_constraints: Vec::new(),
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        bitemporal,
        permission_tree_def: None,
        indexes: Vec::new(),
        size_bytes_estimate: 0,
        primary,
        vector_primary,
        partition_strategy,
        database_id,
        cloned_from: None,
        clone_status: nodedb_types::CloneStatus::default(),
        has_implicit_edges: false,
        declared_primary_key,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    super::super::super::catalog_propose::propose_and_apply(state, &entry)?;

    log_vector_fields(name, &coll.fields);
    create_serial_sequences(state, identity, name, &serial_fields, now)?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created {} '{name}'", variant.label),
    );

    Ok(vec![Response::Execution(Tag::new(variant.response_tag))])
}

/// Reject names that aren't `[A-Za-z0-9_-]+`. Both `collection` and
/// `table` share the rule; only the error label differs.
fn validate_name(name: &str, label: &str) -> PgWireResult<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "invalid {label} name '{name}': only letters, digits, '-', and '_' are allowed"
            ),
        ));
    }
    Ok(())
}

/// Resolve `PrimaryEngine` + optional `VectorPrimaryConfig` from the
/// WITH-clause `primary=` / `vector_field=` knobs. Validates the
/// vector field exists in the column list and the declared `dim`
/// matches the column's `VECTOR(n)` type when both are present.
fn resolve_primary_engine(
    options: &[(String, String)],
    columns: &[(String, String)],
    fields: &[(String, String)],
    collection_type: &nodedb_types::CollectionType,
) -> PgWireResult<(
    nodedb_types::PrimaryEngine,
    Option<nodedb_types::VectorPrimaryConfig>,
)> {
    match nodedb_sql::ddl_ast::parse::vector_primary::parse_vector_primary_options_from_kvs(options)
    {
        Ok(Some(mut vp_cfg)) => {
            let col_list: Vec<(String, String)> = if fields.is_empty() {
                columns.to_vec()
            } else {
                fields.to_vec()
            };
            nodedb_sql::ddl_ast::parse::vector_primary::validate_vector_field(&vp_cfg, &col_list)
                .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
            nodedb_sql::ddl_ast::parse::vector_primary::validate_payload_indexes(
                &mut vp_cfg,
                &col_list,
            )
            .map_err(|e| sqlstate_error("42601", &e.to_string()))?;
            // Infer dim from VECTOR(n) column type when not in WITH clause.
            if let Some((_, type_str)) = col_list
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&vp_cfg.vector_field))
            {
                let upper_t = type_str.to_uppercase();
                if let Some(inner) = upper_t
                    .strip_prefix("VECTOR(")
                    .and_then(|s| s.strip_suffix(')'))
                    && let Ok(d) = inner.trim().parse::<u32>()
                {
                    if vp_cfg.dim == 0 {
                        vp_cfg.dim = d;
                    } else if vp_cfg.dim != d {
                        return Err(sqlstate_error(
                            "42601",
                            &format!(
                                "vector dim mismatch: WITH clause specifies {}, column type VECTOR({}) specifies {}",
                                vp_cfg.dim, d, d
                            ),
                        ));
                    }
                }
            }
            Ok((nodedb_types::PrimaryEngine::Vector, Some(vp_cfg)))
        }
        Ok(None) => Ok((
            nodedb_types::PrimaryEngine::infer_from_collection_type(collection_type),
            None,
        )),
        Err(e) => Err(sqlstate_error("42601", &e.to_string())),
    }
}

/// INFO-log every detected vector field so operators can see what
/// the engine auto-configured during a CREATE.
fn log_vector_fields(collection_name: &str, fields: &[(String, String)]) {
    let vector_fields = extract_vector_fields(fields);
    for (field_name, _dim, metric) in &vector_fields {
        tracing::info!(
            name = %collection_name,
            field = %field_name,
            %metric,
            "auto-configuring vector field"
        );
    }
}

/// Materialise one `StoredSequence` per `SERIAL` column declared on
/// the new collection. Each sequence rides the same propose+apply
/// path as a standalone `CREATE SEQUENCE` so the OWNERS row lands
/// alongside it.
fn create_serial_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection_name: &str,
    serial_fields: &[String],
    now: u64,
) -> PgWireResult<()> {
    for field_name in serial_fields {
        let seq_name = format!("{collection_name}_{field_name}_seq");
        let mut seq_def = crate::control::security::catalog::sequence_types::StoredSequence::new(
            identity.tenant_id.as_u64(),
            seq_name.clone(),
            identity.username.clone(),
        );
        seq_def.created_at = now;
        // Route the auto-created sequence through the proposer +
        // local apply path so the OWNERS row lands alongside the
        // sequence row — the same architectural guarantee CREATE
        // SEQUENCE has, applied to SERIAL columns.
        let seq_entry =
            crate::control::catalog_entry::CatalogEntry::PutSequence(Box::new(seq_def.clone()));
        super::super::super::catalog_propose::propose_and_apply(state, &seq_entry)?;
        let _ = state.sequence_registry.create(seq_def);
        tracing::info!(
            collection = %collection_name,
            field = %field_name,
            sequence = %seq_name,
            "auto-created SERIAL sequence"
        );
    }
    Ok(())
}
