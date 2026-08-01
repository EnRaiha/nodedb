// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use nodedb_sql::types::{EngineType, SqlExpr, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::credential::CredentialStore;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp};

use super::super::insert::{ConvertInsertArgs, convert_insert};
use super::delete::convert_delete;
use super::shared::delete_effective_filter;
use super::update::{UpdateParams, convert_update};

/// Build a `ConvertContext` whose credential store carries a catalog with
/// three collections under tenant 0 / DEFAULT database: `edges`
/// (`has_implicit_edges = true`), `plain` (all flags false), and `crdt_coll`
/// (`crdt = true`). The returned `TempDir` must be kept alive for the lifetime
/// of the context (it backs the catalog's redb file).
fn ctx_with_catalog() -> (ConvertContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store =
        CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store");
    {
        let catalog = store.catalog();
        let mut edges = StoredCollection::new(0, "edges", "owner");
        edges.has_implicit_edges = true;
        catalog
            .put_collection(crate::types::DatabaseId::DEFAULT, &edges)
            .expect("put edges collection");
        let plain = StoredCollection::new(0, "plain", "owner");
        catalog
            .put_collection(crate::types::DatabaseId::DEFAULT, &plain)
            .expect("put plain collection");
        let mut crdt_coll = StoredCollection::new(0, "crdt_coll", "owner");
        crdt_coll.crdt = true;
        catalog
            .put_collection(crate::types::DatabaseId::DEFAULT, &crdt_coll)
            .expect("put crdt collection");
    }

    let ctx = ConvertContext {
        purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
        retention_registry: None,
        array_catalog: None,
        credentials: Some(Arc::new(store)),
        wal: None,
        surrogate_assigner: None,
        cluster_enabled: false,
        bitemporal_retention_registry: None,
        max_vector_dim: 0,
        force_shuffle_join: false,
        shuffle_num_parts: 0,
        force_shuffle_agg: false,
        shuffle_agg_num_parts: 0,
        broadcast_threshold_bytes: 8 * 1024 * 1024,
        shuffle_agg_threshold: 10_000,
        database_id: crate::types::DatabaseId::DEFAULT,
        tenant_id: crate::types::TenantId::new(0),
    };
    (ctx, dir)
}

#[test]
fn pk_delete_on_edge_bearing_collection_routes_bulk_delete() {
    let (ctx, _dir) = ctx_with_catalog();
    let keys = vec![SqlValue::String("edge_3".to_string())];
    let tasks = convert_delete(
        "edges",
        &EngineType::DocumentSchemaless,
        &[],
        &keys,
        TenantId::new(0),
        &ctx,
    )
    .expect("convert_delete");
    assert_eq!(tasks.len(), 1);
    match &tasks[0].plan {
        PhysicalPlan::Document(DocumentOp::BulkDelete { filters, .. }) => {
            // Synthesized PK filter is never empty for a non-empty target_keys.
            assert!(
                !filters.is_empty(),
                "edge-bearing PK delete must carry a non-empty filter"
            );
        }
        other => panic!("expected BulkDelete, got {other:?}"),
    }
}

#[test]
fn pk_delete_on_non_edge_collection_routes_point_delete() {
    let (ctx, _dir) = ctx_with_catalog();
    let keys = vec![SqlValue::String("row_1".to_string())];
    let tasks = convert_delete(
        "plain",
        &EngineType::DocumentSchemaless,
        &[],
        &keys,
        TenantId::new(0),
        &ctx,
    )
    .expect("convert_delete");
    assert_eq!(tasks.len(), 1);
    assert!(
        matches!(
            &tasks[0].plan,
            PhysicalPlan::Document(DocumentOp::PointDelete { .. })
        ),
        "non-edge-bearing PK delete must remain a PointDelete"
    );
}

fn crdt_row(id: &str) -> Vec<(String, SqlValue)> {
    vec![
        ("id".to_string(), SqlValue::String(id.to_string())),
        ("name".to_string(), SqlValue::String("alice".to_string())),
    ]
}

#[test]
fn insert_into_crdt_collection_routes_doc_upsert() {
    let (ctx, _dir) = ctx_with_catalog();
    let rows = vec![crdt_row("k1")];
    let tasks = convert_insert(ConvertInsertArgs {
        collection: "crdt_coll",
        engine: &EngineType::DocumentSchemaless,
        rows: &rows,
        column_defaults: &[],
        column_schema: &[],
        if_absent: false,
        primary_key: Some("id"),
        tenant_id: TenantId::new(0),
        ctx: &ctx,
    })
    .expect("convert_insert");
    assert_eq!(tasks.len(), 1);
    match &tasks[0].plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            document_id,
            fields_json,
            partial,
            ..
        }) => {
            assert_eq!(document_id, "k1");
            assert!(!partial, "INSERT must be a full-replace DocUpsert");
            assert!(fields_json.contains("alice"));
        }
        other => panic!("expected CrdtOp::DocUpsert, got {other:?}"),
    }
}

#[test]
fn insert_into_non_crdt_collection_routes_point_insert() {
    let (ctx, _dir) = ctx_with_catalog();
    let rows = vec![crdt_row("k1")];
    let tasks = convert_insert(ConvertInsertArgs {
        collection: "plain",
        engine: &EngineType::DocumentSchemaless,
        rows: &rows,
        column_defaults: &[],
        column_schema: &[],
        if_absent: false,
        primary_key: Some("id"),
        tenant_id: TenantId::new(0),
        ctx: &ctx,
    })
    .expect("convert_insert");
    assert_eq!(tasks.len(), 1);
    assert!(
        matches!(
            &tasks[0].plan,
            PhysicalPlan::Document(DocumentOp::PointInsert { .. })
        ),
        "non-crdt INSERT must remain a PointInsert"
    );
}

fn update_params<'a>(
    collection: &'a str,
    assignments: &'a [(String, SqlExpr)],
    target_keys: &'a [SqlValue],
    returning: bool,
    ctx: &'a ConvertContext,
) -> UpdateParams<'a> {
    UpdateParams {
        collection,
        engine: &EngineType::DocumentSchemaless,
        assignments,
        filters: &[],
        target_keys,
        returning,
        tenant_id: TenantId::new(0),
        ctx,
    }
}

#[test]
fn update_set_literal_on_crdt_pk_routes_doc_upsert_partial() {
    let (ctx, _dir) = ctx_with_catalog();
    let assignments = vec![(
        "name".to_string(),
        SqlExpr::Literal(SqlValue::String("bob".to_string())),
    )];
    let keys = vec![SqlValue::String("k1".to_string())];
    let tasks = convert_update(update_params("crdt_coll", &assignments, &keys, false, &ctx))
        .expect("convert_update");
    assert_eq!(tasks.len(), 1);
    match &tasks[0].plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            document_id,
            fields_json,
            partial,
            ..
        }) => {
            assert_eq!(document_id, "k1");
            assert!(partial, "UPDATE SET must be a partial DocUpsert");
            assert!(fields_json.contains("name") && fields_json.contains("bob"));
            assert!(
                !fields_json.contains("\"id\""),
                "partial payload must carry only SET fields, got {fields_json}"
            );
        }
        other => panic!("expected partial CrdtOp::DocUpsert, got {other:?}"),
    }
}

#[test]
fn delete_by_pk_on_crdt_routes_doc_delete() {
    let (ctx, _dir) = ctx_with_catalog();
    let keys = vec![SqlValue::String("k1".to_string())];
    let tasks = convert_delete(
        "crdt_coll",
        &EngineType::DocumentSchemaless,
        &[],
        &keys,
        TenantId::new(0),
        &ctx,
    )
    .expect("convert_delete");
    assert_eq!(tasks.len(), 1);
    match &tasks[0].plan {
        PhysicalPlan::Crdt(CrdtOp::DocDelete { document_id, .. }) => {
            assert_eq!(document_id, "k1");
        }
        other => panic!("expected CrdtOp::DocDelete, got {other:?}"),
    }
}

#[test]
fn predicate_update_on_crdt_rejects() {
    let (ctx, _dir) = ctx_with_catalog();
    let assignments = vec![(
        "name".to_string(),
        SqlExpr::Literal(SqlValue::String("bob".to_string())),
    )];
    let err = convert_update(update_params("crdt_coll", &assignments, &[], false, &ctx))
        .expect_err("predicate UPDATE on crdt must reject");
    assert!(matches!(err, crate::Error::BadRequest { .. }));
}

#[test]
fn predicate_delete_on_crdt_rejects() {
    let (ctx, _dir) = ctx_with_catalog();
    let err = convert_delete(
        "crdt_coll",
        &EngineType::DocumentSchemaless,
        &[],
        &[],
        TenantId::new(0),
        &ctx,
    )
    .expect_err("predicate DELETE on crdt must reject");
    assert!(matches!(err, crate::Error::BadRequest { .. }));
}

#[test]
fn non_literal_rhs_update_on_crdt_rejects() {
    let (ctx, _dir) = ctx_with_catalog();
    let assignments = vec![(
        "name".to_string(),
        SqlExpr::Column {
            table: None,
            name: "other".to_string(),
        },
    )];
    let keys = vec![SqlValue::String("k1".to_string())];
    let err = convert_update(update_params("crdt_coll", &assignments, &keys, false, &ctx))
        .expect_err("non-literal RHS UPDATE on crdt must reject");
    assert!(matches!(err, crate::Error::BadRequest { .. }));
}

#[test]
fn update_returning_on_crdt_routes_to_doc_upsert() {
    // RETURNING is stripped before planning and re-injected downstream
    // (pgwire `inject_returning_spec` attaches the spec to the CrdtOp, and the
    // DP handler emits the projected rows). So at the convert layer a RETURNING
    // UPDATE on a crdt collection routes to `DocUpsert` exactly like a plain
    // UPDATE — it is NOT rejected. `returning` on the op is `None` here; the
    // spec is attached later at the protocol boundary.
    let (ctx, _dir) = ctx_with_catalog();
    let assignments = vec![(
        "name".to_string(),
        SqlExpr::Literal(SqlValue::String("bob".to_string())),
    )];
    let keys = vec![SqlValue::String("k1".to_string())];
    let tasks = convert_update(update_params("crdt_coll", &assignments, &keys, true, &ctx))
        .expect("UPDATE ... RETURNING on crdt must route, not reject");
    assert_eq!(tasks.len(), 1);
    match &tasks[0].plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            partial, returning, ..
        }) => {
            assert!(partial, "UPDATE SET must be a partial DocUpsert");
            assert!(
                returning.is_none(),
                "RETURNING spec is attached downstream, not at convert"
            );
        }
        other => panic!("expected partial CrdtOp::DocUpsert, got {other:?}"),
    }
}

#[test]
fn delete_effective_filter_never_empty_for_non_empty_keys() {
    let keys = vec![
        SqlValue::String("a".to_string()),
        SqlValue::String("b".to_string()),
    ];
    let bytes = delete_effective_filter(&[], &keys).expect("synthesize filter");
    assert!(!bytes.is_empty());
}
