// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use nodedb_sql::types::{EngineType, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::credential::CredentialStore;
use crate::types::TenantId;
use nodedb_physical::physical_plan::DocumentOp;

use super::delete::convert_delete;
use super::shared::delete_effective_filter;

/// Build a `ConvertContext` whose credential store carries a catalog with
/// two collections under tenant 0 / DEFAULT database: `edges`
/// (`has_implicit_edges = true`) and `plain` (`false`). The returned
/// `TempDir` must be kept alive for the lifetime of the context (it backs
/// the catalog's redb file).
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
    }

    let ctx = ConvertContext {
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

#[test]
fn delete_effective_filter_never_empty_for_non_empty_keys() {
    let keys = vec![
        SqlValue::String("a".to_string()),
        SqlValue::String("b".to_string()),
    ];
    let bytes = delete_effective_filter(&[], &keys).expect("synthesize filter");
    assert!(!bytes.is_empty());
}
