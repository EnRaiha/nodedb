// SPDX-License-Identifier: BUSL-1.1

//! Copy the database-scoped catalog rows of a clone source into its target.
//!
//! `CLONE DATABASE` stamps a shadow descriptor for every source collection.
//! A descriptor carries the collection's shape, not the rows other catalog
//! tables hold about it:
//!
//! - vector index parameters
//! - vector model metadata
//! - column statistics
//! - index records
//! - RLS and redaction policies
//! - triggers
//! - retention policies
//! - alert rules
//! - continuous aggregates
//! - streaming materialized views
//!
//! Each row is keyed by `database_id` and stays in the source unless this
//! module copies it.
//!
//! A clone missing them answers queries differently from the source
//! database:
//!
//! - no vector index builds
//! - the planner costs every scan with no statistics
//! - row-level security stops filtering
//!
//! Every write here is fatal on failure, for the same reason the shadow
//! stamp is. Nothing later re-copies a row that silently failed to land.

use std::collections::{HashMap, HashSet};

use nodedb_types::{DatabaseId, QualifiedCollection};

use crate::control::security::catalog::SystemCatalog;

/// The collection names a clone stamps a descriptor for, per tenant.
///
/// Only active source collections are stamped. A row keyed on any other
/// collection lands in the target with no descriptor to belong to.
type StampedCollections = HashMap<u64, HashSet<String>>;

/// Copy every database-scoped catalog row of `source` into `target`.
///
/// Each row is rewritten to carry `target` before it is written. The copy
/// lands under the clone's own keys. The source rows stay untouched.
///
/// Call this after the target's shadow collection descriptors are stamped.
/// The per-collection policy tables are enumerated from the source's
/// collection list.
pub fn copy_database_metadata(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
) -> crate::Result<()> {
    let mut stamped = StampedCollections::new();
    for coll in catalog
        .load_all_collections(source)?
        .into_iter()
        .filter(|c| c.is_active)
    {
        stamped.entry(coll.tenant_id).or_default().insert(coll.name);
    }

    copy_collection_shape(catalog, source, target, &stamped)?;
    copy_scoped_objects(catalog, source, target)?;
    copy_collection_policies(catalog, source, target, &stamped)
}

/// Copy the rows describing the shape of a collection: vector index build
/// parameters, per-column embedding models, ANALYZE statistics, and the index
/// identity registry.
fn copy_collection_shape(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
    stamped: &StampedCollections,
) -> crate::Result<()> {
    let source_id = source.as_u64();
    let target_id = target.as_u64();

    for mut params in catalog
        .list_vector_index_params_in_database(source_id)?
        .into_iter()
        .filter(|p| is_stamped(stamped, p.tenant_id, &p.collection))
    {
        params.database_id = target_id;
        catalog.put_vector_index_params(&params).map_err(|e| {
            copy_err(
                "vector index params",
                &format!("{}.{}", params.collection, params.field_name),
                target,
                e,
            )
        })?;
    }

    for mut model in catalog
        .list_vector_models_in_database(source_id)?
        .into_iter()
        .filter(|m| is_stamped(stamped, m.tenant_id, &m.collection))
    {
        model.database_id = target_id;
        catalog.put_vector_model(&model).map_err(|e| {
            copy_err(
                "vector model",
                &format!("{}.{}", model.collection, model.column),
                target,
                e,
            )
        })?;
    }

    for mut stats in catalog
        .load_column_stats_in_database(source_id)?
        .into_iter()
        .filter(|s| is_stamped(stamped, s.tenant_id, &s.collection))
    {
        stats.database_id = target_id;
        catalog.put_column_stats(&stats).map_err(|e| {
            copy_err(
                "column stats",
                &format!("{}.{}", stats.collection, stats.column),
                target,
                e,
            )
        })?;
    }

    for mut record in catalog
        .list_index_records_in_database(source_id)?
        .into_iter()
        .filter(|r| r.is_active && is_stamped(stamped, r.tenant_id, &r.collection))
    {
        record.database_id = target_id;
        catalog
            .put_index_record(&record)
            .map_err(|e| copy_err("index record", &record.name, target, e))?;
    }

    Ok(())
}

/// Copy the database-scoped objects that act on the clone's own collections:
/// triggers, retention policies, alert rules, continuous aggregates, and
/// streaming materialized views.
fn copy_scoped_objects(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
) -> crate::Result<()> {
    let source_id = source.as_u64();
    let target_id = target.as_u64();

    for mut trigger in catalog.load_triggers_for_database(source)? {
        trigger.database_id = target;
        catalog
            .put_trigger(&trigger)
            .map_err(|e| copy_err("trigger", &trigger.name, target, e))?;
    }

    // Schedules are deliberately absent. A trigger fires on a write to the
    // clone's own collection, so it belongs to collection shape. A schedule
    // fires on its own clock instead. Copying one makes the clone issue DML
    // nobody asked for, doubling every external effect the source produces.
    // A missing schedule shows up in `SHOW SCHEDULES` and is cheap to recreate.

    for mut policy in catalog.load_retention_policies_in_database(source_id)? {
        policy.database_id = target_id;
        catalog
            .put_retention_policy(&policy)
            .map_err(|e| copy_err("retention policy", &policy.name, target, e))?;
    }

    for mut rule in catalog.load_alert_rules_in_database(source_id)? {
        rule.database_id = target_id;
        catalog
            .put_alert_rule(&rule)
            .map_err(|e| copy_err("alert rule", &rule.name, target, e))?;
    }

    for mut cagg in catalog.list_continuous_aggregates_in_database(source_id)? {
        cagg.database_id = target_id;
        catalog
            .put_continuous_aggregate(&cagg)
            .map_err(|e| copy_err("continuous aggregate", &cagg.name, target, e))?;
    }

    for mut mv in catalog.load_streaming_mvs_for_database(source)? {
        mv.database_id = target;
        catalog
            .put_streaming_mv(&mv)
            .map_err(|e| copy_err("streaming mv", &mv.name, target, e))?;
    }

    Ok(())
}

/// Copy the RLS and redaction policies of every active source collection.
///
/// Both tables key on the database-qualified collection name rather than a
/// leading `database_id`, so each collection is enumerated by its own prefix
/// and the qualified name is rebuilt for the target.
fn copy_collection_policies(
    catalog: &SystemCatalog,
    source: DatabaseId,
    target: DatabaseId,
    stamped: &StampedCollections,
) -> crate::Result<()> {
    for (tenant_id, names) in stamped {
        for name in names {
            let source_name = QualifiedCollection::new(source, name);
            let target_name = QualifiedCollection::new(target, name);

            for mut policy in
                catalog.list_rls_policies_for_collection(*tenant_id, source_name.as_str())?
            {
                policy.collection = target_name.as_str().to_string();
                catalog
                    .put_rls_policy(&policy)
                    .map_err(|e| copy_err("rls policy", &policy.name, target, e))?;
            }

            for mut policy in
                catalog.list_redaction_policies_for_collection(*tenant_id, source_name.as_str())?
            {
                policy.collection = target_name.as_str().to_string();
                catalog
                    .put_redaction_policy(&policy)
                    .map_err(|e| copy_err("redaction policy", &policy.for_role, target, e))?;
            }
        }
    }

    Ok(())
}

/// Whether a row's `(tenant_id, collection)` names a collection the clone
/// stamps a descriptor for.
fn is_stamped(stamped: &StampedCollections, tenant_id: u64, collection: &str) -> bool {
    stamped
        .get(&tenant_id)
        .is_some_and(|names| names.contains(collection))
}

/// Wrap a failed row copy with the table and row it belongs to.
fn copy_err(kind: &str, name: &str, target: DatabaseId, cause: crate::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!(
            "clone: copying {kind} '{name}' into database {} failed: {cause}",
            target.as_u64()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_types::{StoredVectorIndexParams, VectorModelEntry, VectorModelMetadata};

    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::catalog::column_stats::StoredColumnStats;
    use crate::control::security::catalog::index_record::{IndexKind, StoredIndexRecord};

    const TENANT: u64 = 7;
    const SOURCE: DatabaseId = DatabaseId::new(2);
    const TARGET: DatabaseId = DatabaseId::new(3);

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("open catalog");
        (dir, catalog)
    }

    /// Seed one source collection carrying a vector index, a vector model row,
    /// and column statistics.
    fn seed_source(catalog: &SystemCatalog) {
        let mut coll = StoredCollection::new(TENANT, "chunks", "cloner");
        coll.database_id = SOURCE;
        catalog
            .put_collection(SOURCE, &coll)
            .expect("seed collection");

        catalog
            .put_vector_index_params(&StoredVectorIndexParams {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                field_name: "embedding".into(),
                dim: 384,
                metric: "cosine".into(),
                m: 24,
                ef_construction: 200,
                index_type: String::new(),
                pq_m: 0,
                ivf_cells: 0,
                ivf_nprobe: 0,
            })
            .expect("seed vector index params");

        catalog
            .put_index_record(&StoredIndexRecord {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                name: "chunks_emb_idx".into(),
                kind: IndexKind::Vector,
                collection: "chunks".into(),
                fields: vec!["embedding".into()],
                is_active: true,
            })
            .expect("seed index record");

        catalog
            .put_vector_model(&VectorModelEntry {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                column: "embedding".into(),
                metadata: VectorModelMetadata {
                    model: "all-MiniLM-L6-v2".into(),
                    dimensions: 384,
                    created_at: "2026-01-01".into(),
                    strict_dimensions: true,
                },
            })
            .expect("seed vector model");

        catalog
            .put_column_stats(&StoredColumnStats {
                database_id: SOURCE.as_u64(),
                tenant_id: TENANT,
                collection: "chunks".into(),
                column: "body".into(),
                row_count: 10_000,
                null_count: 12,
                distinct_count: 9_800,
                min_value: Some("aardvark".into()),
                max_value: Some("zebra".into()),
                avg_value_len: Some(31),
                analyzed_at: 1_700_000_000_000,
            })
            .expect("seed column stats");
    }

    #[test]
    fn the_target_holds_every_copied_row_under_its_own_database() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let params = catalog
            .get_vector_index_params(TARGET.as_u64(), TENANT, "chunks", "embedding")
            .expect("read params")
            .expect("params must land in the target");
        assert_eq!(params.database_id, TARGET.as_u64());
        assert_eq!(params.dim, 384);
        assert_eq!(params.m, 24);
        assert_eq!(params.ef_construction, 200);
        assert_eq!(params.metric, "cosine");

        let model = catalog
            .get_vector_model(TARGET.as_u64(), TENANT, "chunks", "embedding")
            .expect("read model")
            .expect("model must land in the target");
        assert_eq!(model.database_id, TARGET.as_u64());
        assert_eq!(model.metadata.dimensions, 384);
        assert_eq!(model.metadata.model, "all-MiniLM-L6-v2");

        let stats = catalog
            .load_column_stats(TARGET.as_u64(), TENANT, "chunks")
            .expect("read stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].database_id, TARGET.as_u64());
        assert_eq!(stats[0].row_count, 10_000);
        assert_eq!(stats[0].distinct_count, 9_800);
        assert_eq!(stats[0].max_value.as_deref(), Some("zebra"));

        let records = catalog
            .list_index_records_in_database(TARGET.as_u64())
            .expect("read index records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].database_id, TARGET.as_u64());
        assert_eq!(records[0].name, "chunks_emb_idx");
        assert_eq!(records[0].kind, IndexKind::Vector);
        assert_eq!(records[0].fields, vec!["embedding".to_string()]);
    }

    #[test]
    fn the_source_rows_survive_the_copy_unchanged() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let params = catalog
            .get_vector_index_params(SOURCE.as_u64(), TENANT, "chunks", "embedding")
            .expect("read params")
            .expect("the source keeps its params");
        assert_eq!(params.database_id, SOURCE.as_u64());
        assert_eq!(params.dim, 384);

        let stats = catalog
            .load_column_stats(SOURCE.as_u64(), TENANT, "chunks")
            .expect("read stats");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].database_id, SOURCE.as_u64());
        assert_eq!(stats[0].row_count, 10_000);

        let records = catalog
            .list_index_records_in_database(SOURCE.as_u64())
            .expect("read index records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].database_id, SOURCE.as_u64());
    }

    /// A database the clone never touched keeps an empty metadata set.
    #[test]
    fn an_unrelated_database_gains_nothing() {
        let (_dir, catalog) = open();
        seed_source(&catalog);

        copy_database_metadata(&catalog, SOURCE, TARGET).expect("copy metadata");

        let other = DatabaseId::new(9);
        assert!(
            catalog
                .list_vector_index_params_in_database(other.as_u64())
                .expect("read params")
                .is_empty()
        );
        assert!(
            catalog
                .load_column_stats_in_database(other.as_u64())
                .expect("read stats")
                .is_empty()
        );
    }
}
