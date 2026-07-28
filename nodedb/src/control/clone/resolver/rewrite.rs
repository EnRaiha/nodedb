// SPDX-License-Identifier: BUSL-1.1

//! Plan rewriting from a target-database read into the equivalent source-database
//! read at the effective source LSN.

use std::sync::Arc;

use nodedb_types::DatabaseId;
use nodedb_types::TenantId;

use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp};
use nodedb_types::SystemTimeScope;

/// Compute the source-side system-time selection for a clone scan rewrite.
///
/// Snapshot clones read the source at a fixed point-in-time (`effective_ms`),
/// so they always collapse to an `AsOf` (or the plan's own selection when the
/// clone carries no ceiling). `AllVersions` (audit log) does not compose with a
/// snapshot clone — the request is rejected with a typed error rather than
/// silently picking an arbitrary snapshot.
fn rewrite_system_time(
    effective_ms: Option<i64>,
    plan_scope: SystemTimeScope,
) -> crate::Result<SystemTimeScope> {
    if matches!(plan_scope, SystemTimeScope::AllVersions) {
        return Err(crate::Error::PlanError {
            detail: "AS OF SYSTEM TIME NULL (all-versions) cannot be read through a \
                     snapshot clone; query the source database directly"
                .into(),
        });
    }
    match effective_ms {
        Some(ms) => Ok(SystemTimeScope::AsOf(ms)),
        None => Ok(plan_scope),
    }
}

/// Rewrite a `PhysicalPlan` to target the source database and collection at
/// the effective source LSN.  Returns `None` for plan types that are not
/// read-type operations (writes, DDL), and also returns `None` for
/// `DocumentOp::PointGet` when the source database has no surrogate binding
/// for `pk_bytes` — i.e. the row never existed in the source, so there is
/// nothing to fetch and no source task should be created.
///
/// `state` is used to resolve the source surrogate for `DocumentOp::PointGet`
/// rewrites — the target surrogate is not valid in the source database, so we
/// perform a read-only catalog lookup against the source-qualified collection.
/// Per-call inputs for [`rewrite_plan_for_source`].
///
/// Bundled into a struct so the function stays under the clippy
/// `too_many_arguments` cap as snapshot-isolation knobs are added.
pub struct RewriteForSourceParams<'a> {
    pub plan: &'a PhysicalPlan,
    pub target_db_id: DatabaseId,
    pub source_db_id: DatabaseId,
    pub tenant_id: TenantId,
    pub target_coll: &'a str,
    pub source_coll: &'a str,
    /// Effective source system-time-ms for `AS OF` rewrites (Document /
    /// Columnar / Timeseries scans).  `None` leaves any pre-existing
    /// `system_as_of_ms` on the plan untouched.
    pub effective_source_ms: Option<i64>,
    /// Source surrogate high-water captured at clone-create time.
    /// Threaded into rewritten KV plans so the source-side scan/get
    /// filters out bindings allocated AFTER the clone's AS-OF
    /// (snapshot isolation for the lazy KV read path).
    pub kv_surrogate_ceiling: Option<u32>,
    pub state: &'a Arc<SharedState>,
}

pub fn rewrite_plan_for_source(
    params: RewriteForSourceParams<'_>,
) -> crate::Result<Option<PhysicalPlan>> {
    use crate::control::planner::sql_plan_convert::convert::db_qualified;

    let RewriteForSourceParams {
        plan,
        target_db_id,
        source_db_id,
        tenant_id,
        target_coll,
        source_coll,
        effective_source_ms,
        kv_surrogate_ceiling,
        state,
    } = params;
    let target_qualified = db_qualified(target_db_id, target_coll);
    let source_qualified = db_qualified(source_db_id, source_coll);

    match plan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            limit,
            offset,
            sort_keys,
            filters,
            distinct,
            projection,
            computed_columns,
            window_functions,
            system_time,
            valid_at_ms,
            prefilter,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(Some(PhysicalPlan::Document(DocumentOp::Scan {
                collection: source_qualified,
                limit: *limit,
                offset: *offset,
                sort_keys: sort_keys.clone(),
                filters: filters.clone(),
                distinct: *distinct,
                projection: projection.clone(),
                computed_columns: computed_columns.clone(),
                window_functions: window_functions.clone(),
                system_time,
                valid_at_ms: *valid_at_ms,
                prefilter: prefilter.clone(),
            })))
        }

        PhysicalPlan::Document(DocumentOp::PointGet {
            collection,
            document_id,
            surrogate: _,
            pk_bytes,
            rls_filters,
            system_time,
            valid_at_ms,
        }) if collection == &target_qualified => {
            // The target surrogate is not valid in the source database — each
            // database maintains its own (collection, pk_bytes) → surrogate
            // mapping.  If the source has no binding for this pk, the row
            // never existed there and there is nothing to fetch — skip the
            // source task entirely (return None) rather than dispatching a
            // task with a sentinel surrogate.
            //
            // Lookup errors are also treated as "skip": surfacing them here
            // would force every read to wait on a degraded surrogate-store
            // probe, but they are still observable in the surrogate
            // assigner's own metrics/logs.
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            let Some(source_surrogate) = state
                .surrogate_assigner
                .lookup(source_db_id, tenant_id, &source_qualified, pk_bytes)
                .ok()
                .flatten()
            else {
                return Ok(None);
            };
            Ok(Some(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: source_qualified,
                document_id: document_id.clone(),
                surrogate: source_surrogate,
                pk_bytes: pk_bytes.clone(),
                rls_filters: rls_filters.clone(),
                system_time,
                valid_at_ms: *valid_at_ms,
            })))
        }

        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection,
            path,
            value,
            filters,
            projection,
            limit,
            offset,
        }) if collection == &target_qualified => {
            Ok(Some(PhysicalPlan::Document(DocumentOp::IndexedFetch {
                collection: source_qualified,
                path: path.clone(),
                value: value.clone(),
                filters: filters.clone(),
                projection: projection.clone(),
                limit: *limit,
                offset: *offset,
            })))
        }

        PhysicalPlan::Kv(KvOp::Scan {
            collection,
            cursor,
            count,
            filters,
            match_pattern,
            sort_keys,
            // The original target-side scan never carries a ceiling
            // (clones-of-clones still funnel through here per-level);
            // the resolver overrides it for source delegation below.
            surrogate_ceiling: _,
        }) if collection == &target_qualified => Ok(Some(PhysicalPlan::Kv(KvOp::Scan {
            collection: source_qualified,
            cursor: cursor.clone(),
            count: *count,
            filters: filters.clone(),
            match_pattern: match_pattern.clone(),
            sort_keys: sort_keys.clone(),
            surrogate_ceiling: kv_surrogate_ceiling,
        }))),

        PhysicalPlan::Kv(KvOp::Get {
            collection,
            key,
            rls_filters,
            surrogate_ceiling: _,
        }) if collection == &target_qualified => Ok(Some(PhysicalPlan::Kv(KvOp::Get {
            collection: source_qualified,
            key: key.clone(),
            rls_filters: rls_filters.clone(),
            surrogate_ceiling: kv_surrogate_ceiling,
        }))),

        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection,
            projection,
            limit,
            filters,
            rls_filters,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(Some(PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: source_qualified,
                projection: projection.clone(),
                limit: *limit,
                filters: filters.clone(),
                rls_filters: rls_filters.clone(),
                sort_keys: sort_keys.clone(),
                system_time,
                valid_at_ms: *valid_at_ms,
                prefilter: prefilter.clone(),
                computed_columns: computed_columns.clone(),
            })))
        }

        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection,
            time_range,
            projection,
            limit,
            filters,
            sort_keys,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            rls_filters,
            system_time,
            valid_at_ms,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(Some(PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: source_qualified,
                time_range: *time_range,
                projection: projection.clone(),
                limit: *limit,
                filters: filters.clone(),
                sort_keys: sort_keys.clone(),
                bucket_interval_ms: *bucket_interval_ms,
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                gap_fill: gap_fill.clone(),
                computed_columns: computed_columns.clone(),
                rls_filters: rls_filters.clone(),
                system_time,
                valid_at_ms: *valid_at_ms,
            })))
        }

        // Write operations, DDL, and all other engine plans are not replicated
        // to the source — they execute against the target only.  Exhaustively
        // listing every PhysicalPlan top-level variant ensures the compiler
        // catches any newly added variants.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(None),
    }
}

/// Extract the raw (db_qualified) collection name from a plan.
pub(super) fn extract_collection_from_plan(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(DocumentOp::Scan { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::PointGet { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. }) => Some(collection),
        PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. }) => Some(collection),
        PhysicalPlan::Kv(KvOp::Scan { collection, .. }) => Some(collection),
        PhysicalPlan::Kv(KvOp::Get { collection, .. }) => Some(collection),
        PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. }) => Some(collection),
        PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. }) => Some(collection),
        // All other plan types do not carry a collection in a form the clone
        // resolver needs to inspect.  Exhaustively listing every top-level
        // PhysicalPlan variant here ensures the compiler reports new variants.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Strip the `"<db_id>/"` prefix added by `db_qualified()`, returning the
/// bare collection name.  If the collection was stored without a prefix
/// (default database, id == 0), the string is returned as-is.
pub(super) fn strip_db_prefix(db_id: DatabaseId, qualified: &str) -> &str {
    if db_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", db_id.as_u64());
    if let Some(stripped) = qualified.strip_prefix(prefix.as_str()) {
        stripped
    } else {
        qualified
    }
}
