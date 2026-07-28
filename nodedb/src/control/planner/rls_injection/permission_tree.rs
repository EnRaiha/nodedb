// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree filter injection.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::FilterOp;
use crate::control::security::auth_context::AuthContext;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, QueryOp};
use nodedb_physical::physical_task::PhysicalTask;

use super::filters::merge_filters;

// ── Permission Tree Injection ──────────────────────────────────────────────

/// Inject permission tree filters into physical tasks.
///
/// For each task whose collection has a `PermissionTreeDef`, resolves the
/// set of accessible resource IDs for the current user and injects an
/// `IN (...)` ScanFilter on the resource column.
///
/// Called after `inject_rls()` — permission tree filters are AND-combined
/// with standard RLS filters.
///
/// Superusers bypass permission tree filtering entirely.
pub fn inject_permission_tree(
    tasks: &mut [PhysicalTask],
    cache: &crate::control::security::permission_tree::PermissionCache,
    auth: &AuthContext,
) -> crate::Result<()> {
    if auth.is_superuser() {
        return Ok(());
    }
    for task in tasks.iter_mut() {
        let tenant_id = task.tenant_id.as_u64();
        inject_permission_tree_for_plan(tenant_id, &mut task.plan, cache, auth)?;
    }
    Ok(())
}

/// Core dispatch: inject permission tree filter into a single physical plan.
///
/// Selects the required permission level based on the operation type:
/// - Scans/reads → `def.read_level`
/// - Upserts/inserts → `def.write_level`
/// - Deletes → `def.delete_level`
fn inject_permission_tree_for_plan(
    tenant_id: u64,
    plan: &mut PhysicalPlan,
    cache: &crate::control::security::permission_tree::PermissionCache,
    auth: &AuthContext,
) -> crate::Result<()> {
    // Extract (collection, mutable filters, required_level_key).
    let (collection, filters, level_key) = match plan {
        // Read operations.
        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            filters,
            ..
        }) => (collection.as_str(), Some(filters), PermTreeLevel::Read),

        // Write operations (upsert/insert routed through DocumentOp::Upsert).
        PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
        | PhysicalPlan::Kv(KvOp::Put { collection, .. })
        | PhysicalPlan::Kv(KvOp::Insert { collection, .. })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent { collection, .. })
        | PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate { collection, .. })
        | PhysicalPlan::Kv(KvOp::BatchPut { collection, .. }) => {
            (collection.as_str(), None, PermTreeLevel::Write)
        }

        // Delete operations.
        PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Kv(KvOp::Delete { collection, .. }) => {
            (collection.as_str(), None, PermTreeLevel::Delete)
        }

        _ => return Ok(()),
    };

    let Some(def) = cache.get_tree_def(tenant_id, collection) else {
        return Ok(()); // No permission tree on this collection.
    };

    let required_level = match level_key {
        PermTreeLevel::Read => &def.read_level,
        PermTreeLevel::Write => &def.write_level,
        PermTreeLevel::Delete => &def.delete_level,
    };

    let user_id = &auth.id;
    let user_roles = &auth.roles;

    // For write/delete operations without scan filters, do a blanket permission check:
    // the user must have at least the required level on ANY resource in the tree.
    // Per-row filtering is only possible for scan operations.
    if filters.is_none() {
        let accessible = crate::control::security::permission_tree::resolver::accessible_resources(
            cache,
            def,
            tenant_id,
            user_id,
            user_roles,
            required_level,
        );
        if accessible.is_empty() {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: TenantId::new(tenant_id),
                resource: format!(
                    "permission tree on '{collection}': user has no '{required_level}' access"
                ),
            });
        }
        return Ok(());
    }

    // For scan operations, inject an IN filter on the resource column.
    let accessible = crate::control::security::permission_tree::resolver::accessible_resources(
        cache,
        def,
        tenant_id,
        user_id,
        user_roles,
        required_level,
    );

    let in_filter = crate::bridge::scan_filter::ScanFilter {
        field: def.resource_column.clone(),
        op: FilterOp::In,
        value: nodedb_types::Value::Array(
            accessible
                .into_iter()
                .map(nodedb_types::Value::String)
                .collect(),
        ),
        clauses: Vec::new(),
        expr: None,
    };

    let filter_bytes =
        zerompk::to_msgpack_vec(&vec![in_filter]).map_err(|e| crate::Error::PlanError {
            detail: format!("permission tree filter serialization: {e}"),
        })?;

    if let Some(filters) = filters {
        merge_filters(filters, &filter_bytes)?;
    }

    Ok(())
}

/// Which permission level to check based on operation type.
enum PermTreeLevel {
    Read,
    Write,
    Delete,
}
