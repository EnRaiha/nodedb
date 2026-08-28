// SPDX-License-Identifier: BUSL-1.1

//! CDC-driven cache invalidation for the permission tree.
//!
//! When rows in the permission table or resource hierarchy are mutated,
//! the in-memory cache must be updated to reflect the change. This module
//! provides functions to handle each kind of mutation.

use tracing::debug;

use super::cache::PermissionCache;
use super::types::PermissionGrant;

/// Handle an INSERT/UPDATE on the permission grant table.
///
/// Upserts the grant into the cache. The grant's `resource_id` and `grantee`
/// determine the cache key; the `level` and `inherited` are the new values.
pub fn on_grant_upsert(cache: &mut PermissionCache, tenant_id: u64, grant: &PermissionGrant) {
    cache.put_grant(tenant_id, grant);
    cache.bump_tenant_version(tenant_id);
    debug!(
        tenant_id,
        resource_id = %grant.resource_id,
        grantee = %grant.grantee,
        level = %grant.level,
        "permission_tree: grant upserted"
    );
}

/// Handle a DELETE on the permission grant table.
///
/// Removes the grant from the cache. Downstream queries will fall back to
/// inherited permissions from ancestors.
pub fn on_grant_delete(
    cache: &mut PermissionCache,
    tenant_id: u64,
    resource_id: &str,
    grantee: &str,
) {
    cache.remove_grant(tenant_id, resource_id, grantee);
    cache.bump_tenant_version(tenant_id);
    debug!(
        tenant_id,
        resource_id, grantee, "permission_tree: grant deleted"
    );
}

/// Handle an INSERT/UPDATE of a resource hierarchy edge.
///
/// The resource `child_id` now has parent `parent_id`. Updates the parent map
/// and children map accordingly.
pub fn on_edge_upsert(
    cache: &mut PermissionCache,
    tenant_id: u64,
    child_id: &str,
    parent_id: &str,
) {
    // Remove old edge first (if child was previously under a different parent).
    cache.remove_edge(tenant_id, child_id);
    cache.put_edge(tenant_id, child_id, parent_id);
    cache.bump_tenant_version(tenant_id);
    debug!(
        tenant_id,
        child_id, parent_id, "permission_tree: edge upserted"
    );
}

/// Handle a DELETE of a resource hierarchy edge.
///
/// The resource `child_id` is no longer under any parent (becomes a root or
/// is being deleted entirely).
pub fn on_edge_delete(cache: &mut PermissionCache, tenant_id: u64, child_id: &str) {
    cache.remove_edge(tenant_id, child_id);
    cache.bump_tenant_version(tenant_id);
    debug!(tenant_id, child_id, "permission_tree: edge deleted");
}

/// Bulk reload all edges and grants for a tenant.
///
/// Called on startup or when the cache is suspected to be stale.
/// Clears existing state for the tenant before loading.
pub fn full_reload(
    cache: &mut PermissionCache,
    tenant_id: u64,
    edges: &[(String, String)],
    grants: &[PermissionGrant],
) {
    cache.load_edges(tenant_id, edges);
    cache.load_grants(tenant_id, grants);
    cache.bump_tenant_version(tenant_id);
    debug!(
        tenant_id,
        edges = edges.len(),
        grants = grants.len(),
        "permission_tree: full reload complete"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_mutator_bumps_the_tenant_version() {
        let mut cache = PermissionCache::new();
        assert_eq!(cache.tenant_version(1), 0);

        let grant = PermissionGrant {
            resource_id: "doc-1".into(),
            grantee: "user-1".into(),
            level: "editor".into(),
            inherited: false,
        };
        on_grant_upsert(&mut cache, 1, &grant);
        assert_eq!(cache.tenant_version(1), 1);

        on_grant_delete(&mut cache, 1, "doc-1", "user-1");
        assert_eq!(cache.tenant_version(1), 2);

        on_edge_upsert(&mut cache, 1, "doc-1", "folder-a");
        assert_eq!(cache.tenant_version(1), 3);

        on_edge_delete(&mut cache, 1, "doc-1");
        assert_eq!(cache.tenant_version(1), 4);

        full_reload(&mut cache, 1, &[], &[]);
        assert_eq!(cache.tenant_version(1), 5);

        // An unrelated tenant is never touched.
        assert_eq!(cache.tenant_version(2), 0);
    }

    #[test]
    fn grant_upsert_and_delete() {
        let mut cache = PermissionCache::new();

        let grant = PermissionGrant {
            resource_id: "doc-1".into(),
            grantee: "user-1".into(),
            level: "editor".into(),
            inherited: false,
        };
        on_grant_upsert(&mut cache, 1, &grant);
        assert_eq!(
            cache.get_grant(1, "doc-1", "user-1").map(|(l, _)| l),
            Some("editor")
        );

        on_grant_delete(&mut cache, 1, "doc-1", "user-1");
        assert!(cache.get_grant(1, "doc-1", "user-1").is_none());
    }

    #[test]
    fn edge_upsert_replaces_old_parent() {
        let mut cache = PermissionCache::new();

        on_edge_upsert(&mut cache, 1, "doc-1", "folder-a");
        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-a"));

        // Move doc-1 to folder-b.
        on_edge_upsert(&mut cache, 1, "doc-1", "folder-b");
        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-b"));
        // Old parent should no longer have doc-1 as child.
        assert!(cache.get_children(1, "folder-a").is_empty());
    }

    #[test]
    fn full_reload_populates_cache() {
        let mut cache = PermissionCache::new();

        let edges = vec![
            ("folder-1".into(), "workspace".into()),
            ("doc-1".into(), "folder-1".into()),
        ];
        let grants = vec![PermissionGrant {
            resource_id: "workspace".into(),
            grantee: "user-1".into(),
            level: "owner".into(),
            inherited: false,
        }];

        full_reload(&mut cache, 1, &edges, &grants);

        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-1"));
        assert_eq!(cache.get_parent(1, "folder-1"), Some("workspace"));
        assert_eq!(
            cache.get_grant(1, "workspace", "user-1").map(|(l, _)| l),
            Some("owner")
        );
    }
}
