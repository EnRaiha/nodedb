// SPDX-License-Identifier: BUSL-1.1

// All match arms on PermissionTarget must be exhaustive. Wildcard catch-alls
// are denied here so that adding a new variant (e.g. when database scoping
// gains another axis) forces a compile error at every match site rather
// than silently falling through.
#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_types::id::DatabaseId;

use crate::types::TenantId;

use super::role::Role;

/// Permission types for RBAC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// SELECT, point_get, vector_search, range_scan, crdt_read, graph queries.
    Read,
    /// INSERT, UPDATE, DELETE, crdt_apply, vector_insert, edge_put.
    Write,
    /// CREATE COLLECTION, CREATE INDEX.
    Create,
    /// DROP COLLECTION, DROP INDEX.
    Drop,
    /// ALTER COLLECTION, schema changes, policy changes.
    Alter,
    /// GRANT/REVOKE, user management within scope.
    Admin,
    /// Read metrics, health checks, EXPLAIN, slow query log.
    Monitor,
    /// Call a user-defined function (`SELECT func(...)`, UDF in expression).
    Execute,
    /// BACKUP / RESTORE TENANT. Granted tenant-scoped via
    /// `GRANT BACKUP ON TENANT <name>`.
    Backup,
}

/// What the permission applies to.
///
/// Every `match` on this enum must be exhaustive — no `_ =>` arms. Adding a
/// new variant is intentionally a compile error at every match site.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PermissionTarget {
    /// Entire cluster (node management, topology).
    Cluster,
    /// All collections within a tenant.
    Tenant(TenantId),
    /// A specific database (CREATE COLLECTION, DROP DATABASE, etc.).
    Database(DatabaseId),
    /// A specific collection within a tenant and database.
    ///
    /// The `database_id` field scopes the collection grant. Grants stored in
    /// `_system.collection_grants` include `database_id` in their key triple
    /// `(tenant_id, database_id, collection)`.
    Collection {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
    },
    /// System catalog (superuser only).
    SystemCatalog,
}

/// Check if a role implicitly grants a permission on a target.
///
/// Superuser is checked before this function is called.
pub fn role_grants_permission(role: &Role, permission: Permission) -> bool {
    match role {
        Role::Superuser => true,
        // ClusterAdmin has no implicit data permissions; it operates exclusively
        // through require_cluster_admin / require_database_owner_or_higher helpers.
        Role::ClusterAdmin => false,
        Role::TenantAdmin => true,
        Role::ReadWrite => matches!(
            permission,
            Permission::Read | Permission::Write | Permission::Execute
        ),
        Role::ReadOnly => matches!(permission, Permission::Read | Permission::Execute),
        // Monitor is metrics/health/audit only. It previously also conferred
        // `Permission::Read`, which contradicted its own definition and let a
        // monitoring principal read collection data.
        Role::Monitor => matches!(permission, Permission::Monitor),
        // Database-scoped roles grant permissions within their database.
        // The database match is enforced at the call site via PermissionTarget.
        Role::DatabaseOwner(_) => true,
        Role::DatabaseEditor(_) => matches!(
            permission,
            Permission::Read | Permission::Write | Permission::Create | Permission::Execute
        ),
        Role::DatabaseReader(_) => matches!(permission, Permission::Read | Permission::Execute),
        Role::Custom(_) => false, // Custom roles need explicit grants
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_permission_mapping() {
        assert!(role_grants_permission(&Role::ReadOnly, Permission::Read));
        assert!(!role_grants_permission(&Role::ReadOnly, Permission::Write));

        assert!(role_grants_permission(&Role::ReadWrite, Permission::Read));
        assert!(role_grants_permission(&Role::ReadWrite, Permission::Write));
        assert!(!role_grants_permission(&Role::ReadWrite, Permission::Drop));

        assert!(role_grants_permission(
            &Role::TenantAdmin,
            Permission::Admin
        ));
        assert!(role_grants_permission(&Role::TenantAdmin, Permission::Drop));
    }

    #[test]
    fn database_scoped_roles_grant_within_their_tier() {
        let db = DatabaseId::new(7);
        // Owner: full
        assert!(role_grants_permission(
            &Role::DatabaseOwner(db),
            Permission::Drop
        ));
        // Editor: read/write/create/execute, not admin/drop/alter
        assert!(role_grants_permission(
            &Role::DatabaseEditor(db),
            Permission::Write
        ));
        assert!(!role_grants_permission(
            &Role::DatabaseEditor(db),
            Permission::Drop
        ));
        // Reader: read/execute only
        assert!(role_grants_permission(
            &Role::DatabaseReader(db),
            Permission::Read
        ));
        assert!(!role_grants_permission(
            &Role::DatabaseReader(db),
            Permission::Write
        ));
    }

    #[test]
    fn custom_role_grants_nothing_by_default() {
        let role = Role::Custom("data_scientist".into());
        for perm in [
            Permission::Read,
            Permission::Write,
            Permission::Create,
            Permission::Drop,
            Permission::Alter,
            Permission::Admin,
            Permission::Monitor,
            Permission::Execute,
        ] {
            assert!(
                !role_grants_permission(&role, perm),
                "custom role unexpectedly granted {perm:?}"
            );
        }
    }
}
