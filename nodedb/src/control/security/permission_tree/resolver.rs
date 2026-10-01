// SPDX-License-Identifier: BUSL-1.1

//! Permission resolution: walk the resource hierarchy to determine effective access.
//!
//! The resolver walks the parent chain from a resource toward the root,
//! checking for grants at each level. The first explicit (non-inherited) grant
//! found determines the effective permission. If the root is reached with no
//! grant, access is denied (returns the lowest level, typically "none").

use super::cache::PermissionCache;
use super::scope::TreeScope;
use super::types::PermissionTreeDef;

/// Resolve the effective permission level for a user on a resource.
///
/// Walks from `resource_id` toward the root via the parent map.
/// At each node, checks for an explicit grant for the user or any of their roles.
/// Returns the first matching level, or the lowest level (index 0) if none found.
///
/// # Arguments
/// * `cache` — In-memory permission cache (read-only reference).
/// * `def` — Permission tree definition for the collection.
/// * `scope` — The database and tenant the tree lives in.
/// * `user_id` — The user to check.
/// * `user_roles` — Roles the user belongs to (also checked as grantees).
/// * `resource_id` — The resource to check access on.
///
/// # Returns
/// The effective permission level name (e.g., "editor", "viewer", "none").
pub fn resolve_permission(
    cache: &PermissionCache,
    def: &PermissionTreeDef,
    scope: TreeScope,
    user_id: &str,
    user_roles: &[String],
    resource_id: &str,
) -> String {
    let no_access = def
        .levels
        .first()
        .cloned()
        .unwrap_or_else(|| "none".to_owned());

    let mut current = resource_id.to_owned();
    // Guard against cycles: max iterations = reasonable tree depth.
    let max_depth = 256;

    for _ in 0..max_depth {
        // Check for explicit grant at this node for the user.
        if let Some((level, _inherited)) = cache.get_grant(scope, &current, user_id) {
            return level.to_owned();
        }

        // Check for grants via roles.
        for role in user_roles {
            if let Some((level, _inherited)) = cache.get_grant(scope, &current, role) {
                return level.to_owned();
            }
        }

        // Walk to parent.
        match cache.get_parent(scope, &current) {
            Some(parent) => current = parent.to_owned(),
            None => break, // Reached root.
        }
    }

    no_access
}

/// Find all resource IDs that a user can access at or above a required level.
///
/// Iterates all known resources of the scope and resolves each one.
/// Returns the set of resource IDs where the effective permission meets
/// the required threshold.
///
/// This is used by RLS injection to pre-compute the accessible resource set
/// and inject an `IN (...)` filter into the physical plan.
///
/// # Performance
/// O(R × D) where R = number of resources, D = average tree depth.
/// For typical collaborative workspaces (< 50K resources, depth ≤ 5),
/// this completes in microseconds since all data is in-memory.
pub fn accessible_resources(
    cache: &PermissionCache,
    def: &PermissionTreeDef,
    scope: TreeScope,
    user_id: &str,
    user_roles: &[String],
    required_level: &str,
) -> Vec<String> {
    let required_ordinal = match def.level_ordinal(required_level) {
        Some(ord) => ord,
        None => return Vec::new(), // Unknown level = deny all.
    };

    let all_ids = cache.all_resource_ids(scope);
    let mut accessible = Vec::new();

    for rid in all_ids {
        let effective = resolve_permission(cache, def, scope, user_id, user_roles, rid);
        if let Some(effective_ordinal) = def.level_ordinal(&effective)
            && effective_ordinal >= required_ordinal
        {
            accessible.push(rid.to_owned());
        }
    }

    accessible
}

/// Resolve permission for a single user+resource and check against a threshold.
///
/// Convenience wrapper that returns `true` if the user meets the required level.
pub fn check_permission(
    cache: &PermissionCache,
    def: &PermissionTreeDef,
    scope: TreeScope,
    user_id: &str,
    user_roles: &[String],
    resource_id: &str,
    required_level: &str,
) -> bool {
    let effective = resolve_permission(cache, def, scope, user_id, user_roles, resource_id);
    def.level_meets_requirement(&effective, required_level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::permission_tree::types::PermissionGrant;
    use crate::types::DatabaseId;

    const S1: TreeScope = TreeScope {
        database_id: DatabaseId::DEFAULT,
        tenant_id: 1,
    };

    fn setup() -> (PermissionCache, PermissionTreeDef) {
        let mut cache = PermissionCache::new();
        let def = PermissionTreeDef {
            resource_column: "id".into(),
            graph_index: "resource_tree".into(),
            permission_table: "permissions".into(),
            levels: vec![
                "none".into(),
                "viewer".into(),
                "commenter".into(),
                "editor".into(),
                "owner".into(),
            ],
            read_level: "viewer".into(),
            write_level: "editor".into(),
            delete_level: "owner".into(),
        };

        // Hierarchy: workspace → folder-design → doc-mockup
        cache.put_edge(S1, "folder-design", "workspace-acme");
        cache.put_edge(S1, "doc-mockup", "folder-design");

        // Grant: user-42 is editor on folder-design.
        cache.put_grant(
            S1,
            &PermissionGrant {
                resource_id: "folder-design".into(),
                grantee: "user-42".into(),
                level: "editor".into(),
                inherited: false,
            },
        );

        // Grant: user-99 is viewer on workspace-acme.
        cache.put_grant(
            S1,
            &PermissionGrant {
                resource_id: "workspace-acme".into(),
                grantee: "user-99".into(),
                level: "viewer".into(),
                inherited: false,
            },
        );

        (cache, def)
    }

    #[test]
    fn resolve_inherits_from_parent() {
        let (cache, def) = setup();

        // user-42 has editor on folder-design → doc-mockup inherits editor.
        let level = resolve_permission(&cache, &def, S1, "user-42", &[], "doc-mockup");
        assert_eq!(level, "editor");
    }

    #[test]
    fn resolve_direct_grant() {
        let (cache, def) = setup();

        // user-42 has direct editor on folder-design.
        let level = resolve_permission(&cache, &def, S1, "user-42", &[], "folder-design");
        assert_eq!(level, "editor");
    }

    #[test]
    fn resolve_no_access() {
        let (cache, def) = setup();

        // user-unknown has no grants anywhere.
        let level = resolve_permission(&cache, &def, S1, "user-unknown", &[], "doc-mockup");
        assert_eq!(level, "none");
    }

    #[test]
    fn resolve_via_role() {
        let (mut cache, def) = setup();

        // Grant role "design-team" editor on folder-design.
        cache.put_grant(
            S1,
            &PermissionGrant {
                resource_id: "folder-design".into(),
                grantee: "design-team".into(),
                level: "editor".into(),
                inherited: false,
            },
        );

        // user-77 is in role "design-team".
        let level = resolve_permission(
            &cache,
            &def,
            S1,
            "user-77",
            &["design-team".into()],
            "doc-mockup",
        );
        assert_eq!(level, "editor");
    }

    #[test]
    fn resolve_workspace_level_grant() {
        let (cache, def) = setup();

        // user-99 has viewer on workspace → inherits to folder and doc.
        let level = resolve_permission(&cache, &def, S1, "user-99", &[], "doc-mockup");
        assert_eq!(level, "viewer");
    }

    #[test]
    fn accessible_resources_filters_correctly() {
        let (cache, def) = setup();

        // user-42 can access folder-design (editor) and doc-mockup (inherited editor).
        // user-42 cannot access workspace-acme (no grant).
        let accessible = accessible_resources(&cache, &def, S1, "user-42", &[], "viewer");
        assert!(accessible.contains(&"folder-design".to_owned()));
        assert!(accessible.contains(&"doc-mockup".to_owned()));
        assert!(!accessible.contains(&"workspace-acme".to_owned()));
    }

    #[test]
    fn check_permission_threshold() {
        let (cache, def) = setup();

        // user-42 is editor on doc-mockup (inherited).
        assert!(check_permission(
            &cache,
            &def,
            S1,
            "user-42",
            &[],
            "doc-mockup",
            "viewer"
        ));
        assert!(check_permission(
            &cache,
            &def,
            S1,
            "user-42",
            &[],
            "doc-mockup",
            "editor"
        ));
        assert!(!check_permission(
            &cache,
            &def,
            S1,
            "user-42",
            &[],
            "doc-mockup",
            "owner"
        ));
    }

    #[test]
    fn override_at_lower_level() {
        let (mut cache, def) = setup();

        // Override: user-99 is viewer at workspace, but editor at doc-mockup.
        cache.put_grant(
            S1,
            &PermissionGrant {
                resource_id: "doc-mockup".into(),
                grantee: "user-99".into(),
                level: "editor".into(),
                inherited: false,
            },
        );

        // Direct grant at doc-mockup takes precedence (short-circuits before walking up).
        let level = resolve_permission(&cache, &def, S1, "user-99", &[], "doc-mockup");
        assert_eq!(level, "editor");
    }

    /// A tree and its grants in one database resolve nothing in another
    /// database of the same tenant.
    #[test]
    fn grants_in_one_database_do_not_resolve_in_another() {
        let (cache, def) = setup();
        let other = TreeScope::new(DatabaseId::new(9), 1);

        let level = resolve_permission(&cache, &def, other, "user-42", &[], "doc-mockup");
        assert_eq!(level, "none");
        assert!(accessible_resources(&cache, &def, other, "user-42", &[], "viewer").is_empty());
        assert_eq!(
            resolve_permission(&cache, &def, S1, "user-42", &[], "doc-mockup"),
            "editor"
        );
    }
}
