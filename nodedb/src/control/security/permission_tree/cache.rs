// SPDX-License-Identifier: BUSL-1.1

//! In-memory permission cache: parent hierarchy + grant lookups.
//!
//! Loaded from the governed collections and permission tables by a reload,
//! and kept current by the Event Plane's permission step. `progress` records
//! how far the cache reflects each core's writes. Lives entirely in the
//! Control Plane (Send + Sync).
//!
//! Hierarchy and grants are held per [`TreeScope`]: a tree in one database
//! never resolves against another database's rows. The plan-cache version is
//! per tenant, so a change in any database of a tenant stales its plans.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, info};

use super::scope::{TreeKey, TreeScope};
use super::sources::SourceIndex;
use super::sync_state::ApplyProgress;
use super::types::{PermissionGrant, PermissionTreeDef};

/// Permission state of one scope: resource hierarchy + permission grants.
#[derive(Debug, Default)]
struct ScopePermissions {
    /// Resource hierarchy: `child_id → parent_id`.
    /// Walk this map upward to find ancestors.
    parent_map: HashMap<String, String>,

    /// Permission grants: `(resource_id, grantee) → (level, inherited)`.
    /// Only explicit (non-inherited) grants are stored; inherited access is
    /// resolved dynamically by walking the parent chain.
    grants: HashMap<(String, String), (String, bool)>,

    /// Reverse children map: `parent_id → set of child_ids`.
    /// Used for cache invalidation: when a grant changes, evict all descendants.
    children_map: HashMap<String, HashSet<String>>,
}

/// Central permission cache shared across all sessions.
///
/// Thread-safe: wrapped in `Arc<tokio::sync::RwLock<_>>` by SharedState.
#[derive(Debug)]
pub struct PermissionCache {
    /// Per-scope permission state.
    scopes: HashMap<TreeScope, ScopePermissions>,

    /// Monotonic version per tenant, bumped on every grant, edge, or tree
    /// mutation in any of the tenant's databases. A cached physical plan
    /// stamps this at build time so a plan cache can detect a revoked grant
    /// instead of replaying a frozen filter.
    versions: HashMap<u64, u64>,

    /// Per-collection permission tree definitions.
    tree_defs: HashMap<TreeKey, PermissionTreeDef>,

    /// How far the cache reflects each core's writes.
    progress: ApplyProgress,

    /// The source collections of `tree_defs`, readable without this cache's
    /// lock. Rebuilt on every tree-definition change.
    sources: Arc<SourceIndex>,
}

impl Default for PermissionCache {
    fn default() -> Self {
        Self::new()
    }
}

/// One collection a reload scans, and what its rows carry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeSource {
    pub key: TreeKey,
    pub kind: TreeSourceKind,
}

/// What a source collection's rows carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TreeSourceKind {
    /// A governed collection: each row's `id` and `parent_id` form an edge.
    Hierarchy,
    /// A permission table: each row is a grant.
    Grants,
}

impl PermissionCache {
    /// An empty cache. It needs a reload before planning can use it.
    pub fn new() -> Self {
        Self {
            scopes: HashMap::new(),
            versions: HashMap::new(),
            tree_defs: HashMap::new(),
            progress: ApplyProgress::new(),
            sources: Arc::new(SourceIndex::default()),
        }
    }

    /// The lock-free index of this cache's source collections.
    pub fn sources(&self) -> Arc<SourceIndex> {
        Arc::clone(&self.sources)
    }

    /// How far the cache reflects each core's writes.
    pub fn progress(&self) -> &ApplyProgress {
        &self.progress
    }

    /// Mutable access to the apply progress, for the permission step and a
    /// reload.
    pub fn progress_mut(&mut self) -> &mut ApplyProgress {
        &mut self.progress
    }

    /// Every collection a reload scans, deduplicated.
    pub fn tree_sources(&self) -> Vec<TreeSource> {
        let mut sources: HashSet<TreeSource> = HashSet::new();
        for (key, def) in &self.tree_defs {
            sources.insert(TreeSource {
                key: key.clone(),
                kind: TreeSourceKind::Hierarchy,
            });
            sources.insert(TreeSource {
                key: key.scope.collection(def.permission_table.clone()),
                kind: TreeSourceKind::Grants,
            });
        }
        sources.into_iter().collect()
    }

    /// Replace a scope's hierarchy and grants with a reload's result, and
    /// bump its tenant's version so a cached plan built from the old state
    /// goes stale.
    pub fn replace_scope_state(
        &mut self,
        scope: TreeScope,
        edges: &[(String, String)],
        grants: &[PermissionGrant],
    ) {
        self.scopes.insert(scope, ScopePermissions::default());
        self.load_edges(scope, edges);
        self.load_grants(scope, grants);
        self.bump_tenant_version(scope.tenant_id);
    }

    /// Drop the state of every scope `keep` does not name. A reload calls
    /// this so a scope whose trees were all removed holds no stale grants.
    pub fn retain_scopes(&mut self, keep: &HashSet<TreeScope>) {
        let dropped: Vec<TreeScope> = self
            .scopes
            .keys()
            .filter(|scope| !keep.contains(scope))
            .copied()
            .collect();
        for scope in dropped {
            self.scopes.remove(&scope);
            self.bump_tenant_version(scope.tenant_id);
        }
    }

    /// Register a permission tree definition for a collection. Registering
    /// the definition already held changes nothing, so the DDL node and the
    /// metadata applier can both apply one change.
    pub fn register_tree_def(&mut self, key: TreeKey, def: PermissionTreeDef) {
        if self.get_tree_def(&key) == Some(&def) {
            return;
        }
        info!(
            database_id = key.scope.database_id.as_u64(),
            tenant_id = key.scope.tenant_id,
            collection = %key.collection,
            levels = ?def.levels,
            "permission_tree: registered"
        );
        let tenant_id = key.scope.tenant_id;
        self.tree_defs.insert(key, def);
        self.sources.rebuild(&self.tree_defs);
        // The new sources can already hold rows no reload has read.
        self.progress.mark_reload_needed();
        self.bump_tenant_version(tenant_id);
    }

    /// Remove a permission tree definition for a collection. Removing an
    /// absent definition changes nothing.
    pub fn unregister_tree_def(&mut self, key: &TreeKey) {
        if self.tree_defs.remove(key).is_none() {
            return;
        }
        self.sources.rebuild(&self.tree_defs);
        self.progress.mark_reload_needed();
        self.bump_tenant_version(key.scope.tenant_id);
        info!(
            database_id = key.scope.database_id.as_u64(),
            tenant_id = key.scope.tenant_id,
            collection = %key.collection,
            "permission_tree: unregistered"
        );
    }

    /// Get the permission tree definition for a collection (if any).
    pub fn get_tree_def(&self, key: &TreeKey) -> Option<&PermissionTreeDef> {
        self.tree_defs.get(key)
    }

    /// Load a parent→child edge into the hierarchy.
    pub fn put_edge(&mut self, scope: TreeScope, child_id: &str, parent_id: &str) {
        let state = self.scopes.entry(scope).or_default();
        state
            .parent_map
            .insert(child_id.to_owned(), parent_id.to_owned());
        state
            .children_map
            .entry(parent_id.to_owned())
            .or_default()
            .insert(child_id.to_owned());
    }

    /// Remove a parent→child edge from the hierarchy.
    pub fn remove_edge(&mut self, scope: TreeScope, child_id: &str) {
        let Some(state) = self.scopes.get_mut(&scope) else {
            return;
        };
        if let Some(old_parent) = state.parent_map.remove(child_id)
            && let Some(children) = state.children_map.get_mut(&old_parent)
        {
            children.remove(child_id);
            if children.is_empty() {
                state.children_map.remove(&old_parent);
            }
        }
    }

    /// Load a permission grant into the cache.
    pub fn put_grant(&mut self, scope: TreeScope, grant: &PermissionGrant) {
        let state = self.scopes.entry(scope).or_default();
        state.grants.insert(
            (grant.resource_id.clone(), grant.grantee.clone()),
            (grant.level.clone(), grant.inherited),
        );
    }

    /// Remove a permission grant from the cache.
    pub fn remove_grant(&mut self, scope: TreeScope, resource_id: &str, grantee: &str) {
        if let Some(state) = self.scopes.get_mut(&scope) {
            state
                .grants
                .remove(&(resource_id.to_owned(), grantee.to_owned()));
        }
    }

    /// Look up the explicit grant for a (resource, grantee) pair.
    /// Returns `None` if no grant exists at this exact resource.
    pub fn get_grant(
        &self,
        scope: TreeScope,
        resource_id: &str,
        grantee: &str,
    ) -> Option<(&str, bool)> {
        self.scopes
            .get(&scope)?
            .grants
            .get(&(resource_id.to_owned(), grantee.to_owned()))
            .map(|(level, inherited)| (level.as_str(), *inherited))
    }

    /// Get the parent of a resource. Returns `None` if root.
    pub fn get_parent(&self, scope: TreeScope, resource_id: &str) -> Option<&str> {
        self.scopes
            .get(&scope)?
            .parent_map
            .get(resource_id)
            .map(|s| s.as_str())
    }

    /// Get all children of a resource (direct, not recursive).
    pub fn get_children(&self, scope: TreeScope, resource_id: &str) -> Vec<&str> {
        self.scopes
            .get(&scope)
            .and_then(|s| s.children_map.get(resource_id))
            .map(|children| children.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get all resource IDs of a scope (for iteration during accessible_resources).
    pub fn all_resource_ids(&self, scope: TreeScope) -> Vec<&str> {
        self.scopes
            .get(&scope)
            .map(|s| {
                // Collect from all sources: edges (parent_map), reverse edges
                // (children_map), AND grant keys (for root resources with
                // direct grants but no parent/child edges).
                let mut ids: HashSet<&str> = HashSet::new();
                for k in s.parent_map.keys() {
                    ids.insert(k.as_str());
                }
                for k in s.parent_map.values() {
                    ids.insert(k.as_str());
                }
                for k in s.children_map.keys() {
                    ids.insert(k.as_str());
                }
                for (resource_id, _) in s.grants.keys() {
                    ids.insert(resource_id.as_str());
                }
                ids.into_iter().collect()
            })
            .unwrap_or_default()
    }

    /// Get all grantees that have explicit grants for a given resource.
    pub fn grantees_for_resource(&self, scope: TreeScope, resource_id: &str) -> Vec<&str> {
        self.scopes
            .get(&scope)
            .map(|s| {
                s.grants
                    .keys()
                    .filter(|(rid, _)| rid == resource_id)
                    .map(|(_, grantee)| grantee.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Bulk load edges from a list of (child_id, parent_id) pairs.
    pub fn load_edges(&mut self, scope: TreeScope, edges: &[(String, String)]) {
        for (child, parent) in edges {
            self.put_edge(scope, child, parent);
        }
        debug!(
            database_id = scope.database_id.as_u64(),
            tenant_id = scope.tenant_id,
            edges = edges.len(),
            "permission_tree: loaded edges"
        );
    }

    /// Bulk load grants.
    pub fn load_grants(&mut self, scope: TreeScope, grants: &[PermissionGrant]) {
        for grant in grants {
            self.put_grant(scope, grant);
        }
        debug!(
            database_id = scope.database_id.as_u64(),
            tenant_id = scope.tenant_id,
            grants = grants.len(),
            "permission_tree: loaded grants"
        );
    }

    /// Number of resources tracked for a scope.
    pub fn resource_count(&self, scope: TreeScope) -> usize {
        self.scopes
            .get(&scope)
            .map(|s| s.parent_map.len())
            .unwrap_or(0)
    }

    /// Number of grants tracked for a scope.
    pub fn grant_count(&self, scope: TreeScope) -> usize {
        self.scopes.get(&scope).map(|s| s.grants.len()).unwrap_or(0)
    }

    /// Check if any permission tree definitions are registered.
    pub fn has_tree_defs(&self) -> bool {
        !self.tree_defs.is_empty()
    }

    /// Check if any permission tree definition is registered for this
    /// tenant, in any database.
    ///
    /// Used by plan-time enforcement for operations that name no collection:
    /// they cannot be shown to avoid a governed collection, so the question
    /// widens to the whole tenant.
    pub fn has_tree_defs_for_tenant(&self, tenant_id: u64) -> bool {
        self.tree_defs
            .keys()
            .any(|key| key.scope.tenant_id == tenant_id)
    }

    /// Check if any tree def in `key`'s scope uses its collection as the
    /// permission table.
    pub fn tree_defs_using_permission_table(&self, key: &TreeKey) -> bool {
        self.tree_defs.iter().any(|(governed, def)| {
            governed.scope == key.scope && def.permission_table == key.collection
        })
    }

    /// Check if `key` is a governed collection: its rows form the resource
    /// hierarchy.
    pub fn tree_defs_using_graph(&self, key: &TreeKey) -> bool {
        self.tree_defs.contains_key(key)
    }

    /// Bump and return the per-tenant permission version. Called from every
    /// mutator in `invalidation.rs`, in the same critical section as the
    /// state change, so a stamped plan cache entry goes stale on the spot.
    pub fn bump_tenant_version(&mut self, tenant_id: u64) -> u64 {
        let version = self.versions.entry(tenant_id).or_default();
        *version += 1;
        *version
    }

    /// Current permission version for a tenant. `0` if the tenant has never
    /// been mutated.
    pub fn tenant_version(&self, tenant_id: u64) -> u64 {
        self.versions.get(&tenant_id).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DatabaseId;

    const S1: TreeScope = TreeScope {
        database_id: DatabaseId::DEFAULT,
        tenant_id: 1,
    };
    const S2: TreeScope = TreeScope {
        database_id: DatabaseId::DEFAULT,
        tenant_id: 2,
    };

    fn def() -> PermissionTreeDef {
        sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def")
    }

    fn grant(resource: &str, grantee: &str, level: &str) -> PermissionGrant {
        PermissionGrant {
            resource_id: resource.into(),
            grantee: grantee.into(),
            level: level.into(),
            inherited: false,
        }
    }

    #[test]
    fn edge_hierarchy() {
        let mut cache = PermissionCache::new();
        // workspace → folder → doc
        cache.put_edge(S1, "folder-1", "workspace-1");
        cache.put_edge(S1, "doc-1", "folder-1");

        assert_eq!(cache.get_parent(S1, "doc-1"), Some("folder-1"));
        assert_eq!(cache.get_parent(S1, "folder-1"), Some("workspace-1"));
        assert_eq!(cache.get_parent(S1, "workspace-1"), None); // Root.

        let children = cache.get_children(S1, "workspace-1");
        assert_eq!(children, vec!["folder-1"]);
    }

    #[test]
    fn grant_lookup() {
        let mut cache = PermissionCache::new();
        cache.put_grant(S1, &grant("folder-1", "user-42", "editor"));

        let (level, inherited) = cache.get_grant(S1, "folder-1", "user-42").unwrap();
        assert_eq!(level, "editor");
        assert!(!inherited);

        assert!(cache.get_grant(S1, "folder-1", "user-99").is_none());
    }

    #[test]
    fn remove_edge() {
        let mut cache = PermissionCache::new();
        cache.put_edge(S1, "doc-1", "folder-1");
        assert_eq!(cache.get_parent(S1, "doc-1"), Some("folder-1"));

        cache.remove_edge(S1, "doc-1");
        assert_eq!(cache.get_parent(S1, "doc-1"), None);
        assert!(cache.get_children(S1, "folder-1").is_empty());
    }

    #[test]
    fn remove_grant() {
        let mut cache = PermissionCache::new();
        cache.put_grant(S1, &grant("doc-1", "user-1", "viewer"));
        assert!(cache.get_grant(S1, "doc-1", "user-1").is_some());
        cache.remove_grant(S1, "doc-1", "user-1");
        assert!(cache.get_grant(S1, "doc-1", "user-1").is_none());
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = PermissionCache::new();
        cache.put_edge(S1, "doc-1", "folder-1");
        cache.put_edge(S2, "doc-1", "folder-2");

        assert_eq!(cache.get_parent(S1, "doc-1"), Some("folder-1"));
        assert_eq!(cache.get_parent(S2, "doc-1"), Some("folder-2"));
    }

    /// One tenant's hierarchy, grants, and trees in one database are
    /// invisible from another database.
    #[test]
    fn database_isolation() {
        let db1 = TreeScope::new(DatabaseId::new(7), 1);
        let db2 = TreeScope::new(DatabaseId::new(8), 1);
        let mut cache = PermissionCache::new();
        cache.register_tree_def(db1.collection("docs"), def());
        cache.put_edge(db1, "doc-1", "folder-1");
        cache.put_grant(db1, &grant("folder-1", "user-1", "viewer"));

        assert!(cache.get_tree_def(&db1.collection("docs")).is_some());
        assert!(cache.get_tree_def(&db2.collection("docs")).is_none());
        assert!(cache.tree_defs_using_graph(&db1.collection("docs")));
        assert!(!cache.tree_defs_using_graph(&db2.collection("docs")));
        assert!(cache.tree_defs_using_permission_table(&db1.collection("grants")));
        assert!(!cache.tree_defs_using_permission_table(&db2.collection("grants")));
        assert_eq!(cache.get_parent(db2, "doc-1"), None);
        assert!(cache.get_grant(db2, "folder-1", "user-1").is_none());
        assert!(cache.all_resource_ids(db2).is_empty());
    }

    #[test]
    fn tenant_version_bumps_and_is_isolated_per_tenant() {
        let mut cache = PermissionCache::new();
        assert_eq!(cache.tenant_version(1), 0);
        assert_eq!(cache.bump_tenant_version(1), 1);
        assert_eq!(cache.bump_tenant_version(1), 2);
        assert_eq!(cache.tenant_version(1), 2);
        // A different tenant's bumps do not affect this one's version.
        assert_eq!(cache.bump_tenant_version(2), 1);
        assert_eq!(cache.tenant_version(1), 2);
    }

    #[test]
    fn tree_def_registration() {
        let mut cache = PermissionCache::new();
        let def = PermissionTreeDef {
            resource_column: "id".into(),
            graph_index: "tree".into(),
            permission_table: "perms".into(),
            levels: vec!["none".into(), "viewer".into(), "editor".into()],
            read_level: "viewer".into(),
            write_level: "editor".into(),
            delete_level: "editor".into(),
        };
        cache.register_tree_def(S1.collection("documents"), def.clone());
        assert!(cache.get_tree_def(&S1.collection("documents")).is_some());
        assert!(cache.get_tree_def(&S1.collection("other")).is_none());
        assert!(cache.get_tree_def(&S2.collection("documents")).is_none());

        // The DDL node and the metadata applier both apply one change.
        let version = cache.tenant_version(1);
        cache.register_tree_def(S1.collection("documents"), def);
        assert_eq!(cache.tenant_version(1), version);

        cache.unregister_tree_def(&S1.collection("documents"));
        assert!(cache.get_tree_def(&S1.collection("documents")).is_none());
        let version = cache.tenant_version(1);
        cache.unregister_tree_def(&S1.collection("documents"));
        assert_eq!(cache.tenant_version(1), version);
    }

    #[test]
    fn a_reload_replaces_the_scope_state_and_bumps_its_version() {
        let mut cache = PermissionCache::new();
        cache.put_edge(S1, "doc-1", "folder-1");
        cache.put_grant(S1, &grant("doc-1", "user-1", "viewer"));
        let before = cache.bump_tenant_version(1);

        cache.replace_scope_state(S1, &[("doc-2".into(), "folder-2".into())], &[]);

        assert_eq!(cache.get_parent(S1, "doc-1"), None);
        assert_eq!(cache.get_parent(S1, "doc-2"), Some("folder-2"));
        assert!(cache.get_grant(S1, "doc-1", "user-1").is_none());
        assert!(cache.tenant_version(1) > before);
    }

    #[test]
    fn retaining_scopes_drops_the_rest_and_bumps_their_tenant() {
        let mut cache = PermissionCache::new();
        cache.put_grant(S1, &grant("doc-1", "user-1", "viewer"));
        cache.put_grant(S2, &grant("doc-1", "user-1", "viewer"));
        let before = cache.tenant_version(2);

        cache.retain_scopes(&HashSet::from([S1]));

        assert!(cache.get_grant(S1, "doc-1", "user-1").is_some());
        assert!(cache.get_grant(S2, "doc-1", "user-1").is_none());
        assert!(cache.tenant_version(2) > before);
    }

    #[test]
    fn tree_sources_name_the_governed_collection_and_the_permission_table() {
        let scope = TreeScope::new(DatabaseId::new(7), 1);
        let mut cache = PermissionCache::new();
        cache.register_tree_def(scope.collection("docs"), def());
        let mut sources = cache.tree_sources();
        sources.sort_by(|a, b| a.key.collection.cmp(&b.key.collection));
        assert_eq!(
            sources,
            vec![
                TreeSource {
                    key: scope.collection("docs"),
                    kind: TreeSourceKind::Hierarchy,
                },
                TreeSource {
                    key: scope.collection("grants"),
                    kind: TreeSourceKind::Grants,
                },
            ]
        );
        assert!(cache.progress().needs_reload_for(&[]));
    }
}
