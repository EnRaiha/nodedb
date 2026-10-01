// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree enforcement over physical plans.
//!
//! The walk is exhaustive over [`PhysicalPlan`] and over every engine's own
//! operation enum, split one module per engine the same way the row-level
//! security pass beside it is. Each variant resolves to exactly one of three
//! outcomes:
//!
//! - **Filter** — the op reads or acts on rows of a namable collection through
//!   a plan node that carries a filter slot, so the caller's permitted subtree
//!   is ANDed into that slot at the operation's level (read / write / delete).
//!   A write that names its rows directly instead of selecting them is checked
//!   against the level rather than filtered, because there is nothing to
//!   narrow.
//! - **Refuse** — the op touches a governed collection through a result shape
//!   that cannot carry the subtree filter, so the plan is rejected with
//!   `Error::PlanError`.
//! - **No-op** — the op is DDL, maintenance, a control action, or an operation
//!   whose plan carries no resolvable collection.
//!
//! The verdicts track the RLS pass's, and where they legitimately differ the
//! arm says so: this pass enforces writes and deletes (it has levels for them,
//! and no separate write-path check covers a permission tree), and it can
//! narrow a mutation to the permitted subtree wherever the mutation selects
//! its rows through a predicate.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::permission_tree::PermissionCache;
use nodedb_physical::physical_task::PhysicalTask;

use super::context::PermCtx;

/// Inject permission-tree filters into physical tasks.
///
/// For each task whose collection carries a `PermissionTreeDef`, resolves the
/// set of accessible resource ids for the current identity and injects an
/// `IN (...)` filter on the resource column, or refuses the plan where no slot
/// can carry it.
///
/// **Caller**: session query execution, after `inject_rls()` — permission-tree
/// filters are AND-combined with the row-level-security filters already in the
/// slots.
///
/// **Superuser bypass**: a superuser produces no context, so nothing is walked.
pub fn inject_permission_tree(
    tasks: &mut [PhysicalTask],
    cache: &PermissionCache,
    auth: &AuthContext,
) -> crate::Result<()> {
    for task in tasks.iter_mut() {
        // No context means a superuser, which is a property of the session
        // rather than of the task — so the whole batch is bypassed.
        let Some(ctx) = PermCtx::new(cache, task.tenant_id.as_u64(), auth, task.database_id) else {
            return Ok(());
        };
        walk(&ctx, &mut task.plan)?;
    }
    Ok(())
}

/// Core dispatch: resolve the permission tree for one physical plan.
///
/// Exhaustive over [`PhysicalPlan`] so a new engine forces a decision here,
/// and each engine module is exhaustive over its own operations so a new
/// operation forces one there.
pub(super) fn walk(ctx: &PermCtx<'_>, plan: &mut PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(op) => super::document::apply_document(ctx, op),
        PhysicalPlan::Kv(op) => super::kv::apply_kv(ctx, op),
        PhysicalPlan::Vector(op) => super::vector::apply_vector(ctx, op),
        PhysicalPlan::Text(op) => super::text::apply_text(ctx, op),
        PhysicalPlan::Columnar(op) => super::columnar::apply_columnar(ctx, op),
        PhysicalPlan::Timeseries(op) => super::columnar::apply_timeseries(ctx, op),
        PhysicalPlan::Spatial(op) => super::columnar::apply_spatial(ctx, op),
        PhysicalPlan::Graph(op) => super::graph::apply_graph(ctx, op),
        PhysicalPlan::Query(op) => super::query::apply_query(ctx, op),
        PhysicalPlan::Crdt(op) => super::crdt::apply_crdt(ctx, op),
        PhysicalPlan::Meta(op) => super::meta::apply_meta(ctx, op),
        PhysicalPlan::Array(op) => super::array::apply_array(ctx, op),
        PhysicalPlan::ClusterArray(op) => super::array::apply_cluster_array(ctx, op),
        PhysicalPlan::ClusterEvent(op) => super::array::apply_cluster_event(ctx, op),
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::permission_tree::types::{PermissionGrant, PermissionTreeDef};
    use crate::control::security::permission_tree::{PermissionCache, TreeScope, resolver};
    use crate::types::{DatabaseId, TenantId};

    pub(in crate::control::planner::rls_injection::permission_tree) const TENANT: u64 = 1;

    /// The identity `regular_auth()` authenticates as, as the grant table
    /// spells it: `AuthContext::id` is the numeric user id rendered as text.
    const ALICE: &str = "42";

    /// A cache holding a tree on the default database's `collection` over
    /// three resources.
    ///
    /// `doc_a` is granted to alice at `owner`, so she clears read, write, and
    /// delete. `doc_b` is granted at `viewer`, so she clears read only.
    /// `doc_c` is granted to nobody and is outside her subtree at every level.
    pub(in crate::control::planner::rls_injection::permission_tree) fn cache_with_tree(
        collection: &str,
    ) -> PermissionCache {
        cache_with_tree_in(DatabaseId::DEFAULT, collection)
    }

    /// [`cache_with_tree`] with the tree and its grants in `database_id`.
    pub(in crate::control::planner::rls_injection::permission_tree) fn cache_with_tree_in(
        database_id: DatabaseId,
        collection: &str,
    ) -> PermissionCache {
        let scope = TreeScope::new(database_id, TENANT);
        let mut cache = PermissionCache::new();
        cache.register_tree_def(
            scope.collection(collection),
            PermissionTreeDef {
                resource_column: "doc_id".into(),
                graph_index: "resource_tree".into(),
                permission_table: "permissions".into(),
                levels: ["none", "viewer", "commenter", "editor", "owner"]
                    .iter()
                    .map(|l| (*l).to_owned())
                    .collect(),
                read_level: "viewer".into(),
                write_level: "editor".into(),
                delete_level: "owner".into(),
            },
        );
        for (resource, grantee, level) in [
            ("doc_a", ALICE, "owner"),
            ("doc_b", ALICE, "viewer"),
            ("doc_c", "someone_else", "owner"),
        ] {
            cache.put_grant(
                scope,
                &PermissionGrant {
                    resource_id: resource.into(),
                    grantee: grantee.into(),
                    level: level.into(),
                    inherited: false,
                },
            );
        }
        cache
    }

    /// An ordinary (non-superuser) authenticated session.
    pub(in crate::control::planner::rls_injection::permission_tree) fn regular_auth() -> AuthContext
    {
        use crate::control::security::identity::{
            AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
        };

        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(TENANT),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
        );
        AuthContext::from_identity(&identity, "s_test".into())
    }

    /// A superuser session, which bypasses the pass entirely.
    pub(in crate::control::planner::rls_injection::permission_tree) fn superuser_auth()
    -> AuthContext {
        use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet};

        let identity = AuthenticatedIdentity::new_internal_service(
            1,
            "root",
            TenantId::new(TENANT),
            Vec::new(),
            true,
            None,
            DatabaseSet::All,
        );
        AuthContext::from_identity(&identity, "s_root".into())
    }

    /// Run the pass over `plan` as alice, against `cache`.
    pub(in crate::control::planner::rls_injection::permission_tree) fn apply(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
        cache: &PermissionCache,
    ) -> crate::Result<()> {
        apply_as(plan, cache, &regular_auth())
    }

    /// Run the pass over `plan` as `auth`, against `cache`.
    pub(in crate::control::planner::rls_injection::permission_tree) fn apply_as(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
        cache: &PermissionCache,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        match super::PermCtx::new(cache, TENANT, auth, DatabaseId::DEFAULT) {
            Some(ctx) => super::walk(&ctx, plan),
            None => Ok(()),
        }
    }

    /// Run the pass with an empty cache: nothing must change.
    pub(in crate::control::planner::rls_injection::permission_tree) fn apply_without_tree(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<()> {
        apply(plan, &PermissionCache::new())
    }

    /// Assert the pass refused with a typed plan error naming `collection`.
    pub(in crate::control::planner::rls_injection::permission_tree) fn assert_refused(
        result: crate::Result<()>,
        collection: &str,
    ) {
        match result {
            Err(crate::Error::PlanError { detail }) => assert!(
                detail.contains(collection),
                "refusal must name the collection; got {detail}"
            ),
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// The resource ids of the `IN (...)` filter the pass wrote into `slot`.
    ///
    /// Panics if the slot holds no such filter, so a test that expects the
    /// subtree restriction fails loudly when nothing was injected.
    pub(in crate::control::planner::rls_injection::permission_tree) fn injected_resources(
        slot: &[u8],
    ) -> Vec<String> {
        let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
            zerompk::from_msgpack(slot).expect("decode injected filters");
        let subtree = filters
            .iter()
            .find(|f| f.field == "doc_id")
            .expect("subtree filter must be injected");
        match &subtree.value {
            nodedb_types::Value::Array(values) => values
                .iter()
                .map(|v| match v {
                    nodedb_types::Value::String(s) => s.clone(),
                    other => panic!("resource id must be a string, got {other:?}"),
                })
                .collect(),
            other => panic!("subtree filter must carry an array, got {other:?}"),
        }
    }

    /// The resource ids alice can read, sorted for comparison.
    pub(in crate::control::planner::rls_injection::permission_tree) fn readable() -> Vec<String> {
        vec!["doc_a".to_owned(), "doc_b".to_owned()]
    }

    /// Sorted copy of `ids`, so assertions do not depend on cache iteration
    /// order.
    pub(in crate::control::planner::rls_injection::permission_tree) fn sorted(
        mut ids: Vec<String>,
    ) -> Vec<String> {
        ids.sort();
        ids
    }

    /// Guard the fixture itself: alice must hold delete on exactly `doc_a`, so
    /// a delete-level assertion elsewhere is meaningful.
    #[test]
    fn fixture_separates_the_levels() {
        let cache = cache_with_tree("docs");
        let scope = TreeScope::new(DatabaseId::DEFAULT, TENANT);
        let def = cache
            .get_tree_def(&scope.collection("docs"))
            .expect("tree def");
        let auth = regular_auth();
        let delete = resolver::accessible_resources(
            &cache,
            def,
            scope,
            &auth.id,
            &auth.roles,
            &def.delete_level,
        );
        assert_eq!(sorted(delete), vec!["doc_a".to_owned()]);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        ColumnarOp, DocumentOp, ExchangeMode, ExchangeOp, QueryOp,
    };

    use nodedb_physical::physical_task::PhysicalTask;

    use super::test_support::{
        TENANT, apply, apply_as, apply_without_tree, cache_with_tree, cache_with_tree_in,
        injected_resources, readable, regular_auth, sorted, superuser_auth,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn columnar_scan(collection: &str) -> PhysicalPlan {
        columnar_scan_in(DatabaseId::DEFAULT, collection)
    }

    fn columnar_scan_in(database_id: DatabaseId, collection: &str) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(database_id, collection),
            projection: Vec::new(),
            limit: 0,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
            prefilter: None,
            computed_columns: Vec::new(),
        })
    }

    fn scan_subtree(plan: &PhysicalPlan) -> Vec<String> {
        match plan {
            PhysicalPlan::Columnar(ColumnarOp::Scan { rls_filters, .. }) => {
                sorted(injected_resources(rls_filters))
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The document scan that this pass always covered still receives the
    /// subtree filter in the slot it always used.
    #[test]
    fn document_scan_still_receives_the_subtree_filter() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            limit: 0,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
            prefilter: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::Scan { filters, .. }) => {
                assert_eq!(sorted(injected_resources(filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A columnar scan over a governed collection narrows to the readable
    /// subtree instead of returning every row.
    #[test]
    fn columnar_scan_is_narrowed_to_the_readable_subtree() {
        let cache = cache_with_tree("events");
        let mut plan = columnar_scan("events");
        assert!(apply(&mut plan, &cache).is_ok());
        assert_eq!(scan_subtree(&plan), readable());
    }

    /// A collection with no permission tree is untouched.
    #[test]
    fn a_collection_without_a_tree_is_untouched() {
        let mut plan = columnar_scan("events");
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A tree on a different collection must not filter this one.
    #[test]
    fn a_tree_on_another_collection_is_untouched() {
        let cache = cache_with_tree("docs");
        let mut plan = columnar_scan("events");
        let before = plan.clone();
        assert!(apply(&mut plan, &cache).is_ok());
        assert_eq!(plan, before);
    }

    /// A superuser bypasses the pass: no context is built and no plan is
    /// touched.
    #[test]
    fn a_superuser_bypasses_the_pass() {
        let cache = cache_with_tree("events");
        let mut plan = columnar_scan("events");
        let before = plan.clone();
        assert!(apply_as(&mut plan, &cache, &superuser_auth()).is_ok());
        assert_eq!(plan, before);
    }

    /// A governed scan nested under `Exchange` is still filtered — the
    /// converter wraps sharded sources before this pass runs.
    #[test]
    fn a_governed_scan_under_exchange_is_still_filtered() {
        let cache = cache_with_tree("events");
        let mut plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(columnar_scan("events")),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => {
                assert_eq!(scan_subtree(child), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// …and under `PostProcess`, the subquery-body wrapper.
    #[test]
    fn a_governed_scan_under_post_process_is_still_filtered() {
        let cache = cache_with_tree("events");
        let mut plan = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(columnar_scan("events")),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
                assert_eq!(scan_subtree(input), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    fn task_in(database_id: DatabaseId, plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            database_id,
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    /// The pass resolves each task's tree in that task's database: a tree in
    /// one database filters a scan there, and leaves the same collection name
    /// in the default database and in another database untouched.
    #[test]
    fn injection_resolves_trees_in_the_task_database() {
        let db1 = DatabaseId::new(7);
        let db2 = DatabaseId::new(8);
        let cache = cache_with_tree_in(db1, "events");
        let mut tasks = vec![
            task_in(db1, columnar_scan_in(db1, "events")),
            task_in(db2, columnar_scan_in(db2, "events")),
            task_in(DatabaseId::DEFAULT, columnar_scan("events")),
        ];
        let untouched_db2 = tasks[1].plan.clone();
        let untouched_default = tasks[2].plan.clone();

        super::inject_permission_tree(&mut tasks, &cache, &regular_auth()).expect("inject");

        assert_eq!(scan_subtree(&tasks[0].plan), readable());
        assert_eq!(tasks[1].plan, untouched_db2);
        assert_eq!(tasks[2].plan, untouched_default);
    }

    /// A default-database tree never filters a named database's collection
    /// of the same name.
    #[test]
    fn a_default_database_tree_does_not_apply_in_another_database() {
        let db = DatabaseId::new(7);
        let cache = cache_with_tree("events");
        let mut tasks = vec![task_in(db, columnar_scan_in(db, "events"))];
        let before = tasks[0].plan.clone();

        super::inject_permission_tree(&mut tasks, &cache, &regular_auth()).expect("inject");

        assert_eq!(tasks[0].plan, before);
    }

    /// A collection name not qualified for the task's database is refused
    /// rather than resolved in another database.
    #[test]
    fn a_name_qualified_for_another_database_is_refused() {
        let cache = cache_with_tree_in(DatabaseId::new(7), "events");
        let mut tasks = vec![task_in(
            DatabaseId::new(7),
            columnar_scan_in(DatabaseId::new(8), "events"),
        )];
        assert!(super::inject_permission_tree(&mut tasks, &cache, &regular_auth()).is_err());
    }
}
