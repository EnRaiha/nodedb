// SPDX-License-Identifier: BUSL-1.1

//! Shim that re-exports the shared test harness from
//! `nodedb-test-support`. Lets the moved cluster tests keep using
//! `common::pgwire_harness::TestServer`-style paths verbatim.

#[allow(unused_imports)]
pub use nodedb_test_support::{
    array_sync, cluster_harness, make_cdc_event, now_ms, occ_shuffle, pgwire_auth_helpers,
    pgwire_harness, tx_batch_helpers,
};

/// Mint the same exact-plan authorization capability required by production
/// gateway entry points, using a trusted superuser identity scoped to `ctx`.
#[allow(dead_code)]
pub fn authorize_gateway_plan(
    shared: &nodedb::control::state::SharedState,
    ctx: &nodedb::control::gateway::core::QueryContext,
    plan: nodedb_physical::physical_plan::PhysicalPlan,
) -> nodedb::control::server::shared::authorization::AuthorizedTask {
    use nodedb::control::security::audit::NoopAuditEmitter;
    use nodedb::control::server::shared::authorization::authorize_task_set;
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    let identity = nodedb_test_support::pgwire_auth_helpers::superuser();
    let task = PhysicalTask {
        tenant_id: ctx.tenant_id,
        vshard_id: nodedb::types::VShardId::new(0),
        database_id: ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: ctx.txn_id,
    };
    authorize_task_set(
        &identity,
        std::slice::from_ref(&task),
        &shared.permissions,
        &shared.roles,
        &NoopAuditEmitter,
    )
    .expect("authorize cluster gateway plan")
    .into_tasks()
    .into_iter()
    .next()
    .expect("one authorized cluster gateway plan")
}
