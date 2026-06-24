// SPDX-License-Identifier: BUSL-1.1

//! Query routing: consistency selection, and the execute_planned_sql entry
//! point for DML/query dispatch.
//!
//! Cross-node forwarding is handled by the gateway (`SharedState.gateway`).
//! The old `forward_sql` / `remote_leader_for_tasks` helpers have been
//! replaced by `gateway.execute(ctx, plan)` which ships the pre-planned
//! physical plan via `ExecuteRequest` instead of a raw SQL string.

mod calvin_dispatch;
mod catalog;
mod check_enforcement;
mod clone_dispatch;
mod clone_write_dispatch;
mod execute;
mod gateway_dispatch;
mod kv_wrapping;
mod planning;
mod set_ops;
mod streaming;
