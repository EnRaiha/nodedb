// SPDX-License-Identifier: BUSL-1.1

//! Query routing: consistency selection, and the execute_planned_sql entry
//! point for DML/query dispatch.
//!
//! Cross-node forwarding is handled by the gateway (`SharedState.gateway`).
//! The old `forward_sql` / `remote_leader_for_tasks` helpers have been
//! replaced by `gateway.execute(ctx, plan)` which ships the pre-planned
//! physical plan via `ExecuteRequest` instead of a raw SQL string.

mod calvin_dispatch;
mod calvin_response;
mod catalog;
mod check_enforcement;
mod clone_dispatch;
mod clone_write_dispatch;
mod cluster_array;
mod dispatch_loop;
pub(in crate::control::server::pgwire::handler) mod execute;
mod execute_dml_hooks;
mod execute_entry;
mod gateway_dispatch;
mod planning;
mod pre_dispatch;
pub(in crate::control::server::pgwire::handler) mod result_shaping;
mod set_ops;
pub(in crate::control::server::pgwire::handler) mod setup_error;
mod streaming;
