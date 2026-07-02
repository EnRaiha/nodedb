// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL family handlers + router.
//!
//! Handlers here build [`DdlResult`] / [`DdlError`] directly, carrying no
//! pgwire types. [`try_dispatch`] recognizes the migrated families and routes
//! to them; every other statement returns `None` so the transitional pgwire
//! delegation in the parent [`super::dispatch`] handles it.

pub mod alert;
mod auth_support;
pub mod change_stream;
pub mod cluster;
pub mod constraint;
pub mod consumer_group;
pub mod continuous_agg;
pub mod custom_type;
pub mod dsl;
pub mod function;
pub mod grant;
pub mod graph_ops;
pub mod kv_atomic;
pub mod kv_sorted_index;
pub mod last_value;
pub mod maintenance;
pub mod materialized_view;
pub mod oidc;
pub mod procedure;
pub mod query_functions;
pub mod rate_gate;
pub mod retention_policy;
pub mod rls;
pub mod role;
pub mod router;
pub mod schedule;
pub mod sequence;
pub mod service_account;
pub mod synonym_group;
pub mod timeseries;
pub mod topic;
pub mod transfer;
pub mod tree_ops;
pub mod trigger;
pub mod typeguard;
pub mod user;
pub mod version_history;
pub mod weighted_pick;

pub use self::router::try_dispatch;
