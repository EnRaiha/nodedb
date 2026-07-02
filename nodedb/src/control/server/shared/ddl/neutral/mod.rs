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
pub mod constraint;
pub mod consumer_group;
pub mod continuous_agg;
pub mod function;
pub mod grant;
pub mod materialized_view;
pub mod oidc;
pub mod procedure;
pub mod retention_policy;
pub mod rls;
pub mod role;
pub mod router;
pub mod schedule;
pub mod sequence;
pub mod service_account;
pub mod topic;
pub mod trigger;
pub mod typeguard;
pub mod user;
pub mod version_history;

pub use self::router::try_dispatch;
