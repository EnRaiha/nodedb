// SPDX-License-Identifier: Apache-2.0

mod alert;
mod backup;
mod change_stream;
mod cluster_admin;
mod collection;
mod conflict_policy;
mod copy_from;
mod copy_to;
mod custom_type;
pub mod database;
mod dispatch;
mod grant;
mod graph_stats;
mod helpers;
mod index;
mod maintenance;
mod materialized_view;
mod oidc_provider;
mod policy_tokens;
mod redaction;
mod retention;
mod rls;
mod schedule;
mod sequence;
mod synonym_group;
mod tenant;
mod trigger;
mod user_auth;
pub mod vector_primary;

pub use dispatch::parse;
