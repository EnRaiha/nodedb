// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for a [`CatalogEntry`] — dispatched by
//! DDL family.
//!
//! Split into two phases so readers of `applied_index` observe a
//! consistent view:
//!
//! - [`apply_post_apply_side_effects_sync`] (in `sync`) runs the
//!   synchronous in-memory cache updates **inline** on the raft
//!   applier thread, BEFORE the metadata applier bumps the
//!   `AppliedIndexWatcher`.
//! - [`spawn_post_apply_async_side_effects`] (in `async_dispatch`)
//!   spawns tokio tasks for the genuinely async work — runs on
//!   **every node** (leader and followers) so each node's local
//!   Data Plane observes catalog mutations symmetrically.

// Per-family modules (existing).
pub mod api_key;
pub mod auth_user;
pub mod change_stream;
pub mod collection;
pub mod continuous_aggregate;
pub mod custom_type;
pub mod database;
pub mod function;
pub mod materialized_view;
pub mod owner;
pub mod permission;
pub mod procedure;
pub mod redaction;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod scope_grant;
pub mod sequence;
pub mod streaming_materialized_view;
pub mod synonym_group;
pub mod tenant;
pub mod trigger;
pub mod user;

// Orchestration modules.
mod async_dispatch;
pub(crate) mod gateway_invalidation;
mod sync;

pub(crate) use async_dispatch::collection::{ReclaimFailure, reclaim_collection_storage};
pub use async_dispatch::spawn_post_apply_async_side_effects;
pub use sync::apply_post_apply_side_effects_sync;
