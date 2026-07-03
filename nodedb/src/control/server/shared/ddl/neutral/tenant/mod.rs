// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral tenant DDL handlers.
//!
//! - [`create`] — `CREATE TENANT` (proposes `CatalogEntry::PutTenant`).
//! - [`alter`] — `ALTER TENANT SET QUOTA` (in-memory; quota is not
//!   part of `StoredTenant` — quota replication is a separate concern).
//! - [`alter_quota`] — `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)` —
//!   persists quota to `_system.tenant_quotas`.
//! - [`drop`] — `DROP TENANT` (proposes `DeleteTenant`).
//! - [`purge`] — `PURGE TENANT <id> CONFIRM` (Data Plane meta op).
//! - [`show_in_database`] — `SHOW TENANT QUOTA/USAGE FOR <name> IN DATABASE <db>`.
//! - [`move_tenant`] — `MOVE TENANT <name> FROM <db> TO <db>` (async, 5-phase).
//!
//! `SHOW TENANTS` / `SHOW TENANT <ident>` / `SHOW TENANTS WITH NAME <name>`
//! were migrated separately to `neutral::inspect` and are not part of this
//! family. `SHOW TENANT USAGE` / `SHOW TENANT QUOTA` (bare, no `IN DATABASE`)
//! were confirmed parser-shadowed dead code on the pgwire router — the typed
//! `ddl_ast` tenant parser never returns `None` for `SHOW TENANT
//! USAGE|QUOTA...`, so those two forms always resolved to either the typed
//! `IN DATABASE` variant or a `42601` parse error before reaching the pgwire
//! string handlers. They were deleted, not migrated; no neutral string prefix
//! exists for them, so the same input still hits the parse gate and still
//! returns `42601` — byte-identical.

pub mod alter;
pub mod alter_quota;
pub mod create;
pub mod drop;
pub mod move_tenant;
pub mod purge;
pub mod show_in_database;
mod support;

pub use alter::alter_tenant;
pub use alter_quota::handle_alter_tenant_quota;
pub use create::create_tenant;
pub use drop::drop_tenant;
pub use move_tenant::handle_move_tenant;
pub use purge::purge_tenant;
pub use show_in_database::{
    handle_show_tenant_quota_in_database, handle_show_tenant_usage_in_database,
};
