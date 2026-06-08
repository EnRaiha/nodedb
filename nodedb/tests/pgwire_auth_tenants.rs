// SPDX-License-Identifier: BUSL-1.1

//! Pgwire DDL coverage for the tenant surface. Split into submodules
//! under `pgwire_auth_tenants/`:
//!
//! - `lifecycle`       — CREATE / DROP TENANT, `IF [NOT] EXISTS`, `WITH ADMIN`
//! - `introspection`   — `SHOW TENANT[S]` by name / id + privilege gate
//! - `grants`          — `GRANT / REVOKE <perm> ON TENANT <name>`
//! - `name_resolution` — DROP/ALTER/PURGE TENANT by name (mirroring CREATE/SHOW)
//!
//! The entry file stays thin so future additions land in the topical
//! submodule rather than bloating one ~500-line test file.

mod common;

#[path = "pgwire_auth_tenants/grants.rs"]
mod grants;
#[path = "pgwire_auth_tenants/introspection.rs"]
mod introspection;
#[path = "pgwire_auth_tenants/lifecycle.rs"]
mod lifecycle;
#[path = "pgwire_auth_tenants/name_resolution.rs"]
mod name_resolution;
