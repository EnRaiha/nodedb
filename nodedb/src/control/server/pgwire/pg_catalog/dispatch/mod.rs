// SPDX-License-Identifier: BUSL-1.1

//! pg_catalog query interception and dispatch.

pub mod materialize;
pub mod route;
pub mod schema;

pub use route::{try_pg_catalog, try_pg_catalog_with_params};
pub use schema::{extract_pg_catalog_table, pg_catalog_projected_schema, pg_catalog_schema};
