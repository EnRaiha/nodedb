// SPDX-License-Identifier: BUSL-1.1

//! `CREATE TABLE` DDL handler — strict-default Postgres-style syntax.
//!
//! Relocated verbatim from the pgwire `pgwire::ddl::collection::create::table`
//! module (now deleted). Thin wrapper over [`super::build::build_and_persist`] —
//! the entire validation + storage + replication body is shared with the
//! [`super::handler::create_collection`] path; the only TABLE-specific knobs
//! are the labels, the mandatory column list, and the strict-by-default engine
//! mapping.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::build::{Variant, build_and_persist};
use super::request::CreateCollectionRequest;

/// Handle `CREATE [IF NOT EXISTS] TABLE <name> (<col_list>) [WITH (engine='...')]`.
///
/// All fields are pre-parsed from the `nodedb-sql` AST:
/// - `engine`: value of `engine=` from the WITH clause (lowercased), or `None` for default
///   (which is `document_strict` for CREATE TABLE).
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw inner content of `BALANCED ON (...)`, if present.
pub async fn create_table(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateCollectionRequest<'_>,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    build_and_persist(
        state,
        identity,
        req,
        database_id,
        &Variant {
            label: "table",
            response_tag: "CREATE TABLE",
            // CREATE TABLE requires columns by convention.
            require_columns: true,
            // CREATE TABLE default (engine=None) → document_strict.
            default_strict: true,
        },
    )
    .await
}
