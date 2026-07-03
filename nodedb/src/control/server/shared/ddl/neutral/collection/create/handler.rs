// SPDX-License-Identifier: BUSL-1.1

//! The `create_collection` handler.
//!
//! Relocated verbatim from the pgwire `pgwire::ddl::collection::create::handler`
//! module (now deleted). Thin wrapper over [`super::build::build_and_persist`] —
//! the entire validation + storage + replication body is shared with the
//! [`super::table::create_table`] path; the only collection-specific knobs are
//! the labels and the schemaless-by-default engine mapping.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::build::{Variant, build_and_persist};
use super::request::CreateCollectionRequest;

/// CREATE COLLECTION <name> [(<col> <type>, ...)] [WITH (engine='...')]
///
/// All fields are pre-parsed from the `nodedb-sql` AST:
/// - `engine`: value of `engine=` from the WITH clause (lowercased), or `None` for default.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: free-standing modifier keywords: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`.
/// - `balanced_raw`: raw inner content of `BALANCED ON (...)`, if present.
pub async fn create_collection(
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
            label: "collection",
            response_tag: "CREATE COLLECTION",
            // CREATE COLLECTION default (engine=None) → schemaless.
            require_columns: false,
            default_strict: false,
        },
    )
    .await
}
