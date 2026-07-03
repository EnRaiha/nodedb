// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    _state: &SharedState,
    _identity: &AuthenticatedIdentity,
    _sql: &str,
    _upper: &str,
    _parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Triggers (CREATE / DROP / ALTER / SHOW) are served by the protocol-neutral DDL router.

    // Schema introspection.
    // DESCRIBE SEQUENCE is served by the protocol-neutral DDL router.
    // DESCRIBE <collection> / `\D <collection>` is served by the protocol-neutral DDL router.

    // CREATE TABLE — fully dispatched via typed AST (ast.rs).
    // CREATE COLLECTION — fully dispatched via typed AST (ast.rs).
    // CREATE INDEX / CREATE UNIQUE INDEX — fully dispatched via typed AST (ast.rs).
    // CREATE SEQUENCE — served by the protocol-neutral DDL router.
    // CREATE RLS POLICY — served by the protocol-neutral DDL router.
    // ALTER COLLECTION (all sub-operations) — fully dispatched via typed AST (ast.rs).

    // DROP { COLLECTION | TABLE } — fully dispatched via typed AST (ast.rs
    // DropCollection); the text-based dispatch used to read `parts[2]` for
    // the name, which broke `DROP COLLECTION IF EXISTS <name>` (read "IF"
    // as the name) and never recognised the `DROP TABLE` spelling at all.
    // UNDROP COLLECTION|TABLE — served by the protocol-neutral DDL router.
    // SHOW COLLECTIONS — served by the protocol-neutral DDL router.

    // DROP INDEX — served by the protocol-neutral DDL router.
    // SHOW INDEXES / SHOW INDEX — served by the protocol-neutral DDL router.

    // ALTER TABLE ADD COLUMN — handled via typed AST (ast.rs AlterCollection).
    // The only remaining ALTER TABLE path is undirected fallthrough.

    // DROP RLS POLICY / SHOW RLS POLICIES — served by the protocol-neutral DDL router.

    // DEFINE FIELD / DEFINE EVENT have been migrated to the protocol-neutral
    // router (`shared::ddl::neutral::field_def`), which is tried before this
    // transitional pgwire delegation runs.

    // CREATE SPATIAL INDEX has been migrated to the protocol-neutral router
    // (`shared::ddl::neutral::spatial`), which is tried before this transitional
    // pgwire delegation runs.

    None
}
