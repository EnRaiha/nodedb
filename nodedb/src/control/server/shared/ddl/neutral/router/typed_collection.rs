// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `CollectionStmt`: create/drop collection & table,
//! indexes, sequences, alter collection, and reindex.

use nodedb_sql::ddl_ast::statement::{CollectionStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::collection;
use super::super::maintenance;
use super::super::sequence::{self, CreateSequenceRequest};
use super::helpers::collection_exists;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        // CREATE COLLECTION / CREATE TABLE. The `if_not_exists: true` guard
        // short-circuit runs inline here, before the create handler.
        // `build_and_persist` (name/duplicate/engine validation,
        // schema construction, `StoredCollection` assembly, propose+apply,
        // SERIAL sequence auto-creation) lives in `collection::create`.
        // `register_created` registers the collection for a buffered CREATE.
        NodedbStatement::Collection(CollectionStmt::CreateCollection {
            name,
            if_not_exists,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        }) => {
            if *if_not_exists && collection_exists(state, identity, name, database_id) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "CREATE COLLECTION".to_string(),
                    rows_affected: None,
                }]));
            }
            let result = collection::create_collection(
                state,
                identity,
                &collection::CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
                database_id,
            )
            .await;
            Some(register_created(state, result).await)
        }

        NodedbStatement::Collection(CollectionStmt::CreateTable {
            name,
            if_not_exists,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        }) => {
            if *if_not_exists && collection_exists(state, identity, name, database_id) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "CREATE TABLE".to_string(),
                    rows_affected: None,
                }]));
            }
            let result = collection::create_table(
                state,
                identity,
                &collection::CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
                database_id,
            )
            .await;
            Some(register_created(state, result).await)
        }

        // DROP { COLLECTION | TABLE } [IF EXISTS] <name> [PURGE] [CASCADE
        // [FORCE]] — parser folds both spellings into `DropCollection`.
        // The handler honours `if_exists` internally via its existence-check
        // matrix (no guard short-circuit); the catalog propose + single-node
        // fallback, cascade dependent enumeration, soft vs hard delete, the
        // implicit-sequence sweep, and the audit pair live in
        // `collection::drop`.
        NodedbStatement::Collection(CollectionStmt::DropCollection {
            name,
            if_exists,
            purge,
            cascade,
            cascade_force,
        }) => Some(
            collection::drop_collection(
                state,
                identity,
                &collection::DropCollectionRequest {
                    name,
                    if_exists: *if_exists,
                    purge: *purge,
                    cascade: *cascade,
                    cascade_force: *cascade_force,
                    database_id,
                },
            )
            .await,
        ),

        // CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON <collection>
        // (<field>) [WHERE ...].
        // The two-phase Building→Ready backfill, peer fan-out, Register
        // refresh, and owner-ledger propose live in `collection::index`.
        NodedbStatement::Collection(CollectionStmt::CreateIndex {
            unique,
            index_name,
            collection: coll,
            field,
            case_insensitive,
            where_condition,
            if_not_exists,
        }) => Some(
            collection::create_index(
                state,
                identity,
                &collection::CreateIndexRequest {
                    is_unique: *unique,
                    index_name_opt: index_name.as_deref(),
                    collection: coll,
                    field,
                    case_insensitive: *case_insensitive,
                    where_condition: where_condition.as_deref(),
                    database_id,
                    if_not_exists: *if_not_exists,
                },
            )
            .await,
        ),

        NodedbStatement::Collection(CollectionStmt::CreateSequence {
            name,
            if_not_exists,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            format_template_raw,
            reset_period_raw,
            gap_free,
            scope: _,
        }) => {
            // IF NOT EXISTS on a non-existing sequence falls through to the
            // planner (no create arm matches `if_not_exists: true`), so this
            // returns None.
            let tenant_id = identity.tenant_id.as_u64();
            if *if_not_exists
                && !state
                    .sequence_registry
                    .exists(database_id.as_u64(), tenant_id, name)
            {
                return None;
            }
            Some(
                sequence::create_sequence(
                    state,
                    identity,
                    database_id,
                    &CreateSequenceRequest {
                        name,
                        if_not_exists: *if_not_exists,
                        start: *start,
                        increment: *increment,
                        min_value: *min_value,
                        max_value: *max_value,
                        cycle: *cycle,
                        cache: *cache,
                        format_template_raw: format_template_raw.as_deref(),
                        reset_period_raw: reset_period_raw.as_deref(),
                        gap_free: *gap_free,
                    },
                )
                .await,
            )
        }

        NodedbStatement::Collection(CollectionStmt::AlterSequence {
            name,
            action,
            with_value,
        }) => Some(
            sequence::alter_sequence(
                state,
                identity,
                database_id,
                name,
                action,
                with_value.as_deref(),
            )
            .await,
        ),

        NodedbStatement::Collection(CollectionStmt::DropSequence { name, if_exists }) => {
            Some(sequence::drop_sequence(state, identity, database_id, name, *if_exists).await)
        }

        NodedbStatement::Collection(CollectionStmt::ShowSequences) => {
            Some(sequence::show_sequences(state, identity, database_id))
        }

        NodedbStatement::Collection(CollectionStmt::DescribeSequence { name }) => Some(
            sequence::describe_sequence(state, identity, database_id, name),
        ),

        // `ALTER COLLECTION <name> <operation>` for every typed
        // `AlterCollectionOp` variant (ADD/DROP/RENAME/ALTER COLUMN, OWNER TO,
        // SET RETENTION / APPEND_ONLY / LAST_VALUE_CACHE / LEGAL_HOLD, ADD
        // MATERIALIZED_SUM, SET ON CONFLICT). `dispatch_alter_collection` is a
        // total match over `AlterCollectionOp` — no variant falls through. Each
        // sub-handler's catalog / register / audit side effects and command tag
        // (`ALTER TABLE` for ADD COLUMN, `ALTER COLLECTION` otherwise) live in
        // `collection::alter`.
        NodedbStatement::Collection(CollectionStmt::AlterCollection { name, operation }) => Some(
            collection::dispatch_alter_collection(state, identity, database_id, name, operation)
                .await,
        ),

        NodedbStatement::Collection(CollectionStmt::Reindex {
            collection,
            index_name,
            concurrent,
        }) => Some(
            maintenance::handle_reindex(
                state,
                identity,
                collection,
                index_name.as_deref(),
                *concurrent,
                database_id,
            )
            .await,
        ),

        _ => None,
    }
}

/// Finish a CREATE COLLECTION / CREATE TABLE: register the new collection on
/// this node's Data Plane when the proposal left that to the handler.
///
/// A durable outcome ran the entry's awaited post-apply, which registered the
/// collection on every core. A buffered outcome registered nothing, and the
/// open transaction's later statements read the new shape.
async fn register_created(
    state: &SharedState,
    created: Result<collection::create::build::CreatedCollection, DdlError>,
) -> Result<Vec<DdlResult>, DdlError> {
    let created = created?;
    collection::register_proposed_collection(state, created.outcome, &created.collection)
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    Ok(created.results)
}
