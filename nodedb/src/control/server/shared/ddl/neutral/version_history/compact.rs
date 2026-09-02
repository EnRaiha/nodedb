// SPDX-License-Identifier: BUSL-1.1

//! COMPACT HISTORY ON collection WHERE id = 'doc-id' BEFORE 'checkpoint'

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// COMPACT HISTORY ON collection WHERE id = 'doc-id' BEFORE 'checkpoint'
///
/// Discards oplog entries before the specified checkpoint. Current state and
/// the operations after the checkpoint are retained. Checkpoints created
/// before the cutoff are deleted from the catalog.
///
/// The proposed entry carries the checkpoint's version vector, so every node
/// that applies it compacts its own oplog from post-apply. A single node has
/// no applier, so this handler dispatches the compaction itself.
///
/// Compaction replaces the document with a shallow snapshot, and `fork_at`
/// refuses a shallow document outright. `AS OF` / `AT VERSION` reads of this
/// document therefore stop working on a compacted node, at every version,
/// including versions above the cutoff. Only the current-state read survives.
pub async fn compact_history(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, doc_id, checkpoint_name) = parse_compact(sql)?;
    let tenant_id = identity.tenant_id;

    // Resolve checkpoint to version vector + timestamp.
    let catalog = state.credentials.catalog();
    let record = catalog
        .get_checkpoint(tenant_id.as_u64(), &collection, &doc_id, &checkpoint_name)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| {
            err(
                "42704",
                format!("checkpoint '{checkpoint_name}' not found for {collection}/{doc_id}"),
            )
        })?;

    // Count on the leader: the replicated entry carries the boundary, not a
    // row count, and the audit line names how many rows it removes.
    let deleted = catalog
        .count_checkpoints_before(tenant_id.as_u64(), &collection, &doc_id, record.created_at)
        .map_err(|e| err("XX000", e.to_string()))?;
    let outcome = super::replicate::propose_compact_history(
        state,
        tenant_id.as_u64(),
        &collection,
        &doc_id,
        record.created_at,
        database_id.as_u64(),
        &record.version_vector_json,
    )?;

    // Single node: no applier runs, so post-apply never fires. Dispatch the
    // compaction the post-apply lane dispatches everywhere else.
    if outcome.needs_local_apply() {
        crate::control::catalog_entry::post_apply::compact_async(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &collection,
            &record.version_vector_json,
            state,
        )
        .await;
    }

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!(
                "COMPACT HISTORY on {collection}/{doc_id} before '{checkpoint_name}' ({deleted} checkpoints removed)"
            ),
        );

    Ok(vec![DdlResult::Status {
        command: "COMPACT HISTORY".to_string(),
        rows_affected: None,
    }])
}

/// Parse: COMPACT HISTORY ON collection WHERE id = 'doc-id' BEFORE 'checkpoint'
fn parse_compact(sql: &str) -> Result<(String, String, String), DdlError> {
    let rest = sql["COMPACT HISTORY ON ".len()..].trim();

    // Collection: before WHERE.
    let where_pos = find_ascii_case_insensitive(rest, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE".to_string()))?;
    let collection = rest[..where_pos].trim().to_lowercase();
    let after_where = rest[where_pos + 5..].trim();

    // id = 'doc-id' BEFORE 'checkpoint'
    let before_pos = find_ascii_case_insensitive(after_where, "BEFORE")
        .ok_or_else(|| err("42601", "expected BEFORE '<checkpoint>'".to_string()))?;
    let id_clause = after_where[..before_pos].trim();
    let checkpoint_part = after_where[before_pos + 6..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let checkpoint_name = checkpoint_part
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned();

    let eq_pos = id_clause
        .find('=')
        .ok_or_else(|| err("42601", "expected 'id = <value>'".to_string()))?;
    let value_part = id_clause[eq_pos + 1..].trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();

    Ok((collection, doc_id, checkpoint_name))
}
