// SPDX-License-Identifier: BUSL-1.1

//! A transaction's post-commit effects: cross-node trigger writes and
//! `PUBLISH TO`.
//!
//! Every `PUBLISH TO` message commits in its transaction's redo record: a
//! joined body's in its statement's record, every other body's and a stored
//! procedure's in its own. The Event Plane delivers each from there (see
//! `crate::event::topic::committed`).
//!
//! Cross-node writes commit in the body's redo record as outbox messages, and
//! the Event Plane delivers them from there (see
//! `crate::event::topic::committed::outbox`). One request holds every
//! statement the body sent to other nodes, applied by its receiver as one
//! transaction, all or none. The request's dedup key is the source event's replicated
//! identity `(source_vshard, source_lsn, source_sequence)` plus the body's
//! tag, so re-sending it after a lost reply or a leader change applies it
//! once.

use super::super::transaction::{PostCommitEffects, RemoteWrite};
use super::StatementExecutor;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::sql_dispatch::PreparedPublish;
use crate::event::cross_shard::types::CrossShardWriteRequest;

/// The owner a stored procedure's messages name: a procedure runs with no
/// body.
const UNNAMED_PUBLISH_OWNER: &str = "procedure";

/// Wrap statements as one procedural block for the receiver's parser.
fn block_sql(statements: &[String]) -> String {
    let mut sql = String::from("BEGIN\n");
    for statement in statements {
        sql.push_str(statement.trim().trim_end_matches(';').trim_end());
        sql.push_str(";\n");
    }
    sql.push_str("END;");
    sql
}

impl StatementExecutor<'_> {
    /// One request holding every remote write of a committed body, addressed
    /// to the first write's vShard. `None` for a body with no remote write.
    ///
    /// The receiver applies the block as one transaction, staging each write
    /// on its vShard's leader, and commits it all or none: through Calvin
    /// when it spans vShards. The request's key is the source event's
    /// replicated identity with the body's tag, so a retry after a leader
    /// change finds it applied.
    fn cross_shard_request(
        &self,
        writes: Vec<RemoteWrite>,
    ) -> crate::Result<Option<CrossShardWriteRequest>> {
        let Some(target_vshard) = writes.first().map(|write| write.target_vshard) else {
            return Ok(None);
        };
        let origin = self
            .cross_shard_origin
            .as_ref()
            .ok_or(crate::Error::Internal {
                detail: "a body without a cross-shard origin held remote writes".into(),
            })?;
        let origin_tag = self
            .body
            .as_ref()
            .map(|body| body.origin_tag(self.database_id))
            .unwrap_or_default();

        let statements: Vec<String> = writes.into_iter().map(|write| write.sql).collect();
        Ok(Some(CrossShardWriteRequest {
            sql: block_sql(&statements),
            tenant_id: self.tenant_id.as_u64(),
            database_id: self.database_id.as_u64(),
            source_vshard: origin.source_vshard,
            source_lsn: origin.source_lsn,
            source_sequence: origin.source_sequence,
            origin: format!("{origin_tag}/remote"),
            cascade_depth: self.cascade_depth(),
            source_collection: origin.source_collection.clone(),
            target_vshard,
        }))
    }

    /// The outbox messages that carry the cross-node writes `remote` of this
    /// transaction. They commit in its redo record with its writes and its
    /// applied key, so a committed body never loses its request (see
    /// `crate::event::topic::committed::outbox`).
    pub(super) fn outbox_messages(
        &self,
        remote: Vec<RemoteWrite>,
    ) -> crate::Result<Vec<crate::wal::RedoPublish>> {
        let owner = self.body.as_ref().map_or_else(
            || UNNAMED_PUBLISH_OWNER.to_owned(),
            |body| body.origin_tag(self.database_id),
        );
        match self.cross_shard_request(remote)? {
            Some(request) => Ok(vec![
                crate::event::topic::committed::outbox::outbox_message(&owner, &request)?,
            ]),
            None => Ok(Vec::new()),
        }
    }

    /// Hand a joined body's effects to its statement's transaction: its
    /// publishes commit in that transaction's redo record, and drop when it
    /// rolls back. A joined body carries no cross-shard origin, so every
    /// statement it runs stages here and it holds no remote write.
    pub(super) fn defer_to_statement(
        &self,
        ctx: &DmlTxnCtx<'_>,
        effects: PostCommitEffects,
    ) -> crate::Result<()> {
        if !effects.remote.is_empty() {
            return Err(crate::Error::Internal {
                detail: "a trigger body joined to its statement held a cross-node write".into(),
            });
        }
        let publishes = self.redo_publishes(effects.publishes);
        ctx.sessions.buffer_publishes(ctx.session_id, publishes);
        Ok(())
    }

    /// `publishes` as a redo record carries them, each naming this body as
    /// its owner, or [`UNNAMED_PUBLISH_OWNER`] with no body.
    pub(super) fn redo_publishes(
        &self,
        publishes: Vec<PreparedPublish>,
    ) -> Vec<crate::wal::RedoPublish> {
        let owner = self.body.as_ref().map_or_else(
            || UNNAMED_PUBLISH_OWNER.to_owned(),
            |body| body.origin_tag(self.database_id),
        );
        publishes
            .into_iter()
            .map(|publish| crate::wal::RedoPublish {
                owner: owner.clone(),
                database_id: publish.database_id,
                tenant_id: publish.tenant_id,
                topic: publish.topic,
                payload: publish.payload,
                metadata_floor: publish.metadata_floor,
                position: None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::block_sql;

    #[test]
    fn statements_become_one_block() {
        let sql = block_sql(&[
            "INSERT INTO a (id) VALUES ('x')".into(),
            " INSERT INTO b (id) VALUES ('y'); ".into(),
        ]);
        assert_eq!(
            sql,
            "BEGIN\nINSERT INTO a (id) VALUES ('x');\nINSERT INTO b (id) VALUES ('y');\nEND;"
        );
    }
}
