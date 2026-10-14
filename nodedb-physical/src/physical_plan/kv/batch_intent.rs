// SPDX-License-Identifier: Apache-2.0

//! How a KV batch put treats a key that is already stored.

use nodedb_types::RlsWriteCheck;

use crate::physical_plan::document::UpdateValue;

/// The rule `KvOp::BatchPut` applies to each entry's key.
///
/// A key the batch names twice is judged against the batch's own earlier
/// entry, as one statement's rows are judged in row order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvBatchIntent {
    /// Overwrite every key. Reports the keys the batch created as
    /// `inserted`. RESP `MSET` and the native batch put.
    #[default]
    Put,
    /// SQL `UPSERT`: overwrite every key. Reports every entry written as
    /// `affected`, as one `KvOp::Put` per row does.
    Upsert,
    /// SQL `INSERT`: a stored or repeated key refuses the whole batch with
    /// `unique_violation` (SQLSTATE 23505). Nothing is written then.
    Insert,
    /// SQL `INSERT ... ON CONFLICT DO NOTHING`: a stored or repeated key is
    /// skipped. Reports the entries written as `affected`. Which rows are
    /// written depends on stored state, so the batch resolves to a mutation
    /// list before it is proposed or journaled.
    InsertIfAbsent,
    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...`: an absent key
    /// stores its entry, a stored or repeated key stores the merge of
    /// `updates` into the stored body. `rls_write_check` decides each body
    /// that persists. Resolves to a mutation list before it is proposed or
    /// journaled, as `KvOp::InsertOnConflictUpdate` does under a policy.
    Merge {
        updates: Vec<(String, UpdateValue)>,
        rls_write_check: RlsWriteCheck,
    },
}

impl KvBatchIntent {
    /// Whether the rows this intent writes depend on stored state. Such a
    /// batch resolves to a mutation list first, so a replica and a WAL replay
    /// store exactly the rows the leader decided.
    pub fn depends_on_stored_rows(&self) -> bool {
        match self {
            KvBatchIntent::InsertIfAbsent | KvBatchIntent::Merge { .. } => true,
            KvBatchIntent::Put | KvBatchIntent::Upsert | KvBatchIntent::Insert => false,
        }
    }
}
