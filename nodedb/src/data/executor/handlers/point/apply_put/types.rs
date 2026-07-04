// SPDX-License-Identifier: BUSL-1.1

//! Parameter and outcome types for [`CoreLoop::apply_point_put`], plus the
//! enforcement-error mapping shared with the delete path.

use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;

/// Parameters for [`CoreLoop::apply_point_put`](crate::data::executor::core_loop::CoreLoop::apply_point_put).
pub(in crate::data::executor) struct PointPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// Whether to index the document's text into the inverted BM25 index.
    ///
    /// `true` for native writes (PointPut/Insert/Upsert/batch/insert-select),
    /// which own the full write. `false` for CRDT-sync materialization: that
    /// path receives text via a separate `FtsIndexDoc` sync frame, so indexing
    /// here too would double-index the same surrogate.
    pub index_text: bool,
    /// Roles held by the authenticated user, consumed by role-gated state
    /// transition constraints. Empty for internal/system callers.
    pub user_roles: &'a [String],
    /// Whether to run stateless PUT enforcement (append-only, period lock,
    /// state transitions, transition-check predicates).
    ///
    /// `true` for user-DML callers (PointPut/Insert/Upsert/batch/
    /// insert-select), which must be admission-checked. `false` for
    /// CRDT-sync materialization: those deltas already passed admission on
    /// their origin replica (CRDT constraint validation happens at the Raft
    /// commit phase), so re-running enforcement here would double-check
    /// already-accepted writes.
    pub enforce: bool,
    /// Whether to apply the SIDE-index side-effects: the secondary-index
    /// write, the spatial index write, the vector index write, and the
    /// column-stats observe.
    ///
    /// `true` for autocommit user-DML callers (PointPut/Insert/Upsert/batch/
    /// insert-select/CRDT-materialize), which own the full write. `false` for
    /// the transactional path (`tx_point_put`): those side-effects have no
    /// undo variant yet, so enabling them inside a transaction would leave a
    /// rollback hole. The CORE side-effects (primary doc write — bitemporal
    /// or plain — including its versioned-index tuples, FTS/inverted index,
    /// doc_cache, aggregate_cache invalidation, UNIQUE enforcement, generated
    /// columns) run regardless.
    pub enable_side_indexes: bool,
}

/// Capture of the mutations an [`CoreLoop::apply_point_put`](crate::data::executor::core_loop::CoreLoop::apply_point_put)
/// performed, so a transactional caller can build an undo entry that fully
/// reverses it.
pub(in crate::data::executor) struct PointPutOutcome {
    /// Prior stored bytes when this put replaced an existing row, else `None`.
    pub prior_value: Option<Vec<u8>>,
    /// System-time key the bitemporal version row (and its versioned index
    /// entries) were appended at. `Some(t)` on the bitemporal branch, `None`
    /// on the plain overwrite branch.
    pub bitemporal_sys_from_ms: Option<i64>,
    /// `(field, value)` pairs whose versioned index entries this op wrote at
    /// `bitemporal_sys_from_ms`. Empty when not bitemporal / none written.
    pub bitemporal_index_tuples: Vec<(String, String)>,
    /// `(field, value)` pairs this op INSERTED into the plain (non-bitemporal)
    /// secondary index. Empty on the bitemporal path (which uses
    /// `bitemporal_index_tuples`) and when `enable_side_indexes` was unset (the
    /// transactional path). A transactional caller pushes the reverse (remove)
    /// on rollback. Autocommit callers ignore it.
    pub secondary_index_added: Vec<(String, String)>,
    /// `(field, value)` pairs this op REMOVED from the plain (non-bitemporal)
    /// secondary index because an UPDATE changed the field value. Empty on the
    /// bitemporal path and when `enable_side_indexes` was unset. A transactional
    /// caller re-inserts these on rollback. Autocommit callers ignore it.
    pub secondary_index_removed: Vec<(String, String)>,
    /// `(index_key, vector_id)` pairs this put inserted into HNSW vector
    /// indexes, so a transactional caller can push `UndoEntry::InsertVector`
    /// reversals. Empty unless `enable_side_indexes` was set (the transactional
    /// path currently disables vector side-indexing, so this stays empty there
    /// until that flag flips). Autocommit callers ignore it.
    pub vector_inserts: Vec<(
        (nodedb_types::DatabaseId, crate::types::TenantId, String),
        u32,
    )>,
}

/// Map an enforcement check's `ErrorCode` onto the crate's typed `Error`.
///
/// The enforcement modules under `enforcement/` are shared with the
/// transactional path (`tx_point_put`), which surfaces `ErrorCode` directly.
/// `apply_point_put` runs inside `crate::Result`, so violations are
/// translated here to the equivalent `crate::Error` variant.
pub(in crate::data::executor) fn map_enforcement_error(e: ErrorCode) -> crate::Error {
    match e {
        ErrorCode::AppendOnlyViolation { collection } => crate::Error::AppendOnlyViolation {
            collection,
            detail: "append-only collection: UPDATE rejected".to_string(),
        },
        ErrorCode::PeriodLocked { collection } => crate::Error::PeriodLocked {
            collection,
            detail: "period is closed or locked".to_string(),
        },
        ErrorCode::StateTransitionViolation { collection, detail } => {
            crate::Error::StateTransitionViolation { collection, detail }
        }
        ErrorCode::TransitionCheckViolation { collection } => {
            crate::Error::TransitionCheckViolation {
                collection,
                detail: "transition check predicate failed".to_string(),
            }
        }
        ErrorCode::RetentionViolation { collection } => crate::Error::RetentionViolation {
            collection,
            detail: "row is younger than the configured retention period".to_string(),
        },
        ErrorCode::LegalHoldActive { collection } => crate::Error::LegalHoldActive {
            collection,
            detail: "collection has an active legal hold: DELETE rejected".to_string(),
        },
        other => crate::Error::Storage {
            engine: "enforcement".into(),
            detail: format!("unexpected enforcement error: {other:?}"),
        },
    }
}
