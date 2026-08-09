// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::generated;
use crate::data::executor::{doc_format, strict_format};

use super::enforce::PutEnforcement;
use super::types::{PointPutOutcome, PointPutParams};
use super::unique::{UniqueCheck, check_unique_constraints};

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction.
    ///
    /// Stores the document, auto-indexes text fields, updates column stats,
    /// and populates the document cache. Does NOT commit the transaction.
    ///
    /// `surrogate` is the stable numeric identity for this document, used
    /// to key the inverted index. `document_id` is the hex-encoded form of
    /// the surrogate (the redb storage key).
    ///
    /// On `Err` the caller MUST drop `txn` without committing. Every
    /// side-effect this writes — row body, inverted index, secondary and
    /// versioned indexes — goes into `txn`, so abandoning it is what keeps
    /// them one all-or-nothing unit. Committing after an error would publish
    /// a row whose indexes are missing entries nothing later re-derives.
    ///
    /// Returns a [`PointPutOutcome`] capturing the prior stored bytes (present
    /// when this put replaced an existing row) plus the bitemporal system time
    /// and versioned index tuples written, so a transactional caller can build
    /// a fully-reversible undo entry. Autocommit callers read only
    /// `prior_value` and thread it into `emit_write_event` so the Event Plane's
    /// `WriteOp` tag reflects the actual mutation.
    ///
    /// # `value` is an incoming body, so failing to read it as a document is an answer
    ///
    /// `value` always arrives WITH the write — a client PointPut / PointInsert
    /// body, an UPSERT body, a batch-insert row, a MERGE arm's post-image, a
    /// staged sub-plan body, a CRDT-sync delta, or a WAL redo post-image. It is
    /// never a row this function read back out of the store in order to
    /// reconcile something against it. Its only guaranteed property is that the
    /// collection accepts it: a schemaless MessagePack/JSON map, a strict
    /// collection's pre-encode MessagePack, or an opaque body that is neither —
    /// notably a strict row's Binary Tuple during WAL redo replay, where
    /// `doc_configs` is empty and `doc_format::decode_document` cannot read a
    /// Binary Tuple without the schema by design.
    ///
    /// That is why the `if let Ok(doc) = decode_document(value)` guards below
    /// are not swallowed errors. Each one gates a per-FIELD derivation —
    /// generated columns, FTS text, column statistics, secondary/UNIQUE index
    /// values, geometry detection — and a body with no readable fields yields
    /// nothing for any of them, so skipping produces the same state as running
    /// them. Where a decode is instead load-bearing, this path already fails:
    /// the strict encode below rejects a body it cannot read, and the staged
    /// UNIQUE pre-check (`stage_write::stage_point_document`) propagates rather
    /// than admitting an unchecked row.
    ///
    /// This reasoning stops holding the moment `value` becomes stored state. A
    /// stored row that will not decode is corruption, and skipping a side
    /// effect for it leaves an index asserting entries that nothing re-derives
    /// — which is why every decode of STORED bytes on this path (`old_value`
    /// for the secondary-index diff, the enforcement pre-image) propagates.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let PointPutParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            index_text,
            user_roles,
            enforce,
            wal_lsn,
        } = params;
        // Evaluate generated columns before encoding.
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let value = if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            // Incoming body, per the invariant above: a body with no readable
            // fields has no column for a generated expression to read or write,
            // so it is stored as supplied rather than rejected.
            if let Ok(mut doc) = doc_format::decode_document(value) {
                if let Err(e) = generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                ) {
                    return Err(crate::Error::Storage {
                        engine: "generated".into(),
                        detail: format!("generated column evaluation failed: {e:?}"),
                    });
                }
                doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            doc_format::canonicalize_document_for_storage(value)
        };
        let value = &value;

        // A resolve-time stamp carried in `active_bitemporal_stamps` (present on
        // the commit-time base install and on WAL replay of an 8-tuple document
        // redo) forces the versioned branch at the EXACT stamp the redo carries,
        // independent of `doc_configs` — which is empty during WAL replay. This
        // is what keeps a normal restart from writing a SECOND version of the
        // row and a crash-window restart from landing it on the plain table.
        // Absent an override, keep the autocommit behavior: derive bitemporality
        // from config and mint a fresh monotonic stamp.
        let (bitemporal, sys_from_ms, valid_from_ms, valid_until_ms) =
            match self.active_bitemporal_stamps.get(&surrogate.as_u32()) {
                Some(stamp) => (
                    true,
                    stamp.sys_from_ms,
                    stamp.valid_from_ms,
                    stamp.valid_until_ms,
                ),
                None => (
                    self.is_bitemporal(database_id, tid, collection),
                    self.bitemporal_now_ms(),
                    i64::MIN,
                    i64::MAX,
                ),
            };

        // Strict (Binary Tuple) encoding pipeline. Runs in two steps under
        // a single doc-config lookup:
        //   (1) When the schema has an auto-generated `_rowid` primary key
        //       (injected by `build_strict_schema` when no explicit PK is
        //       declared), the client INSERT payload won't contain it.
        //       Inject it from the surrogate before encoding so the NOT NULL
        //       constraint is satisfied.
        //   (2) Encode the (possibly-injected) MessagePack into Binary Tuple.
        // Downstream indexing reads the rebound `value` so it sees the
        // injected `_rowid` alongside the user's fields.
        let value_with_rowid: Vec<u8>;
        let (value, stored): (&[u8], Vec<u8>) = if let Some(config) =
            self.doc_configs.get(&config_key)
            && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            let encoded_input: &[u8] = if schema
                .columns
                .first()
                .is_some_and(|c| c.name == "_rowid" && !c.nullable)
                && let Ok(mut decoded) = nodedb_types::json_from_msgpack(value)
                && let serde_json::Value::Object(ref mut obj) = decoded
                && !obj.contains_key("_rowid")
            {
                obj.insert(
                    "_rowid".to_string(),
                    serde_json::Value::Number((surrogate.0 as i64).into()),
                );
                value_with_rowid =
                    nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.to_vec());
                &value_with_rowid
            } else {
                value
            };

            let stored = if bitemporal && schema.bitemporal {
                strict_format::bytes_to_binary_tuple_bitemporal(
                    encoded_input,
                    schema,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                )
            } else {
                strict_format::bytes_to_binary_tuple(encoded_input, schema)
            }
            .map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e.to_string(),
            })?;

            (encoded_input, stored)
        } else {
            (value, value.to_vec())
        };

        // Read the prior stored value before the write lands, but only when
        // something downstream actually needs it: bitemporal collections
        // always need the current version (it becomes `prior` below), and
        // enforcement-configured collections need it to feed the stateless
        // PUT checks. The common case (non-bitemporal, no put-enforcement
        // configured) skips this read entirely — `prior` for that case
        // comes solely from `put_in_txn`'s own return value.
        //
        // The plain (non-bitemporal) secondary-index diff also needs the old
        // bytes: an UPDATE that changes an indexed field must drop the stale
        // index entry, which requires knowing the prior value. So read the old
        // value whenever the collection has index paths — exactly the case that
        // would otherwise leak stale entries.
        let need_old = bitemporal
            || (enforce
                && self
                    .doc_configs
                    .get(&config_key)
                    .is_some_and(|config| config.enforcement.has_put_checks()))
            || self
                .doc_configs
                .get(&config_key)
                .is_some_and(|config| !config.index_paths.is_empty());
        let old_value = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, document_id)?
        } else if need_old {
            self.sparse.get(database_id, tid, collection, document_id)?
        } else {
            None
        };
        // Decode the pre-write document for the non-bitemporal secondary-index
        // SET diff. Borrowed here (before `old_value` may be moved into `prior`
        // on the bitemporal branch below); bitemporal reverses via versioned
        // index tuples instead, so it needs no old-doc diff.
        // Strict collections store the old row as a Binary Tuple, which
        // `doc_format::decode_document` cannot decode without the schema —
        // route through the storage-mode-aware helper so strict UPDATEs also
        // compute their real old index values (and thus drop stale entries).
        // `None` here means "there is no prior row to diff against" — an INSERT,
        // a bitemporal collection (which reverses via versioned index tuples
        // instead), or an unregistered collection with no index paths. A prior
        // row that exists but will not decode is NOT that case: it would leave
        // the row's old index entries asserted forever, so it fails the write.
        let old_doc_for_index: Option<serde_json::Value> = if bitemporal {
            None
        } else {
            match (old_value.as_ref(), self.doc_configs.get(&config_key)) {
                (Some(b), Some(config)) => Some(self.decode_stored_document(config, b)?),
                _ => None,
            }
        };

        // Admission runs on the pre-image and the incoming body, before any
        // store or index is touched, so a refusal leaves nothing behind.
        // `value` is already the MessagePack form for both storage modes by
        // this point (strict encodes to its tuple separately, into `stored`).
        self.check_stateless_put_enforcement(
            enforce,
            PutEnforcement {
                config_key: &config_key,
                database_id,
                tid,
                collection,
                value,
                old_value: &old_value,
                user_roles,
            },
        )?;

        // Bitemporal collections version every write: append a new version
        // at `sys_from = now()`, returning the current (pre-write) version
        // read above as the `prior` slot. Non-bitemporal collections use
        // the legacy overwrite path, returning the old bytes redb replaced.
        let prior = if bitemporal {
            self.sparse.versioned_put_in_txn(
                txn,
                crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    doc_id: document_id,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                    body: &stored,
                },
            )?;
            old_value
        } else {
            self.sparse
                .put_in_txn(txn, database_id, tid, collection, document_id, &stored)?
        };

        // Pre-image capture for the column-stats read-modify-write, so a
        // transactional caller can restore the exact prior stats on rollback.
        let mut stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage> = Vec::new();

        // Text indexing and stats use the original JSON input, not the stored
        // bytes — Binary Tuple requires a schema to decode, and the input JSON
        // is already available here regardless of storage mode.
        //
        // Incoming body, per the invariant above: a body that is not a readable
        // document contributes no indexable text and no per-column statistics,
        // so there is nothing for this block to do. Note the contrast one level
        // in — once the document IS readable, an inverted-index write that
        // fails is a real failure and rejects the write.
        if let Ok(doc) = doc_format::decode_document(value) {
            // Shared extraction: the DELETE-rollback re-index path recomputes
            // the exact same text from the restored body via this helper.
            let text_content = crate::data::executor::fts_text::extract_fts_text(&doc);
            // Empty text is NOT skipped: an update that strips a document of
            // every indexable word must take it out of the index, and only
            // the index side knows whether a prior version put it in.
            if index_text {
                // The index write lands in the CALLER'S transaction — the very
                // one the row body was written into above (the inverted index
                // is opened on `sparse.db()`, so both are the same redb
                // database). Propagating therefore makes row + index one
                // all-or-nothing durable unit: every caller returns before
                // `txn.commit()`, redb rolls the uncommitted transaction back
                // on drop, and neither the row nor its postings land.
                //
                // Swallowing it would be permanent, not transient: this path
                // emits no `FtsIndex` WAL record, so replay cannot re-derive
                // the missing postings; the next write to the document indexes
                // only the NEW text; and no query can tell the gap apart from a
                // document that genuinely does not match. The row would stay
                // invisible to full-text search until a manual reindex.
                if let Err(e) = self.inverted.index_document_in_txn(
                    txn,
                    crate::engine::sparse::inverted::IndexDocScope {
                        database_id,
                        tid: crate::types::TenantId::new(tid),
                        collection,
                        surrogate,
                    },
                    &text_content,
                ) {
                    // Recorded here, at the detection site, and never
                    // re-emitted as the error propagates: a log line is gone at
                    // the next restart, the capture is an fsync'd report that
                    // survives it and names which collection's index refused.
                    crate::diag::fts_index_update_failed(&e, collection, surrogate.as_u32());
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed; rejecting the write");
                    return Err(e);
                }
            }

            match self
                .stats_store
                .observe_document_in_txn(txn, database_id, tid, collection, &doc)
            {
                Ok(pre) => stats_prior = pre,
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
                }
            }

            self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);
        }

        self.doc_cache
            .put(database_id, tid, collection, document_id, &stored);

        // Secondary index extraction: if this collection has registered
        // index paths, extract values and write them into the INDEXES redb
        // B-Tree inside the CALLER'S write txn. Using the non-_in_txn
        // variant here would deadlock — `execute_point_put` already owns
        // the only writer.
        //
        // UNIQUE enforcement runs first: for every `unique: true` path we
        // check whether the incoming value already belongs to a different
        // document and reject with a typed constraint error. The check
        // uses the sparse engine's read API, which opens a separate read
        // transaction (redb MVCC) — the read view won't see our outer
        // write txn but that's precisely the semantics we want for the
        // "does another row already hold this value" question.
        let mut bitemporal_index_tuples: Vec<(String, String)> = Vec::new();
        let mut secondary_index_added: Vec<(String, String)> = Vec::new();
        let mut secondary_index_removed: Vec<(String, String)> = Vec::new();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // Incoming body, per the invariant above. `extract_index_values` reads
        // named paths out of a decoded document, so a body that is not one
        // yields no index values — and therefore no UNIQUE candidate to
        // conflict with and no entry to write. Skipping is the same outcome as
        // running both, not a check quietly waived.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Ok(doc) = doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            // UNIQUE enforcement is a CORE side-effect: it must run in both the
            // autocommit and transactional paths (a violation rejects the write
            // before commit).
            check_unique_constraints(UniqueCheck {
                sparse: &self.sparse,
                database_id,
                tid,
                collection,
                doc: &doc,
                document_id,
                paths: &paths,
                bitemporal,
            })?;
            if bitemporal {
                // Versioned index entries are keyed at the SAME system time as
                // the primary version row written above (`sys_from_ms`), so a
                // single `bitemporal_sys_from_ms` in the undo entry reverses
                // both together. These are CORE (undoable via the captured
                // tuples).
                for path in &paths {
                    if let Some(ref pred) = path.predicate
                        && !pred.evaluate_json(&doc)
                    {
                        continue;
                    }
                    for v in crate::engine::document::store::extract_index_values(
                        &doc,
                        &path.path,
                        path.is_array,
                    ) {
                        let value = if path.case_insensitive {
                            v.to_lowercase()
                        } else {
                            v
                        };
                        self.sparse.versioned_index_put_in_txn(
                            txn,
                            crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                field: &path.path,
                                value: &value,
                                doc_id: document_id,
                                sys_from_ms,
                            },
                        )?;
                        bitemporal_index_tuples.push((path.path.clone(), value));
                    }
                }
            } else {
                // Non-bitemporal secondary index write. The
                // SET diff against `old_doc_for_index` inserts new values and
                // removes stale ones (fixing the leaked-entry-on-UPDATE bug).
                // The (added, removed) tuples are captured so a transactional
                // caller can reverse them on rollback.
                let (added, removed) = self.apply_secondary_indexes_in_txn(
                    txn,
                    crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                        database_id,
                        tid,
                        collection,
                        old_doc: old_doc_for_index.as_ref(),
                        new_doc: &doc,
                        doc_id: document_id,
                        index_paths: &paths,
                    },
                )?;
                secondary_index_added = added;
                secondary_index_removed = removed;
            }
        }

        let spatial_inserts =
            self.apply_point_put_spatial(database_id, tid, collection, document_id, value);
        let vector_inserts = self.apply_point_put_vector_indexes(
            crate::data::executor::handlers::point::apply_put::VectorIndexPutParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                value,
                wal_lsn: wal_lsn.map(|l| l.as_u64()).unwrap_or(0),
            },
        )?;
        // Sparse inverted-index maintenance mirrors the dense-vector side-effect
        // above: a no-op unless the strict schema declares a `SparseVector`
        // column, so non-sparse collections are byte-identical to before.
        self.apply_point_put_sparse_indexes(database_id, tid, collection, document_id, value);

        Ok(PointPutOutcome {
            prior_value: prior,
            stored_value: stored,
            bitemporal_sys_from_ms: if bitemporal { Some(sys_from_ms) } else { None },
            bitemporal_index_tuples,
            secondary_index_added,
            secondary_index_removed,
            vector_inserts,
            spatial_inserts,
            stats_prior,
        })
    }
}

#[cfg(test)]
mod tests {
    use redb::TableDefinition;

    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::point::put::PointPutExec;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::surrogate_to_doc_id;
    use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::Surrogate;
    use std::time::{Duration, Instant};

    const TID: u64 = 1;
    const COLL: &str = "articles";
    const SURROGATE: Surrogate = Surrogate(7);
    /// Raw JSON body — `doc_format::decode_document`'s JSON fallback accepts
    /// it, and its single string field is what `extract_fts_text` feeds the
    /// inverted index, so this document has real text to index.
    const BODY: &[u8] = br#"{"title":"alpha bravo charlie"}"#;

    /// A table sharing `DOC_LENGTHS`'s redb name but with incompatible
    /// key/value types.
    ///
    /// Installing it is how these tests obtain a deterministic, structural
    /// inverted-index failure with no mock layer: the very first thing
    /// `index_document_in_txn` does is open `DOC_LENGTHS`, which then fails
    /// with a table-type mismatch on every attempt.
    const POISONED_DOC_LENGTHS: TableDefinition<u64, u64> =
        TableDefinition::new("text.doc_lengths");

    /// Swap the real `DOC_LENGTHS` table for the type-mismatched one so every
    /// subsequent inverted-index write fails.
    fn poison_inverted_index(core: &CoreLoop) {
        let db = core.sparse.db().clone();
        let txn = db.begin_write().unwrap();
        txn.delete_table(DOC_LENGTHS).unwrap();
        txn.open_table(POISONED_DOC_LENGTHS).unwrap();
        txn.commit().unwrap();
    }

    fn point_put_task(row_key: &str) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: COLL.into(),
                document_id: row_key.into(),
                value: BODY.to_vec(),
                surrogate: SURROGATE,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        })
    }

    fn stored_row(core: &CoreLoop, row_key: &str) -> Option<Vec<u8>> {
        core.sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, COLL, row_key)
            .unwrap()
    }

    /// Control: with a healthy index the write commits AND the document is
    /// counted into the corpus. Without this, a passing failure test could not
    /// distinguish "the poison caused the rejection" from "this document was
    /// never writable in the first place".
    #[test]
    fn healthy_index_commits_the_row_and_indexes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = surrogate_to_doc_id(SURROGATE);

        let task = point_put_task(&row_key);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );

        assert_eq!(resp.status, Status::Ok);
        assert!(stored_row(&core, &row_key).is_some(), "row must be stored");
        let (doc_count, _avg_len) = core
            .inverted
            .corpus_stats(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID), COLL)
            .unwrap();
        assert_eq!(doc_count, 1, "the committed row must be in the FTS corpus");
    }

    /// The defect this guards: an inverted-index failure used to be logged and
    /// stepped over, leaving a committed row that full-text search could never
    /// see and that nothing — not WAL replay, not the next write — would ever
    /// re-index. The write must now be rejected outright, with no row left
    /// behind in the store or in the read-through document cache.
    #[test]
    fn index_failure_rejects_the_write_and_leaves_no_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = surrogate_to_doc_id(SURROGATE);
        poison_inverted_index(&core);

        let task = point_put_task(&row_key);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );

        assert_eq!(
            resp.status,
            Status::Error,
            "the client must be told the write failed, not receive a silent ack"
        );
        assert!(
            stored_row(&core, &row_key).is_none(),
            "the rejected write must leave no committed row — a stored row whose \
             index update failed is invisible to full-text search forever"
        );
        assert!(
            core.doc_cache
                .get(DatabaseId::DEFAULT.as_u64(), TID, COLL, &row_key)
                .is_none(),
            "the rejected write must not populate the document cache either, or \
             reads would serve a row that is not in durable storage"
        );
    }

    /// The same guarantee stated at the helper level, because every caller of
    /// `apply_point_put` (autocommit put/insert, upsert, batch write, merge,
    /// transactional sub-plan, WAL redo) depends on it: the error surfaces
    /// instead of being absorbed, so the caller's transaction is dropped
    /// un-committed and the row never lands.
    #[test]
    fn apply_point_put_propagates_index_failure_instead_of_absorbing_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = surrogate_to_doc_id(SURROGATE);
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                index_text: true,
                user_roles: &[],
                enforce: true,
                wal_lsn: None,
            },
        );

        assert!(
            result.is_err(),
            "an inverted-index failure must propagate to the caller"
        );
        // Dropping the transaction un-committed is exactly what every caller
        // does on this error, and it is what makes row + index one unit.
        drop(txn);
        assert!(
            stored_row(&core, &row_key).is_none(),
            "aborting the shared transaction must roll the row body back too"
        );
    }

    /// `index_text: false` (CRDT-sync materialization, which receives its text
    /// through a separate FTS frame) must stay unaffected: it never calls the
    /// index at all, so a broken index cannot block it.
    #[test]
    fn index_text_disabled_is_unaffected_by_a_broken_index() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = surrogate_to_doc_id(SURROGATE);
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                index_text: false,
                user_roles: &[],
                enforce: true,
                wal_lsn: None,
            },
        );

        assert!(
            result.is_ok(),
            "a put that does not index must not be gated"
        );
        txn.commit().unwrap();
        assert!(stored_row(&core, &row_key).is_some());
    }
}
