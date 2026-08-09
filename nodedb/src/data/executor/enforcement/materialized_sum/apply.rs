// SPDX-License-Identifier: BUSL-1.1

//! Applying folded materialized-sum deltas to their target rows.
//!
//! The target write is a full document write, not a byte poke at the store. It
//! goes through [`CoreLoop::apply_point_put`] inside the CALLER'S transaction,
//! so the target row gets everything any other write of that row would get —
//! WAL-consistent transaction membership, inverted-index maintenance, secondary
//! and versioned index maintenance, column statistics, document-cache
//! population, aggregate-cache invalidation — and lands or rolls back together
//! with the source row that caused it.
//!
//! The previous implementation wrote with a bare `sparse.put`, which has none of
//! those. A balance updated that way left the target's FTS postings, secondary
//! indexes and column statistics asserting the value it used to hold, and put
//! the row's new bytes outside the transaction the source row was landing in.
//!
//! # Identity comes from the plan, never from a store probe
//!
//! Rows are keyed by an 8-hex surrogate
//! ([`surrogate_to_doc_id`](crate::engine::document::store::surrogate_to_doc_id)),
//! so a join-key VALUE is not a storage key. The Control Plane resolves each
//! join value to its target row's surrogate at plan time and the resolution
//! arrives on
//! [`EnforcementCtx::resolved_targets`](crate::data::executor::enforcement::images::EnforcementCtx).
//! Deriving it here would mean a Data-Plane copy of the primary-key → surrogate
//! map, which is Control-Plane catalog state.

use redb::WriteTransaction;
use rust_decimal::Decimal;

use nodedb_physical::physical_plan::MaterializedSumBinding;
use nodedb_types::Surrogate;

use super::delta::{fold_sum_deltas, json_to_decimal};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::point::apply_put::{PointPutOutcome, PointPutParams};
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::engine::document::store::surrogate_to_doc_id;

/// A target row this write updated, captured so a transactional caller can
/// reverse it.
pub(in crate::data::executor) struct TargetWrite {
    /// Target collection name.
    pub collection: String,
    /// Storage key of the target row — the hex-encoded surrogate.
    pub document_id: String,
    /// The target row's surrogate, so an undo entry addresses the same identity
    /// the forward write used. The old code had no surrogate to record and
    /// pushed `Surrogate::ZERO`.
    pub surrogate: Surrogate,
    /// Everything the derived write mutated: the pre-image, the versioned and
    /// secondary index tuples, the vector and spatial inserts, the column-stats
    /// pre-images. A transactional caller reverses a target write with exactly
    /// the undo entries it uses for the source row — anything less leaves the
    /// target's indexes asserting a balance a rollback removed.
    pub outcome: PointPutOutcome,
}

impl CoreLoop {
    /// Apply every binding's folded deltas to their target rows.
    ///
    /// Each binding is folded (see
    /// [`fold_sum_deltas`](super::delta::fold_sum_deltas)) into signed deltas,
    /// the deltas for one target are summed so a single row is read and written
    /// once, and each surviving non-zero total is applied as a read-modify-write
    /// through `apply_point_put` inside `txn`.
    ///
    /// A `&mut CoreLoop` because the target write is a real document write.
    /// Bindings are passed in rather than read from `doc_configs` here for the
    /// same reason: the caller owns the immutable borrow of the config.
    pub(in crate::data::executor) fn apply_materialized_sums(
        &mut self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        bindings: &[MaterializedSumBinding],
        images: &RowImages<'_>,
    ) -> crate::Result<Vec<TargetWrite>> {
        let mut writes: Vec<TargetWrite> = Vec::new();
        for binding in bindings {
            for (join_value, delta) in coalesce(fold_sum_deltas(binding, images)?) {
                // A zero net delta leaves the stored total unchanged, so the
                // read-modify-write would rewrite the row byte-for-byte. An
                // UPDATE that touched no amount produces exactly this.
                if delta == Decimal::ZERO {
                    continue;
                }
                match self.apply_one_delta(txn, ctx, binding, &join_value, delta) {
                    Ok(write) => writes.push(write),
                    Err(e) => {
                        // The caller drops `txn`, which reverses every target
                        // row this pass already wrote — but not the read-through
                        // cache entries those writes populated. Left behind, they
                        // serve balances that no longer exist in storage.
                        for write in &writes {
                            self.doc_cache.invalidate(
                                ctx.database_id,
                                ctx.tid,
                                &write.collection,
                                &write.document_id,
                            );
                        }
                        return Err(e);
                    }
                }
            }
        }
        Ok(writes)
    }

    /// Read one target row, add `delta` to its balance column, and write it back
    /// through the full document write path.
    fn apply_one_delta(
        &mut self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        binding: &MaterializedSumBinding,
        join_value: &str,
        delta: Decimal,
    ) -> crate::Result<TargetWrite> {
        let surrogate = resolved_target(ctx, join_value).ok_or_else(|| {
            crate::Error::MaterializedSumTargetNotFound {
                target_collection: binding.target_collection.clone(),
                join_column: binding.join_column.clone(),
                join_value: join_value.to_string(),
            }
        })?;
        let document_id = surrogate_to_doc_id(surrogate);

        // The TARGET collection's encoding is resolved from `doc_configs`, not
        // assumed: the target is a different collection from the source and may
        // be strict (Binary Tuples), which the schemaless decoder cannot read.
        let format = self.sparse_body_format(
            crate::types::DatabaseId::new(ctx.database_id),
            crate::types::TenantId::new(ctx.tid),
            &binding.target_collection,
        );
        if matches!(format, SparseBodyFormat::VectorSidecar) {
            // A vector-primary collection's rows are TAGGED `zerompk` sidecars
            // written by the vector upsert handler, not document bodies. The
            // document write path below would store an untagged map over them,
            // which reads back as tag arrays. Refusing is the only outcome that
            // does not corrupt the row.
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target collection '{}' is vector-primary; its rows are metadata \
                     sidecars and cannot carry a materialized sum",
                    binding.target_collection
                ),
            });
        }

        let old_bytes = self.read_target_row(txn, ctx, &binding.target_collection, &document_id)?;
        let Some(old_bytes) = old_bytes else {
            // The Control Plane resolved this join value to a surrogate, so the
            // row is expected to exist. Skipping instead would leave the stored
            // total short of the `SUM(...)` that `VERIFY_BALANCE` recomputes
            // over every source row — the feature would report itself broken.
            return Err(crate::Error::MaterializedSumTargetNotFound {
                target_collection: binding.target_collection.clone(),
                join_column: binding.join_column.clone(),
                join_value: join_value.to_string(),
            });
        };

        let mut target_doc = decode_scanned_document(&old_bytes, format.as_format_ref())?;
        let current = target_doc
            .get(&binding.target_column)
            .and_then(json_to_decimal)
            .unwrap_or(Decimal::ZERO);
        let new_balance = current + delta;

        // Always stored as a string: `f64` is lossy past 15 significant digits,
        // and a balance is exactly the column where that shows up.
        let Some(object) = target_doc.as_object_mut() else {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target row {}/{document_id} is not an object",
                    binding.target_collection
                ),
            });
        };
        object.insert(
            binding.target_column.clone(),
            serde_json::Value::String(new_balance.to_string()),
        );

        // `apply_point_put` takes an incoming BODY and encodes it into whatever
        // the target collection stores — a Binary Tuple for a strict target —
        // so the body handed to it is MessagePack for every storage mode. The
        // decode above still has to be format-aware, because the bytes read back
        // out of the store are in the collection's own encoding.
        let body = doc_format::encode_to_msgpack(&target_doc);

        let put = self.apply_point_put(
            txn,
            PointPutParams {
                database_id: ctx.database_id,
                tid: ctx.tid,
                collection: &binding.target_collection,
                document_id: &document_id,
                surrogate,
                value: &body,
                index_text: true,
                // A derived write carries no user intent and no user roles: its
                // admission was decided when the SOURCE row was admitted. Running
                // the target's own PUT admission (append-only, period lock,
                // role-gated state transitions) against it would refuse a write
                // the user never issued, on a row whose only changed column is
                // one the engine maintains.
                user_roles: &[],
                enforce: false,
                wal_lsn: ctx.wal_lsn,
            },
        );
        let outcome = match put {
            Ok(outcome) => outcome,
            Err(e) => {
                // A rejection late in `apply_point_put` lands after it has already
                // cached the row it wrote. The caller drops `txn`, so that cache
                // entry would outlive a balance update that never committed.
                self.doc_cache.invalidate(
                    ctx.database_id,
                    ctx.tid,
                    &binding.target_collection,
                    &document_id,
                );
                return Err(e);
            }
        };

        Ok(TargetWrite {
            collection: binding.target_collection.clone(),
            document_id,
            surrogate,
            outcome,
        })
    }

    /// Read the target row's current stored bytes.
    ///
    /// The plain read goes through the CALLER'S write transaction so a second
    /// delta against the same row in the same transaction sees the first one's
    /// result. A bitemporal target reads its current version the same way
    /// `apply_point_put` reads its own pre-image.
    fn read_target_row(
        &self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        target_collection: &str,
        document_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(ctx.database_id, ctx.tid, target_collection) {
            self.sparse.versioned_get_current(
                ctx.database_id,
                ctx.tid,
                target_collection,
                document_id,
            )
        } else {
            self.sparse.get_in_txn(
                txn,
                ctx.database_id,
                ctx.tid,
                target_collection,
                document_id,
            )
        }
    }
}

/// The surrogate the Control Plane resolved this join value to.
fn resolved_target(ctx: &EnforcementCtx<'_>, join_value: &str) -> Option<Surrogate> {
    ctx.resolved_targets
        .iter()
        .find(|(value, _)| value.as_str() == join_value)
        .map(|(_, surrogate)| *surrogate)
}

/// Sum the deltas that address the same target, preserving first-seen order.
///
/// Two deltas against one row would otherwise be two read-modify-writes, and
/// the second would have to observe the first — which is the whole reason the
/// plain read goes through the caller's transaction. Summing first makes the
/// question moot for deltas produced by a single fold.
fn coalesce(deltas: Vec<super::delta::SumDelta>) -> Vec<(String, Decimal)> {
    let mut totals: Vec<(String, Decimal)> = Vec::with_capacity(deltas.len());
    for delta in deltas {
        match totals
            .iter()
            .position(|(join_value, _)| join_value.as_str() == delta.join_value.as_str())
        {
            Some(index) => totals[index].1 += delta.delta,
            None => totals.push((delta.join_value, delta.delta)),
        }
    }
    totals
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::enforcement::funnel::{
        WriteEnforcementOutcome, run_write_enforcement,
    };
    use crate::data::executor::strict_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const SOURCE: &str = "ms_entries";
    const TARGET: &str = "ms_accounts";
    const ACCOUNT: &str = "a1";
    const TARGET_SURROGATE: Surrogate = Surrogate(4242);

    /// A strict target: `owner` is untouched by the sum and exists purely to
    /// prove the whole row survived the write-back.
    fn strict_target_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("owner", ColumnType::String),
            ColumnDef::required("balance", ColumnType::String),
        ])
        .expect("schema")
    }

    fn binding() -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    /// Register the source collection so the funnel finds the binding on it.
    fn register_source(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(SOURCE);
        config.enforcement.materialized_sum_sources = vec![binding()];
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
            config,
        );
    }

    /// Drive one source INSERT through the funnel and commit its transaction.
    fn insert_source_row(core: &mut CoreLoop, new_doc: &serde_json::Value) {
        let txn = core.sparse.begin_write().expect("begin write");
        let outcome: WriteEnforcementOutcome = run_write_enforcement(
            core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[(ACCOUNT.to_string(), TARGET_SURROGATE)],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc },
        )
        .expect("enforcement must apply the materialized sum");
        assert_eq!(
            outcome.target_writes.len(),
            1,
            "the source row credits exactly one target"
        );
        assert_eq!(outcome.target_writes[0].surrogate, TARGET_SURROGATE);
        txn.commit().expect("commit");
    }

    /// MATERIALIZED SUM over a `document_strict` target must total correctly AND
    /// leave the row a Binary Tuple.
    ///
    /// The target is a different collection from the source, so its encoding has
    /// to be resolved from `doc_configs` on BOTH halves of the read-modify-write.
    /// Reading with the schemaless decoder failed on every strict row (the
    /// feature was simply broken there); writing msgpack back would have been
    /// worse — the row survives the statement and is unreadable to every strict
    /// reader afterwards.
    ///
    /// The target row is seeded under `surrogate_to_doc_id`, the key every
    /// reader of that collection uses. Seeding under the raw join VALUE would
    /// only prove that a lookup keyed by the same wrong value finds it.
    #[test]
    fn a_strict_target_totals_correctly_and_stays_a_binary_tuple() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        let schema = strict_target_schema();
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET).with_storage_mode(StorageMode::Strict {
                schema: schema.clone(),
            }),
        );
        register_source(&mut core);

        // Seed the target row in the encoding a strict collection actually
        // stores: a Binary Tuple, not msgpack.
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::String(ACCOUNT.into()));
        row.insert("owner".to_string(), Value::String("alice".into()));
        row.insert("balance".to_string(), Value::String("100".into()));
        let tuple = strict_format::value_to_binary_tuple(&Value::Object(row), &schema)
            .expect("encode seed tuple");
        let target_key = surrogate_to_doc_id(TARGET_SURROGATE);
        core.sparse
            .put(DB, TID, TARGET, &target_key, &tuple)
            .expect("seed target row");

        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 25}),
        );
        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 75}),
        );

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");

        // The stored bytes must still be a Binary Tuple. `binary_tuple_to_json`
        // is what every reader of this collection uses; if the write-back had
        // emitted msgpack this returns `None` and the row is lost.
        let decoded = strict_format::binary_tuple_to_json(&stored, &schema)
            .expect("the stored row must still be a readable Binary Tuple");

        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("200"),
            "100 + 25 + 75 must be totalled onto the strict row: {decoded:?}"
        );
        assert_eq!(
            decoded.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "columns the sum does not touch must survive the re-encode"
        );
        assert_eq!(decoded.get("id").and_then(|v| v.as_str()), Some(ACCOUNT));
    }

    /// The schemaless target keeps working — the encoding is chosen per
    /// collection, so fixing strict must not have moved schemaless onto the
    /// strict encoder.
    #[test]
    fn a_schemaless_target_still_totals_and_stays_msgpack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let seed = serde_json::json!({"id": ACCOUNT, "owner": "alice", "balance": "100"});
        let body = doc_format::encode_to_msgpack(&seed);
        let target_key = surrogate_to_doc_id(TARGET_SURROGATE);
        core.sparse
            .put(DB, TID, TARGET, &target_key, &body)
            .expect("seed target row");

        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 50}),
        );

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded =
            doc_format::decode_document(&stored).expect("a schemaless row must stay msgpack");
        assert_eq!(decoded.get("balance").and_then(|v| v.as_str()), Some("150"));
        assert_eq!(decoded.get("owner").and_then(|v| v.as_str()), Some("alice"));
    }

    /// A DELETE subtracts, against the SAME storage key an insert credited.
    #[test]
    fn a_delete_subtracts_from_the_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let target_key = surrogate_to_doc_id(TARGET_SURROGATE);
        let seed = serde_json::json!({"id": ACCOUNT, "balance": "100"});
        core.sparse
            .put(
                DB,
                TID,
                TARGET,
                &target_key,
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");

        let old_doc = serde_json::json!({"account_id": ACCOUNT, "amount": 30});
        let txn = core.sparse.begin_write().expect("begin write");
        run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[(ACCOUNT.to_string(), TARGET_SURROGATE)],
                wal_lsn: None,
            },
            RowImages::Delete { old_doc: &old_doc },
        )
        .expect("a delete must be applied, not ignored");
        txn.commit().expect("commit");

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("70"),
            "a deleted row's contribution must come back off the total"
        );
    }

    /// A join value with no resolved target fails the write with the typed
    /// error that names the collection, column and value.
    #[test]
    fn an_unresolved_target_fails_with_the_typed_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let new_doc = serde_json::json!({"account_id": "a-missing", "amount": 5});
        let txn = core.sparse.begin_write().expect("begin write");
        let error = run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc: &new_doc },
        )
        .err()
        .unwrap_or_else(|| panic!("an unresolvable target must fail the write"));

        match error {
            crate::Error::MaterializedSumTargetNotFound {
                target_collection,
                join_column,
                join_value,
            } => {
                assert_eq!(target_collection, TARGET);
                assert_eq!(join_column, "account_id");
                assert_eq!(join_value, "a-missing");
            }
            other => panic!("expected MaterializedSumTargetNotFound, got {other:?}"),
        }
    }
}
