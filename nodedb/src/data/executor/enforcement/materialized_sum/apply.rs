// SPDX-License-Identifier: BUSL-1.1

//! Applying folded materialized-sum deltas to their target rows, for every
//! binding the plan did not defer onto a task of its own.
//!
//! The target write is a full document write, not a byte poke at the store. It
//! goes through [`CoreLoop::apply_point_put`] inside the CALLER'S transaction,
//! so the target row gets everything any other write of that row would get —
//! WAL-consistent transaction membership, inverted-index maintenance, secondary
//! and versioned index maintenance, column statistics, document-cache
//! population, aggregate-cache invalidation — and lands or rolls back together
//! with the source row that caused it. The read-modify-write itself lives in
//! [`super::rmw`], shared with the cross-shard handler so the two paths cannot
//! total differently.
//!
//! The previous implementation wrote with a bare `sparse.put`, which has none of
//! those. A balance updated that way left the target's FTS postings, secondary
//! indexes and column statistics asserting the value it used to hold, and put
//! the row's new bytes outside the transaction the source row was landing in.
//!
//! # A DEFERRED target is not applied here
//!
//! This transaction belongs to the source collection's core, and a target that
//! homes to a different vShard has no rows on it — a write here would land the
//! balance in a store no reader of the target collection ever looks at. When the
//! Control Plane can settle such a binding's delta at plan time it appends an
//! [`ApplyBalanceDelta`](nodedb_physical::physical_plan::DocumentOp::ApplyBalanceDelta)
//! task homed on the target's vShard, dual-homed with the source write through
//! Calvin. Two things say so here, and between them the delta is applied
//! exactly once:
//!
//! * an INSERT-shaped write names the binding on the plan's deferral list, and
//!   a named binding is skipped;
//! * every other shape's balance is settled from row IMAGES, and the settlement
//!   REMOVES that binding's `(target collection, join value)` pair from the
//!   plan's resolution — so a cross-shard target with no resolved surrogate is
//!   one somebody else is applying.
//!
//! Neither is re-derived: a second derivation of "did the Control Plane defer
//! this?" is free to disagree with the first, and disagreement is a
//! double-counted or a dropped balance. Only the co-residency question is asked
//! on both planes, and it is asked through the one plane-neutral function
//! ([`sum_target_is_co_resident`](crate::query::sum_target_is_co_resident))
//! that exists so the two answers cannot differ.
//!
//! A cross-shard target the plan DOES resolve is still applied here: the
//! Control-Plane orchestrators (`MERGE`, `UPDATE ... FROM`, `INSERT ... SELECT`,
//! and the staged-transaction expanders) resolve their rows and dispatch their
//! own concrete work without appending a sibling balance task, so for them the
//! resolution still means "this transaction owns it".
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
//!
//! A replica reaches this code too — replication ships the SOURCE row and every
//! node re-executes the plan, so every node folds its own delta — and it gets
//! the same resolution the same way: carried on the replicated record, decided
//! once by the node that accepted the statement. Nothing on this path resolves
//! anything, on a leader or anywhere else.

use redb::WriteTransaction;
use rust_decimal::Decimal;

use nodedb_physical::physical_plan::MaterializedSumBinding;
use nodedb_types::Surrogate;

use super::delta::fold_sum_deltas;
use super::rmw::BalanceRmw;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::types::DatabaseId;

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
    /// The MessagePack body this write handed to `apply_point_put` — NOT the
    /// bytes that reached storage.
    ///
    /// A durable redo record replays through `apply_point_put`, which encodes
    /// the body into whatever the target collection stores. Journalling the
    /// STORED bytes would hand a strict target's Binary Tuple back to the
    /// encoder on replay and store a tuple of a tuple.
    pub body: Vec<u8>,
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
            // Whether one core owns both rows. The Control Plane asked the SAME
            // question, from the same plane-neutral function, when it decided
            // whether the balance could ride this transaction — so the two
            // cannot disagree about which bindings this core is responsible
            // for.
            let co_resident = crate::query::sum_target_is_co_resident(
                DatabaseId::new(ctx.database_id),
                ctx.collection,
                &binding.target_collection,
            );
            // A binding the plan DEFERRED is applied by its own
            // `ApplyBalanceDelta` task on the target's core. Applying it here as
            // well would double-count it — and this transaction belongs to the
            // source's core, so the row it wrote would land in a store no reader
            // of the target collection consults.
            //
            // The deferral is read off the plan, never re-derived here, for the
            // same reason the target's identity is: the Control Plane decided it
            // when it appended the sibling task, and a second derivation is free
            // to disagree with the first.
            if ctx
                .deferred_sum_targets
                .contains(&binding.target_collection)
            {
                continue;
            }
            for (join_value, delta) in coalesce(fold_sum_deltas(binding, images)?) {
                // A zero net delta leaves the stored total unchanged, so the
                // read-modify-write would rewrite the row byte-for-byte. An
                // UPDATE that touched no amount produces exactly this.
                if delta == Decimal::ZERO {
                    continue;
                }
                // A cross-shard target the plan carries NO resolution for was
                // settled at plan time and travels on its own
                // `ApplyBalanceDelta` task, homed where the target's rows
                // actually live. Applying it here would count it twice — and
                // this transaction belongs to the source's core, so the row it
                // wrote would land in a store no reader of the target
                // collection consults.
                //
                // The absence of the resolution IS the instruction; nothing is
                // re-derived. A cross-shard target the plan DID resolve was
                // resolved by a Control-Plane orchestrator that ships no
                // sibling task — `MERGE`, `UPDATE ... FROM`, `INSERT ... SELECT`
                // and the staged-transaction expanders all dispatch their own
                // concrete work — so it is still this transaction's to apply.
                if !co_resident
                    && resolved_target(ctx, &binding.target_collection, &join_value).is_none()
                {
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

    /// Resolve the target row and hand the balance move to the shared
    /// read-modify-write.
    ///
    /// Identity comes from the plan and only from the plan: the surrogate
    /// resolved on the Control Plane is the one thing that may address the
    /// target row.
    ///
    /// A join value the plan carries no entry for is
    /// [`MaterializedSumResolutionMissing`](crate::Error::MaterializedSumResolutionMissing),
    /// NOT `MaterializedSumTargetNotFound`. "The target row does not exist" is a
    /// verdict on the user's statement and it is reached on the Control Plane,
    /// where the resolution pass fails the statement before any row is written.
    /// Whatever reaches here has already passed that gate — including every
    /// write a replica re-executes, which the leader accepted and resolved — so
    /// an absent entry means the resolution and the fold disagree about which
    /// rows participate. On a replica there is additionally no user to report a
    /// user error to, and reporting one would blame the application for a row
    /// the leader found.
    fn apply_one_delta(
        &mut self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        binding: &MaterializedSumBinding,
        join_value: &str,
        delta: Decimal,
    ) -> crate::Result<TargetWrite> {
        let surrogate =
            resolved_target(ctx, &binding.target_collection, join_value).ok_or_else(|| {
                crate::Error::MaterializedSumResolutionMissing {
                    target_collection: binding.target_collection.clone(),
                    join_column: binding.join_column.clone(),
                    join_value: join_value.to_string(),
                }
            })?;
        self.apply_balance_delta(
            txn,
            &BalanceRmw {
                database_id: ctx.database_id,
                tid: ctx.tid,
                target_collection: &binding.target_collection,
                target_column: &binding.target_column,
                surrogate,
                delta,
                join_column: &binding.join_column,
                join_value,
                wal_lsn: ctx.wal_lsn,
            },
        )
    }
}

/// The surrogate the Control Plane resolved THIS BINDING's join value to.
///
/// The binding's target collection is half the lookup key. One source may drive
/// two bindings that read the same join column into different targets: keyed on
/// the value alone, the second binding would find the first binding's entry and
/// this pass would write its balance into a row of the wrong collection —
/// silently, since a resolution IS present.
fn resolved_target(
    ctx: &EnforcementCtx<'_>,
    target_collection: &str,
    join_value: &str,
) -> Option<Surrogate> {
    nodedb_physical::physical_plan::resolved_sum_surrogate(
        ctx.resolved_targets,
        target_collection,
        join_value,
    )
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
    use crate::data::executor::doc_format;
    use crate::data::executor::enforcement::funnel::{
        WriteEnforcementOutcome, run_write_enforcement,
    };
    use crate::data::executor::strict_format;
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::{ResolvedSumTarget, StorageMode};
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    const DB: u64 = 0;
    const TID: u64 = 1;
    /// The source collection. Every test below drives the INLINE fold, whose
    /// entire premise is that one core owns both rows: the target is seeded into
    /// and read back out of THIS core's own store, which is only a meaningful
    /// assertion when the two collections are co-resident.
    const SOURCE: &str = "local_charges";
    /// A target that shares `SOURCE`'s vShard — asserted by
    /// [`the_local_fixture_is_co_resident`], not assumed.
    const TARGET: &str = "local_balances";
    /// A target that does NOT share `SOURCE`'s vShard, for the one test that
    /// pins the opposite rule.
    const REMOTE_TARGET: &str = "remote_balances";
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

    fn binding_onto(target: &str) -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    /// Register the source collection so the funnel finds the binding on it.
    fn register_source(core: &mut CoreLoop) {
        register_source_onto(core, TARGET);
    }

    /// Register the source collection with a binding onto `target`.
    fn register_source_onto(core: &mut CoreLoop, target: &str) {
        let mut config = CollectionConfig::new(SOURCE);
        config.enforcement.materialized_sum_sources = vec![binding_onto(target)];
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
            config,
        );
    }

    /// The premise every other test in this module rests on.
    ///
    /// The inline fold writes the target row inside the SOURCE write's
    /// transaction, on the source's core — and each core opens its own document
    /// store, so that write is only visible to a reader of the target collection
    /// when both collections home to the same vShard. Asserted rather than
    /// assumed: a change to the collection hash that silently made this pair
    /// cross-shard would otherwise turn every assertion below into a test of the
    /// DEFERRED path wearing the inline path's name.
    #[test]
    fn the_local_fixture_is_co_resident() {
        assert!(
            crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
            "'{SOURCE}' and '{TARGET}' must share a vShard for the inline fold to be observable"
        );
        assert!(
            !crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, REMOTE_TARGET),
            "'{REMOTE_TARGET}' must NOT share '{SOURCE}'s vShard; it pins the deferred path"
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
                resolved_targets: &[ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
                deferred_sum_targets: &[],
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
                resolved_targets: &[ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
                deferred_sum_targets: &[],
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

    /// A join value the plan carries no resolution for fails the write with the
    /// INTERNAL error, not the user-facing "target not found".
    ///
    /// CO-RESIDENT, and that is load-bearing: a co-resident binding is one this
    /// core applies itself, so nothing ever removes its join values from the
    /// resolution and an absent one can only mean the plan is short. The
    /// cross-shard half of the rule is
    /// [`an_unresolved_cross_shard_target_is_deferred_rather_than_refused`].
    ///
    /// Nothing here says the target row is missing — the plan simply never
    /// carried its identity. A replica re-executing a write the leader accepted
    /// hits exactly this shape when the resolution fails to reach it, and
    /// reporting it as `MaterializedSumTargetNotFound` would tell an operator
    /// the application referenced an account that does not exist.
    #[test]
    fn an_unresolved_target_fails_as_an_internal_shortfall() {
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
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc: &new_doc },
        )
        .err()
        .unwrap_or_else(|| panic!("an unresolvable target must fail the write"));

        match error {
            crate::Error::MaterializedSumResolutionMissing {
                target_collection,
                join_column,
                join_value,
            } => {
                assert_eq!(target_collection, TARGET);
                assert_eq!(join_column, "account_id");
                assert_eq!(join_value, "a-missing");
            }
            other => panic!(
                "an absent resolution must surface as MaterializedSumResolutionMissing — \
                 'target not found' is the Control Plane's verdict on the user's statement, \
                 got {other:?}"
            ),
        }
    }

    /// The MIRROR of the test above, and the rule that makes it precise: for a
    /// CROSS-SHARD binding an absent resolution is not a shortfall at all — it
    /// is how the Control Plane says "this one travels on its own task".
    ///
    /// Residency is what separates the two, and it separates them totally. A
    /// co-resident binding is never omitted from the resolution, so absent means
    /// the plan is short and the write must refuse. A cross-shard binding is
    /// always omitted once it has been settled, so absent means somebody else is
    /// applying it and this core must stand down. There is no third state for
    /// the check to guess at.
    ///
    /// Standing down has to be silent AND total: refusing would fail a write the
    /// Control Plane deliberately split, and applying would put the balance in
    /// this core's store — which no reader of the target collection opens — and
    /// then count it a second time when the sibling task ran.
    #[test]
    fn an_unresolved_cross_shard_target_is_deferred_rather_than_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                REMOTE_TARGET.to_string(),
            ),
            CollectionConfig::new(REMOTE_TARGET),
        );
        register_source_onto(&mut core, REMOTE_TARGET);

        // Seeded so that an accidental apply would be VISIBLE as a moved total
        // rather than failing on an absent row and looking like a refusal.
        let target_key = surrogate_to_doc_id(TARGET_SURROGATE);
        let seed = serde_json::json!({"id": ACCOUNT, "balance": "100"});
        core.sparse
            .put(
                DB,
                TID,
                REMOTE_TARGET,
                &target_key,
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");

        let new_doc = serde_json::json!({"account_id": ACCOUNT, "amount": 5});
        let txn = core.sparse.begin_write().expect("begin write");
        let outcome: WriteEnforcementOutcome = run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                // Empty: the Control Plane settled this binding and removed the
                // join value when it appended the sibling balance task.
                resolved_targets: &[],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc: &new_doc },
        )
        .expect("a settled cross-shard binding must not fail the source write");
        assert!(
            outcome.target_writes.is_empty(),
            "the balance travels on its own task; this core must write no target row"
        );
        txn.commit().expect("commit");

        let stored = core
            .sparse
            .get(DB, TID, REMOTE_TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("100"),
            "the total must be untouched here — the sibling task owns it"
        );
    }
}
