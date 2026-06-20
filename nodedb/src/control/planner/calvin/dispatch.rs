// SPDX-License-Identifier: BUSL-1.1

//! Calvin dispatch classification and routing for cross-shard writes.
//!
//! This module is the single chokepoint for deciding whether a set of
//! [`PhysicalTask`]s should be dispatched via:
//!
//! - The single-shard fast path (existing path, no Calvin involvement).
//! - Calvin static dispatch (all write keys known upfront).
//! - Calvin dependent-read dispatch (OLLP) (write keys depend on a pre-read).
//! - Best-effort non-atomic dispatch (each vshard independently, no atomicity).
//!
//! # Note on predicate_class
//!
//! The ideal implementation of `predicate_class` would serialize the `Filter`
//! AST via zerompk and normalize bound parameter values to their type tags.
//! However, `nodedb_sql::types::Filter` does not derive `zerompk::ToMessagePack`
//! or `zerompk::FromMessagePack`. As a declared fallback, `predicate_class`
//! accepts the canonical SQL text string (post-parse-canonicalization) and
//! normalizes numeric and string literals to their type tags before hashing.
//! This is a degraded path relative to AST-level hashing.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::calvin::sequencer::inbox::Inbox;
use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
use nodedb_types::TenantId;

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::calvin::types::{DispatchClass, DispatchOutcome};
use crate::control::server::pgwire::session::TransactionState;
use crate::control::server::pgwire::session::cross_shard_mode::CrossShardTxnMode;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{
    DocumentOp, GraphOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
};
use nodedb_physical::physical_task::PhysicalTask;

pub use crate::control::planner::calvin::predicate::predicate_class;

// ── is_write_plan ─────────────────────────────────────────────────────────────

/// Returns `true` if the plan is a write operation.
///
/// Centralizing this avoids scattered `match` arms when new write variants
/// are added. Reads, scans, and query operators return `false`.
pub fn is_write_plan(plan: &PhysicalPlan) -> bool {
    match plan {
        // Document writes
        PhysicalPlan::Document(op) => matches!(
            op,
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::InsertSelect { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::UpdateFromJoin { .. }
        ),
        // KV writes
        PhysicalPlan::Kv(op) => matches!(
            op,
            KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::BatchPut { .. }
        ),
        // Vector writes
        PhysicalPlan::Vector(op) => matches!(
            op,
            VectorOp::Insert { .. }
                | VectorOp::BatchInsert { .. }
                | VectorOp::Delete { .. }
                | VectorOp::DeleteBySurrogate { .. }
                | VectorOp::SparseInsert { .. }
                | VectorOp::SparseDelete { .. }
                | VectorOp::MultiVectorInsert { .. }
        ),
        // Graph writes
        PhysicalPlan::Graph(op) => {
            matches!(op, GraphOp::EdgePut { .. } | GraphOp::EdgeDelete { .. })
        }
        // Timeseries writes
        PhysicalPlan::Timeseries(op) => matches!(op, TimeseriesOp::Ingest { .. }),
        // Columnar writes
        PhysicalPlan::Columnar(op) => {
            use nodedb_physical::physical_plan::ColumnarOp;
            matches!(op, ColumnarOp::Insert { .. })
        }
        // CRDT writes
        PhysicalPlan::Crdt(op) => {
            use nodedb_physical::physical_plan::CrdtOp;
            matches!(op, CrdtOp::ListInsert { .. } | CrdtOp::ListDelete { .. })
        }
        // Array writes
        PhysicalPlan::Array(op) => {
            use nodedb_physical::physical_plan::ArrayOp;
            matches!(
                op,
                ArrayOp::Put { .. } | ArrayOp::Delete { .. } | ArrayOp::Flush { .. }
            )
        }
        // Everything else: reads, scans, queries, meta, spatial, text
        PhysicalPlan::Spatial(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_) => false,
    }
}

// ── is_dependent_predicate ────────────────────────────────────────────────────

/// Returns `true` if the plan contains a value-dependent predicate that
/// requires OLLP dependent-read dispatch instead of static Calvin dispatch.
///
/// The detection criterion: the plan is a `BulkUpdate` or `BulkDelete`
/// (predicate is not a point-equality on the collection's primary key).
/// Point-equality writes (`PointPut`, `PointInsert`, `PointDelete`,
/// `PointUpdate`) have their write keys statically known and are routed
/// via the static Calvin path.
pub fn is_dependent_predicate(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(DocumentOp::BulkUpdate { .. })
            | PhysicalPlan::Document(DocumentOp::BulkDelete { .. })
    )
}

// ── classify_dispatch ─────────────────────────────────────────────────────────

/// Classify the dispatch class of a task slice by collecting the unique set of
/// write vShards.
///
/// 0 or 1 unique write vShards → `SingleShard`.
/// 2+ unique write vShards → `MultiShard` with the full `BTreeSet<u32>`.
pub fn classify_dispatch(tasks: &[PhysicalTask]) -> DispatchClass {
    let mut vshards: BTreeSet<u32> = BTreeSet::new();
    let mut last_vshard = None;

    for task in tasks {
        if is_write_plan(&task.plan) {
            let id = task.vshard_id.as_u32();
            vshards.insert(id);
            last_vshard = Some(task.vshard_id);
        }
    }

    match vshards.len() {
        0 => DispatchClass::SingleShard {
            vshard: tasks
                .first()
                .map(|t| t.vshard_id)
                .unwrap_or(VShardId::new(0)),
        },
        1 => DispatchClass::SingleShard {
            vshard: last_vshard
                .expect("invariant: vshards.len() == 1 means last_vshard was set during the loop"),
        },
        _ => DispatchClass::MultiShard { vshards },
    }
}

// ── build_static_tx_class ────────────────────────────────────────────────────

/// Build a `TxClass` from a static write task slice.
///
/// Extracts `(collection, surrogate)` pairs from each write task to build
/// `EngineKeySet`s, constructs the `ReadWriteSet`, msgpack-encodes plans into
/// `Vec<u8>`, and calls `TxClass::new`.
///
/// Returns `Err(SequencerUnavailable)` if msgpack encoding of plans fails.
pub fn build_static_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
) -> crate::Result<TxClass> {
    use std::collections::HashMap;

    // Collect surrogates per collection for non-edge write tasks.
    let mut doc_surrogates: HashMap<String, Vec<u32>> = HashMap::new();
    // Collect edge identity (surrogate pairs) and routing homes
    // (from_key of src/dst string keys) per collection for graph edges.
    let mut edge_pairs: HashMap<String, Vec<(u32, u32)>> = HashMap::new();
    let mut edge_homes: HashMap<String, Vec<u32>> = HashMap::new();

    for task in tasks {
        if !is_write_plan(&task.plan) {
            continue;
        }
        // Graph edges route by from_key(src)/from_key(dst), not by collection.
        if let PhysicalPlan::Graph(GraphOp::EdgePut {
            collection,
            src_id,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        }) = &task.plan
        {
            edge_pairs
                .entry(collection.clone())
                .or_default()
                .push((src_surrogate.as_u32(), dst_surrogate.as_u32()));
            let homes = edge_homes.entry(collection.clone()).or_default();
            homes.push(VShardId::from_key(src_id.as_bytes()).as_u32());
            homes.push(VShardId::from_key(dst_id.as_bytes()).as_u32());
            continue;
        }
        let collection = collection_name_from_plan(&task.plan);
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates
            .entry(collection)
            .or_default()
            .push(surrogate);
    }

    // Build write set — one EngineKeySet per collection, sorted for
    // determinism.
    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(collection, surrogates)| EngineKeySet::Document {
            collection,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    // Emit one Edge keyset per collection, carrying surrogate-pair identity
    // (for locking) and from_key routing homes (for participating vShards).
    for (collection, pairs) in edge_pairs {
        let homes = edge_homes.remove(&collection).unwrap_or_default();
        write_sets.push(EngineKeySet::Edge {
            collection,
            edges: SortedVec::new(pairs),
            home_vshards: SortedVec::new(homes),
        });
    }
    // Sort by collection name for determinism.
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    let read_set = ReadWriteSet::new(vec![]);

    // Encode all plans as msgpack bytes.
    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin TxClass: {e}"),
    })?;

    TxClass::new(read_set, write_set, plans_bytes, tenant_id, None).map_err(|e| Error::BadRequest {
        detail: format!("invalid TxClass: {e}"),
    })
}

/// Build a `TxClass` for a dependent-read (OLLP) transaction.
///
/// For `BulkUpdate`/`BulkDelete` plans that have `ollp_predicted_surrogates`
/// set, the OLLP collection's write set is built from `predicted_surrogates`.
/// All other tasks in the batch are included using static surrogate extraction,
/// exactly as `build_static_tx_class` does. This ensures multi-shard Calvin
/// txns that contain an OLLP bulk operation alongside static-key writes still
/// produce a valid multi-vshard `TxClass`.
///
/// Returns `Err` if encoding fails or the resulting TxClass is invalid.
pub fn build_dependent_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    collection: &str,
    predicted_surrogates: &[u32],
) -> crate::Result<TxClass> {
    use std::collections::BTreeMap;

    // Accumulate per-collection surrogate sets. The OLLP collection uses the
    // predicted surrogates; all other tasks use static key extraction.
    let mut doc_surrogates: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    // Seed with the OLLP collection's predicted surrogates.
    doc_surrogates
        .entry(collection.to_owned())
        .or_default()
        .extend_from_slice(predicted_surrogates);

    // Add static surrogates for all non-OLLP tasks.
    for task in tasks {
        let coll = collection_name_from_plan(&task.plan);
        if coll.is_empty() || coll == collection {
            continue;
        }
        let surrogate = surrogate_from_plan(&task.plan);
        doc_surrogates.entry(coll).or_default().push(surrogate);
    }

    let mut write_sets: Vec<EngineKeySet> = doc_surrogates
        .into_iter()
        .map(|(coll, surrogates)| EngineKeySet::Document {
            collection: coll,
            surrogates: SortedVec::new(surrogates),
        })
        .collect();
    write_sets.sort_by(|a, b| a.collection().cmp(b.collection()));

    let write_set = ReadWriteSet::new(write_sets);
    let read_set = ReadWriteSet::new(vec![]);

    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin dependent TxClass: {e}"),
    })?;

    TxClass::new(read_set, write_set, plans_bytes, tenant_id, None).map_err(|e| Error::BadRequest {
        detail: format!("invalid dependent TxClass: {e}"),
    })
}

/// Extract the collection name from a write plan.
fn collection_name_from_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { collection, .. }
            | DocumentOp::PointInsert { collection, .. }
            | DocumentOp::PointDelete { collection, .. }
            | DocumentOp::PointUpdate { collection, .. }
            | DocumentOp::BatchInsert { collection, .. }
            | DocumentOp::Upsert { collection, .. }
            | DocumentOp::BulkUpdate { collection, .. }
            | DocumentOp::BulkDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Kv(
            KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::InsertOnConflictUpdate { collection, .. }
            | KvOp::Delete { collection, .. }
            | KvOp::BatchPut { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Vector(
            VectorOp::Insert { collection, .. }
            | VectorOp::BatchInsert { collection, .. }
            | VectorOp::Delete { collection, .. }
            | VectorOp::DeleteBySurrogate { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Graph(
            GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
        ) => collection.clone(),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => collection.clone(),
        _ => String::new(),
    }
}

/// Extract a surrogate from a write plan (returns 0 when unavailable).
fn surrogate_from_plan(plan: &PhysicalPlan) -> u32 {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut { surrogate, .. }
            | DocumentOp::PointInsert { surrogate, .. }
            | DocumentOp::PointDelete { surrogate, .. }
            | DocumentOp::PointUpdate { surrogate, .. },
        ) => surrogate.as_u32(),
        _ => 0,
    }
}

// ── dispatch_calvin_or_fast ───────────────────────────────────────────────────

/// Route a set of tasks to the appropriate dispatch path.
///
/// Decision tree:
/// 1. `InBlock` + `MultiShard` → `Err(CrossShardInExplicitTransaction)`.
/// 2. `MultiShard` + `Strict` + no inbox → `Err(SequencerUnavailable)`.
/// 3. `MultiShard` + `Strict` → Calvin static path via inbox.
/// 4. `MultiShard` + `BestEffortNonAtomic` → independent per-vshard dispatch.
/// 5. `SingleShard` → existing single-shard fast path.
///
/// The single-shard and best-effort paths are modeled here as outcomes only —
/// the caller is responsible for the actual Data Plane dispatch, since this
/// module lives in the Control Plane and has no direct Data Plane handle.
pub async fn dispatch_calvin_or_fast(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    tx_state: TransactionState,
    inbox: Option<&Inbox>,
    _orchestrator: Option<&Arc<OllpOrchestrator>>,
    tenant_id: TenantId,
) -> crate::Result<DispatchOutcome> {
    let class = classify_dispatch(tasks);

    match &class {
        DispatchClass::MultiShard { .. } => {
            // Reject cross-shard writes inside explicit transaction blocks.
            if tx_state == TransactionState::InBlock {
                return Err(Error::CrossShardInExplicitTransaction);
            }

            match mode {
                CrossShardTxnMode::Strict => {
                    let inbox = inbox.ok_or(Error::SequencerUnavailable)?;
                    let tx_class = build_static_tx_class(tasks, tenant_id)?;
                    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
                        detail: format!("Calvin sequencer rejected transaction: {e}"),
                    })?;
                    Ok(DispatchOutcome::CalvinStatic { inbox_seq })
                }
                CrossShardTxnMode::BestEffortNonAtomic => Ok(DispatchOutcome::BestEffortNonAtomic),
            }
        }
        DispatchClass::SingleShard { .. } => Ok(DispatchOutcome::SingleShard),
    }
}

// ── dispatch_dependent_read ───────────────────────────────────────────────────

/// Outer retry loop for OLLP dependent-read Calvin transactions.
///
/// Calls the orchestrator's `submit_with_retry`, which runs a single attempt.
/// On `OllpError`, retries by calling `orchestrator.on_retry_required` then
/// re-submitting, up to `ollp_max_retries`.
pub async fn dispatch_dependent_read(
    orchestrator: &OllpOrchestrator,
    inbox: &Inbox,
    predicate_class_hash: u64,
    tenant_id: TenantId,
    tx_builder: impl Fn() -> crate::Result<TxClass>,
    ollp_max_retries: u8,
) -> crate::Result<u64> {
    use crate::control::cluster::calvin::executor::ollp::error::OllpError;

    let mut retry_count: u32 = 0;

    loop {
        let result = orchestrator
            .submit_with_retry(inbox, predicate_class_hash, tenant_id, || {
                tx_builder().map_err(|_e| {
                    nodedb_cluster::error::CalvinError::Sequencer(
                        nodedb_cluster::calvin::sequencer::error::SequencerError::Unavailable,
                    )
                })
            })
            .await;

        match result {
            Ok(inbox_seq) => return Ok(inbox_seq),
            Err(OllpError::CircuitOpen { .. })
            | Err(OllpError::Sequencer(_))
            | Err(OllpError::Exhausted { .. })
            | Err(OllpError::TenantBudgetExceeded { .. }) => {
                if retry_count >= ollp_max_retries as u32 {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries,
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry_count)
                    .await;
                retry_count += 1;
            }
        }
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
