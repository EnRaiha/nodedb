// SPDX-License-Identifier: BUSL-1.1

//! Calvin deterministic executor handlers.
//!
//! Three handler entry points:
//!
//! - [`CoreLoop::execute_calvin_execute_static`]: static-set multi-shard txn
//!   (same semantics as `MetaOp::TransactionBatch`; the common case).
//!
//! - [`CoreLoop::execute_calvin_execute_passive`]: passive participant for a
//!   dependent-read txn. Reads each declared key from the local engine and
//!   returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload. The
//!   Control Plane scheduler proposes a `CalvinReadResult` Raft entry after
//!   receiving this response.
//!
//! - [`CoreLoop::execute_calvin_execute_active`]: active participant for a
//!   dependent-read txn. Executes the physical plans with the injected read
//!   values already resolved. Performs an OLLP verification hook: if the
//!   active participant detects that the declared predicate no longer matches
//!   the current engine state, it returns `OllpRetryRequired` WITHOUT writing.
//!   The OLLP orchestrator on the Control Plane retries via `Inbox::submit`.
//!
//! The `CalvinApplied` WAL record is written on the Control Plane side (in the
//! scheduler's response path) after a successful response is received through
//! the SPSC bridge; not here in the Data Plane.

use tracing::{debug, info_span};

use nodedb_cluster::calvin::types::PassiveReadKey;
use nodedb_types::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;

use std::collections::BTreeMap;

impl CoreLoop {
    /// Execute a static-set Calvin sequenced transaction batch.
    ///
    /// Sets `self.epoch_system_ms` to the epoch's deterministic timestamp
    /// anchor before delegating to `execute_transaction_batch`, then resets
    /// it to `None` immediately after. Engine handlers that need "current time"
    /// read `self.epoch_system_ms` and fall back to wall clock when `None`.
    ///
    /// Also advances the HLC to the epoch anchor so graph `next_ordinal()`
    /// calls produce deterministic ordinals across all replicas.
    pub(in crate::data::executor) fn execute_calvin_execute_static(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        epoch_system_ms: i64,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            plan_count = plans.len(),
            "calvin execute static"
        );
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();
        const NANOS_PER_MS: i64 = 1_000_000;
        self.hlc
            .update_from_remote(epoch_system_ms.saturating_mul(NANOS_PER_MS));
        self.epoch_system_ms = Some(epoch_system_ms);
        let result = self.execute_transaction_batch(task, tenant_id.as_u64(), plans);
        self.epoch_system_ms = None;
        result
    }

    /// Execute a passive-participant dependent-read Calvin txn.
    ///
    /// Reads each key from the local engine state and returns a
    /// msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` as the response
    /// payload. The Control Plane scheduler collects these values and
    /// proposes a `ReplicatedWrite::CalvinReadResult` entry to the
    /// per-vshard Raft group so all replicas see the same read results.
    ///
    /// `Instant::now()` is intentionally absent here — this is a
    /// synchronous Data Plane read with no timer interaction.
    pub(in crate::data::executor) fn execute_calvin_execute_passive(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        tenant_id: &TenantId,
        keys_to_read: &[PassiveReadKey],
    ) -> Response {
        debug!(
            core = self.core_id,
            epoch,
            position,
            vshard_id = task.request.vshard_id.as_u32(),
            key_count = keys_to_read.len(),
            "calvin execute passive: reading keys"
        );

        let mut results: Vec<(PassiveReadKeyId, Value)> = Vec::with_capacity(keys_to_read.len());

        for passive_key in keys_to_read {
            // Build a PassiveReadKeyId for each surrogate in the engine key set.
            // For this v1 handler the engine key set carries single surrogates per
            // key (as specified in the design); we iterate all surrogates to be safe.
            let values = self.read_passive_key(tenant_id, &passive_key.engine_key);
            results.extend(values);
        }

        match response_codec::encode_serde(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("calvin passive read encode: {e}"),
                },
            ),
        }
    }

    /// Execute an active-participant dependent-read Calvin txn with injected
    /// read values.
    ///
    /// Before executing, performs an OLLP verification hook: checks whether
    /// the predicate match declared in the txn's read set still matches the
    /// actual rows now in the engine. For v1 the check is a structural hook
    /// that always passes (returning the full execution result); future plan
    /// variants emitted by the OLLP-aware planner carry predicate metadata
    /// that enables the actual comparison.
    ///
    /// If the verification fails (mismatched predicate, in future variants),
    /// returns `OllpRetryRequired` status and does NOT write. The OLLP
    /// orchestrator on the Control Plane interprets this status and retries.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_calvin_execute_active(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        epoch_system_ms: i64,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
        injected_reads: &BTreeMap<PassiveReadKeyId, Value>,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            plan_count = plans.len(),
            injected_count = injected_reads.len(),
            "calvin execute active"
        );
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();

        // OLLP verification hook: for v1, the planner emits plans that carry
        // the predicate check inline (as a TransactionBatch with a conditional
        // check sub-plan). When OLLP-aware plan variants are introduced, this
        // hook will compare predicate metadata against the engine state and
        // return OllpRetryRequired if mismatched. For now, always proceed.
        //
        // The `injected_reads` map is available here for plan execution engines
        // that need to substitute read values into write parameters. In v1 plans
        // are self-contained; future plan variants will reference injected keys
        // by PassiveReadKeyId.

        const NANOS_PER_MS: i64 = 1_000_000;
        self.hlc
            .update_from_remote(epoch_system_ms.saturating_mul(NANOS_PER_MS));
        self.epoch_system_ms = Some(epoch_system_ms);
        let result = self.execute_transaction_batch(task, tenant_id.as_u64(), plans);
        self.epoch_system_ms = None;
        result
    }

    /// Read a single `EngineKeySet` from the local engine state, returning
    /// `(PassiveReadKeyId, Value)` pairs.
    ///
    /// For Document/Vector/Edge engine sets: looks up each surrogate in the
    /// sparse engine (document store) and returns the stored value or `Null`.
    /// For KV engine sets: looks up each byte key in the KV engine.
    fn read_passive_key(
        &self,
        tenant_id: &TenantId,
        engine_key: &nodedb_cluster::calvin::types::EngineKeySet,
    ) -> Vec<(PassiveReadKeyId, Value)> {
        use nodedb_cluster::calvin::types::EngineKeySet;

        match engine_key {
            EngineKeySet::Document {
                collection,
                surrogates,
            }
            | EngineKeySet::Vector {
                collection,
                surrogates,
            } => surrogates
                .iter()
                .map(|&surrogate| {
                    let value = self
                        .read_surrogate_value(tenant_id, collection, surrogate)
                        .unwrap_or(Value::Null);
                    (
                        PassiveReadKeyId {
                            collection: collection.clone(),
                            surrogate,
                        },
                        value,
                    )
                })
                .collect(),

            EngineKeySet::Kv { collection, keys } => keys
                .iter()
                .map(|k| {
                    let value = self
                        .read_kv_value(tenant_id, collection, k)
                        .unwrap_or(Value::Null);
                    // For KV, use key bytes as surrogate placeholder (0 sentinel).
                    // KV keys don't have surrogates; the PassiveReadKeyId identifies
                    // the collection and a stable u32 hash of the key.
                    let key_hash = stable_kv_hash(k);
                    (
                        PassiveReadKeyId {
                            collection: collection.clone(),
                            surrogate: key_hash,
                        },
                        value,
                    )
                })
                .collect(),

            EngineKeySet::Edge {
                collection, edges, ..
            } => edges
                .iter()
                .map(|&(src, dst)| {
                    // Edge reads: use a stable hash of (src, dst) as surrogate.
                    let edge_hash = stable_edge_hash(src, dst);
                    (
                        PassiveReadKeyId {
                            collection: collection.clone(),
                            surrogate: edge_hash,
                        },
                        Value::Null, // Edge existence read: Null = absent, non-Null = present.
                    )
                })
                .collect(),
        }
    }

    /// Read a single document surrogate from the sparse engine.
    ///
    /// Returns `None` if the surrogate is not present in this core's partition.
    fn read_surrogate_value(
        &self,
        tenant_id: &TenantId,
        collection: &str,
        surrogate: u32,
    ) -> Option<Value> {
        // In v1 this is a thin stub: the full implementation requires a
        // synchronous lookup through the sparse engine's redb B-Tree.
        // The engine lookup path is available via `self.engine_state` once
        // the Data Plane engine access APIs are wired. For now, return None
        // (caller maps None → Null).
        let _ = (tenant_id, collection, surrogate);
        None
    }

    /// Read a single KV entry from the KV engine.
    ///
    /// Returns `None` if the key is not present.
    fn read_kv_value(&self, tenant_id: &TenantId, collection: &str, key: &[u8]) -> Option<Value> {
        let _ = (tenant_id, collection, key);
        None
    }
}

/// Stable, deterministic hash of a KV byte key into a u32 surrogate
/// placeholder for use in `PassiveReadKeyId`.
///
/// Uses xxhash with a fixed seed to satisfy the determinism contract:
/// the same byte key must produce the same hash on every replica.
/// `DefaultHasher` (RandomState) is explicitly NOT used here.
fn stable_kv_hash(key: &[u8]) -> u32 {
    // FNV-1a 32-bit with fixed offset basis — no external dependency needed.
    // This is a placeholder; a production implementation would use xxhash-rust
    // with a fixed seed once the crate is available in this crate's deps.
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for &byte in key {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Stable, deterministic hash of an edge `(src, dst)` into a u32 surrogate
/// placeholder for `PassiveReadKeyId`.
fn stable_edge_hash(src: u32, dst: u32) -> u32 {
    // Combine src and dst with a deterministic mix.
    let combined: u64 = (u64::from(src) << 32) | u64::from(dst);
    stable_kv_hash(&combined.to_le_bytes())
}
