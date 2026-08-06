// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.
//!
//! When a remote node sends an `ExecuteRequest` to this node (because this
//! node is the leader for the target vShard), the [`LocalPlanExecutor`]
//! validates descriptor versions, decodes the `PhysicalPlan`, and fans it
//! across ALL local Data-Plane cores via
//! [`crate::control::server::exchange::execute_plan_all_local_cores`] before
//! returning the merged result to the caller.
//!
//! At 1 core/node the fan is over a single core and behaviour is identical to
//! the prior single-core dispatch.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use tracing::{Instrument, info_span};

use nodedb_cluster::forward::{ChunkSink, PlanExecutor};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::execute_plan_all_local_cores;
use crate::control::state::SharedState;
use crate::control::trace_export::EmitSpanParams;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::wire as plan_wire;

use super::support::{
    PLAN_DECODE_FAILED, SinkOutcome, plan_contains_exchange, stream_error_to_typed,
};

fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> Result<(), TypedClusterError> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
                | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: crate::Error::CrdtApplyRequiresAdmission.to_string(),
        });
    }
    Ok(())
}

/// Executes pre-planned `PhysicalPlan` on the local Data Plane.
pub struct LocalPlanExecutor {
    state: Arc<SharedState>,
}

impl LocalPlanExecutor {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl PlanExecutor for LocalPlanExecutor {
    async fn execute_plan(&self, req: ExecuteRequest) -> ExecuteResponse {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan", trace_id = %trace_id, tenant_id);
        let resp = self.execute_plan_inner(req).instrument(span).await;
        // Emit one OTLP executor span per leaseholder so the gateway's
        // upstream span joins the N leaseholder spans into a single
        // distributed trace via the shared `trace_id`.
        exporter.emit(EmitSpanParams {
            span_name: "executor.execute_plan",
            trace_id,
            start,
            end: SystemTime::now(),
            tenant_id,
            vshard_id: 0,
            status_ok: resp.success,
        });
        resp
    }

    async fn execute_plan_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan_streaming", trace_id = %trace_id, tenant_id);
        let outcome = self
            .execute_plan_streaming_inner(req, sink)
            .instrument(span)
            .await;
        exporter.emit(EmitSpanParams {
            span_name: "executor.execute_plan_streaming",
            trace_id,
            start,
            end: SystemTime::now(),
            tenant_id,
            vshard_id: 0,
            status_ok: outcome.is_none(),
        });
        outcome
    }
}

impl LocalPlanExecutor {
    /// Shared validation + decode prologue for both the one-shot and streaming
    /// paths: validate deadline + descriptor versions, decode the plan, reject
    /// unresolved Exchange nodes.  Returns `(plan, database_id, deadline)` on
    /// success or a typed cluster error to surface to the caller.
    fn validate_and_decode(
        &self,
        req: &ExecuteRequest,
    ) -> Result<
        (
            nodedb_physical::physical_plan::PhysicalPlan,
            DatabaseId,
            Duration,
        ),
        TypedClusterError,
    > {
        // ── 1. Deadline check ─────────────────────────────────────────────────
        if req.deadline_remaining_ms == 0 {
            return Err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
        }

        let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
            self.state.tuning.network.default_deadline_secs,
        ));

        let database_id = DatabaseId::from(req.database_id);

        // ── 2. Descriptor version validation ──────────────────────────────────
        let catalog_ref = self.state.credentials.catalog();
        {
            let catalog = catalog_ref;
            for entry in &req.descriptor_versions {
                match catalog.get_collection(database_id, req.tenant_id, &entry.collection) {
                    Ok(Some(stored)) => {
                        let actual = if stored.descriptor_version == 0 {
                            1
                        } else {
                            stored.descriptor_version
                        };
                        if actual != entry.version {
                            tracing::debug!(
                                collection = %entry.collection,
                                expected_version = entry.version,
                                actual_version = actual,
                                "exec receiver: descriptor version mismatch"
                            );
                            return Err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: actual,
                            });
                        }
                    }
                    Ok(None) => {
                        if entry.version != 0 {
                            return Err(TypedClusterError::DescriptorMismatch {
                                collection: entry.collection.clone(),
                                expected_version: entry.version,
                                actual_version: 0,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("catalog lookup failed: {e}"),
                        });
                    }
                }
            }
        }

        // ── 3. Decode the PhysicalPlan ────────────────────────────────────────
        let mut plan = match plan_wire::decode(&req.plan_bytes) {
            Ok(p) => p,
            Err(e) => {
                return Err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: format!("plan decode failed: {e}"),
                });
            }
        };

        // ── 3a. Re-resolve an unresolved PK point-get surrogate ───────────────
        //
        // The query coordinator resolves `WHERE pk = <v>` → surrogate against
        // ITS OWN local catalog. The surrogate↔PK map is sharded to the
        // collection's data-group members, so a coordinator that is NOT a
        // member of that group misses the binding and ships `Surrogate::ZERO`.
        // We (the owner) ARE a group member, so our local catalog HAS the
        // binding — re-resolve here before the plan reaches the Data Plane.
        //
        // Scope is intentionally tight: only `DocumentOp::PointGet` reads, only
        // when the carried surrogate is ZERO and `pk_bytes` is non-empty. A
        // non-ZERO carried surrogate is authoritative (immutable first-wins
        // bind) and is left untouched; a genuinely-absent PK stays ZERO and
        // correctly resolves to not-found.
        if let nodedb_physical::physical_plan::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointGet {
                surrogate,
                pk_bytes,
                collection,
                ..
            },
        ) = &mut plan
            && *surrogate == nodedb_types::Surrogate::ZERO
            && !pk_bytes.is_empty()
            && let Ok(Some(resolved)) = catalog_ref.get_surrogate_for_pk(
                database_id,
                crate::types::TenantId::new(req.tenant_id),
                collection,
                pk_bytes,
            )
        {
            *surrogate = resolved;
        }

        // ── 3b. Reject unresolved Exchange nodes ──────────────────────────────
        if plan_contains_exchange(&plan) {
            return Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "received plan with unresolved Exchange node; coordinator must resolve \
                          data movement before cross-node dispatch"
                    .into(),
            });
        }

        Ok((plan, database_id, deadline))
    }

    /// One-shot execution: validate + decode, fan across all local cores,
    /// merge, and return the merged payload.
    async fn execute_plan_inner(&self, req: ExecuteRequest) -> ExecuteResponse {
        let (plan, database_id, deadline) = match self.validate_and_decode(&req) {
            Ok(t) => t,
            Err(e) => return ExecuteResponse::err(e),
        };

        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let trace_id = nodedb_types::TraceId(req.trace_id);

        if let PhysicalPlan::ClusterEvent(
            nodedb_physical::physical_plan::ClusterEventOp::PublishTopic {
                database_id: topic_database_id,
                topic_name,
                payload,
            },
        ) = &plan
        {
            if *topic_database_id != database_id {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: "topic publish database does not match RPC database".into(),
                });
            }
            return match crate::event::topic::publish::publish_to_topic(
                &self.state,
                database_id,
                req.tenant_id,
                topic_name,
                payload,
            )
            .await
            {
                Ok(sequence) => match zerompk::to_msgpack_vec(&sequence) {
                    Ok(payload) => ExecuteResponse::ok(vec![payload], 0, 0),
                    Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: format!("topic response encoding failed: {error}"),
                    }),
                },
                Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                }),
            };
        }

        if let PhysicalPlan::ClusterEvent(
            nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream {
                database_id: stream_database_id,
                stream_name,
                group_name,
                partition,
                limit,
                committed_offsets,
            },
        ) = &plan
        {
            if let Err(error) = reject_consume_database_mismatch(*stream_database_id, database_id) {
                return ExecuteResponse::err(error);
            }
            let limit = match usize::try_from(*limit) {
                Ok(limit) => limit,
                Err(_) => {
                    return ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: "CDC consume limit exceeds platform range".into(),
                    });
                }
            };
            let params = crate::event::cdc::consume::ConsumeParams {
                database_id: *stream_database_id,
                tenant_id: req.tenant_id,
                stream_name,
                group_name,
                partition: *partition,
                limit,
            };
            if let Err(error) =
                crate::event::cdc::consume::validate_consume_identity(&self.state, &params)
            {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                });
            }
            let committed_offsets =
                match crate::event::cdc::consume::decode_remote_committed_offsets(committed_offsets)
                {
                    Ok(offsets) => offsets,
                    Err(error) => {
                        return ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: error.to_string(),
                        });
                    }
                };
            // The events are handed to an authenticated peer node, not to a
            // subscriber: this is an internal buffer read, and the requesting
            // node applies its caller's column redaction at the delivery
            // surface that asked for them (the stream SELECT, the HTTP poll,
            // and the SSE endpoint each redact what `consume_remote` returns).
            // Redaction policies are catalog state replicated to every node, so
            // the rules are the same on both sides of this hop.
            return match crate::event::cdc::consume::consume_local_with_offsets(
                &self.state,
                &params,
                Some(&committed_offsets),
            ) {
                Ok(result) => {
                    let events = result
                        .events
                        .iter()
                        .map(|event| event.as_ref().clone())
                        .collect::<Vec<_>>();
                    match zerompk::to_msgpack_vec(&events) {
                        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, 0),
                        Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("CDC response encoding failed: {error}"),
                        }),
                    }
                }
                Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                }),
            };
        }

        // ── Replicable write: drive through Raft, NOT local cores ─────────────
        //
        // The gateway forwarded this plan here because THIS node is the leader
        // for the target data group. Fanning a replicable write across local
        // cores only would commit to this node's Data Plane and NEVER propose
        // it to the Raft group — coordinator + other voters never apply it, and
        // SQL still returns Ok (silent write loss). So for any plan that
        // `to_replicated_entry` recognizes as a replicable write, propose it
        // through the SAME proposer the local pgwire write path uses: proposing
        // via the local proposer targets the group this node leads → commit →
        // all voters apply. The resolved surrogate is already carried on the
        // forwarded entry (coordinator-side), and owner-side re-resolution at
        // apply (wal_replication/decode.rs `bind_or_lookup`) covers the rest.
        //
        // Reads / non-replicable plans (`to_replicated_entry == None`) fall
        // through to `execute_plan_all_local_cores` unchanged.
        //
        // The vshard the gateway routed this plan to is not carried on the wire;
        // it is a pure function of the plan's primary collection (every data
        // group is `CollectionHomed`), so we re-derive it exactly as the gateway
        // router's `CollectionHomed` arm does (`vshard_for_collection`). This is
        // the same group this node leads, so the local proposer targets it.
        let vshard_id = crate::types::VShardId::new(
            crate::control::gateway::version_set::touched_collections(&plan)
                .into_iter()
                .next()
                .map(|name| nodedb_cluster::routing::vshard_for_collection(database_id, &name))
                .unwrap_or(0),
        );
        if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
            return ExecuteResponse::err(error);
        }

        if let Some(proposer) = self.state.async_raft_proposer()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                tenant_id,
                database_id,
                vshard_id,
                &plan,
            )
        {
            return match crate::control::wal_replication::propose_replicated_entry(
                &self.state,
                proposer,
                entry,
            )
            .await
            {
                // Replicated writes carry no read watermark → 0. The write's
                // per-collection version rides alongside the payload but is not
                // needed here: it floors a SESSION's later reads, and this RPC
                // seam serves a remote coordinator that holds no session.
                Ok((payload, _write_version)) => ExecuteResponse::ok(vec![payload], 0, 0),
                Err(e) => ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: e.to_string(),
                }),
            };
        }

        match tokio::time::timeout(
            deadline,
            execute_plan_all_local_cores(
                &self.state,
                tenant_id,
                database_id,
                plan,
                trace_id,
                req.txn_id,
            ),
        )
        .await
        {
            Ok(Ok(result)) => ExecuteResponse::ok(
                vec![result.payload],
                result.watermark_lsn.as_u64(),
                result.read_version_lsn.as_u64(),
            ),
            Ok(Err(e)) => ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: e.to_string(),
            }),
            Err(_) => ExecuteResponse::err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }

    /// Streaming execution: validate + decode, fan across all local cores via
    /// `gather_all_cores_stream`, and push each result frame to `sink` as it
    /// arrives.
    ///
    /// Returns `None` on a clean end, or `Some(err)` on a terminal failure
    /// (validation rejection, stream error, or deadline). A `send_chunk` error
    /// means the coordinator is gone: return `None` (there is no peer to
    /// receive a terminal frame).
    async fn execute_plan_streaming_inner(
        &self,
        req: ExecuteRequest,
        mut sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        let (plan, database_id, deadline) = match self.validate_and_decode(&req) {
            Ok(t) => t,
            Err(e) => return Some(e),
        };

        if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
            return Some(error);
        }
        if matches!(plan, PhysicalPlan::ClusterEvent(_)) {
            return Some(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "ClusterEvent operations do not support streaming RPC".into(),
            });
        }

        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let trace_id = nodedb_types::TraceId(req.trace_id);

        // Cluster RPC receiver (remote-node local execution): forward the
        // incoming request's transaction context so a transactional streaming
        // read honours its staged overlay. Inert when `None`.
        let mut stream = match crate::control::server::exchange::gather::gather_all_cores_stream(
            &self.state,
            tenant_id,
            database_id,
            plan,
            trace_id,
            req.txn_id,
        ) {
            Ok(s) => s,
            Err(e) => {
                return Some(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: e.to_string(),
                });
            }
        };

        let stream_fut = async {
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(b) => {
                        if let Err(_e) = sink
                            .send_chunk(
                                b.payload,
                                b.watermark_lsn.as_u64(),
                                b.read_version_lsn.as_u64(),
                            )
                            .await
                        {
                            // Coordinator gone — stop, no terminal frame.
                            return SinkOutcome::CoordinatorGone;
                        }
                    }
                    Err(e) => {
                        return SinkOutcome::StreamError(stream_error_to_typed(e));
                    }
                }
            }
            SinkOutcome::CleanEnd
        };

        match tokio::time::timeout(deadline, stream_fut).await {
            Ok(SinkOutcome::CleanEnd) => None,
            Ok(SinkOutcome::CoordinatorGone) => None,
            Ok(SinkOutcome::StreamError(e)) => Some(e),
            Err(_) => Some(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }
}

fn reject_consume_database_mismatch(
    stream_database_id: crate::types::DatabaseId,
    envelope_database_id: crate::types::DatabaseId,
) -> Result<(), TypedClusterError> {
    if stream_database_id == envelope_database_id {
        Ok(())
    } else {
        Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "CDC consume database does not match RPC database".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::CrdtOp;

    #[test]
    fn consume_stream_rejects_database_mismatch() {
        let op = nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream {
            database_id: crate::types::DatabaseId::new(7),
            stream_name: "topic:orders".into(),
            group_name: "analytics".into(),
            partition: Some(0),
            limit: 1,
            committed_offsets: vec![(0, 0, 0)],
        };
        let nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream { database_id, .. } = op
        else {
            panic!("expected typed consume operation");
        };
        assert!(matches!(
            reject_consume_database_mismatch(database_id, crate::types::DatabaseId::new(8)),
            Err(TypedClusterError::Internal { .. })
        ));
    }

    #[test]
    fn consume_stream_rejects_duplicate_caller_offsets() {
        assert!(matches!(
            crate::event::cdc::consume::decode_remote_committed_offsets(&[(3, 7, 1), (3, 8, 1)]),
            Err(crate::event::cdc::consume::ConsumeError::InvalidRemoteOffsets(_))
        ));
    }

    #[test]
    fn every_remote_execution_mode_rejects_unadmitted_crdt_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: "docs".into(),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });

        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(TypedClusterError::Internal { .. })
        ));
    }
}
