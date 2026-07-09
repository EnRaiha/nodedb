// SPDX-License-Identifier: BUSL-1.1

//! Steady-state frame handling: parses a request body, builds the
//! `PhysicalPlan` for the requested op, and dispatches it to the Data Plane
//! (directly via SPSC, or through the Raft proposer gate for CRDT applies).

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, GraphOp, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;

use super::Session;

impl Session {
    /// Parse a request frame and dispatch to the Data Plane.
    pub(super) async fn handle_frame(
        &mut self,
        request_id: RequestId,
        payload: &[u8],
    ) -> crate::Result<Vec<u8>> {
        // Parse the JSON request body.
        let body: serde_json::Value =
            sonic_rs::from_slice(payload).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid JSON: {e}"),
            })?;

        let op = body["op"]
            .as_str()
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "missing 'op' field".into(),
            })?;

        // Auth handshake: must be first frame.
        if op == "auth" {
            return self.handle_auth_frame(&body).await;
        }

        // All other ops require auth. In trust mode, auto-authenticate on first frame.
        self.ensure_authenticated()?;

        // Check and rehydrate identity if credential version has advanced.
        self.rehydrate_identity_if_stale();

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "not authenticated".into(),
                });
            }
        };

        // Tenant from authenticated identity, not from client payload.
        let tenant_id = identity.tenant_id;

        // Resolve and bind database on first request. Explicit handshake override
        // is not yet wired from the native wire protocol; every session uses the
        // resolution chain default (user default → tenant default → built-in default).
        if self.current_database.is_none() {
            self.current_database = Some(Self::resolve_database(identity, None));
        }
        let database_id = self.current_database.unwrap_or(DatabaseId::DEFAULT);

        let collection = body["collection"].as_str().unwrap_or("default").to_string();

        // Determine vShard from collection + document_id for data locality.
        let vshard_key = body["document_id"].as_str().unwrap_or(&collection);
        let vshard_id = VShardId::from_key(vshard_key.as_bytes());

        let plan = self.build_plan(op, &body, database_id, tenant_id, collection)?;

        // CRDT applies must be quorum-durable: route them through the Raft
        // proposer gate so the delta replicates to followers instead of landing
        // only on the receiving node (which loses it on leader failover). Every
        // other op keeps the direct SPSC path below. The success/error response
        // shape is preserved identically to the SPSC path.
        if op == "crdt_apply" {
            return self
                .dispatch_replicated_crdt(request_id, tenant_id, database_id, &body, plan)
                .await;
        }

        self.dispatch_and_respond(request_id, tenant_id, database_id, vshard_id, plan)
            .await
    }

    /// Build the `PhysicalPlan` for a given op from the parsed request body.
    fn build_plan(
        &self,
        op: &str,
        body: &serde_json::Value,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: String,
    ) -> crate::Result<PhysicalPlan> {
        let plan = match op {
            "point_get" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let pk_bytes = document_id.as_bytes().to_vec();
                let surrogate = self
                    .state
                    .surrogate_assigner
                    .lookup(database_id, tenant_id, &collection, &pk_bytes)?
                    .unwrap_or(nodedb_types::Surrogate::ZERO);
                PhysicalPlan::Document(DocumentOp::PointGet {
                    collection,
                    document_id,
                    surrogate,
                    pk_bytes,
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                })
            }
            "vector_search" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let top_k = body["top_k"].as_u64().unwrap_or(10) as usize;
                PhysicalPlan::Vector(VectorOp::Search {
                    collection,
                    query_vector,
                    top_k,
                    ef_search: 0,
                    metric: DistanceMetric::L2,
                    filter_bitmap: None,
                    field_name: String::new(),
                    rls_filters: Vec::new(),
                    inline_prefilter_plan: None,
                    // The HTTP/JSON session protocol surfaces only the
                    // primitive top_k field today. Advanced ANN tuning is
                    // SQL-only; defaulting keeps the wire shape uniform.
                    ann_options: Default::default(),
                    skip_payload_fetch: false,
                    payload_filters: Vec::new(),
                })
            }
            "range_scan" => {
                let field = body["field"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'field'".into(),
                    })?
                    .to_string();
                let limit = body["limit"].as_u64().unwrap_or(100) as usize;
                PhysicalPlan::Document(DocumentOp::RangeScan {
                    collection,
                    field,
                    lower: None,
                    upper: None,
                    limit,
                })
            }
            "crdt_read" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                PhysicalPlan::Crdt(CrdtOp::Read {
                    collection,
                    document_id,
                })
            }
            "crdt_apply" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let delta_b64 = body["delta"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'delta'".into(),
                    })?;
                // Decode base64 delta. For now accept raw bytes if not valid base64.
                let delta = delta_b64.as_bytes().to_vec();
                let peer_id = body["peer_id"].as_u64().unwrap_or(0);
                let surrogate = self.state.surrogate_assigner.assign(
                    database_id,
                    tenant_id,
                    &collection,
                    document_id.as_bytes(),
                )?;
                PhysicalPlan::Crdt(CrdtOp::Apply {
                    collection,
                    document_id,
                    delta,
                    peer_id,
                    mutation_id: 0,
                    surrogate,
                    provenance: None,
                    // Direct session write, not a replicated peer sync.
                    constraint_version_required: 0,
                })
            }
            "graph_rag_fusion" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let vector_top_k = body["vector_top_k"].as_u64().unwrap_or(20) as usize;
                let edge_label = body["edge_label"].as_str().map(String::from);
                let direction_str = body["direction"].as_str().unwrap_or("out");
                let direction = match direction_str {
                    "in" => crate::engine::graph::edge_store::Direction::In,
                    "both" => crate::engine::graph::edge_store::Direction::Both,
                    _ => crate::engine::graph::edge_store::Direction::Out,
                };
                let expansion_depth = body["expansion_depth"].as_u64().unwrap_or(2) as usize;
                let final_top_k = body["final_top_k"].as_u64().unwrap_or(10) as usize;
                let vector_k = body["vector_k"].as_f64().unwrap_or(60.0);
                let graph_k = body["graph_k"].as_f64().unwrap_or(10.0);
                PhysicalPlan::Graph(GraphOp::RagFusion {
                    collection,
                    query_vector,
                    vector_top_k,
                    edge_label,
                    direction,
                    expansion_depth,
                    final_top_k,
                    rrf_k: (vector_k, graph_k),
                    rrf_k_triple: None,
                    vector_field: body["vector_field"].as_str().unwrap_or("").to_string(),
                    options: Default::default(),
                    bm25_query: None,
                    bm25_field: None,
                })
            }
            "alter_collection_policy" => {
                let policy = &body["policy"];
                if policy.is_null() {
                    return Err(crate::Error::BadRequest {
                        detail: "missing 'policy' field".into(),
                    });
                }
                let policy_json =
                    sonic_rs::to_string(policy).map_err(|e| crate::Error::BadRequest {
                        detail: format!("invalid policy JSON: {e}"),
                    })?;
                PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                    collection,
                    policy_json,
                })
            }
            _ => {
                return Err(crate::Error::BadRequest {
                    detail: format!("unknown op: {op}"),
                });
            }
        };

        Ok(plan)
    }

    /// Route a `crdt_apply` op through the Raft proposer gate so the delta
    /// replicates to followers instead of landing only on the receiving node.
    async fn dispatch_replicated_crdt(
        &self,
        request_id: RequestId,
        tenant_id: TenantId,
        database_id: DatabaseId,
        body: &serde_json::Value,
        plan: PhysicalPlan,
    ) -> crate::Result<Vec<u8>> {
        // `collection` was moved into the plan above; re-read it from the
        // request body (indexing only borrows). The replicated vshard is
        // collection-keyed (matching every other write path and the Raft data
        // group routing), NOT the document-keyed `vshard_id` used for local
        // core locality above.
        let collection = body["collection"].as_str().unwrap_or("default");
        let payload = crate::control::server::sync::raft_dispatch::dispatch_write_replicated(
            &self.state,
            tenant_id,
            database_id,
            collection,
            plan,
            Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            crate::event::EventSource::User,
        )
        .await?;
        let payload_str = String::from_utf8_lossy(&payload).into_owned();
        let resp_json = format!(
            r#"{{"request_id":{},"status":"ok","payload":"{}","watermark_lsn":0,"error_code":null}}"#,
            request_id.as_u64(),
            payload_str,
        );
        Ok(resp_json.into_bytes())
    }

    /// Dispatch a plan to the Data Plane over the direct SPSC path, await the
    /// response, and serialize it to the JSON wire shape.
    async fn dispatch_and_respond(
        &self,
        request_id: RequestId,
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: PhysicalPlan,
    ) -> crate::Result<Vec<u8>> {
        // The sync/native steady-state session builds its own Request and
        // enqueues directly (it does not flow through the autocommit funnel), so
        // it passes the write-admission gate here. An uncontended point write
        // takes the fast path holding its per-vShard deterministic locks (guard
        // held across enqueue + response); a contended or bulk write is submitted
        // through the deterministic scheduler and its applied response is
        // serialized and returned; reads / control ops are `Exempt`.
        use crate::control::server::shared::write_admission::{
            WriteAdmission, WriteTarget, admit, bare_ok_response, route_write_to_calvin,
        };
        let (admission, _admission_guard) = match admit(
            &self.state,
            &WriteTarget {
                tenant_id,
                database_id,
                vshard_id,
                plan: &plan,
            },
        ) {
            WriteAdmission::ExemptRead => (
                crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::Read,
                ),
                None,
            ),
            WriteAdmission::FastPath { guard } => {
                (crate::bridge::envelope::Admission::Admitted, guard)
            }
            WriteAdmission::RouteToCalvin => {
                let routed =
                    route_write_to_calvin(&self.state, tenant_id, database_id, vshard_id, plan)
                        .await?;
                let response = routed.unwrap_or_else(|| bare_ok_response(RequestId::new(0)));
                return Ok(serialize_dispatch_response_json(&response));
            }
        };
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan,
            deadline: Instant::now()
                + Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission,
        };

        // Register for response routing before dispatching.
        let mut rx = self.state.tracker.register(request_id);

        // Dispatch to Data Plane via SPSC.
        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        // Await response from Data Plane (routed back via the response poller).
        let response = tokio::time::timeout(
            Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            async { rx.recv().await.ok_or(()) },
        )
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })?;

        Ok(serialize_dispatch_response_json(&response))
    }
}

/// Serialize a Data-Plane [`Response`] to this path's JSON wire shape. Shared by
/// the fast-dispatch path and the scheduler-routed path so both emit an
/// identical envelope.
fn serialize_dispatch_response_json(response: &crate::bridge::envelope::Response) -> Vec<u8> {
    let status_str = match response.status {
        Status::Ok => "ok",
        Status::Partial => "partial",
        Status::Error => "error",
    };

    let payload_str = if response.payload.is_empty() {
        String::new()
    } else {
        // Return raw payload as lossy UTF-8 for now.
        String::from_utf8_lossy(&response.payload).into_owned()
    };

    let error_code_str = response.error_code.as_ref().map(|ec| format!("{ec:?}"));

    let resp_json = format!(
        r#"{{"request_id":{},"status":"{}","payload":"{}","watermark_lsn":{},"error_code":{}}}"#,
        response.request_id.as_u64(),
        status_str,
        payload_str,
        response.watermark_lsn.as_u64(),
        error_code_str
            .as_ref()
            .map(|s| format!("\"{s}\""))
            .unwrap_or_else(|| "null".to_string()),
    );

    resp_json.into_bytes()
}
