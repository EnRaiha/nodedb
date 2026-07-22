// SPDX-License-Identifier: BUSL-1.1

//! ILP batch preflight, authorization, and dispatch.

use std::collections::BTreeMap;
use std::sync::Arc;

use sonic_rs;
use tracing::warn;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::{ArcAuditEmitter, AuditEmitter};
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;

/// EWMA-based rate estimator for adaptive ILP batch sizing.
pub(super) struct IlpRateEstimator {
    /// Smoothed rate in lines/second.
    rate: f64,
    /// EWMA smoothing factor (0.2 = responsive to recent changes).
    alpha: f64,
    /// Last measurement timestamp.
    last_ts: std::time::Instant,
}

impl IlpRateEstimator {
    pub(super) fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.2,
            last_ts: std::time::Instant::now(),
        }
    }

    /// Record that `lines` were flushed since the last call.
    pub(super) fn record(&mut self, lines: u64) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_ts).as_secs_f64();
        self.last_ts = now;

        if elapsed > 0.0 {
            let instant_rate = lines as f64 / elapsed;
            if self.rate == 0.0 {
                self.rate = instant_rate;
            } else {
                self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            }
        }
    }

    /// Suggest (batch_size, window_ms) based on current rate.
    pub(super) fn suggest_batch_params(&self) -> (u64, u64) {
        if self.rate > 100_000.0 {
            // High rate: large batches, short window.
            (10_000, 10)
        } else if self.rate > 1_000.0 {
            // Medium rate: moderate batches.
            (1_000, 50)
        } else {
            // Low rate: small batches, long window.
            (100, 100)
        }
    }
}

/// Preflighted raw ILP lines for one canonical measurement.
///
/// `raw_batch` preserves the accepted physical source lines in their original
/// order; only their routing and authorization key is canonicalized.
#[derive(Debug, PartialEq, Eq)]
struct IlpMeasurementBatch {
    measurement: String,
    raw_batch: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IlpPreflightFailure {
    Parse,
    PermissionDenied,
}

/// Parse and authorize every unique collection before quota accounting, WAL,
/// catalog mutation, plan construction, gateway execution, or SPSC dispatch.
///
/// A `BTreeMap` gives canonical deterministic authorization/dispatch order,
/// while appending each original raw line preserves source order within a
/// measurement group.
fn preflight_ilp_batch(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    batch: &str,
    permissions: &crate::control::security::permission::PermissionStore,
    roles: &crate::control::security::role::RoleStore,
    audit: &dyn AuditEmitter,
) -> Result<Vec<IlpMeasurementBatch>, IlpPreflightFailure> {
    let parsed = crate::engine::timeseries::ilp::parse_batch(batch)
        .map_err(|_| IlpPreflightFailure::Parse)?;
    if parsed.lines().is_empty() {
        return Err(IlpPreflightFailure::Parse);
    }

    let mut grouped = BTreeMap::<String, String>::new();
    for line in parsed.lines() {
        let entry = grouped.entry(line.measurement.to_string()).or_default();
        if !entry.is_empty() {
            entry.push('\n');
        }
        entry.push_str(line.raw);
    }

    let mut groups = Vec::with_capacity(grouped.len());
    for (measurement, raw_batch) in grouped {
        authorize_collection(
            identity,
            database_id,
            &measurement,
            Permission::Write,
            permissions,
            roles,
            audit,
        )
        .map_err(|_| IlpPreflightFailure::PermissionDenied)?;
        groups.push(IlpMeasurementBatch {
            measurement,
            raw_batch,
        });
    }
    Ok(groups)
}

/// Dispatch an authorized, strictly parsed ILP batch to the Data Plane.
pub(super) async fn flush_ilp_batch(
    state: &SharedState,
    context: &AuthenticatedIlpContext,
    batch: &str,
) -> crate::Result<u64> {
    let identity = context.identity();
    let database_id = context.database_id();
    let audit = ArcAuditEmitter(Arc::clone(&state.audit));
    let groups = preflight_ilp_batch(
        identity,
        database_id,
        batch,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|_| crate::Error::BadRequest {
        detail: "ILP batch rejected".into(),
    })?;

    // Quota accounting must only begin after the full batch is known valid and
    // all collection permissions have passed. The tenant is never caller input.
    let tenant_id = identity.tenant_id;
    state.check_tenant_quota(tenant_id)?;
    let _request = TenantRequestAccounting::start(state, tenant_id);

    flush_ilp_batch_inner(state, tenant_id, database_id, groups).await
}

/// Cancellation-safe tenant request accounting for one ILP batch.
struct TenantRequestAccounting<'a> {
    state: &'a SharedState,
    tenant_id: TenantId,
}

impl<'a> TenantRequestAccounting<'a> {
    fn start(state: &'a SharedState, tenant_id: TenantId) -> Self {
        state.tenant_request_start(tenant_id);
        Self { state, tenant_id }
    }
}

impl Drop for TenantRequestAccounting<'_> {
    fn drop(&mut self) {
        self.state.tenant_request_end(self.tenant_id);
    }
}

/// Inner dispatch logic for ILP batch (separated for clean quota bookkeeping).
async fn flush_ilp_batch_inner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    groups: Vec<IlpMeasurementBatch>,
) -> crate::Result<u64> {
    let mut total_accepted = 0u64;

    for group in groups {
        let collection = group.measurement;
        // Route all lines for a canonical collection to the same vShard as its
        // collection scan. The parser has already authenticated this exact key.
        let vshard_id = VShardId::from_collection_in_database(database_id, &collection);
        let payload_bytes = group.raw_batch.into_bytes();

        // Append to WAL first — returns the assigned LSN for dedup tracking.
        let wal_lsn = crate::control::server::wal_dispatch::wal_append_timeseries(
            &state.wal,
            crate::control::server::wal_dispatch::TimeseriesWalAppendContext {
                tenant_id,
                vshard_id,
                database_id,
                collection: &collection,
            },
            &payload_bytes,
            None,
            Some(&state.credentials),
        )?
        .map(|lsn| lsn.as_u64());

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.clone(),
            payload: payload_bytes,
            format: "ilp".to_string(),
            wal_lsn,
            surrogates: Vec::new(),
            provenance: None,
        });

        let response = match state.gateway.get() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id,
                    trace_id: TraceId::generate(),
                    database_id,
                    txn_id: None,
                };
                gw.execute(&gw_ctx, plan)
                    .await
                    .inspect_err(|err| {
                        let msg = GatewayErrorMap::to_resp(err);
                        warn!(
                            collection = %collection,
                            vshard_id = vshard_id.as_u32(),
                            error = %msg,
                            "ILP gateway dispatch error (batch dropped)"
                        );
                    })
                    .map(|payloads| {
                        let payload = payloads
                            .into_iter()
                            .next()
                            .map(Payload::from_vec)
                            .unwrap_or_else(Payload::empty);
                        Response {
                            request_id: RequestId::new(0),
                            status: Status::Ok,
                            attempt: 0,
                            partial: false,
                            payload,
                            watermark_lsn: Lsn::new(0),
                            error_code: None,
                            read_set_valid: None,
                            read_version_lsn: crate::types::Lsn::ZERO,
                            write_set: Vec::new(),
                        }
                    })?
            }
            None => {
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    state,
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan,
                    TraceId::ZERO,
                )
                .await?
            }
        };

        // Durable-at-ack barrier: the batch was appended to the WAL above, but
        // `wal_append_timeseries` only buffers the record and mints its `Lsn` —
        // the fsync is deferred. Without this barrier a `kill -9` loses the
        // buffered bytes after the caller was already told the rows were
        // accepted. Timeseries rows live only in the `MutationEngine` memtable
        // until a restart replays the WAL, so the WAL is their sole durability
        // path. Mirrors the barrier in `submit_to_data_plane` and
        // `dispatch_utils::dispatch::dispatch_to_data_plane_inner`.
        if response.status == Status::Ok
            && let Some(lsn) = wal_lsn
        {
            state.wal.wait_durable(Lsn::new(lsn)).await?;
        }

        if !response.payload.is_empty()
            && let Ok(v) = sonic_rs::from_slice::<serde_json::Value>(&response.payload)
        {
            total_accepted += v.get("accepted").and_then(|a| a.as_u64()).unwrap_or(0);

            if let Some(schema_cols) = v.get("schema_columns").and_then(|s| s.as_array()) {
                let fields: Vec<(String, String)> = schema_cols
                    .iter()
                    .filter_map(|pair| {
                        let arr = pair.as_array()?;
                        Some((
                            arr.first()?.as_str()?.to_string(),
                            arr.get(1)?.as_str()?.to_string(),
                        ))
                    })
                    .collect();

                let catalog = state.credentials.catalog();
                if !fields.is_empty()
                    && let Ok(Some(mut coll)) =
                        catalog.get_collection(database_id, tenant_id.as_u64(), &collection)
                    && coll.fields != fields
                {
                    coll.fields = fields;
                    if let Err(e) = catalog.put_collection(database_id, &coll) {
                        tracing::warn!(
                            collection = %collection,
                            error = %e,
                            "failed to propagate ILP schema to catalog",
                        );
                    }
                }
            }
        }
    }

    Ok(total_accepted)
}

#[cfg(test)]
mod tests {
    use super::{IlpPreflightFailure, preflight_ilp_batch};
    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Permission,
    };
    use crate::control::security::permission::PermissionStore;
    use crate::control::security::role::RoleStore;
    use crate::types::{DatabaseId, TenantId};

    fn identity(database_id: DatabaseId) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 7,
            username: "ingester".into(),
            tenant_id: TenantId::new(9),
            auth_method: AuthMethod::ApiKey,
            roles: Vec::new(),
            is_superuser: false,
            default_database: Some(database_id),
            accessible_databases: DatabaseSet::Some(smallvec::smallvec![database_id]),
        }
    }

    fn grant_write(permissions: &PermissionStore, collection: &str) {
        let target = format!("collection:9:{collection}");
        permissions
            .grant(&target, "user:ingester", Permission::Write, "admin", None)
            .expect("in-memory grant succeeds");
    }

    fn preflight(
        batch: &str,
        permissions: &PermissionStore,
    ) -> Result<Vec<super::IlpMeasurementBatch>, IlpPreflightFailure> {
        preflight_ilp_batch(
            &identity(DatabaseId::new(7)),
            DatabaseId::new(7),
            batch,
            permissions,
            &RoleStore::new(),
            &NoopAuditEmitter,
        )
    }

    #[test]
    fn groups_two_measurements_in_canonical_order_and_preserves_source_order() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        grant_write(&permissions, "mem");

        let groups = preflight("mem value=1i\ncpu value=2i\nmem value=3i\n", &permissions)
            .expect("all measurements are writable");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].measurement, "cpu");
        assert_eq!(groups[0].raw_batch, "cpu value=2i");
        assert_eq!(groups[1].measurement, "mem");
        assert_eq!(groups[1].raw_batch, "mem value=1i\nmem value=3i");
    }

    #[test]
    fn comments_blanks_and_escaped_measurements_use_canonical_grouping() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu load");

        let groups = preflight(
            "# comment\n\n cpu\\ load value=1i\ncpu\\ load value=2i\n",
            &permissions,
        )
        .expect("escaped measurement is writable");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].measurement, "cpu load");
        assert_eq!(
            groups[0].raw_batch,
            " cpu\\ load value=1i\ncpu\\ load value=2i"
        );
    }

    #[test]
    fn empty_or_comment_only_batch_is_rejected_before_quota_or_dispatch() {
        let permissions = PermissionStore::new();

        assert_eq!(
            preflight(" \n# comment\n", &permissions),
            Err(IlpPreflightFailure::Parse)
        );
    }

    #[test]
    fn malformed_batch_fails_before_any_measurement_can_be_authorized_or_dispatched() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");

        assert_eq!(
            preflight(
                "cpu value=1i\nthis is not valid ILP trailing\n",
                &permissions
            ),
            Err(IlpPreflightFailure::Parse)
        );
    }

    #[test]
    fn second_ungranted_collection_rejects_before_authorized_work_can_run() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let mut authorized_work_runs = 0;

        if preflight("cpu value=1i\nmem value=2i\n", &permissions).is_ok() {
            authorized_work_runs += 1;
        }

        assert_eq!(authorized_work_runs, 0);
    }

    #[test]
    fn denied_batch_emits_one_audit_event_for_its_first_canonical_denial() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let audit = CapturingEmitter::new();

        assert_eq!(
            preflight_ilp_batch(
                &identity(DatabaseId::new(7)),
                DatabaseId::new(7),
                "cpu value=1i\nmem value=2i\n",
                &permissions,
                &RoleStore::new(),
                &audit,
            ),
            Err(IlpPreflightFailure::PermissionDenied)
        );
        assert_eq!(audit.recorded().len(), 1);
    }

    #[test]
    fn read_only_collection_is_not_sufficient_for_ilp_ingest() {
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:9:cpu",
                "user:ingester",
                Permission::Read,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");

        assert_eq!(
            preflight("cpu value=1i\n", &permissions),
            Err(IlpPreflightFailure::PermissionDenied)
        );
    }

    #[test]
    fn non_default_database_is_used_for_collection_authorization() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let database_id = DatabaseId::new(7);
        let roles = RoleStore::new();

        assert_eq!(
            preflight_ilp_batch(
                &identity(DatabaseId::DEFAULT),
                database_id,
                "cpu value=1i\n",
                &permissions,
                &roles,
                &NoopAuditEmitter,
            ),
            Err(IlpPreflightFailure::PermissionDenied)
        );
        let groups = preflight_ilp_batch(
            &identity(database_id),
            database_id,
            "cpu value=1i\n",
            &permissions,
            &roles,
            &NoopAuditEmitter,
        )
        .expect("the explicitly bound non-default database is authorized");

        assert_eq!(groups[0].measurement, "cpu");
    }
}
