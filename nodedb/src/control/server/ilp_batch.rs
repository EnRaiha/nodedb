// SPDX-License-Identifier: BUSL-1.1

//! ILP batch preflight, authorization, and dispatch.

use std::collections::BTreeMap;
use std::sync::Arc;

use tracing::warn;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    TxnDispatchPosition, dispatch_authorized_strict_atomic_tasks_to_calvin,
};
use crate::control::security::audit::{ArcAuditEmitter, AuditEmitter};
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::server::shared::authorization::{authorize_collection, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::Surrogate;

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
/// `raw_lines` preserve physical source order; map iteration canonicalizes
/// measurement order. `catalog_fields` is a rebuildable control-plane projection
/// of the authoritative timeseries-engine schema.
#[derive(Debug, PartialEq, Eq)]
struct IlpMeasurementBatch {
    measurement: String,
    raw_lines: Vec<String>,
    catalog_fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IlpPreflightFailure {
    Parse,
    PermissionDenied,
}

/// Parse and authorize every unique collection before quota accounting, task
/// construction, sequencer submission, or catalog projection work.
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

    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for line in parsed.lines() {
        grouped
            .entry(line.measurement.to_string())
            .or_default()
            .push(line.raw.to_owned());
    }

    let mut groups = Vec::with_capacity(grouped.len());
    for (measurement, raw_lines) in grouped {
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
        let grouped_source = raw_lines.join("\n");
        let parsed_group = crate::engine::timeseries::ilp::parse_batch(&grouped_source)
            .map_err(|_| IlpPreflightFailure::Parse)?;
        let schema = crate::engine::timeseries::ilp_ingest::infer_schema(parsed_group.lines());
        let catalog_fields = schema
            .columns
            .iter()
            .map(|(name, ty)| {
                let sql_type = match ty {
                    crate::engine::timeseries::columnar_memtable::ColumnType::Timestamp => {
                        "TIMESTAMP"
                    }
                    crate::engine::timeseries::columnar_memtable::ColumnType::Float64 => "FLOAT",
                    crate::engine::timeseries::columnar_memtable::ColumnType::Int64 => "BIGINT",
                    crate::engine::timeseries::columnar_memtable::ColumnType::Symbol => "VARCHAR",
                };
                (name.clone(), sql_type.to_owned())
            })
            .collect();
        groups.push(IlpMeasurementBatch {
            measurement,
            raw_lines,
            catalog_fields,
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
    flush_authenticated_ilp_batch(state, context.identity(), context.database_id(), batch).await
}

/// Strictly parse, authorize, and atomically ingest canonical ILP produced by
/// another authenticated external transport such as OTLP.
pub(crate) async fn flush_authenticated_ilp_batch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    batch: &str,
) -> crate::Result<u64> {
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
    let _request = state.tenant_request_guard(tenant_id);

    flush_ilp_batch_inner(state, identity, database_id, groups).await
}

/// Inner dispatch logic for ILP batch (separated for clean quota bookkeeping).
async fn flush_ilp_batch_inner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    groups: Vec<IlpMeasurementBatch>,
) -> crate::Result<u64> {
    let tenant_id = identity.tenant_id;
    let total_rows = preflighted_row_count(&groups)?;
    let tasks = build_ilp_calvin_tasks(tenant_id, database_id, &groups)?;
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let authorized =
        authorize_task_set(identity, &tasks, &state.permissions, &state.roles, &emitter)
            .map_err(crate::Error::from)?;

    // One Calvin submit stages every measurement and makes the TransactionRedo
    // the sole durability record; no per-measurement WAL or direct dispatch may
    // race ahead of a later measurement failure.
    let _ = dispatch_authorized_strict_atomic_tasks_to_calvin(
        state,
        authorized,
        tenant_id,
        TxnDispatchPosition::Autocommit,
        &[],
        None,
    )
    .await?;

    // Timeseries owns authoritative schema. Catalog fields are a rebuildable
    // control-plane projection; update failures are loud but cannot turn an
    // already committed Calvin write into a retryable client failure.
    let catalog = state.credentials.catalog();
    for group in groups {
        match catalog.merge_collection_fields(
            database_id,
            tenant_id.as_u64(),
            &group.measurement,
            &group.catalog_fields,
        ) {
            Ok(_) => {}
            // This is a rebuildable control-plane projection. The data commit is
            // already durable, so logging is required but retrying the client
            // request would risk a duplicate write.
            Err(error) => warn!(
                collection = %group.measurement,
                error = %error,
                "failed to merge ILP catalog schema projection after committed Calvin write"
            ),
        }
    }
    Ok(total_rows)
}

fn preflighted_row_count(groups: &[IlpMeasurementBatch]) -> crate::Result<u64> {
    groups.iter().try_fold(0u64, |total, group| {
        u64::try_from(group.raw_lines.len())
            .ok()
            .and_then(|count| total.checked_add(count))
            .ok_or(crate::Error::BadRequest {
                detail: "ILP row count exceeds protocol limit".into(),
            })
    })
}

/// Convert canonical preflight groups into one deterministic Calvin task each.
fn build_ilp_calvin_tasks(
    tenant_id: TenantId,
    database_id: DatabaseId,
    groups: &[IlpMeasurementBatch],
) -> crate::Result<Vec<PhysicalTask>> {
    groups
        .iter()
        .map(|group| {
            let payload = zerompk::to_msgpack_vec(&group.raw_lines).map_err(|error| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("failed to encode canonical ILP lines: {error}"),
                }
            })?;
            let surrogates = (1..=group.raw_lines.len())
                .map(|row| {
                    u32::try_from(row)
                        .map(Surrogate::new)
                        .map_err(|_| crate::Error::BadRequest {
                            detail: "ILP measurement row count exceeds u32 overlay-token limit"
                                .into(),
                        })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(PhysicalTask {
                tenant_id,
                database_id,
                vshard_id: VShardId::from_collection_in_database(database_id, &group.measurement),
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: group.measurement.clone(),
                    payload,
                    format: "ilp-msgpack".into(),
                    wal_lsn: None,
                    surrogates,
                    provenance: None,
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            })
        })
        .collect()
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
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_types::Surrogate;

    fn identity(database_id: DatabaseId) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "ingester",
            TenantId::new(9),
            AuthMethod::ApiKey,
            Vec::new(),
            Some(database_id),
            DatabaseSet::Some(smallvec::smallvec![database_id]),
        )
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
        assert_eq!(groups[0].raw_lines, vec!["cpu value=2i"]);
        assert_eq!(groups[1].measurement, "mem");
        assert_eq!(groups[1].raw_lines, vec!["mem value=1i", "mem value=3i"]);
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
            groups[0].raw_lines,
            vec![" cpu\\ load value=1i", "cpu\\ load value=2i"]
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

    #[test]
    fn accepted_count_is_preflighted_row_total_not_task_count() {
        let groups = vec![
            super::IlpMeasurementBatch {
                measurement: "cpu".into(),
                raw_lines: vec!["cpu value=1i".into(), "cpu value=2i".into()],
                catalog_fields: Vec::new(),
            },
            super::IlpMeasurementBatch {
                measurement: "mem".into(),
                raw_lines: vec!["mem value=3i".into()],
                catalog_fields: Vec::new(),
            },
        ];
        assert_eq!(super::preflighted_row_count(&groups).expect("count"), 3);
    }

    #[test]
    fn task_builder_is_deterministic_and_uses_overlay_tokens() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        grant_write(&permissions, "mem");
        let database_id = DatabaseId::new(7);
        let groups = preflight_ilp_batch(
            &identity(database_id),
            database_id,
            "mem value=1i\ncpu value=2i\ncpu value=3i\n",
            &permissions,
            &RoleStore::new(),
            &NoopAuditEmitter,
        )
        .expect("preflight");
        let tasks =
            super::build_ilp_calvin_tasks(TenantId::new(9), database_id, &groups).expect("tasks");
        assert_eq!(tasks.len(), 2);
        let PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            wal_lsn,
            surrogates,
            ..
        }) = &tasks[0].plan
        else {
            panic!("timeseries task")
        };
        assert_eq!(collection, "cpu");
        assert_eq!(format, "ilp-msgpack");
        assert_eq!(*wal_lsn, None);
        assert_eq!(surrogates, &vec![Surrogate::new(1), Surrogate::new(2)]);
        assert_eq!(
            zerompk::from_msgpack::<Vec<String>>(payload).expect("payload"),
            vec!["cpu value=2i", "cpu value=3i"]
        );
        assert_eq!(tasks[0].tenant_id, TenantId::new(9));
        assert_eq!(tasks[0].database_id, database_id);
        assert_eq!(
            tasks[0].vshard_id,
            VShardId::from_collection_in_database(database_id, "cpu")
        );
    }
}
