// SPDX-License-Identifier: BUSL-1.1

//! Authorize, quota-check, and Calvin-dispatch a preflighted ILP batch, with
//! a best-effort background catalog schema projection merge.

use std::sync::{Arc, LazyLock};

use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{
    TxnDispatchPosition, dispatch_authorized_strict_atomic_tasks_to_calvin,
};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::Surrogate;

use super::preflight::{IlpMeasurementBatch, preflight_ilp_batch};

/// Dispatch an authorized, strictly parsed ILP batch to the Data Plane.
pub(crate) async fn flush_ilp_batch(
    state: &Arc<SharedState>,
    context: &AuthenticatedIlpContext,
    batch: &str,
) -> crate::Result<u64> {
    flush_authenticated_ilp_batch(
        state,
        context.identity(),
        context.database_id(),
        context.peer_addr(),
        batch,
    )
    .await
}

/// Strictly parse, authorize, and atomically ingest canonical ILP produced by
/// another authenticated external transport such as OTLP.
pub(crate) async fn flush_authenticated_ilp_batch(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    peer_addr: &str,
    batch: &str,
) -> crate::Result<u64> {
    // Blacklist + account status, no rate limit: ILP/OTLP ingest is not the
    // per-query traffic the rate-limiter's cost table models, so charging it
    // against a query rate limit would throttle a legitimate high-volume
    // ingest client. A blacklisted or suspended/banned account must not be
    // able to keep ingesting, though — `check_blacklist_and_status` runs
    // that half of `check_request_admission`'s gate (plus the
    // internal-service exemption every other transport gets) using the real
    // peer address of the ILP connection or OTLP HTTP/gRPC request. The scope
    // is resolved against that same address, so `$auth.risk_score` is stamped
    // and an IP-conditional grant is evaluated for this sender rather than
    // being withheld.
    let request =
        ClientRequestScope::for_database(identity, state.auth_stores(), database_id, peer_addr);
    crate::control::server::session_auth::check_blacklist_and_status(state, &request)?;

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

    flush_ilp_batch_inner(state, identity, database_id, peer_addr, groups).await
}

/// At most one schema-projection merge is ever in flight, process-wide.
///
/// The merge is a replicated catalog DDL, and every catalog DDL already
/// serializes on `SharedState::metadata_ddl_lock`. A second concurrent merge
/// could therefore only park a second blocking-pool thread on a lock that
/// admits one holder, so one permit is both the useful and the safe bound —
/// ingest can never grow the blocking pool no matter how many ILP connections
/// or OTLP requests are live.
static SCHEMA_PROJECTION_SLOT: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));

/// Merge the ingest-inferred schema projection for `groups` off the caller's
/// task.
///
/// `merge_collection_fields_replicated` is a fully synchronous replicated DDL:
/// it takes the metadata DDL preparation lock, acquires the distributed
/// preparation lease, drains prior-version descriptor leases and waits for the
/// local apply — a chain whose bounds are tens of seconds. Running it inline on
/// the ingest task is what starved ILP: `handle_ilp_connection` awaits the
/// flush inside its `select!`, so for the whole of that chain the connection
/// polls neither the socket-read branch nor the coalescing timer, and no
/// subsequent batch is dispatched at all.
///
/// It is therefore run on the blocking pool and deliberately NOT awaited. That
/// costs no durability: the Calvin write above is already committed, and the
/// projection is rebuildable and self-healing — every ILP batch re-supplies its
/// measurement's full field set, so a merge skipped because the slot was busy
/// is re-attempted by the next batch. Failures stay loud in the log, exactly as
/// they were when this ran inline.
fn spawn_schema_projection_merge(
    state: &Arc<SharedState>,
    database_id: DatabaseId,
    tenant_id: TenantId,
    groups: Vec<IlpMeasurementBatch>,
) {
    // Bound the permit to `'static` explicitly: it is moved into the blocking
    // task and must outlive this frame.
    let slot: &'static Semaphore = &SCHEMA_PROJECTION_SLOT;
    let Ok(permit) = slot.try_acquire() else {
        debug!(
            measurements = groups.len(),
            "skipping ILP catalog schema projection: a merge is already in flight"
        );
        return;
    };
    let state = Arc::clone(state);
    tokio::task::spawn_blocking(move || {
        // Held for the whole merge so the next batch's `try_acquire` observes
        // a busy slot rather than queueing another blocking thread.
        let _permit = permit;
        for group in groups {
            match crate::control::catalog_entry::merge_collection_fields_replicated(
                &state,
                database_id,
                tenant_id.as_u64(),
                &group.measurement,
                &group.catalog_fields,
            ) {
                Ok(_) => {}
                // This is a rebuildable control-plane projection. The data
                // commit is already durable, so logging is required but
                // retrying the client request would risk a duplicate write.
                Err(error) => warn!(
                    collection = %group.measurement,
                    error = %error,
                    "failed to merge ILP catalog schema projection after committed Calvin write"
                ),
            }
        }
    });
}

/// Inner dispatch logic for ILP batch (separated for clean quota bookkeeping).
async fn flush_ilp_batch_inner(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    peer_addr: &str,
    groups: Vec<IlpMeasurementBatch>,
) -> crate::Result<u64> {
    let tenant_id = identity.tenant_id;
    let total_rows = preflighted_row_count(&groups)?;
    let mut tasks = build_ilp_calvin_tasks(tenant_id, database_id, &groups)?;

    // This transport builds its physical tasks itself instead of going through
    // the SQL planner, so it has to run the planner's row-level-security pass
    // over them explicitly — without this the line-protocol listener would be a
    // way to write rows a write policy forbids, with the same identity and the
    // same collection that `INSERT` refuses. The resolved scope is the same one
    // the metering pass below uses, so the policy is evaluated for exactly the
    // principal this batch is billed and audited as.
    //
    // It runs BEFORE `authorize_task_set` and before dispatch: the pass mutates
    // the tasks (it compiles the write predicate onto each `Ingest`), and an
    // authorized task set is what gets dispatched, so injecting afterwards
    // would dispatch the un-injected copies.
    //
    // Resolved against the sender's real address like the admission scope
    // above, so a `WHEN`/`REQUIRE IP` scope grant contributes to `$auth.*`
    // here — an RLS policy or metering rule keyed on such a grant must not
    // read differently on this transport than it does on a planned `INSERT`.
    let scope =
        ClientRequestScope::for_database(identity, state.auth_stores(), database_id, peer_addr)
            .into_scope();
    crate::control::planner::rls_injection::inject_rls(&mut tasks, &state.rls, scope.auth())?;

    // A spent hard quota refuses the batch before any of it is staged. The
    // charge below runs once the atomic Calvin write has committed, so it can
    // never be where a cap blocks anything. Checked across every measurement
    // in the batch, because the batch commits atomically: admitting part of
    // it is not an option the write path offers.
    if state.metering_config.enabled {
        for task in &tasks {
            let info = PlanMeteringInfo::extract(&task.plan);
            crate::control::server::shared::quota_admission::admit_quota_for_dispatch(
                state, &scope, &info,
            )?;
        }
    }

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

    // Metered here, once the whole batch's atomic Calvin write has already
    // committed: one usage event per measurement (= one dispatched
    // `PhysicalTask`), each with that measurement's own row count. ILP is
    // deliberately exempt from the query-cost RATE LIMITER
    // (`check_blacklist_and_status` above skips it entirely — ILP's
    // sustained high-volume traffic shape doesn't fit the query cost
    // table), but that is orthogonal to metering: this is real,
    // tenant-attributable write work and must be billed like any other.
    // `tasks` and `groups` are built 1:1 from the same preflighted list
    // (`build_ilp_calvin_tasks` iterates `groups` in order), so zipping
    // them pairs each task with the row count it actually carried.
    if state.metering_config.enabled {
        for (task, group) in tasks.iter().zip(groups.iter()) {
            let info = PlanMeteringInfo::extract(&task.plan);
            let rows = u64::try_from(group.raw_lines.len()).ok();
            meter_dispatch(state, &scope, &info, rows);
        }
    }

    // Timeseries owns authoritative schema. Catalog fields are a rebuildable
    // control-plane projection; update failures are loud but cannot turn an
    // already committed Calvin write into a retryable client failure.
    //
    // The merge goes through the replicated metadata path, never a local
    // catalog write: the projection lives inside the replicated collection
    // descriptor, and mutating that record in place would leave this node's
    // copy no longer byte-equal to the replicated entry at the same descriptor
    // version — which wedges the metadata applier on the next replay. That path
    // is synchronous and slow, so it runs off this task entirely.
    spawn_schema_projection_merge(state, database_id, tenant_id, groups);
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
                    // Filled by `inject_rls` in `flush_ilp_batch_inner`, which
                    // runs over these tasks before they are authorized or
                    // dispatched. Left empty here so this builder stays a pure
                    // function of the preflighted batch.
                    rls_write_check: Vec::new(),
                    // The line-protocol listener answers with an ingest ack, not
                    // a row set — there is no SQL statement behind it to carry a
                    // projection, and so no rows whose visibility needs gating.
                    returning: None,
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::flush_authenticated_ilp_batch;
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::state::SharedState;

    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet};
    use crate::control::security::permission::PermissionStore;
    use crate::control::security::role::RoleStore;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::WalManager;
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
            .grant(
                &target,
                "user:ingester",
                crate::control::security::identity::Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");
    }

    #[tokio::test]
    async fn authenticated_flush_rejects_unwritable_measurement_before_dispatch_or_catalog_projection()
     {
        let directory = tempfile::tempdir().expect("create ILP batch test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("ilp-batch.wal"))
                .expect("open ILP batch test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = Arc::new(SharedState::new(dispatcher, wal).expect("construct ILP batch state"));
        let database_id = DatabaseId::new(7);

        let error = flush_authenticated_ilp_batch(
            &state,
            &identity(database_id),
            database_id,
            "127.0.0.1:9009",
            "cpu value=1i\n",
        )
        .await
        .expect_err("regular identity without write permission is rejected during preflight");

        assert!(matches!(
            error,
            crate::Error::BadRequest { detail } if detail == "ILP batch rejected"
        ));
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(database_id, 9, "cpu")
                .expect("read catalog collection projection")
                .is_none(),
            "early authorization denial must not create a collection/schema projection"
        );
    }

    /// The line-protocol listener builds its physical tasks itself instead of
    /// going through the SQL planner, so it has to run the row-level-security
    /// injection pass explicitly. Before that call existed, this transport
    /// reached the Data Plane without the pass running at all — a write policy
    /// that refuses an `INSERT` into a collection did nothing to an ILP batch
    /// into the same collection under the same identity.
    ///
    /// The policy here names an `$auth` field the identity does not carry, so
    /// the pass fails closed and refuses before dispatch — an outcome only the
    /// injection pass can produce, which is what makes this a regression test
    /// for the pass being called rather than for anything downstream.
    #[tokio::test]
    async fn ilp_ingest_runs_the_row_level_security_injection_pass() {
        use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
        use crate::control::security::rls::{PolicyType, RlsPolicy};

        let directory = tempfile::tempdir().expect("create ILP RLS test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("ilp-rls.wal"))
                .expect("open ILP RLS test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = Arc::new(SharedState::new(dispatcher, wal).expect("construct ILP RLS state"));
        let database_id = DatabaseId::new(7);
        grant_write(&state.permissions, "cpu");
        state
            .rls
            .create_policy(RlsPolicy {
                name: "cpu_owner".into(),
                collection: "cpu".into(),
                tenant_id: 9,
                policy_type: PolicyType::Write,
                compiled_predicate: Some(RlsPredicate::Compare {
                    field: "owner".into(),
                    op: CompareOp::Eq,
                    value: PredicateValue::AuthRef("nonexistent_field".into()),
                }),
                mode: Default::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .expect("create ILP write policy");

        let error = flush_authenticated_ilp_batch(
            &state,
            &identity(database_id),
            database_id,
            "127.0.0.1:9009",
            "cpu,owner=mallory value=1i\n",
        )
        .await
        .expect_err("an unresolvable policy must refuse the batch before dispatch");

        assert!(
            matches!(error, crate::Error::RejectedAuthz { .. }),
            "the refusal must come from the RLS pass, got {error:?}"
        );
    }

    // ── Risk gate. ILP is the shared ingest door for native line protocol,
    //    OTLP, and Prometheus remote write, so what happens here happens to
    //    all three. ─────────────────────────────────────────────────────────

    /// Build ingest state whose risk scorer is configured, not merely present.
    fn risk_state(
        risk: crate::control::security::risk::RiskConfig,
    ) -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create ILP risk test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("ilp-risk.wal"))
                .expect("open ILP risk test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_risk_config(dispatcher, wal, risk)
            .expect("construct ILP risk state");
        (state, directory)
    }

    fn rejection_reason(error: &crate::Error) -> String {
        match error {
            crate::Error::RejectedAuthz { resource, .. } => resource.clone(),
            other => panic!("expected an authz rejection, got {other:?}"),
        }
    }

    /// With scoring enabled and every score inside the allow band, an ingest
    /// batch must pass the admission gate and fail only on its own merits —
    /// here, the missing write grant that preflight refuses.
    ///
    /// Before the sender's address reached this scope there was no score to
    /// enforce, so the gate failed closed: turning on `[auth.risk]` took ILP,
    /// OTLP and Prometheus remote write offline for every client.
    #[tokio::test]
    async fn scored_ingest_passes_the_admission_gate_instead_of_failing_closed() {
        let (state, _dir) = risk_state(crate::control::security::risk::RiskConfig {
            enabled: true,
            allow_threshold: 1.0,
            deny_threshold: 2.0,
            ..Default::default()
        });
        let database_id = DatabaseId::new(7);

        let error = flush_authenticated_ilp_batch(
            &state,
            &identity(database_id),
            database_id,
            "10.0.0.7:9009",
            "cpu value=1i\n",
        )
        .await
        .expect_err("the batch still has no write grant");

        assert!(
            matches!(&error, crate::Error::BadRequest { detail } if detail == "ILP batch rejected"),
            "an in-band sender must reach preflight, got {error:?}"
        );
    }

    /// The score is the sender's, not a constant: the same request is refused
    /// by risk policy once the deny band covers it — a verdict only reachable
    /// when the address was actually scored.
    #[tokio::test]
    async fn deny_band_ingest_is_refused_by_the_risk_gate() {
        let (state, _dir) = risk_state(crate::control::security::risk::RiskConfig {
            enabled: true,
            allow_threshold: -1.0,
            deny_threshold: 0.0,
            ..Default::default()
        });
        let database_id = DatabaseId::new(7);
        grant_write(&state.permissions, "cpu");

        let error = flush_authenticated_ilp_batch(
            &state,
            &identity(database_id),
            database_id,
            "10.0.0.7:9009",
            "cpu value=1i\n",
        )
        .await
        .expect_err("a deny-band sender must be refused");

        assert_eq!(rejection_reason(&error), "denied by risk policy");
    }

    #[test]
    fn schema_projection_slot_admits_exactly_one_merge_at_a_time() {
        // The bound that keeps ingest from queueing blocking-pool threads
        // behind a metadata DDL lock that admits a single holder.
        let first = super::SCHEMA_PROJECTION_SLOT
            .try_acquire()
            .expect("the first merge takes the only slot");
        assert!(
            super::SCHEMA_PROJECTION_SLOT.try_acquire().is_err(),
            "a second concurrent merge must be refused, not queued"
        );
        drop(first);
        assert!(
            super::SCHEMA_PROJECTION_SLOT.try_acquire().is_ok(),
            "the slot must be reusable once the in-flight merge finishes"
        );
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
        let groups = super::super::preflight::preflight_ilp_batch(
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
            rls_write_check,
            ..
        }) = &tasks[0].plan
        else {
            panic!("timeseries task")
        };
        assert_eq!(collection, "cpu");
        assert_eq!(format, "ilp-msgpack");
        assert_eq!(*wal_lsn, None);
        assert!(
            rls_write_check.is_empty(),
            "the builder must leave the predicate to the injection pass"
        );
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
