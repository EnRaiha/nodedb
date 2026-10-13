// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::EnforceTimeseriesRetention`: drop a timeseries collection's
//! expired partitions.

use std::collections::HashSet;

use nodedb_physical::physical_plan::AggregateIncarnation;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::handlers::now_ms;

impl CoreLoop {
    /// `MetaOp::EnforceTimeseriesRetention`: drop partitions older than
    /// `max_age_ms`. Bitemporal collections use `max_system_ts` as the
    /// retention axis; non-bitemporal partitions fall through to `max_ts`.
    ///
    /// `aggregates` lists the continuous aggregates the catalog roots at
    /// `collection`. An expiring partition folds into each of them before it
    /// is removed, so the pass is refused, and nothing removed, while one of
    /// them is not registered on this core yet. The enforcement loop retries
    /// on its next pass.
    pub(in crate::data::executor::dispatch) fn meta_enforce_timeseries_retention(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        max_age_ms: i64,
        aggregates: &[AggregateIncarnation],
    ) -> Response {
        let unregistered = aggregates.iter().find(|agg| {
            !self.continuous_agg_mgr.is_registered_at(
                task.request.database_id.as_u64(),
                task.request.tenant_id.as_u64(),
                &agg.name,
                agg.incarnation,
            )
        });
        if let Some(agg) = unregistered {
            return self.response_error(
                task,
                crate::Error::ObjectNotInPrerequisiteState {
                    object: format!("continuous aggregate '{}'", agg.name),
                    detail: format!(
                        "retention of '{collection}' waits until continuous aggregate '{}' \
                         registers on this core; the next pass retries",
                        agg.name
                    ),
                },
            );
        }
        let now_ms = now_ms();
        let cutoff = now_ms - max_age_ms;
        let mut deleted = 0usize;
        let ts_base = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
        );

        let bitemporal = self.is_bitemporal(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
        );

        let ts_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let expired: Vec<(i64, String)> = self
            .ts_registries
            .get(&ts_key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, e)| {
                        let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                            e.meta.max_system_ts
                        } else {
                            e.meta.max_ts
                        };
                        axis_ts < cutoff
                            && e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .map(|(&start, e)| (start, e.dir_name.clone()))
                    .collect()
            })
            .unwrap_or_default();
        // The collection directory's stamp must name every record of the
        // partitions removed below, or replay re-appends the ones the WAL
        // still holds. Nothing is removed until it does.
        if !expired.is_empty()
            && let Err(e) = self.persist_ts_collection_stamp(&ts_key)
        {
            return self.response_error(task, e);
        }
        // Every continuous aggregate rooted here holds the expiring
        // partitions, and its image says so on disk, before any is removed.
        // The image is then the only copy of those rows, so nothing is
        // removed until it is written.
        let database_id = task.request.database_id.as_u64();
        let tenant_id = task.request.tenant_id.as_u64();
        if !expired.is_empty() {
            let expiring: HashSet<String> = expired.iter().map(|(_, dir)| dir.clone()).collect();
            if let Err(e) =
                self.fold_expiring_partitions(database_id, tenant_id, collection, &expiring, now_ms)
            {
                return self.response_error(task, e);
            }
        }
        let mut removed: Vec<String> = Vec::new();
        if let Some(registry) = self.ts_registries.get_mut(&ts_key) {
            for (start_ts, dir_name) in expired {
                let partition_path = ts_base.join(&dir_name);
                if partition_path.exists()
                    && let Err(e) = std::fs::remove_dir_all(&partition_path)
                {
                    tracing::warn!(
                        path = %partition_path.display(),
                        error = %e,
                        "failed to delete expired partition"
                    );
                    continue;
                }
                registry.mark_deleted(start_ts);
                removed.push(dir_name);
                deleted += 1;
            }

            if deleted > 0 {
                tracing::info!(
                    collection,
                    deleted,
                    max_age_ms,
                    "retention enforcement complete"
                );
            }
        }

        if !removed.is_empty() {
            self.continuous_agg_mgr
                .forget_partitions(database_id, tenant_id, collection, &removed);
            self.persist_dirty_cagg_images();
        }

        if let Some(lvc) = self.ts_last_value_caches.get_mut(&ts_key) {
            let evicted = lvc.evict_older_than(cutoff);
            if !evicted.is_empty() {
                tracing::debug!(
                    collection,
                    evicted = evicted.len(),
                    "evicted stale LVC entries"
                );
                // Drop the same series from the catalog. Leaving them behind
                // would let it accumulate every series the collection ever saw
                // while the cache it exists to serve has already released them.
                if let Some(catalog) = self.ts_series_catalogs.get_mut(&ts_key) {
                    for id in evicted {
                        catalog.forget(id);
                    }
                }
            }
        }

        let payload = (deleted as u64).to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::MetaOp;

    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::cagg_test_support::{
        COLLECTION, ROWS_A, TENANT, flush, host_def, image_path, ingest, register, task_at, totals,
    };
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn retention_task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                max_age_ms: 1,
                aggregates: Vec::new(),
            }),
            None,
        )
    }

    /// Live partitions of the collection.
    fn partitions(core: &CoreLoop) -> usize {
        core.ts_registries
            .get(&(
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                COLLECTION.to_string(),
            ))
            .map(|r| {
                r.iter()
                    .filter(|(_, e)| {
                        e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    #[test]
    fn history_survives_raw_retention_and_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            for (i, policy) in [RefreshPolicy::OnFlush, RefreshPolicy::Manual]
                .into_iter()
                .enumerate()
            {
                register(&mut core, host_def(&format!("agg_{i}"), policy));
            }
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            flush(&mut core);
            let resp =
                core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            assert_eq!(partitions(&core), 0, "retention removed the raw rows");
            // A Manual aggregate parks the expiring rows for its next cut.
            assert_eq!(totals(&core, "agg_0"), (3, 111));
            assert_eq!(totals(&core, "agg_1"), (0, 0));
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        for (i, policy) in [RefreshPolicy::OnFlush, RefreshPolicy::Manual]
            .into_iter()
            .enumerate()
        {
            register(&mut core, host_def(&format!("agg_{i}"), policy));
        }
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "agg_0"), (3, 111));
        assert_eq!(totals(&core, "agg_1"), (0, 0));
        let cut = nodedb_physical::physical_plan::RefreshCut {
            aggregate: "agg_1".into(),
            incarnation: host_def("agg_1", RefreshPolicy::Manual).incarnation,
            refresh_id: 1,
            periodic: false,
        };
        let resp = core.execute_cagg_refresh_cut(&task_at(9), COLLECTION, &cut);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(
            totals(&core, "agg_1"),
            (3, 111),
            "the parked rows merge at the cut"
        );
    }

    #[test]
    fn retention_refuses_when_image_write_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        // The registered aggregate's image is on disk. A directory in its
        // place makes the next write's rename fail.
        let image = image_path(dir.path());
        assert!(image.is_file(), "the image of the registered aggregate");
        std::fs::remove_file(&image).expect("remove the image");
        std::fs::create_dir_all(&image).expect("block the image path");

        let resp = core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
        assert_ne!(resp.status, Status::Ok);
        assert_eq!(partitions(&core), 1, "no partition is removed");
        assert_eq!(totals(&core, "by_host"), (3, 111));
    }

    /// The catalog roots an aggregate here that this core has not
    /// registered yet: retention removes nothing until it registers, so the
    /// aggregate never misses the expiring rows.
    #[test]
    fn retention_waits_for_every_catalog_aggregate_to_register() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        let expected = [nodedb_physical::physical_plan::AggregateIncarnation {
            name: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::OnFlush).incarnation,
        }];

        let resp =
            core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &expected);
        assert_ne!(
            resp.status,
            Status::Ok,
            "the aggregate is not registered yet"
        );
        assert_eq!(partitions(&core), 1, "no partition is removed");

        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        let resp =
            core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &expected);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(partitions(&core), 0);
        assert_eq!(
            totals(&core, "by_host"),
            (3, 111),
            "the expiring rows folded first"
        );
    }

    /// The bitemporal audit purge removes timeseries partitions too, so it
    /// waits for the same aggregates as raw retention.
    #[test]
    fn temporal_purge_waits_for_every_catalog_aggregate_to_register() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        let expected = vec![nodedb_physical::physical_plan::AggregateIncarnation {
            name: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::OnFlush).incarnation,
        }];
        // A cutoff past every partition: all of them expire.
        let cutoff_system_ms = i64::MAX;
        let task = CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                tenant_id: TENANT,
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                cutoff_system_ms,
                aggregates: expected.clone(),
            }),
            None,
        );

        let resp = core.meta_temporal_purge_columnar(
            &task,
            TENANT,
            COLLECTION,
            cutoff_system_ms,
            &expected,
        );
        assert_ne!(
            resp.status,
            Status::Ok,
            "the aggregate is not registered yet"
        );
        assert_eq!(partitions(&core), 1, "no partition is removed");

        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        let resp = core.meta_temporal_purge_columnar(
            &task,
            TENANT,
            COLLECTION,
            cutoff_system_ms,
            &expected,
        );
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(partitions(&core), 0);
        assert_eq!(
            totals(&core, "by_host"),
            (3, 111),
            "the expiring rows folded first"
        );
    }
}
