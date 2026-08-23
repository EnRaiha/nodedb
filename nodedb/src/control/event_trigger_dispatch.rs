// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-fenced dispatch of DEFINE EVENT THEN-action tasks.
//!
//! One trigger action plans once and can expand into several physical tasks,
//! each dispatched over an awaited WAL append plus an SPSC round trip. Holding
//! a descriptor lease does not freeze the catalog: a DDL that bumps a version
//! drains the outstanding leases, and a drain that times out is force-ended so
//! the DDL proceeds anyway. The action's tasks are therefore re-compared
//! against the executing node's own catalog — the same fence the gateway
//! applies to local and cross-node dispatch.

use std::future::Future;

use nodedb_cluster::DescriptorKind;

use crate::Error;
use crate::control::gateway::version_check::check_descriptor_versions;
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Why a DEFINE EVENT THEN action template could not become executable SQL.
///
/// Both cases mean the template is malformed in a way that could change what
/// the rendered statement does, so rendering refuses rather than guessing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TriggerRenderError {
    #[error("unterminated block comment in event trigger action")]
    UnterminatedBlockComment,

    #[error("event trigger placeholders must not be manually quoted")]
    QuotedPlaceholder,

    #[error("unterminated quoted region in event trigger action")]
    UnterminatedQuote,

    #[error("unterminated dollar quote in event trigger action")]
    UnterminatedDollarQuote,

    #[error("invalid UTF-8 boundary in event trigger action")]
    InvalidUtf8Boundary,
}

/// Why one DEFINE EVENT THEN action did not run to completion.
#[derive(Debug, thiserror::Error)]
pub enum TriggerActionError {
    /// The action template could not be rendered into executable SQL.
    #[error("trigger action rejected: {source}")]
    Rejected {
        #[source]
        source: TriggerRenderError,
    },

    /// Planning the rendered SQL failed.
    #[error("trigger action planning failed: {source}")]
    Plan {
        #[source]
        source: Error,
    },

    /// Descriptor lease admission refused the plan.
    #[error("trigger action refused by descriptor lease admission: {source}")]
    LeaseAdmission {
        #[source]
        source: Error,
    },

    /// The action stopped before any of its tasks reached the Data Plane, so
    /// nothing it would have written was applied.
    #[error("trigger action refused before dispatch ({total} tasks): {source}")]
    Refused {
        total: usize,
        #[source]
        source: Error,
    },

    /// The action stopped part-way through its tasks. The `dispatched` tasks
    /// are already durable and are NOT rolled back — the action applied in
    /// part.
    #[error(
        "trigger action stopped after {dispatched} of {total} tasks were already dispatched and applied: {source}"
    )]
    PartialDispatch {
        dispatched: usize,
        total: usize,
        #[source]
        source: Error,
    },
}

/// The `(collection, version)` pairs a plan was compiled against, in the shape
/// the shared descriptor fence compares.
///
/// Only collection descriptors scoped to this plan's database and tenant carry
/// a catalog version to compare; other descriptor kinds are not collections and
/// are skipped.
pub fn action_fence_entries(
    versions: &DescriptorVersionSet,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Vec<(&str, u64)> {
    versions
        .iter()
        .filter(|(id, _)| {
            id.kind == DescriptorKind::Collection
                && id.database_id == database_id.as_u64()
                && id.tenant_id == tenant_id.as_u64()
        })
        .map(|(id, version)| (id.name.as_str(), version))
        .collect()
}

/// Dispatch every task of one planned trigger action, fencing each task
/// against the current catalog immediately before it is dispatched.
pub async fn dispatch_action_tasks(
    shared: &SharedState,
    tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
    versions: &DescriptorVersionSet,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), TriggerActionError> {
    let entries = action_fence_entries(versions, database_id, tenant_id);
    dispatch_fenced(
        shared.credentials.catalog(),
        database_id,
        tenant_id,
        &entries,
        tasks,
        |task| async move {
            crate::control::server::dispatch_utils::dispatch_to_data_plane(
                shared,
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                task.plan,
                TraceId::ZERO,
            )
            .await
            .map(|_response| ())
        },
    )
    .await
}

/// Run `dispatch` over every task, re-comparing the plan's stamped versions
/// against `catalog` before each one.
async fn dispatch_fenced<T, F, Fut>(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: TenantId,
    entries: &[(&str, u64)],
    tasks: Vec<T>,
    dispatch: F,
) -> Result<(), TriggerActionError>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let total = tasks.len();
    // The index doubles as the count already dispatched: at iteration `i`,
    // exactly `i` tasks have applied.
    for (dispatched, task) in tasks.into_iter().enumerate() {
        // The fence sits inside the loop, not once before it: every dispatch
        // awaits a WAL append and an SPSC round trip, and a DDL whose lease
        // drain is force-ended moves the catalog on during that await. A
        // single check before the loop would clear task 0 and let a later
        // task run against a descriptor version the catalog has left behind.
        if let Err(error) = check_descriptor_versions(
            catalog,
            database_id,
            tenant_id.as_u64(),
            entries.iter().copied(),
        ) {
            return Err(stopped(dispatched, total, Error::from(error)));
        }

        dispatch(task)
            .await
            .map_err(|error| stopped(dispatched, total, error))?;
    }
    Ok(())
}

/// Classify a stop by whether any task of the action already applied.
fn stopped(dispatched: usize, total: usize, source: Error) -> TriggerActionError {
    if dispatched == 0 {
        TriggerActionError::Refused { total, source }
    } else {
        TriggerActionError::PartialDispatch {
            dispatched,
            total,
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use nodedb_cluster::DescriptorId;

    use super::*;
    use crate::control::security::catalog::StoredCollection;

    const TENANT: u64 = 4;

    fn catalog_with(collections: &[(&str, u64)]) -> SystemCatalog {
        let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
        for (name, version) in collections {
            put(&catalog, name, *version);
        }
        catalog
    }

    fn put(catalog: &SystemCatalog, name: &str, version: u64) {
        let mut stored = StoredCollection::new(TENANT, name, "owner");
        stored.descriptor_version = version;
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .expect("store collection");
    }

    async fn run<T>(
        catalog: &SystemCatalog,
        entries: &[(&str, u64)],
        tasks: Vec<T>,
        dispatch: impl Fn(T) -> std::future::Ready<Result<(), Error>>,
    ) -> Result<(), TriggerActionError> {
        dispatch_fenced(
            catalog,
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            entries,
            tasks,
            dispatch,
        )
        .await
    }

    #[tokio::test]
    async fn every_task_dispatches_while_the_descriptor_version_holds() {
        let catalog = catalog_with(&[("orders", 3)]);
        let seen = RefCell::new(Vec::new());
        let result = run(&catalog, &[("orders", 3)], vec![1u32, 2, 3], |task| {
            seen.borrow_mut().push(task);
            std::future::ready(Ok(()))
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_version_bump_between_tasks_stops_the_remaining_tasks() {
        let catalog = catalog_with(&[("orders", 3)]);
        let seen = RefCell::new(Vec::new());
        let result = run(&catalog, &[("orders", 3)], vec![1u32, 2, 3], |task| {
            seen.borrow_mut().push(task);
            // The catalog moves on while task 1 is in flight, exactly as a
            // force-ended lease drain does.
            put(&catalog, "orders", 4);
            std::future::ready(Ok(()))
        })
        .await;
        match result {
            Err(TriggerActionError::PartialDispatch {
                dispatched,
                total,
                source,
            }) => {
                assert_eq!(dispatched, 1);
                assert_eq!(total, 3);
                assert!(matches!(source, Error::RetryableSchemaChanged { .. }));
            }
            other => panic!("expected PartialDispatch, got {other:?}"),
        }
        assert_eq!(*seen.borrow(), vec![1]);
    }

    #[tokio::test]
    async fn a_stale_version_refuses_the_action_before_any_task_runs() {
        let catalog = catalog_with(&[("orders", 5)]);
        let seen = RefCell::new(Vec::new());
        let result = run(&catalog, &[("orders", 4)], vec![1u32, 2], |task| {
            seen.borrow_mut().push(task);
            std::future::ready(Ok(()))
        })
        .await;
        assert!(matches!(
            result,
            Err(TriggerActionError::Refused { total: 2, .. })
        ));
        assert!(seen.borrow().is_empty());
    }

    #[tokio::test]
    async fn a_failed_dispatch_stops_the_remaining_tasks() {
        let catalog = catalog_with(&[("orders", 1)]);
        let result = run(&catalog, &[("orders", 1)], vec![1u32, 2], |task| {
            std::future::ready(if task == 1 {
                Ok(())
            } else {
                Err(Error::Internal {
                    detail: "core unavailable".into(),
                })
            })
        })
        .await;
        assert!(matches!(
            result,
            Err(TriggerActionError::PartialDispatch {
                dispatched: 1,
                total: 2,
                ..
            })
        ));
    }

    #[test]
    fn fence_entries_keep_only_this_tenant_and_database_collections() {
        let mut versions = DescriptorVersionSet::new();
        versions.record(
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT,
                DescriptorKind::Collection,
                "orders",
            ),
            3,
        );
        versions.record(
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT,
                DescriptorKind::Index,
                "orders_by_id",
            ),
            9,
        );
        versions.record(
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT + 1,
                DescriptorKind::Collection,
                "other_tenant_orders",
            ),
            2,
        );
        let entries = action_fence_entries(&versions, DatabaseId::DEFAULT, TenantId::new(TENANT));
        assert_eq!(entries, vec![("orders", 3)]);
    }
}
