// SPDX-License-Identifier: BUSL-1.1

//! A DEFINE EVENT THEN action is fenced against the catalog it planned against.
//!
//! A descriptor lease lets the Event Plane plan a trigger action at descriptor
//! version V. A DDL bumping V drains the outstanding leases, but a drain that
//! times out is force-ended and the DDL proceeds — so the lease does not keep
//! the plan fresh. An action can expand into several tasks, each dispatched
//! over an awaited WAL append plus an SPSC round trip, so the catalog can move
//! between one task and the next.
//!
//! These cases drive the version-set extraction the trigger path performs and
//! the shared fence it feeds, and pin the error a stale action surfaces:
//! `Error::RetryableSchemaChanged`. The per-task re-comparison inside the
//! dispatch loop is covered by the unit tests next to that loop, which can
//! move the catalog mid-loop; driving a real mid-loop version bump from here
//! would need a live `SharedState` plus a running Data Plane and a DDL landing
//! inside one dispatch await, which the harness cannot express.

use nodedb::Error;
use nodedb::control::event_trigger_dispatch::action_fence_entries;
use nodedb::control::gateway::version_check::check_descriptor_versions;
use nodedb::control::planner::descriptor_set::DescriptorVersionSet;
use nodedb::control::security::catalog::{StoredCollection, SystemCatalog};
use nodedb::types::{DatabaseId, TenantId};
use nodedb_cluster::{DescriptorId, DescriptorKind};

const TENANT: u64 = 11;

fn catalog_with(collections: &[(&str, u64)]) -> SystemCatalog {
    let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
    for (name, version) in collections {
        let mut stored = StoredCollection::new(TENANT, name, "owner");
        stored.descriptor_version = *version;
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .expect("store collection");
    }
    catalog
}

fn planned_versions(collections: &[(&str, u64)]) -> DescriptorVersionSet {
    let mut versions = DescriptorVersionSet::new();
    for (name, version) in collections {
        versions.record(
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT,
                DescriptorKind::Collection,
                *name,
            ),
            *version,
        );
    }
    versions
}

fn fence(catalog: &SystemCatalog, versions: &DescriptorVersionSet) -> Result<(), Error> {
    let entries = action_fence_entries(versions, DatabaseId::DEFAULT, TenantId::new(TENANT));
    check_descriptor_versions(
        catalog,
        DatabaseId::DEFAULT,
        TENANT,
        entries.iter().copied(),
    )?;
    Ok(())
}

#[test]
fn trigger_action_at_the_current_descriptor_version_is_dispatched() {
    let catalog = catalog_with(&[("orders", 6)]);
    assert!(fence(&catalog, &planned_versions(&[("orders", 6)])).is_ok());
}

#[test]
fn trigger_action_at_a_superseded_descriptor_version_is_refused_as_retryable() {
    let catalog = catalog_with(&[("orders", 7)]);
    match fence(&catalog, &planned_versions(&[("orders", 6)])) {
        Err(Error::RetryableSchemaChanged { descriptor }) => assert_eq!(descriptor, "orders"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}

#[test]
fn every_collection_a_trigger_action_touches_is_fenced() {
    let catalog = catalog_with(&[("orders", 2), ("audit_log", 5)]);
    assert!(
        fence(
            &catalog,
            &planned_versions(&[("orders", 2), ("audit_log", 5)])
        )
        .is_ok()
    );
    match fence(
        &catalog,
        &planned_versions(&[("orders", 2), ("audit_log", 4)]),
    ) {
        Err(Error::RetryableSchemaChanged { descriptor }) => assert_eq!(descriptor, "audit_log"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}

#[test]
fn a_trigger_action_writing_to_a_dropped_collection_is_refused() {
    let catalog = catalog_with(&[]);
    assert!(matches!(
        fence(&catalog, &planned_versions(&[("orders", 3)])),
        Err(Error::RetryableSchemaChanged { .. })
    ));
}

#[test]
fn descriptors_of_another_tenant_do_not_fence_this_action() {
    let catalog = catalog_with(&[("orders", 2)]);
    let mut versions = planned_versions(&[("orders", 2)]);
    versions.record(
        DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            TENANT + 1,
            DescriptorKind::Collection,
            "orders",
        ),
        99,
    );
    assert!(fence(&catalog, &versions).is_ok());
}
