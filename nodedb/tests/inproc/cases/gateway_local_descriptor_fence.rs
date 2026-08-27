// SPDX-License-Identifier: BUSL-1.1

//! The local dispatch path fences a plan against this node's own catalog.
//!
//! A descriptor lease lets a node plan against descriptor version V. A DDL
//! bumping V drains the outstanding leases, but a drain that times out is
//! force-ended and the DDL proceeds — so a node that stalled can wake up and
//! execute work planned against the superseded V. The cross-node path compares
//! the versions carried on `ExecuteRequest`; the local path runs the same
//! comparison over the plan's `GatewayVersionSet` before it reaches the cores.
//!
//! These cases drive the comparison against a real catalog and pin the error
//! the gateway surfaces: `Error::RetryableSchemaChanged`, which the cache-miss
//! retry absorbs by re-planning against fresh state. Driving a full
//! `dispatch_local` needs a live `SharedState` plus a running Data Plane, and
//! stamping a stale version set there is not reachable from a test — the
//! gateway builds the set from the catalog at plan time — so the fence itself
//! is exercised here at the layer both dispatch paths call.

use nodedb::Error;
use nodedb::control::gateway::version_check::{DescriptorCheckError, check_descriptor_versions};
use nodedb::control::security::catalog::{StoredCollection, SystemCatalog};
use nodedb::types::DatabaseId;

const TENANT: u64 = 3;

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

fn fence(catalog: &SystemCatalog, entries: &[(&str, u64)]) -> Result<(), DescriptorCheckError> {
    check_descriptor_versions(
        catalog,
        DatabaseId::DEFAULT,
        TENANT,
        entries.iter().map(|(name, version)| (*name, *version)),
    )
}

#[test]
fn plan_at_the_current_descriptor_version_dispatches_locally() {
    let catalog = catalog_with(&[("orders", 6)]);
    assert!(fence(&catalog, &[("orders", 6)]).is_ok());
}

#[test]
fn plan_at_a_superseded_descriptor_version_is_refused_as_retryable() {
    let catalog = catalog_with(&[("orders", 7)]);
    let err = fence(&catalog, &[("orders", 6)]).expect_err("stale plan must be refused");
    match Error::from(err) {
        Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "orders"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}

#[test]
fn plan_with_no_bound_version_passes_for_an_absent_collection() {
    let catalog = catalog_with(&[]);
    assert!(fence(&catalog, &[("orders", 0)]).is_ok());
}

#[test]
fn plan_with_a_bound_version_is_refused_for_an_absent_collection() {
    let catalog = catalog_with(&[]);
    assert!(matches!(
        fence(&catalog, &[("orders", 4)]),
        Err(DescriptorCheckError::VersionMismatch {
            actual_version: 0,
            ..
        })
    ));
}

#[test]
fn unstamped_catalog_version_is_compared_as_one() {
    let catalog = catalog_with(&[("orders", 0)]);
    assert!(fence(&catalog, &[("orders", 1)]).is_ok());
    assert!(matches!(
        fence(&catalog, &[("orders", 0)]),
        Err(DescriptorCheckError::VersionMismatch {
            actual_version: 1,
            ..
        })
    ));
}

#[test]
fn every_collection_in_the_plan_is_fenced() {
    let catalog = catalog_with(&[("orders", 2), ("users", 5)]);
    assert!(fence(&catalog, &[("orders", 2), ("users", 5)]).is_ok());
    let err = fence(&catalog, &[("orders", 2), ("users", 4)]).expect_err("second entry is stale");
    match Error::from(err) {
        Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "users"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}
