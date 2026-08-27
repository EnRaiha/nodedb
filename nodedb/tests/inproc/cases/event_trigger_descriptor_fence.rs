// SPDX-License-Identifier: BUSL-1.1

//! A deferred action is fenced against the catalog it planned against.
//!
//! A descriptor lease lets the Event Plane plan an action at descriptor
//! version V, but holding a lease does not prove the plan is current: a lease
//! grant never compares the requested version against the catalog, so a plan
//! stamped just before a DDL commits still acquires its lease afterwards, at
//! the version that DDL superseded. A DEFINE EVENT THEN action now commits as
//! one transaction, and COMMIT re-compares the leases the plan retained
//! against the catalog before writing anything.
//!
//! These cases drive that comparison over the `(descriptor, version)` holds a
//! lease scope carries, and pin the error a stale action surfaces:
//! `Error::RetryableSchemaChanged`.

use nodedb::Error;
use nodedb::control::gateway::version_check::check_descriptor_holds;
use nodedb::control::security::catalog::{StoredCollection, SystemCatalog};
use nodedb::types::DatabaseId;
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

/// The holds a plan's lease scope carries for `collections`.
fn held(collections: &[(&str, u64)]) -> Vec<(DescriptorId, u64)> {
    collections
        .iter()
        .map(|(name, version)| {
            (
                DescriptorId::new(
                    DatabaseId::DEFAULT.as_u64(),
                    TENANT,
                    DescriptorKind::Collection,
                    *name,
                ),
                *version,
            )
        })
        .collect()
}

fn fence(catalog: &SystemCatalog, holds: &[(DescriptorId, u64)]) -> Result<(), Error> {
    check_descriptor_holds(catalog, holds)?;
    Ok(())
}

#[test]
fn an_action_at_the_current_descriptor_version_commits() {
    let catalog = catalog_with(&[("orders", 6)]);
    assert!(fence(&catalog, &held(&[("orders", 6)])).is_ok());
}

#[test]
fn an_action_at_a_superseded_descriptor_version_is_refused_as_retryable() {
    let catalog = catalog_with(&[("orders", 7)]);
    match fence(&catalog, &held(&[("orders", 6)])) {
        Err(Error::RetryableSchemaChanged { descriptor }) => assert_eq!(descriptor, "orders"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}

#[test]
fn every_collection_an_action_touches_is_fenced() {
    let catalog = catalog_with(&[("orders", 2), ("audit_log", 5)]);
    assert!(fence(&catalog, &held(&[("orders", 2), ("audit_log", 5)])).is_ok());
    match fence(&catalog, &held(&[("orders", 2), ("audit_log", 4)])) {
        Err(Error::RetryableSchemaChanged { descriptor }) => assert_eq!(descriptor, "audit_log"),
        other => panic!("expected RetryableSchemaChanged, got {other:?}"),
    }
}

#[test]
fn an_action_writing_to_a_dropped_collection_is_refused() {
    let catalog = catalog_with(&[]);
    assert!(matches!(
        fence(&catalog, &held(&[("orders", 3)])),
        Err(Error::RetryableSchemaChanged { .. })
    ));
}

#[test]
fn descriptors_of_another_tenant_do_not_fence_this_action() {
    let catalog = catalog_with(&[("orders", 2)]);
    let mut holds = held(&[("orders", 2)]);
    // Another tenant's collection of the same name is compared in its own
    // scope, where it is absent — and an absent collection at a bound version
    // is a mismatch, so it must not be mixed into this tenant's comparison.
    holds.push((
        DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            TENANT + 1,
            DescriptorKind::Collection,
            "orders",
        ),
        0,
    ));
    assert!(fence(&catalog, &holds).is_ok());
}

#[test]
fn a_non_collection_hold_is_not_fenced() {
    let catalog = catalog_with(&[("orders", 2)]);
    let holds = vec![(
        DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            TENANT,
            DescriptorKind::Index,
            "orders_by_id",
        ),
        99,
    )];
    assert!(fence(&catalog, &holds).is_ok());
}
