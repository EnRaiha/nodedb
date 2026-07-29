// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: CDC (Change Data Capture).
//!
//! Writes by Tenant A must NOT appear in Tenant B's change stream subscription.
//! The ChangeStream is scoped by `(collection, tenant_id)` — this test verifies it.

use crate::helpers::{TENANT_A, TENANT_B};
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation, ChangeStream};
use nodedb::types::{Lsn, TenantId};

#[test]
fn cdc_stream_isolated_between_tenants() {
    let stream = ChangeStream::new(1024);

    // Subscribe Tenant B to "orders" — should only see Tenant B's events.
    let _sub_b = stream.subscribe(Some("orders".into()), Some(TenantId::new(TENANT_B)));

    // Publish a change event for Tenant A on "orders".
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: "order_1".into(),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(1),
        after: None,
    });

    // Publish a change event for Tenant B on "orders".
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: "order_2".into(),
        operation: ChangeOperation::Insert,
        timestamp_ms: 2000,
        tenant_id: TenantId::new(TENANT_B),
        lsn: Lsn::new(2),
        after: None,
    });

    // The ring is shared by every tenant on the node, so the query itself is
    // tenant-scoped rather than returning everything for the caller to filter:
    // asking as one tenant returns that tenant's events and nothing else.
    let a_events = stream.query_changes(TenantId::new(TENANT_A), Some("orders"), 0, 100);
    assert_eq!(
        a_events.len(),
        1,
        "Tenant A must see exactly its own event: {a_events:?}"
    );
    assert_eq!(a_events[0].document_id, "order_1");
    assert_eq!(a_events[0].tenant_id, TenantId::new(TENANT_A));

    let b_events = stream.query_changes(TenantId::new(TENANT_B), Some("orders"), 0, 100);
    assert_eq!(
        b_events.len(),
        1,
        "Tenant B must see exactly its own event: {b_events:?}"
    );
    assert_eq!(b_events[0].document_id, "order_2");
    assert_eq!(b_events[0].tenant_id, TenantId::new(TENANT_B));
}

#[test]
fn cdc_different_collections_isolated() {
    let stream = ChangeStream::new(1024);

    // Same tenant, different collections.
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: "o1".into(),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(1),
        after: None,
    });
    stream.publish(ChangeEvent {
        collection: "users".into(),
        document_id: "u1".into(),
        operation: ChangeOperation::Insert,
        timestamp_ms: 2000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(2),
        after: None,
    });

    // Query only "orders" — should not include "users".
    let order_events = stream.query_changes(TenantId::new(TENANT_A), Some("orders"), 0, 100);
    for event in &order_events {
        assert_eq!(event.collection, "orders");
    }
}
