// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for session store (transaction lifecycle, params, cursors, live).

use crate::control::server::shared::session::state::TransactionState;
use crate::control::server::shared::session::store::SessionStore;

#[test]
fn transaction_lifecycle() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);

    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

    store.begin(&addr, crate::types::Lsn::new(1)).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::InBlock);

    store.commit(&addr).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

    store.begin(&addr, crate::types::Lsn::new(1)).unwrap();
    store.fail_transaction(&addr);
    assert_eq!(store.transaction_state(&addr), TransactionState::Failed);

    store.rollback(&addr).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);
}

#[test]
fn session_parameters() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);

    assert_eq!(
        store.get_parameter(&addr, "client_encoding"),
        Some("UTF8".into())
    );

    store.set_parameter(&addr, "application_name".into(), "test_app".into());
    assert_eq!(
        store.get_parameter(&addr, "application_name"),
        Some("test_app".into())
    );
}

#[test]
fn session_cleanup() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);
    assert_eq!(store.count(), 1);

    store.remove(&addr);
    assert_eq!(store.count(), 0);
}

#[test]
fn live_subscription_store_and_check() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();
    store.ensure_session(addr);

    assert!(!store.has_live_subscriptions(&addr));

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    assert!(store.has_live_subscriptions(&addr));
}

#[test]
fn live_subscription_drain_empty() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5002".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // No events published — drain returns empty.
    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
}

#[test]
fn live_subscription_drain_receives_events() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5003".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // Publish a matching event.
    stream.publish(crate::control::change_stream::ChangeEvent {
        lsn: crate::types::Lsn::new(1),
        tenant_id: crate::types::TenantId::new(1),
        collection: "orders".into(),
        document_id: "o42".into(),
        operation: crate::control::change_stream::ChangeOperation::Insert,
        timestamp_ms: 0,
        after: None,
    });

    let notifications = store.drain_live_notifications(&addr);
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].0, "live_orders");
    assert_eq!(notifications[0].1, "INSERT:o42");
}

#[test]
fn live_subscription_filters_by_collection() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5004".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // Publish event for a different collection — should be filtered out.
    stream.publish(crate::control::change_stream::ChangeEvent {
        lsn: crate::types::Lsn::new(1),
        tenant_id: crate::types::TenantId::new(1),
        collection: "users".into(),
        document_id: "u1".into(),
        operation: crate::control::change_stream::ChangeOperation::Update,
        timestamp_ms: 0,
        after: None,
    });

    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
}

#[test]
fn live_subscription_no_session_returns_empty() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5005".parse().unwrap();
    // No session created — should return empty, not panic.
    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
    assert!(!store.has_live_subscriptions(&addr));
}
