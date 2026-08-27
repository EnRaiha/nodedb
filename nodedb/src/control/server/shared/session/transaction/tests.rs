// SPDX-License-Identifier: BUSL-1.1

//! Transaction lifecycle and savepoint behaviour on `SessionStore`.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::lease::QueryLeaseScope;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::super::state::{PendingOffsetCommit, TransactionState};
use super::super::store::SessionStore;

fn task() -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(1),
        plan: PhysicalPlan::Meta(MetaOp::WalAppend {
            payload: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

#[test]
fn savepoint_rollback_truncates_aligned_lease_holders() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:6010".parse().expect("address");
    store.ensure_session(addr);
    store.begin(addr, Lsn::new(1), 0).expect("begin");

    let scope = Arc::new(QueryLeaseScope::empty());
    assert!(store.buffer_write(addr, task()));
    assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
    store.create_savepoint(addr, "sp".into(), BTreeMap::new());
    assert!(store.buffer_write(addr, task()));
    assert!(store.attach_tx_lease_scope_since(addr, 1, Arc::clone(&scope)));

    store
        .rollback_to_savepoint(addr, "sp")
        .expect("rollback to savepoint");
    store.read_session(addr, |session| {
        assert_eq!(session.tx_buffer.len(), 1);
        assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
        assert!(session.tx_lease_scopes[0].is_some());
    });
}

#[test]
fn rollback_to_savepoint_discards_deferred_offsets_after_the_mark() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:6013".parse().expect("address");
    store.ensure_session(addr);
    store.begin(addr, Lsn::new(1), 0).expect("begin");

    let before = PendingOffsetCommit {
        database_id: DatabaseId::DEFAULT,
        tenant_id: 1,
        stream: "orders".into(),
        group: "analytics".into(),
        partition_id: 0,
        offset: crate::event::cdc::CdcOffset::new(10, 1),
    };
    assert!(store.defer_offset_commit(addr, before));
    store.create_savepoint(addr, "sp".into(), BTreeMap::new());
    assert!(store.defer_offset_commit(
        addr,
        PendingOffsetCommit {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            stream: "orders".into(),
            group: "analytics".into(),
            partition_id: 0,
            offset: crate::event::cdc::CdcOffset::new(20, 1),
        },
    ));

    store
        .rollback_to_savepoint(addr, "sp")
        .expect("rollback to savepoint");
    let pending = store.take_pending_offsets(addr);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].offset, crate::event::cdc::CdcOffset::new(10, 1));
}

#[test]
fn commit_returns_lease_holders_after_transitioning_session_to_idle() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:6012".parse().expect("address");
    store.ensure_session(addr);
    store.begin(addr, Lsn::new(1), 0).expect("begin");

    let scope = Arc::new(QueryLeaseScope::empty());
    assert!(store.buffer_write(addr, task()));
    assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));

    let (tasks, holders) = store.commit(addr).expect("commit");
    assert_eq!(tasks.len(), 1);
    assert_eq!(holders.len(), 1);
    assert!(
        holders[0]
            .as_ref()
            .is_some_and(|holder| Arc::ptr_eq(holder, &scope))
    );
    assert_eq!(store.transaction_state(addr), TransactionState::Idle);
    store.read_session(addr, |session| {
        assert!(session.tx_buffer.is_empty());
        assert!(session.tx_lease_scopes.is_empty());
    });

    // The returned holders, which `run_commit` owns, keep the scope alive
    // after the session has transitioned to Idle.
    assert_eq!(Arc::strong_count(&scope), 2);
    drop(holders);
    assert_eq!(Arc::strong_count(&scope), 1);
}

#[test]
fn rollback_and_database_switch_clear_aligned_lease_holders() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:6011".parse().expect("address");
    store.ensure_session(addr);
    let scope = Arc::new(QueryLeaseScope::empty());
    store.begin(addr, Lsn::new(1), 0).expect("begin");
    assert!(store.buffer_write(addr, task()));
    assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
    store.rollback(addr).expect("rollback");
    store.read_session(addr, |session| {
        assert!(session.tx_buffer.is_empty());
        assert!(session.tx_lease_scopes.is_empty());
        assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
    });

    store.begin(addr, Lsn::new(2), 0).expect("begin");
    assert!(store.buffer_write(addr, task()));
    assert!(store.attach_tx_lease_scope_since(addr, 0, scope));
    store.reset_for_database_switch(addr, DatabaseId::new(2));
    store.read_session(addr, |session| {
        assert!(session.tx_buffer.is_empty());
        assert!(session.tx_lease_scopes.is_empty());
        assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
    });
}
