// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::broadcast;

use nodedb_cluster::{
    MetadataApplier, MetadataCache, MetadataEntry, PendingDdlObject, encode_entry,
};
use nodedb_types::{DatabaseId, Hlc};

use crate::bridge::dispatch::Dispatcher;
use crate::control::catalog_entry;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;
use crate::wal::WalManager;

use super::types::MetadataCommitApplier;

fn make_applier() -> (
    MetadataCommitApplier,
    Arc<RwLock<MetadataCache>>,
    Arc<CredentialStore>,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let credentials =
        Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
    let cache = Arc::new(RwLock::new(MetadataCache::new()));
    let (tx, _rx) = broadcast::channel(16);
    let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let applier = MetadataCommitApplier::new(cache.clone(), tx, credentials.clone(), token_state);
    (applier, cache, credentials, tmp)
}

fn put_collection_entry(name: &str) -> MetadataEntry {
    let stored = StoredCollection::new(7, name, "tester");
    let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
    MetadataEntry::CatalogDdl {
        payload: catalog_entry::encode(&catalog_entry).unwrap(),
    }
}

fn pending_create_object(name: &str) -> PendingDdlObject {
    PendingDdlObject::Create {
        entry: Box::new(put_collection_entry(name)),
    }
}

/// An applier wired to a real `SharedState` (weak handle installed), the
/// only shape under which `DdlPendingPropose` / `DdlPendingFinalize` /
/// `DdlPendingCancel` do anything — they are no-ops without it, matching
/// every other `self.shared`-gated apply path in this module.
fn make_applier_with_shared() -> (MetadataCommitApplier, Arc<SharedState>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&tmp.path().join("test.wal")).expect("open wal"));
    let credentials =
        Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open catalog"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
        .expect("construct shared state");
    let (tx, _rx) = broadcast::channel(16);
    let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let applier = MetadataCommitApplier::new(
        state.metadata_cache.clone(),
        tx,
        state.credentials.clone(),
        token_state,
    );
    applier.install_shared(Arc::downgrade(&state));
    (applier, state, tmp)
}

#[tokio::test(flavor = "multi_thread")]
async fn propose_then_finalize_applies_and_clears_the_record() {
    let (applier, state, _tmp) = make_applier_with_shared();
    let token = 1;
    let propose = MetadataEntry::DdlPendingPropose {
        token,
        objects: vec![pending_create_object("pending_orders")],
        proposed_at: Hlc::default(),
    };
    assert_eq!(applier.apply(&[(1, encode_entry(&propose).unwrap())]), 1);
    assert!(
        state.pending_ddl.contains(token),
        "propose reserves the record"
    );
    assert!(
        state
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
            .unwrap()
            .is_none(),
        "propose alone must not write the catalog"
    );

    let finalize = MetadataEntry::DdlPendingFinalize { token };
    assert_eq!(applier.apply(&[(2, encode_entry(&finalize).unwrap())]), 2);
    assert!(
        !state.pending_ddl.contains(token),
        "finalize clears the record"
    );
    assert!(
        state
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
            .unwrap()
            .is_some(),
        "finalize replays the reserved object's host-side effects"
    );

    // Double-apply (Raft re-delivery): no record left, so this must be a
    // silent no-op rather than an error or a repeat write.
    assert_eq!(applier.apply(&[(3, encode_entry(&finalize).unwrap())]), 3);
    assert!(!state.pending_ddl.contains(token));
}

#[tokio::test(flavor = "multi_thread")]
async fn propose_then_cancel_clears_without_touching_the_catalog() {
    let (applier, state, _tmp) = make_applier_with_shared();
    let token = 2;
    let propose = MetadataEntry::DdlPendingPropose {
        token,
        objects: vec![pending_create_object("pending_widgets")],
        proposed_at: Hlc::default(),
    };
    assert_eq!(applier.apply(&[(1, encode_entry(&propose).unwrap())]), 1);
    assert!(state.pending_ddl.contains(token));

    let cancel = MetadataEntry::DdlPendingCancel { token };
    assert_eq!(applier.apply(&[(2, encode_entry(&cancel).unwrap())]), 2);
    assert!(
        !state.pending_ddl.contains(token),
        "cancel clears the record"
    );
    assert!(
        state
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "pending_widgets")
            .unwrap()
            .is_none(),
        "cancel must never write the catalog"
    );

    // Double-apply (Raft re-delivery): no record left, must stay a no-op.
    assert_eq!(applier.apply(&[(3, encode_entry(&cancel).unwrap())]), 3);
    assert!(!state.pending_ddl.contains(token));
}

#[tokio::test(flavor = "multi_thread")]
async fn finalize_and_cancel_for_an_unknown_token_are_noops() {
    let (applier, state, _tmp) = make_applier_with_shared();
    let unknown = 999;
    assert_eq!(
        applier.apply(&[(
            1,
            encode_entry(&MetadataEntry::DdlPendingFinalize { token: unknown }).unwrap()
        )]),
        1,
        "finalize with no matching propose must not wedge the watermark"
    );
    assert_eq!(
        applier.apply(&[(
            2,
            encode_entry(&MetadataEntry::DdlPendingCancel { token: unknown }).unwrap()
        )]),
        2,
        "cancel with no matching propose must not wedge the watermark"
    );
    assert!(!state.pending_ddl.contains(unknown));
}

#[test]
fn apply_put_collection_writes_through_to_redb() {
    let (applier, cache, credentials, _tmp) = make_applier();
    let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
    assert_eq!(applier.apply(&[(11, bytes)]), 11);

    let cache_guard = cache.read().unwrap();
    assert_eq!(cache_guard.applied_index, 11);
    assert_eq!(cache_guard.catalog_entries_applied, 1);
    drop(cache_guard);

    let loaded = credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, 7, "orders")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "orders");
    assert_eq!(loaded.owner, "tester");
}

#[test]
fn apply_deactivate_preserves_record() {
    let (applier, _cache, credentials, _tmp) = make_applier();

    // Seed.
    applier.apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())]);

    let drop_entry = MetadataEntry::CatalogDdl {
        payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 7,
            name: "archived".into(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        })
        .unwrap(),
    };
    applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

    let loaded = credentials
        .catalog()
        .get_collection(DatabaseId::DEFAULT, 7, "archived")
        .unwrap()
        .expect("preserved");
    assert!(!loaded.is_active);
}

#[test]
fn join_token_transition_updates_and_persists_shared_mirror() {
    let (applier, _cache, credentials, _tmp) = make_applier();
    let hash = [0x44; 32];
    let entries = [
        MetadataEntry::JoinTokenTransition {
            token_hash: hash,
            transition: nodedb_cluster::JoinTokenTransitionKind::Register {
                expires_at_ms: 10_000,
            },
            ts_ms: 1,
        },
        MetadataEntry::JoinTokenTransition {
            token_hash: hash,
            transition: nodedb_cluster::JoinTokenTransitionKind::BeginInFlight {
                node_addr: "127.0.0.1:9000".into(),
                lease_id: [0x55; 16],
            },
            ts_ms: 2,
        },
        MetadataEntry::JoinTokenTransition {
            token_hash: hash,
            transition: nodedb_cluster::JoinTokenTransitionKind::MarkConsumed {
                node_addr: "127.0.0.1:9000".into(),
                lease_id: [0x55; 16],
                recovery_bundle: vec![1, 2, 3],
            },
            ts_ms: 3,
        },
    ];
    for (offset, entry) in entries.iter().enumerate() {
        let index = offset as u64 + 1;
        assert_eq!(
            applier.apply(&[(index, encode_entry(entry).expect("encode"))]),
            index
        );
    }
    let persisted = credentials
        .catalog()
        .list_join_token_states()
        .expect("load token state");
    assert!(matches!(
        persisted.as_slice(),
        [nodedb_cluster::JoinTokenState {
            lifecycle: nodedb_cluster::JoinTokenLifecycle::Consumed { ts_ms: 3, .. },
            ..
        }]
    ));
}

#[test]
fn apply_empty_batch_is_noop() {
    let (applier, _cache, _credentials, _tmp) = make_applier();
    assert_eq!(applier.apply(&[]), 0);
}

#[test]
fn apply_noop_entry_advances_cache_watermark() {
    let (applier, cache, _credentials, _tmp) = make_applier();
    // A committed Raft no-op (empty payload) at index 1 — the shape of every
    // group's first entry on a fresh single-node start. It mutates nothing, but
    // the cache watermark must advance in lockstep with the Raft applied index
    // the tick loop takes from the return value; otherwise the startup
    // applied-index sanity check reads a spurious gap and fails the boot.
    assert_eq!(applier.apply(&[(1, Vec::new())]), 1);
    assert_eq!(cache.read().unwrap().applied_index, 1);
    assert_eq!(
        cache.read().unwrap().catalog_entries_applied,
        0,
        "a no-op applies no catalog entry"
    );
}
