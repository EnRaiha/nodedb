// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::record::{FtsIndexPayload, RecordType, SyncSeqAdvancePayload};

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

#[test]
fn crdt_signing_root_survives_chained_runtime_rotation_and_restart() {
    fn write_key(path: &std::path::Path, byte: u8) {
        std::fs::write(path, [byte; 32]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_dir");
    let key_a = dir.path().join("key-a");
    let key_b = dir.path().join("key-b");
    let key_c = dir.path().join("key-c");
    write_key(&key_a, 0x11);
    write_key(&key_b, 0x22);
    write_key(&key_c, 0x33);

    let mut wal = WalManager::open_encrypted(&wal_dir, false, &key_a).unwrap();
    let stable_root = wal.crdt_signing_root().unwrap().unwrap();
    wal.append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        b"a",
    )
    .unwrap();
    wal.rotate_key(&key_b).unwrap();
    drop(wal);

    let mut wal = WalManager::open_encrypted_rotating(&wal_dir, false, &key_b, &key_a).unwrap();
    assert_eq!(wal.crdt_signing_root().unwrap(), Some(stable_root));
    wal.append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        b"b",
    )
    .unwrap();
    wal.rotate_key(&key_c).unwrap();
    drop(wal);

    let wal = WalManager::open_encrypted_rotating(&wal_dir, false, &key_c, &key_b).unwrap();
    assert_eq!(wal.crdt_signing_root().unwrap(), Some(stable_root));
}

#[test]
fn set_encryption_ring_rewraps_root_across_chained_transitions() {
    fn write_key(path: &std::path::Path, byte: u8) {
        std::fs::write(path, [byte; 32]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal_dir");
    let key_a_path = dir.path().join("key-a");
    let key_b_path = dir.path().join("key-b");
    let key_c_path = dir.path().join("key-c");
    write_key(&key_a_path, 0x41);
    write_key(&key_b_path, 0x42);
    write_key(&key_c_path, 0x43);
    let key_a = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_a_path).unwrap();
    let key_b = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_b_path).unwrap();
    let key_c = nodedb_wal::crypto::WalEncryptionKey::from_file(&key_c_path).unwrap();

    let mut wal = WalManager::open_for_testing(&wal_dir).unwrap();
    wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(key_a.clone()))
        .unwrap();
    let root = wal.crdt_signing_root().unwrap();
    wal.append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        b"a",
    )
    .unwrap();
    wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::with_previous(
        key_b.clone(),
        key_a,
    ))
    .unwrap();
    assert_eq!(wal.crdt_signing_root().unwrap(), root);
    wal.append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        b"b",
    )
    .unwrap();
    wal.set_encryption_ring(nodedb_wal::crypto::KeyRing::with_previous(key_c, key_b))
        .unwrap();
    assert_eq!(wal.crdt_signing_root().unwrap(), root);
    drop(wal);

    let wal =
        WalManager::open_encrypted_rotating(&wal_dir, false, &key_c_path, &key_b_path).unwrap();
    assert_eq!(wal.crdt_signing_root().unwrap(), root);
}

#[test]
fn sync_seq_advance_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let lsn = wal
        .append_sync_seq_advance(0xCAFE_BABE_DEAD_BEEF, 7, 42, 1_000_000)
        .unwrap();
    assert_eq!(lsn, Lsn::new(1));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].header.record_type,
        RecordType::SyncSeqAdvance as u32
    );
    let payload = SyncSeqAdvancePayload::from_bytes(&records[0].payload).unwrap();
    assert_eq!(payload.producer_id, 0xCAFE_BABE_DEAD_BEEF);
    assert_eq!(payload.epoch, 7);
    assert_eq!(payload.stream_id, 42);
    assert_eq!(payload.seq, 1_000_000);
}

#[test]
fn append_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(1);
    let v = VShardId::new(0);
    let db = DatabaseId::DEFAULT;

    let lsn1 = wal.append_put(t, v, db, b"key1=value1").unwrap();
    let lsn2 = wal.append_put(t, v, db, b"key2=value2").unwrap();
    let lsn3 = wal.append_delete(t, v, db, b"key1").unwrap();

    assert_eq!(lsn1, Lsn::new(1));
    assert_eq!(lsn2, Lsn::new(2));
    assert_eq!(lsn3, Lsn::new(3));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].payload, b"key1=value1");
    assert_eq!(records[2].payload, b"key1");
}

#[test]
fn append_transaction_redo_returns_monotonic_lsn() {
    use crate::wal::{RedoRecord, RedoSubRecord};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(3);
    let v = VShardId::new(1);
    let db = DatabaseId::DEFAULT;

    let record = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: vec![1, 2, 3],
        }],
        calvin_stamp: None,
    };

    let lsn1 = wal.append_transaction_redo(t, v, db, &record).unwrap();
    let lsn2 = wal.append_transaction_redo(t, v, db, &record).unwrap();
    let lsn3 = wal.append_transaction_redo(t, v, db, &record).unwrap();

    assert_eq!(lsn1, Lsn::new(1));
    assert_eq!(lsn2, Lsn::new(2));
    assert_eq!(lsn3, Lsn::new(3));
    assert!(lsn2 > lsn1);
    assert!(lsn3 > lsn2);

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(
        records[0].header.record_type,
        RecordType::TransactionRedo as u32
    );
    let decoded = RedoRecord::from_bytes(&records[0].payload).unwrap();
    assert_eq!(decoded, record);
}

#[test]
fn crdt_delta_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(5);
    let v = VShardId::new(42);
    let db = DatabaseId::DEFAULT;

    let lsn = wal
        .append_crdt_delta(t, v, db, b"loro-delta-bytes")
        .unwrap();
    assert_eq!(lsn, Lsn::new(1));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].header.record_type, RecordType::CrdtDelta as u32);
    assert_eq!(records[0].header.tenant_id, 5);
    assert_eq!(records[0].header.vshard_id, 42);
    assert_eq!(records[0].payload, b"loro-delta-bytes");
}

#[test]
fn next_lsn_continues_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    {
        let wal = WalManager::open_for_testing(&path).unwrap();
        wal.append_put(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            b"a",
        )
        .unwrap();
        wal.append_put(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            b"b",
        )
        .unwrap();
        wal.sync().unwrap();
    }

    let wal = WalManager::open_for_testing(&path).unwrap();
    assert_eq!(wal.next_lsn(), Lsn::new(3));

    let lsn = wal
        .append_put(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            b"c",
        )
        .unwrap();
    assert_eq!(lsn, Lsn::new(3));
}

#[test]
fn truncate_reclaims_space() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let t = TenantId::new(1);
    let v = VShardId::new(0);
    let db = DatabaseId::DEFAULT;

    for i in 0..10u32 {
        wal.append_put(t, v, db, format!("val-{i}").as_bytes())
            .unwrap();
    }
    wal.sync().unwrap();

    let result = wal.truncate_before(Lsn::new(5)).unwrap();
    assert_eq!(result.segments_deleted, 0);

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 10);
}

#[test]
fn total_size_and_list_segments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_dir");

    let wal = WalManager::open_for_testing(&path).unwrap();
    wal.append_put(
        TenantId::new(1),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        b"data",
    )
    .unwrap();
    wal.sync().unwrap();

    let size = wal.total_size_bytes().unwrap();
    assert!(size > 0);

    let segments = wal.list_segments().unwrap();
    assert_eq!(segments.len(), 1);
}

#[test]
fn fts_index_append_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal_fts");

    let wal = WalManager::open_for_testing(&path).unwrap();

    let payload = FtsIndexPayload::new(
        nodedb_types::sync::wire::SyncProvenance {
            producer_id: 0xDEAD_BEEF_CAFE_1234,
            epoch: 5,
            stream_id: 99,
            seq: 42,
        },
        "articles",
        "doc-abc",
        "hello world nodedb fts",
    );
    let bytes = payload.to_bytes().unwrap();

    let t = TenantId::new(2);
    let v = VShardId::new(7);
    let db = DatabaseId::DEFAULT;

    let lsn = wal.append_fts_index(t, v, db, &bytes).unwrap();
    assert_eq!(lsn, Lsn::new(1));

    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].header.record_type, RecordType::FtsIndex as u32);
    assert_eq!(records[0].header.tenant_id, 2);
    assert_eq!(records[0].header.vshard_id, 7);

    let decoded = FtsIndexPayload::from_bytes(&records[0].payload).unwrap();
    assert_eq!(decoded.provenance.producer_id, 0xDEAD_BEEF_CAFE_1234);
    assert_eq!(decoded.provenance.epoch, 5);
    assert_eq!(decoded.provenance.stream_id, 99);
    assert_eq!(decoded.provenance.seq, 42);
    assert_eq!(decoded.collection, "articles");
    assert_eq!(decoded.doc_id, "doc-abc");
    assert_eq!(decoded.text, "hello world nodedb fts");
}
