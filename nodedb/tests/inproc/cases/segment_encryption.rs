// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test: snapshot core files are encrypted at rest when a KEK is
//! configured, and the same KEK is required to restore.
//!
//! Asserts:
//! - Bytes stored in the object store contain the authenticated SSEG envelope.
//! - Stored bytes do NOT contain the plaintext snapshot payload or footer.
//! - Drop + reopen with the same key successfully recovers the CoreSnapshot.

use std::sync::Arc;

use nodedb::data::snapshot::{CoreSnapshot, NodeSnapshot};
use nodedb::storage::snapshot_writer::{
    chunk_path, create_base_snapshot, load_core_snapshot, load_manifest,
};
use nodedb::types::replay_stamp::ReplayStamp;
use nodedb_wal::crypto::WalEncryptionKey;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};

const KEK: [u8; 32] = [0x42u8; 32];

fn make_core_snapshot(floor: u64) -> Vec<u8> {
    let snap = CoreSnapshot {
        stamp: ReplayStamp::through(floor),
        ..CoreSnapshot::empty()
    };
    snap.to_bytes().expect("serialize CoreSnapshot")
}

#[tokio::test]
async fn segment_encrypted_at_rest_restart_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local store"));

    let watermark: u64 = 42_000;
    let snap_bytes = make_core_snapshot(watermark);
    let plaintext_marker: Vec<u8> = snap_bytes.clone();

    // Write with KEK.
    let key_v1 = WalEncryptionKey::from_bytes(&KEK).expect("build key");
    let (_, prefix) = create_base_snapshot(
        &store,
        vec![(0, snap_bytes.clone())],
        &NodeSnapshot::default(),
        &[],
        "test-node",
        Some(&key_v1),
    )
    .await
    .expect("create snapshot");

    // Read raw bytes from the object store to verify encryption.
    let written = load_manifest(&store, &prefix, &key_v1)
        .await
        .expect("load manifest");
    let snap_key = chunk_path(&written.core_chunks[0][0].id);
    let raw_result = store.get(&snap_key).await.expect("get core snap");
    let raw: Vec<u8> = raw_result.bytes().await.expect("read bytes").to_vec();

    // The sole version-1 envelope starts with SSEG and has a 36-byte
    // authenticated preamble. The footer is inside ciphertext.
    assert_eq!(&raw[..4], b"SSEG");
    assert_eq!(&raw[4..6], &1u16.to_le_bytes());
    assert!(raw.len() >= 36 + 16, "envelope must include AEAD tag");
    assert!(
        !raw.windows(4).any(|window| window == b"SYNS"),
        "segment footer must not be exposed outside authenticated ciphertext"
    );

    // Plaintext payload must NOT appear anywhere in the stored bytes.
    let witness = &plaintext_marker[..16.min(plaintext_marker.len())];
    let found = raw.windows(witness.len()).any(|w| w == witness);
    assert!(
        !found,
        "plaintext snapshot bytes must not appear in the encrypted segment bytes"
    );

    // Simulate restart: a fresh key instance derives the per-envelope data key
    // from the authenticated random salt and decrypts successfully.
    let key_v2 = WalEncryptionKey::from_bytes(&KEK).expect("build key v2");
    let manifest = load_manifest(&store, &prefix, &key_v2)
        .await
        .expect("load manifest after restart");
    let restored = load_core_snapshot(&store, &prefix, &manifest, 0, &key_v2)
        .await
        .expect("load encrypted snapshot after restart");

    assert_eq!(
        restored.replay_floor(),
        watermark,
        "restored replay floor must match original"
    );
}
