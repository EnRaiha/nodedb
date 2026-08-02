// SPDX-License-Identifier: BUSL-1.1

use super::*;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::domain::{Domain, DomainBound};
use tempfile::TempDir;

fn schema() -> Arc<ArraySchema> {
    Arc::new(
        ArraySchemaBuilder::new("a")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap(),
    )
}

#[test]
fn open_creates_directory_and_empty_manifest() {
    let dir = TempDir::new().unwrap();
    let s = ArrayStore::open(dir.path().join("g"), schema(), 0xCAFE, None).unwrap();
    assert_eq!(s.manifest().segments.len(), 0);
    assert_eq!(s.schema_hash(), 0xCAFE);
    assert_eq!(s.allocate_segment_id_peek(), "0000000001.ndas");
}

/// An array whose segments were flushed under at-rest encryption must reopen
/// when the key is supplied. Opening the manifest's segments before the key is
/// available makes every such array permanently unopenable — the cells the
/// segments hold are no longer in the WAL, the checkpoint that followed the
/// flush truncated it.
#[test]
fn reopen_supplies_the_kek_to_encrypted_segments() {
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig, array_dir};
    use crate::engine::array::test_support::{aid, put_one, schema as engine_schema};
    use nodedb_wal::crypto::WalEncryptionKey;

    let dir = TempDir::new().unwrap();
    let kek = WalEncryptionKey::from_bytes(&[0x5A; 32]).unwrap();

    // Write and flush one encrypted (SEGA) segment.
    {
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.set_kek(kek.clone());
        e.open_array(aid(), engine_schema(), 0xBEEF).unwrap();
        put_one(&mut e, 1, 1, 7, 1);
        assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 1);
    }

    // Reopening with the key must succeed and see the segment.
    let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
    cfg.flush_cell_threshold = 1;
    let mut e = ArrayEngine::new(cfg).unwrap();
    e.set_kek(kek);
    e.open_array(aid(), engine_schema(), 0xBEEF).unwrap();
    assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 1);

    // Reopening WITHOUT the key must still be the typed error it always was —
    // a missing key may never be treated as "open it as plaintext".
    // `ArrayStore` is not `Debug`, so match the result rather than `expect_err`.
    match ArrayStore::open(array_dir(dir.path(), &aid()), engine_schema(), 0xBEEF, None) {
        Ok(_) => panic!("encrypted segment without a KEK must not open"),
        Err(err) => assert!(
            matches!(err, ArrayStoreError::Segment(_)),
            "expected a typed segment-open error, got {err:?}"
        ),
    }
}

#[test]
fn parse_seq_round_trips() {
    assert_eq!(parse_segment_seq("0000000042.ndas"), Some(42));
    assert_eq!(parse_segment_seq("garbage"), None);
}

impl ArrayStore {
    // Test-only helper that doesn't bump the counter so we can
    // observe the next id without consuming it.
    fn allocate_segment_id_peek(&self) -> String {
        format!("{:010}.ndas", self.next_segment_seq)
    }
}
