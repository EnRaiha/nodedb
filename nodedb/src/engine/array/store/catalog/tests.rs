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
    let s = ArrayStore::open(dir.path().join("g"), schema(), 0xCAFE).unwrap();
    assert_eq!(s.manifest().segments.len(), 0);
    assert_eq!(s.schema_hash(), 0xCAFE);
    assert_eq!(s.allocate_segment_id_peek(), "0000000001.ndas");
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
