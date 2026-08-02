// SPDX-License-Identifier: Apache-2.0

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::surrogate::Surrogate;
use nodedb_types::surrogate_bitmap::SurrogateBitmap;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::encode_pk;
use crate::wal_record::ColumnarWalRecord;

use super::engine::MutationEngine;

fn test_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("name", ColumnType::String),
        ColumnDef::nullable("score", ColumnType::Float64),
    ])
    .expect("valid")
}

#[test]
fn insert_and_pk_check() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    let result = engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.75),
        ])
        .expect("insert");

    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::InsertRow { .. }
    ));

    assert_eq!(engine.pk_index().len(), 1);
    assert_eq!(engine.memtable().row_count(), 1);
}

#[test]
fn delete_by_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Null,
        ])
        .expect("insert");

    let result = engine.delete(&Value::Integer(1)).expect("delete");
    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::DeleteRows { .. }
    ));

    // PK should be removed from index.
    assert!(engine.pk_index().is_empty());
}

#[test]
fn delete_nonexistent_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    let err = engine.delete(&Value::Integer(999));
    assert!(matches!(err, Err(ColumnarError::PrimaryKeyNotFound)));
}

#[test]
fn update_row() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.5),
        ])
        .expect("insert");

    // Update: change name and score, keep same PK.
    let result = engine
        .update(
            &Value::Integer(1),
            &[
                Value::Integer(1),
                Value::String("Alice Updated".into()),
                Value::Float(0.75),
            ],
        )
        .expect("update");

    // Should produce 2 WAL records: delete + insert.
    assert_eq!(result.wal_records.len(), 2);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::DeleteRows { .. }
    ));
    assert!(matches!(
        &result.wal_records[1],
        ColumnarWalRecord::InsertRow { .. }
    ));

    // PK index should still have 1 entry.
    assert_eq!(engine.pk_index().len(), 1);
    // Memtable should have 2 rows (original + updated).
    assert_eq!(engine.memtable().row_count(), 2);
}

#[test]
fn memtable_flush_remaps_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    for i in 0..5 {
        engine
            .insert(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("insert");
    }

    // Simulate flush: memtable becomes segment 1.
    let result = engine.on_memtable_flushed(1).expect("flush");
    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::MemtableFlushed {
            segment_id: 1,
            row_count: 5,
            ..
        }
    ));

    // PK index entries should now point to segment 1.
    let pk = encode_pk(&Value::Integer(3));
    let loc = engine.pk_index().get(&pk).expect("pk exists");
    assert_eq!(loc.segment_id, 1);
    assert_eq!(loc.row_index, 3);
}

#[test]
fn multiple_inserts_and_deletes() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    for i in 0..10 {
        engine
            .insert(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("insert");
    }

    // Delete odd-numbered rows.
    for i in (1..10).step_by(2) {
        engine.delete(&Value::Integer(i)).expect("delete");
    }

    assert_eq!(engine.pk_index().len(), 5); // 0, 2, 4, 6, 8.
}

#[test]
fn prefilter_row_boundary_membership() {
    // Insert 5 rows with surrogates 10, 20, 30, 40, 50.
    // A prefilter bitmap containing only {20, 40} should yield exactly
    // the two matching rows and skip the other three.
    let mut engine = MutationEngine::new("col".into(), test_schema());

    let surrogates = [10u32, 20, 30, 40, 50];
    for (i, &s) in surrogates.iter().enumerate() {
        engine
            .insert_with_surrogate(
                &[
                    Value::Integer(i as i64),
                    Value::String(format!("r{i}")),
                    Value::Null,
                ],
                Surrogate(s),
            )
            .expect("insert_with_surrogate");
    }

    assert_eq!(engine.memtable_surrogates().len(), 5);

    let bitmap = SurrogateBitmap::from_iter([Surrogate(20), Surrogate(40)]);

    // Simulate block-boundary check: memtable surrogate range is [10, 50];
    // bitmap range is [20, 40] — they intersect, so block is NOT skipped.
    let surrogates_slice = engine.memtable_surrogates();
    let (mt_min, mt_max) = surrogates_slice
        .iter()
        .flatten()
        .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
            (lo.min(s.0), hi.max(s.0))
        });
    assert_eq!(mt_min, 10);
    assert_eq!(mt_max, 50);
    let bm_min = bitmap.0.min().unwrap();
    let bm_max = bitmap.0.max().unwrap();
    // Ranges overlap — block must NOT be skipped.
    assert!(!(bm_max < mt_min || bm_min > mt_max));

    // Row-boundary: count rows that pass the bitmap membership check.
    let passing: Vec<_> = engine
        .scan_memtable_rows_with_surrogates()
        .filter(|(sur, _)| sur.is_some_and(|s| bitmap.contains(s)))
        .collect();
    assert_eq!(passing.len(), 2);
    // The two matched rows should be at surrogate positions 20 and 40 (indices 1 and 3).
    assert!(passing[0].0 == Some(Surrogate(20)));
    assert!(passing[1].0 == Some(Surrogate(40)));
}

#[test]
fn prefilter_block_boundary_skip() {
    // Insert rows with surrogates 100, 200, 300.
    // A prefilter bitmap containing only {10, 20} (below the memtable range)
    // should cause the block-boundary check to flag a skip.
    let mut engine = MutationEngine::new("col".into(), test_schema());

    for (i, s) in [100u32, 200, 300].iter().enumerate() {
        engine
            .insert_with_surrogate(
                &[
                    Value::Integer(i as i64),
                    Value::String(format!("r{i}")),
                    Value::Null,
                ],
                Surrogate(*s),
            )
            .expect("insert_with_surrogate");
    }

    let surrogates_slice = engine.memtable_surrogates();
    let (mt_min, mt_max) = surrogates_slice
        .iter()
        .flatten()
        .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
            (lo.min(s.0), hi.max(s.0))
        });
    assert_eq!(mt_min, 100);
    assert_eq!(mt_max, 300);

    let bitmap = SurrogateBitmap::from_iter([Surrogate(10), Surrogate(20)]);
    let bm_min = bitmap.0.min().unwrap();
    let bm_max = bitmap.0.max().unwrap();

    // bm_max (20) < mt_min (100) → disjoint ranges → block MUST be skipped.
    assert!(bm_max < mt_min || bm_min > mt_max);
}

#[test]
fn insert_without_surrogate_stores_none() {
    let mut engine = MutationEngine::new("col".into(), test_schema());
    engine
        .insert(&[Value::Integer(1), Value::String("x".into()), Value::Null])
        .expect("insert");
    assert_eq!(engine.memtable_surrogates(), &[None]);
}

#[test]
fn flush_clears_surrogate_table() {
    let mut engine = MutationEngine::new("col".into(), test_schema());
    engine
        .insert_with_surrogate(
            &[Value::Integer(1), Value::String("x".into()), Value::Null],
            Surrogate(42),
        )
        .expect("insert");
    assert_eq!(engine.memtable_surrogates().len(), 1);
    engine.on_memtable_flushed(1).expect("flush");
    assert!(engine.memtable_surrogates().is_empty());
}

/// Every WAL record a mutation can produce must be one the replay path can
/// act on. A segment-rewrite ("compaction commit") record has no producer and
/// no replay handler, so the mutation engine must never emit one.
#[test]
fn mutations_only_emit_replayable_wal_records() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    let mut records = Vec::new();
    for i in 0..10 {
        records.extend(
            engine
                .insert(&[
                    Value::Integer(i),
                    Value::String(format!("u{i}")),
                    Value::Null,
                ])
                .expect("insert")
                .wal_records,
        );
    }
    records.extend(engine.on_memtable_flushed(1).expect("flush").wal_records);
    for i in 0..3 {
        records.extend(
            engine
                .delete(&Value::Integer(i))
                .expect("delete")
                .wal_records,
        );
    }

    assert!(!records.is_empty());
    for rec in &records {
        assert!(
            matches!(
                rec,
                ColumnarWalRecord::InsertRow { .. }
                    | ColumnarWalRecord::DeleteRows { .. }
                    | ColumnarWalRecord::MemtableFlushed { .. }
            ),
            "unexpected WAL record shape: {rec:?}"
        );
    }
}

#[test]
fn segment_id_allocator_returns_err_at_u64_max() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    // Force the counter to its maximum value so the next flush overflows.
    engine.next_segment_id = u64::MAX;
    engine.memtable_segment_id = u64::MAX;

    // A flush at this point must return SegmentIdExhausted, not silently wrap.
    let err = engine
        .on_memtable_flushed(u64::MAX)
        .expect_err("should be exhausted");
    assert!(
        matches!(err, ColumnarError::SegmentIdExhausted),
        "expected SegmentIdExhausted, got {err:?}"
    );
}
