// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::distance::DistanceMetric;
use nodedb_types::vector_dtype::VectorStorageDtype;

fn make_params(dtype: VectorStorageDtype) -> HnswParams {
    HnswParams {
        m: 4,
        m0: 8,
        ef_construction: 32,
        metric: DistanceMetric::L2,
        dtype,
    }
}

#[test]
fn create_empty_index() {
    let idx = HnswIndex::new(3, HnswParams::default());
    assert_eq!(idx.len(), 0);
    assert!(idx.is_empty());
    assert!(idx.entry_point().is_none());
}

#[test]
fn params_default() {
    let p = HnswParams::default();
    assert_eq!(p.m, 16);
    assert_eq!(p.m0, 32);
    assert_eq!(p.ef_construction, 200);
    assert_eq!(p.metric, DistanceMetric::Cosine);
    assert_eq!(p.dtype, VectorStorageDtype::F32);
}

#[test]
fn candidate_ordering() {
    let a = crate::hnsw::graph::types::Candidate { dist: 0.1, id: 1 };
    let b = crate::hnsw::graph::types::Candidate { dist: 0.5, id: 2 };
    assert!(a < b);
}

#[test]
fn f32_default_unchanged() {
    let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
    assert_eq!(idx.dtype(), VectorStorageDtype::F32);
    for i in 0..10u32 {
        idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
    }
    // get_vector works on F32 indexes.
    let v = idx.get_vector(3).unwrap();
    assert_eq!(v[0], 3.0_f32);
    // get_vector_bytes also works.
    assert_eq!(idx.get_vector_bytes(3).unwrap().len(), 12); // 3 dims * 4 bytes
}

#[test]
fn f16_insert_search_smoke() {
    let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F16), 42);
    assert_eq!(idx.dtype(), VectorStorageDtype::F16);
    for i in 0..10u32 {
        idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
    }
    let results = idx.search(&[5.0, 0.0, 0.0], 3, 32);
    assert_eq!(results.len(), 3);
    // Results must be in monotonically non-decreasing distance order.
    for w in results.windows(2) {
        assert!(
            w[0].distance <= w[1].distance,
            "results not sorted: {:?}",
            results
        );
    }
}

#[test]
fn bf16_insert_search_smoke() {
    let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::BF16), 42);
    assert_eq!(idx.dtype(), VectorStorageDtype::BF16);
    for i in 0..10u32 {
        idx.insert(vec![i as f32, 0.0, 0.0]).unwrap();
    }
    let results = idx.search(&[5.0, 0.0, 0.0], 3, 32);
    assert_eq!(results.len(), 3);
    for w in results.windows(2) {
        assert!(
            w[0].distance <= w[1].distance,
            "results not sorted: {:?}",
            results
        );
    }
}

#[test]
fn get_vector_returns_none_on_non_f32_dtype() {
    let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F16), 1);
    idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
    // get_vector_bytes works for F16; get_vector does not (returns None in
    // release, fires debug_assert in dev — so we only assert None in release).
    assert!(idx.get_vector_bytes(0).is_some());
    #[cfg(not(debug_assertions))]
    assert!(idx.get_vector(0).is_none());
}

/// `materialize_vector` is the accessor every vector-copying caller uses, so it
/// must work for narrow dtypes where `get_vector` cannot return a borrow.
#[test]
fn materialize_vector_decodes_every_dtype() {
    for dtype in [
        VectorStorageDtype::F32,
        VectorStorageDtype::F16,
        VectorStorageDtype::BF16,
    ] {
        let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
        idx.insert(vec![1.0, 2.0, 4.0]).unwrap();
        let v = idx
            .materialize_vector(0)
            .unwrap_or_else(|e| panic!("dtype={dtype:?} must materialize, got {e}"));
        assert_eq!(v.len(), 3, "dtype={dtype:?}");
        // F16/BF16 are lossy; these values are exactly representable in both.
        assert_eq!(v, vec![1.0, 2.0, 4.0], "dtype={dtype:?}");
    }
}

/// Rerank must see the vector for a narrow dtype too. Before the `Cow` return
/// this yielded `None` for F16/BF16, which made the FP32 rerank path fail with
/// "fetch_vector returned None" for every candidate in the collection.
#[test]
fn get_vector_or_backing_serves_narrow_dtypes() {
    for dtype in [VectorStorageDtype::F16, VectorStorageDtype::BF16] {
        let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
        idx.insert(vec![1.0, 2.0, 4.0]).unwrap();
        let v = idx
            .get_vector_or_backing(0)
            .unwrap_or_else(|| panic!("dtype={dtype:?} must serve a rerank vector"));
        assert_eq!(&*v, &[1.0, 2.0, 4.0][..], "dtype={dtype:?}");
    }
}

/// An index whose per-node storage is empty and which has no backing attached
/// cannot produce vectors. Every extractor must report that rather than hand back
/// empty vecs — serializing those writes a segment whose header declares vectors
/// the payload does not contain, silently destroying the collection.
#[test]
fn extractors_reject_an_index_with_no_vector_source() {
    let mut idx = HnswIndex::with_seed(3, make_params(VectorStorageDtype::F32), 1);
    idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
    let graph_only = idx.graph_checkpoint_to_bytes().unwrap();
    // Restored WITHOUT `with_backing`: node storage is an empty placeholder.
    let restored = HnswIndex::from_checkpoint(&graph_only)
        .unwrap()
        .expect("graph checkpoint must be recognized");
    assert_eq!(restored.len(), 1, "graph topology must survive");

    assert!(
        matches!(
            restored.materialize_vector(0),
            Err(VectorError::VectorUnavailable { id: 0 })
        ),
        "materialize_vector must report the missing vector source"
    );
    assert!(
        restored.export_vectors().is_err(),
        "export_vectors must not yield empty placeholder vectors"
    );
    assert!(
        restored.extract_vectors_and_surrogates().is_err(),
        "extract_vectors_and_surrogates must not yield empty placeholder vectors"
    );
    assert!(
        restored.checkpoint_to_bytes().is_err(),
        "a full checkpoint cannot be written from an index with no vector data"
    );
}

#[test]
fn get_vector_bytes_works_for_all_dtypes() {
    for (dtype, expected_byte_len) in [
        (VectorStorageDtype::F32, 12usize), // 3 dims * 4 bytes
        (VectorStorageDtype::F16, 6usize),  // 3 dims * 2 bytes
        (VectorStorageDtype::BF16, 6usize), // 3 dims * 2 bytes
    ] {
        let mut idx = HnswIndex::with_seed(3, make_params(dtype), 1);
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let bytes = idx.get_vector_bytes(0).expect("must be Some for valid id");
        assert_eq!(
            bytes.len(),
            expected_byte_len,
            "wrong byte len for dtype={dtype:?}"
        );
    }
}
