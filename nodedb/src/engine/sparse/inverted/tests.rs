// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the inverted index covering indexing, search,
//! removal, fuzzy lookup, and structural tenant purge.

use std::sync::Arc;

use redb::Database;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;

const DB: u64 = 0;
const T: TenantId = TenantId::new(1);

fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test-inverted.redb");
    let db = Arc::new(Database::create(&path).unwrap());
    let idx = InvertedIndex::open(db).unwrap();
    (idx, dir)
}

#[test]
fn index_and_search() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "The quick brown fox jumps over the lazy dog",
    )
    .unwrap();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(2),
        "A fast brown dog runs across the field",
    )
    .unwrap();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(3),
        "Rust programming language for systems",
    )
    .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "brown fox",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, Surrogate::new(1));
}

#[test]
fn search_with_stemming() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "running distributed databases",
    )
    .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "the cat sat on a mat")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "database distribution",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, Surrogate::new(1));
}

#[test]
fn fuzzy_search() {
    let (idx, _dir) = open_temp();
    idx.index_document(
        DB,
        T,
        "docs",
        Surrogate::new(1),
        "distributed database systems",
    )
    .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "databse",
                top_k: 10,
                fuzzy_enabled: true,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(!results.is_empty());
    assert!(results[0].fuzzy);
}

#[test]
fn remove_document() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "hello world")
        .unwrap();
    idx.index_document(DB, T, "docs", Surrogate::new(2), "hello rust")
        .unwrap();

    idx.remove_document(DB, T, "docs", Surrogate::new(1))
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "hello",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, Surrogate::new(2));
}

#[test]
fn empty_query() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "docs", Surrogate::new(1), "some text here")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "docs",
            FtsSearchParams {
                query: "the a is",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn collections_isolated() {
    let (idx, _dir) = open_temp();
    idx.index_document(DB, T, "col_a", Surrogate::new(1), "alpha bravo charlie")
        .unwrap();
    idx.index_document(DB, T, "col_b", Surrogate::new(1), "delta echo foxtrot")
        .unwrap();

    let results = idx
        .search(
            DB,
            T,
            "col_a",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert_eq!(results.len(), 1);

    let results = idx
        .search(
            DB,
            T,
            "col_b",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn purge_tenant_structurally_drops_data() {
    let (idx, _dir) = open_temp();
    let t1 = TenantId::new(1);
    let t2 = TenantId::new(2);
    idx.index_document(DB, t1, "docs", Surrogate::new(1), "alpha bravo")
        .unwrap();
    idx.index_document(DB, t2, "docs", Surrogate::new(1), "alpha bravo")
        .unwrap();

    idx.purge_tenant(DB, t1).unwrap();

    assert!(
        idx.search(
            DB,
            t1,
            "docs",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None
            }
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        !idx.search(
            DB,
            t2,
            "docs",
            FtsSearchParams {
                query: "alpha",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None
            }
        )
        .unwrap()
        .is_empty()
    );
}
