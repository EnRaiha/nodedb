// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for stable hash-based OIDs and pg_index population.
//!
//! Covers:
//! - OID stability across drop+recreate of a collection.
//! - OID determinism: two different collection names produce different OIDs.
//! - pg_index returns rows for a collection with secondary indexes, with correct
//!   indrelid (matching pg_class oid) and indisunique flag.
//! - pg_index returns no rows for a collection with no indexes.
//!
//! Cross-tenant OID distinctness is verified by unit tests in
//! `pg_catalog/oid.rs` (`same_name_different_tenants_produce_different_oids`).
//! Those tests exercise `stable_collection_oid` directly with two different
//! tenant_id values. Creating a second tenant via the pgwire harness would
//! require a second connected client authenticated as a different tenant, which
//! the single-user harness does not support; the unit-test coverage is
//! sufficient because it tests the exact hash function used at runtime.

use crate::harness::TestServer;

/// OID must survive two drop+recreate cycles.
///
/// Two cycles guards against a class of bugs where the OID is correct on
/// first recreate but drifts on the second (e.g. a counter-based scheme
/// silently shadowing the hash). With a stable hash, all three reads
/// must produce the same value.
#[tokio::test]
async fn oid_stable_across_drop_and_recreate() {
    let srv = TestServer::start().await;

    let read_oid = async |label: &str| -> String {
        let rows = srv
            .query_text("SELECT oid FROM pg_class WHERE relname = 'oid_stable_test'")
            .await
            .unwrap_or_else(|e| panic!("query pg_class {label}: {e}"));
        assert_eq!(rows.len(), 1, "expected one pg_class row {label}");
        rows.into_iter().next().unwrap()
    };

    srv.exec("CREATE COLLECTION oid_stable_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("create collection");
    let oid_first = read_oid("after first create").await;

    srv.exec("DROP COLLECTION oid_stable_test")
        .await
        .expect("drop 1");
    srv.exec("CREATE COLLECTION oid_stable_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("recreate 1");
    let oid_second = read_oid("after recreate 1").await;

    srv.exec("DROP COLLECTION oid_stable_test")
        .await
        .expect("drop 2");
    srv.exec("CREATE COLLECTION oid_stable_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("recreate 2");
    let oid_third = read_oid("after recreate 2").await;

    assert_eq!(oid_first, oid_second, "OID drifted on first recreate");
    assert_eq!(oid_second, oid_third, "OID drifted on second recreate");
}

/// Two collections with different names must have different OIDs.
#[tokio::test]
async fn oid_distinct_for_different_names() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION oid_distinct_alpha (id INTEGER PRIMARY KEY)")
        .await
        .expect("create alpha");
    srv.exec("CREATE COLLECTION oid_distinct_beta (id INTEGER PRIMARY KEY)")
        .await
        .expect("create beta");

    // pg_catalog returns all rows — filter by relname in test code.
    let all_rows = srv
        .query_rows("SELECT oid, relname FROM pg_class")
        .await
        .expect("query pg_class");

    let alpha_oid = all_rows
        .iter()
        .find(|row| row.len() >= 2 && row[1] == "oid_distinct_alpha")
        .map(|row| row[0].clone())
        .expect("expected a pg_class row for oid_distinct_alpha");

    let beta_oid = all_rows
        .iter()
        .find(|row| row.len() >= 2 && row[1] == "oid_distinct_beta")
        .map(|row| row[0].clone())
        .expect("expected a pg_class row for oid_distinct_beta");

    assert_ne!(
        alpha_oid, beta_oid,
        "different collection names produced the same OID: {}",
        alpha_oid
    );
}

/// pg_index returns a row for a unique secondary index with correct indrelid and indisunique=true.
#[tokio::test]
async fn pg_index_unique_index_visible() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION idx_unique_test (id INTEGER PRIMARY KEY, email TEXT)")
        .await
        .expect("create collection");
    srv.exec("CREATE UNIQUE INDEX idx_unique_email ON idx_unique_test (email)")
        .await
        .expect("create unique index");

    // Fetch the collection OID from pg_class.
    let class_oids = srv
        .query_text("SELECT oid FROM pg_class WHERE relname = 'idx_unique_test'")
        .await
        .expect("query pg_class");
    assert_eq!(class_oids.len(), 1, "expected one pg_class row");
    let class_oid = &class_oids[0];

    // pg_index must have at least one row where indrelid matches the collection OID
    // and indisunique = true.
    let index_rows = srv
        .query_rows("SELECT indexrelid, indrelid, indisunique, indisprimary FROM pg_index")
        .await
        .expect("query pg_index");

    let matching: Vec<_> = index_rows
        .iter()
        .filter(|row| row.len() >= 4 && &row[1] == class_oid)
        .collect();

    assert!(
        !matching.is_empty(),
        "no pg_index row found with indrelid={class_oid}"
    );

    let unique_row = matching
        .iter()
        .find(|row| row[2] == "t")
        .expect("expected at least one row with indisunique=true");

    assert_eq!(
        unique_row[3], "f",
        "indisprimary should be false, got: {}",
        unique_row[3]
    );
}

/// pg_index returns a row for a non-unique secondary index with indisunique=false.
#[tokio::test]
async fn pg_index_non_unique_index_visible() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION idx_nonunique_test (id INTEGER PRIMARY KEY, status TEXT)")
        .await
        .expect("create collection");
    srv.exec("CREATE INDEX idx_status ON idx_nonunique_test (status)")
        .await
        .expect("create non-unique index");

    let class_oids = srv
        .query_text("SELECT oid FROM pg_class WHERE relname = 'idx_nonunique_test'")
        .await
        .expect("query pg_class");
    assert_eq!(class_oids.len(), 1, "expected one pg_class row");
    let class_oid = &class_oids[0];

    let index_rows = srv
        .query_rows("SELECT indexrelid, indrelid, indisunique, indisprimary FROM pg_index")
        .await
        .expect("query pg_index");

    let matching: Vec<_> = index_rows
        .iter()
        .filter(|row| row.len() >= 4 && &row[1] == class_oid)
        .collect();

    assert!(
        !matching.is_empty(),
        "no pg_index row found with indrelid={class_oid}"
    );

    let non_unique_row = matching
        .iter()
        .find(|row| row[2] == "f")
        .expect("expected at least one row with indisunique=false");

    assert_eq!(
        non_unique_row[3], "f",
        "indisprimary should be false, got: {}",
        non_unique_row[3]
    );
}

/// pg_index returns no rows for a collection that has no secondary indexes.
#[tokio::test]
async fn pg_index_empty_for_collection_without_indexes() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION idx_empty_test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("create collection");

    let class_oids = srv
        .query_text("SELECT oid FROM pg_class WHERE relname = 'idx_empty_test'")
        .await
        .expect("query pg_class");
    assert_eq!(class_oids.len(), 1, "expected one pg_class row");
    let class_oid = &class_oids[0];

    let index_rows = srv
        .query_rows("SELECT indexrelid, indrelid, indisunique, indisprimary FROM pg_index")
        .await
        .expect("query pg_index");

    let matching: Vec<_> = index_rows
        .iter()
        .filter(|row| row.len() >= 4 && &row[1] == class_oid)
        .collect();

    assert!(
        matching.is_empty(),
        "expected no pg_index rows for a collection without indexes, found {} rows",
        matching.len()
    );
}

/// indrelid in pg_index must match the oid from pg_class for the same collection.
///
/// This confirms the `indrelid` foreign-key relationship is stable and consistent
/// with pg_class values.
///
/// Note: pg_catalog virtual tables always return ALL rows and all columns —
/// WHERE and column-projection clauses are not applied server-side. The test
/// fetches the full tables and filters in test code.
#[tokio::test]
async fn pg_index_indrelid_matches_pg_class_oid() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION idx_fk_test (id INTEGER PRIMARY KEY, code TEXT)")
        .await
        .expect("create collection");
    srv.exec("CREATE UNIQUE INDEX idx_code ON idx_fk_test (code)")
        .await
        .expect("create index");

    // pg_catalog returns all rows and all columns regardless of projection.
    // pg_class schema: oid, relname, relnamespace, relkind, relowner
    let class_rows = srv
        .query_rows("SELECT oid, relname FROM pg_class")
        .await
        .expect("query pg_class");

    let expected_indrelid = class_rows
        .iter()
        .find(|row| row.len() >= 2 && row[1] == "idx_fk_test")
        .map(|row| row[0].clone())
        .expect("expected a pg_class row for idx_fk_test");

    // pg_index schema: indexrelid, indrelid, indisunique, indisprimary (columns 0..3)
    let index_rows = srv
        .query_rows("SELECT indexrelid, indrelid, indisunique, indisprimary FROM pg_index")
        .await
        .expect("query pg_index");

    // indrelid is column index 1
    let found = index_rows
        .iter()
        .any(|row| row.len() >= 2 && row[1] == expected_indrelid);

    assert!(
        found,
        "pg_index.indrelid={expected_indrelid} not found in pg_index rows: {index_rows:?}"
    );
}
