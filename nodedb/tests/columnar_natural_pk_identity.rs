// SPDX-License-Identifier: BUSL-1.1

//! Regression test for cross-engine identity on the columnar insert path.
//!
//! Every row in every engine carries a stable global u32 surrogate derived
//! from its collection's declared PRIMARY KEY; cross-engine prefilter/join is
//! a roaring-bitmap intersection over those surrogates, so each distinct row
//! MUST own a distinct surrogate. The document engine's INSERT path already
//! derived identity from the declared key; the columnar/spatial path used a
//! stale helper that ignored the declared primary key, guessed at legacy
//! `id`/`document_id`/`key` column names, and fell back to `Surrogate::ZERO`
//! when none matched. For a columnar collection whose PRIMARY KEY is a natural
//! key on a differently-named column (e.g. `sku`), every row collapsed onto
//! the same zero surrogate and no PK→surrogate binding was ever persisted.
//!
//! This defect is NOT observable through single-engine SQL — columnar storage
//! keys rows by their PK bytes, so `SELECT`/`COUNT`/point-get return distinct
//! rows regardless of the surrogate. The authoritative observation is the
//! persisted surrogate map: after inserting rows with distinct natural keys,
//! the catalog must hold one distinct, non-zero surrogate binding per row.
//! With the bug, the map is empty (the stale helper never allocated a
//! surrogate); with the fix, it holds one distinct binding per key.

mod common;
use std::collections::HashSet;

use common::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, Surrogate, TenantId};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_natural_pk_rows_get_distinct_surrogates() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION parts (sku TEXT PRIMARY KEY, qty INT) WITH (engine='columnar')")
        .await
        .expect("create columnar collection with natural PK");

    server
        .exec("INSERT INTO parts (sku, qty) VALUES ('sku-a', 10)")
        .await
        .expect("insert sku-a");
    server
        .exec("INSERT INTO parts (sku, qty) VALUES ('sku-b', 20)")
        .await
        .expect("insert sku-b");
    server
        .exec("INSERT INTO parts (sku, qty) VALUES ('sku-c', 30)")
        .await
        .expect("insert sku-c");

    // Sanity: all three rows are stored (this alone passes even with the bug,
    // so it is NOT the guard — the surrogate-map assertion below is).
    let count_rows = server
        .query_rows("SELECT COUNT(*) FROM parts")
        .await
        .expect("count parts");
    assert_eq!(
        count_rows[0][0]
            .parse::<u32>()
            .expect("count is an integer"),
        3,
        "all three distinct natural-key rows must be retained, got: {count_rows:?}"
    );

    // The guard: each distinct natural key must have persisted its own
    // distinct, non-zero cross-engine surrogate. The surrogate assigner writes
    // the binding synchronously during plan conversion, so it is durable by
    // the time the INSERT returns.
    let catalog = server.shared.credentials.catalog();
    let bindings = catalog
        .scan_surrogates_for_collection(DatabaseId::DEFAULT, TenantId::new(1), "parts")
        .expect("scan persisted surrogate bindings for parts");

    assert_eq!(
        bindings.len(),
        3,
        "each of the three natural keys must persist its own surrogate binding \
         (the bug allocated none and collapsed every row onto Surrogate::ZERO), \
         got: {bindings:?}"
    );

    let distinct: HashSet<u32> = bindings.iter().map(|(_, s)| s.as_u32()).collect();
    assert_eq!(
        distinct.len(),
        3,
        "the three rows must hold three distinct surrogates, got: {bindings:?}"
    );
    assert!(
        bindings.iter().all(|(_, s)| *s != Surrogate::ZERO),
        "no row may be assigned the reserved zero surrogate, got: {bindings:?}"
    );
}
