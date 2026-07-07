// SPDX-License-Identifier: BUSL-1.1

//! Regression test for cross-engine identity on the KV value-computing atomic
//! path (`KV_INCR` / `KV_INCR_FLOAT` / `KV_CAS` / `KV_GETSET`).
//!
//! Every row in every engine carries a stable global u32 surrogate allocated
//! from the WAL-durable, Raft-replicated counter and content-addressed on the
//! row's primary key. Cross-engine prefilter/join is a roaring-bitmap
//! intersection over those surrogates, so a KV row MUST own a real, non-zero
//! surrogate — the same one a normal insert of that key would allocate.
//!
//! The value-computing atomic ops shared the engine write-back helper
//! `atomic_put`, which hard-coded `Surrogate::ZERO` and never called the CP
//! surrogate assigner. A KV row that only ever existed because of an atomic op
//! therefore had NO persisted PK->surrogate binding at all: it was silently
//! excluded from every cross-engine surrogate join, CDC row_id tracking, and
//! surrogate-indexed access.
//!
//! This defect is NOT observable through a plain `SELECT ... WHERE key=X` —
//! KV storage resolves point-gets by raw key bytes, so the row is returned
//! regardless of its surrogate. The authoritative observation is the persisted
//! surrogate map: an autocommit `KV_INCR` on a key that was NEVER inserted must
//! allocate and persist exactly one non-zero PK->surrogate binding (the
//! assigner writes it synchronously during plan conversion). With the bug the
//! atomic path never touched the assigner, so the catalog map is empty; with
//! the fix it holds one distinct, non-zero binding.

mod common;

use common::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, Surrogate, TenantId};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_on_fresh_key_persists_a_real_surrogate() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION c (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("create kv collection");

    // Autocommit atomic op on a key that was NEVER inserted. `KV_INCR`
    // initializes the counter to 0 then adds the delta, materializing the row.
    // The row's cross-engine identity must be allocated on this path.
    server
        .exec("SELECT KV_INCR('c', 'fresh', 1)")
        .await
        .expect("kv_incr on fresh key");

    // The guard: the fresh key must have persisted its own distinct, non-zero
    // cross-engine surrogate. With the bug, the atomic write-back stored
    // `Surrogate::ZERO` and never called the assigner, so NO binding exists and
    // this scan is empty. With the fix, the assigner allocates + persists one
    // binding synchronously during plan conversion.
    let catalog = server.shared.credentials.catalog();
    let bindings = catalog
        .scan_surrogates_for_collection(DatabaseId::DEFAULT, TenantId::new(1), "c")
        .expect("scan persisted surrogate bindings for c");

    assert_eq!(
        bindings.len(),
        1,
        "the atomic op must persist exactly one PK->surrogate binding for the \
         fresh key (the bug allocated none and stored Surrogate::ZERO), \
         got: {bindings:?}"
    );
    assert!(
        bindings.iter().all(|(_, s)| *s != Surrogate::ZERO),
        "the atomic-op row must not be assigned the reserved zero surrogate, \
         got: {bindings:?}"
    );
}
