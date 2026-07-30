// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `DROP INDEX`: an index a user can see must be an
//! index a user can drop, for every index kind `SHOW INDEXES` reports.
//!
//! Both statements resolve the same catalog index registry, so the listing and
//! the drop can never disagree about which indexes exist, whatever engine
//! backs them. A drop of a name that is not registered is an error rather than
//! a success, since a success that dropped nothing is indistinguishable from
//! one that worked. Every assertion re-reads `SHOW INDEXES` after the
//! statement rather than trusting its command tag.

mod common;

use common::pgwire_harness::TestServer;

/// Index names listed by `SHOW INDEXES` for the current tenant.
async fn listed_indexes(server: &TestServer) -> Vec<String> {
    server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed")
}

/// The single listed index whose name contains `fragment`, panicking when the
/// listing does not hold exactly one. Used where the server, not the test,
/// chooses the index name.
async fn listed_index_containing(server: &TestServer, fragment: &str) -> String {
    let listed = listed_indexes(server).await;
    let mut matches = listed.iter().filter(|n| n.contains(fragment));
    let found = matches
        .next()
        .unwrap_or_else(|| panic!("no listed index contains '{fragment}': {listed:?}"))
        .clone();
    assert!(
        matches.next().is_none(),
        "expected exactly one listed index containing '{fragment}': {listed:?}"
    );
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_a_vector_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX vidx_embedding_idx ON vidx (embedding) METRIC cosine DIM 3")
        .await
        .unwrap();
    assert!(
        listed_indexes(&server)
            .await
            .iter()
            .any(|n| n == "vidx_embedding_idx"),
        "the created vector index must be listed before the drop"
    );

    server
        .exec("DROP INDEX vidx_embedding_idx")
        .await
        .expect("DROP INDEX on a vector index must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "vidx_embedding_idx"),
        "a reported-successful DROP INDEX must remove the index; it is still \
         listed, so the statement was a silent no-op: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_a_fulltext_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION fts_docs (id TEXT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE FULLTEXT INDEX idx_fts_docs_body ON fts_docs (body)")
        .await
        .unwrap();
    let name = listed_index_containing(&server, "body").await;

    server
        .exec(&format!("DROP INDEX {name}"))
        .await
        .expect("DROP INDEX on a full-text index must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        !after.contains(&name),
        "the full-text index '{name}' is still listed after a successful \
         DROP INDEX: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_a_spatial_index() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION geo_places (id TEXT PRIMARY KEY, location TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE SPATIAL INDEX geo_places_loc_idx ON geo_places (location) USING RTREE")
        .await
        .unwrap();
    assert!(
        listed_indexes(&server)
            .await
            .iter()
            .any(|n| n == "geo_places_loc_idx"),
        "the created spatial index must be listed before the drop"
    );

    server
        .exec("DROP INDEX geo_places_loc_idx")
        .await
        .expect("DROP INDEX on a spatial index must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "geo_places_loc_idx"),
        "the spatial index is still listed after a successful DROP INDEX: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_a_secondary_index() {
    // The one kind the drop path does write. Positive lock-in so a fix that
    // routes drops through a shared registry cannot regress it.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sec_users (id TEXT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_sec_users_email ON sec_users (email)")
        .await
        .unwrap();

    server
        .exec("DROP INDEX idx_sec_users_email")
        .await
        .expect("DROP INDEX on a secondary index must succeed");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "idx_sec_users_email"),
        "the secondary index is still listed after a successful DROP INDEX: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_if_exists_removes_the_named_index() {
    // `DROP INDEX IF EXISTS <name>` is documented in `docs/query-language.md`.
    // The name is read from a fixed token position, so `IF` is taken as the
    // index name and the index the user named is left in place.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ife_users (id TEXT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_ife_users_email ON ife_users (email)")
        .await
        .unwrap();

    server
        .exec("DROP INDEX IF EXISTS idx_ife_users_email")
        .await
        .expect("documented IF EXISTS form must be accepted");

    let after = listed_indexes(&server).await;
    assert!(
        !after.iter().any(|n| n == "idx_ife_users_email"),
        "DROP INDEX IF EXISTS must drop the index it names: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_if_exists_tolerates_an_unknown_name() {
    let server = TestServer::start().await;
    server
        .exec("DROP INDEX IF EXISTS idx_never_created")
        .await
        .expect("IF EXISTS must make dropping an absent index a no-op");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_errors_on_an_unknown_name() {
    // Without IF EXISTS the statement must fail — reporting success for an
    // index that was never there is the same silent no-op as reporting
    // success for one that is still there afterwards, and it is what makes
    // the documented IF EXISTS form meaningful.
    let server = TestServer::start().await;
    server
        .expect_error("DROP INDEX idx_never_created", "idx_never_created")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_rejects_a_kind_mismatch() {
    // A kind-qualified drop must not tear down an index of another kind: the
    // qualifier is an assertion about what is being dropped, and honouring it
    // silently would delete something the statement did not name.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mismatch_docs (id TEXT PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_mismatch_email ON mismatch_docs (email)")
        .await
        .unwrap();

    server
        .expect_error("DROP VECTOR INDEX idx_mismatch_email", "btree")
        .await;

    assert!(
        listed_indexes(&server)
            .await
            .iter()
            .any(|n| n == "idx_mismatch_email"),
        "a rejected kind-qualified drop must leave the index in place"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_indexes_on_collection_filters_by_collection() {
    // The filter is on the collection each index is attached to. Filtering on
    // the index name having the collection as a prefix both hid
    // correctly-named indexes and showed unrelated ones.
    let server = TestServer::start().await;
    for collection in ["filter_a", "filter_b"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, email TEXT)"
            ))
            .await
            .unwrap();
    }
    server
        .exec("CREATE INDEX unprefixed_a ON filter_a (email)")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX unprefixed_b ON filter_b (email)")
        .await
        .unwrap();

    let listed = server
        .query_text("SHOW INDEXES ON filter_a")
        .await
        .expect("SHOW INDEXES ON <collection> must succeed");
    assert!(
        listed.iter().any(|n| n == "unprefixed_a"),
        "an index of the named collection must be listed whatever it is called: {listed:?}"
    );
    assert!(
        !listed.iter().any(|n| n == "unprefixed_b"),
        "an index of another collection must not be listed: {listed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_vector_index_can_be_recreated_with_new_parameters() {
    // Removing the ledger row is not the whole drop: the engine-side build
    // parameters registered by CREATE must be reclaimed too. If they survive,
    // the name is free but the collection's vector slot is permanently taken
    // and a re-CREATE is refused as a duplicate.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx_recreate (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX vidx_recreate_idx ON vidx_recreate (embedding) \
             METRIC cosine DIM 3",
        )
        .await
        .unwrap();
    server.exec("DROP INDEX vidx_recreate_idx").await.unwrap();

    server
        .exec(
            "CREATE VECTOR INDEX vidx_recreate_idx ON vidx_recreate (embedding) \
             METRIC l2 DIM 8",
        )
        .await
        .expect("after a drop the column must accept a freshly configured vector index");
}
