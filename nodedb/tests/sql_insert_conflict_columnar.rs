// SPDX-License-Identifier: BUSL-1.1

//! INSERT conflict semantics for columnar-family engines.
//!
//! Columnar storage is OLAP-shaped (append-only segments, zonemap pruning),
//! but `PRIMARY KEY` appears in the ANSI SQL surface and must mean the same
//! thing across every engine NodeDB ships. The resolution is to treat PK on
//! a columnar collection as both a sort key (enforced at segment flush) and
//! a logical uniqueness constraint enforced via a sparse PK index plus
//! positional deletes: duplicate INSERTs tombstone the prior row rather
//! than raising 23505, and readers skip tombstoned row-ids.
//!
//! Spatial extends columnar and inherits the same semantics. Timeseries is
//! a different profile (append-only, time-keyed) and is not covered here.

mod common;

use common::pgwire_harness::TestServer;

// ── Plain columnar ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_duplicate_pk_keeps_latest() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics (\
                id TEXT PRIMARY KEY, region TEXT, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX metrics_pk ON metrics (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics (id, region, value) VALUES ('m1', 'us-east', 1.0)")
        .await
        .unwrap();

    // Duplicate PK — must NOT raise 23505 on columnar (OLAP-shaped), but
    // must also NOT produce two visible rows (the silent-duplicate bug).
    server
        .exec("INSERT INTO metrics (id, region, value) VALUES ('m1', 'us-west', 2.0)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, region, value FROM metrics WHERE id = 'm1'")
        .await
        .unwrap();

    // Regression guard: the original bug was two rows visible for one PK.
    assert_eq!(
        rows.len(),
        1,
        "duplicate PK must not produce two visible rows, got: {rows:?}"
    );

    // Latest-write-wins on the tombstoned prior row. row[0]=id, row[1]=region, row[2]=value.
    assert_eq!(
        rows[0][1], "us-west",
        "expected latest write (us-west), got: {:?}",
        rows[0]
    );
    assert!(
        rows[0][2].contains('2'),
        "expected value 2.0, got: {:?}",
        rows[0]
    );
    assert_ne!(
        rows[0][1], "us-east",
        "prior row must be tombstoned, got: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_full_scan_hides_tombstoned_duplicate() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO m (id, v) VALUES ('b', 20)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, v FROM m ORDER BY id")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        3,
        "full scan must return 3 rows after dup, got: {rows:?}"
    );
    // ORDER BY id → a, b, c with latest-wins on b. row[0]=id, row[1]=v.
    assert_eq!(
        rows[0][0], "a",
        "first row after ORDER BY id must be 'a', got: {rows:?}"
    );
    assert_eq!(rows[1][0], "b", "second row id must be 'b', got: {rows:?}");
    assert_eq!(
        rows[1][1], "20",
        "second row must be b=20 (latest wins), got: {rows:?}"
    );
    assert_eq!(
        rows[2][0], "c",
        "third row after ORDER BY id must be 'c', got: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_on_conflict_do_nothing_keeps_original() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    // DO NOTHING must be a silent no-op (no error, no overwrite).
    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 999) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT v FROM m WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains('1') && !rows[0].contains("999"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_on_conflict_do_update_merges_excluded() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT, note TEXT) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v, note) VALUES ('a', 1, 'orig')")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO m (id, v, note) VALUES ('a', 7, 'new') \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT v, note FROM m WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // row[0]=v, row[1]=note
    assert_eq!(
        rows[0][0], "7",
        "expected v=7 (from EXCLUDED), got: {:?}",
        rows[0]
    );
    assert_eq!(
        rows[0][1], "orig",
        "expected note=orig (unchanged), got: {:?}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_upsert_keyword_overwrites_on_pk() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION m (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX m_pk ON m (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO m (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO m (id, v) VALUES ('a', 42)")
        .await
        .unwrap();

    let rows = server.query_text("SELECT v FROM m").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("42") && !rows[0].contains(" 1 "));
}

// ── Explicit ORDER BY sort key (no PK) ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_order_by_sort_key_accepted() {
    let server = TestServer::start().await;

    // ORDER BY declares a sort key without PK semantics: duplicates on the
    // sort column are allowed and all rows remain visible.
    server
        .exec(
            "CREATE COLLECTION events (\
                bucket TEXT, payload TEXT\
             ) WITH (engine='columnar') ORDER BY (bucket)",
        )
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO events (bucket, payload) VALUES \
             ('b', 'first'), ('a', 'second'), ('b', 'third')",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT bucket, payload FROM events ORDER BY bucket, payload")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "ORDER BY without PK must not dedup; got: {rows:?}"
    );
}

// ── Natural-key PRIMARY KEY on a non-`id` column ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_natural_key_pk_on_non_id_column_keeps_distinct_rows() {
    // A PRIMARY KEY declared on a non-`id` column must drive row identity on
    // columnar too. Two rows with DISTINCT natural keys must both stay visible
    // — the built-in synthetic `id` (NULL for every row) must NOT make them
    // collide into a single tombstoned row (silent data loss).
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION metrics (\
                sku TEXT PRIMARY KEY, region TEXT, value FLOAT\
            ) WITH (engine='columnar')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX metrics_pk ON metrics (sku)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO metrics (sku, region, value) VALUES ('a', 'us-east', 1.0)")
        .await
        .unwrap();
    // Distinct natural key: must NOT collide on an empty synthetic `id`.
    server
        .exec("INSERT INTO metrics (sku, region, value) VALUES ('b', 'us-west', 2.0)")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT sku, region FROM metrics ORDER BY sku")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both distinct natural-key rows must stay visible, got: {rows:?}"
    );
    assert_eq!(rows[0][0], "a", "got: {rows:?}");
    assert_eq!(rows[1][0], "b", "got: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_natural_key_pk_on_non_id_column_keeps_distinct_rows() {
    // Spatial inherits columnar identity: a non-`id` PRIMARY KEY must keep
    // distinct natural-key rows distinct rather than colliding on synthetic id.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places (\
                code TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX places_pk ON places (code)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (code, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO places (code, geom, label) VALUES ('p2', ST_Point(1.0, 1.0), 'other')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT code, label FROM places ORDER BY code")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both distinct natural-key rows must stay visible, got: {rows:?}"
    );
    assert_eq!(rows[0][0], "p1", "got: {rows:?}");
    assert_eq!(rows[1][0], "p2", "got: {rows:?}");
}

// ── Spatial inherits columnar PK semantics ──────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_insert_duplicate_pk_keeps_latest() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places (\
                id TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX places_pk ON places (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (id, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (id, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, label FROM places WHERE id = 'p1'")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "spatial duplicate PK must not produce two rows, got: {rows:?}"
    );
    // row[0]=id, row[1]=label
    assert_eq!(
        rows[0][1], "moved",
        "expected latest (moved), got: {:?}",
        rows[0]
    );
    assert_ne!(
        rows[0][1], "origin",
        "prior row must be tombstoned, got: {:?}",
        rows[0]
    );
}
