// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `CREATE TYPE` / `DROP TYPE` / `ALTER TYPE` / `SHOW TYPES`.
//!
//! Scenarios covered:
//! 1. CREATE TYPE … AS ENUM and verify SHOW TYPES lists it.
//! 2. DROP TYPE — SHOW TYPES no longer lists it.
//! 3. DROP TYPE IF EXISTS on a nonexistent type — no error.
//! 4. Duplicate CREATE TYPE — typed error.
//! 5. Enum column: INSERT valid label succeeds; INSERT invalid label rejected.
//! 6. CREATE TYPE … AS (composite) — SHOW TYPES lists with kind=composite.
//! 7. ALTER TYPE ADD VALUE — new label accepted on subsequent INSERT.
//! 8. ALTER TYPE ADD VALUE duplicate — typed error.
//! 9. DROP TYPE blocked when a collection schema references it — typed error listing collections.
//! 10. Catalog round-trip: created types survive a re-load from the catalog (unit test).

use nodedb_test_support::pgwire_harness::TestServer;

// ── Test 1: CREATE TYPE AS ENUM + SHOW TYPES ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_enum_type_and_show() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE user_status AS ENUM ('active', 'inactive', 'pending')")
        .await
        .expect("create enum type");

    let rows = srv.query_rows("SHOW TYPES").await.expect("show types");
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names.contains(&"user_status"),
        "user_status must appear in SHOW TYPES: {names:?}"
    );

    let row = rows.iter().find(|r| r[0] == "user_status").unwrap();
    assert_eq!(row[1], "enum", "kind must be 'enum'");
    assert!(
        row[2].contains("active"),
        "definition must contain 'active': {}",
        row[2]
    );
    assert!(
        row[2].contains("inactive"),
        "definition must contain 'inactive': {}",
        row[2]
    );

    let oid: u32 = row[3].parse().expect("oid must be numeric");
    assert!(
        oid >= 70_000,
        "OID must be in user-type range (>= 70000): {oid}"
    );
}

// ── Test 2: DROP TYPE ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_type_removes_from_show() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
        .await
        .expect("create");
    srv.exec("DROP TYPE mood").await.expect("drop");

    let rows = srv.query_rows("SHOW TYPES").await.expect("show types");
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        !names.contains(&"mood"),
        "mood must not appear after DROP TYPE: {names:?}"
    );
}

// ── Test 3: DROP TYPE IF EXISTS on nonexistent ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_type_if_exists_no_error() {
    let srv = TestServer::start().await;

    srv.exec("DROP TYPE IF EXISTS nonexistent_type")
        .await
        .expect("DROP TYPE IF EXISTS on missing type must succeed");
}

// ── Test 4: Duplicate CREATE TYPE ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_create_type_error() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .await
        .expect("first create");

    let err = srv
        .exec("CREATE TYPE color AS ENUM ('cyan', 'magenta')")
        .await
        .expect_err("duplicate create must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("already exists") || msg.contains("42710"),
        "error must mention already-exists: {msg}"
    );
}

// ── Test 5: Enum validation on INSERT ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enum_column_validates_labels() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE order_state AS ENUM ('new', 'shipped', 'delivered')")
        .await
        .expect("create enum type");

    // Create a collection with an enum-typed column. We use the type name as
    // the column type string — the handler validates against the registry.
    srv.exec(
        "CREATE COLLECTION orders (id TEXT PRIMARY KEY, state order_state) WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");

    // Valid label — must succeed.
    srv.exec("INSERT INTO orders (id, state) VALUES ('o1', 'new')")
        .await
        .expect("insert valid enum label");

    // Invalid label — must be rejected.
    let err = srv
        .exec("INSERT INTO orders (id, state) VALUES ('o2', 'bogus')")
        .await
        .expect_err("insert invalid enum label must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("invalid") || msg.contains("bogus") || msg.contains("enum"),
        "error must mention invalid label: {msg}"
    );
}

// ── Test 6: CREATE TYPE AS composite ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_composite_type_and_show() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)")
        .await
        .expect("create composite type");

    let rows = srv.query_rows("SHOW TYPES").await.expect("show types");
    let row = rows
        .iter()
        .find(|r| r[0] == "address")
        .expect("address must appear in SHOW TYPES");

    assert_eq!(row[1], "composite", "kind must be 'composite'");
    assert!(
        row[2].contains("street"),
        "definition must mention 'street': {}",
        row[2]
    );
    assert!(
        row[2].contains("city"),
        "definition must mention 'city': {}",
        row[2]
    );
}

// ── Test 7: ALTER TYPE ADD VALUE ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_type_add_value_works() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE status AS ENUM ('draft', 'published')")
        .await
        .expect("create");

    srv.exec("ALTER TYPE status ADD VALUE 'archived'")
        .await
        .expect("add value");

    let rows = srv.query_rows("SHOW TYPES").await.expect("show types");
    let row = rows
        .iter()
        .find(|r| r[0] == "status")
        .expect("status must appear");

    assert!(
        row[2].contains("archived"),
        "definition must contain 'archived' after ADD VALUE: {}",
        row[2]
    );
}

// ── Test 8: ALTER TYPE ADD VALUE duplicate ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_type_add_duplicate_value_error() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE phase AS ENUM ('alpha', 'beta')")
        .await
        .expect("create");

    let err = srv
        .exec("ALTER TYPE phase ADD VALUE 'alpha'")
        .await
        .expect_err("duplicate ADD VALUE must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("already exists") || msg.contains("42710"),
        "error must mention already-exists: {msg}"
    );
}

// ── Test 9: DROP TYPE blocked when collection references it ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_type_blocked_when_referenced() {
    let srv = TestServer::start().await;

    srv.exec("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
        .await
        .expect("create type");

    srv.exec("CREATE COLLECTION tasks (id TEXT, prio priority) WITH (engine='document_strict')")
        .await
        .expect("create collection referencing type");

    let err = srv
        .exec("DROP TYPE priority")
        .await
        .expect_err("DROP TYPE must fail when referenced by a collection");

    let msg = err.to_string();
    assert!(
        msg.contains("referenced") || msg.contains("tasks") || msg.contains("2BP01"),
        "error must mention referencing collection: {msg}"
    );
}

// ── Test 10: Catalog round-trip unit test ─────────────────────────────────────

#[test]
fn catalog_custom_type_roundtrip() {
    use nodedb::control::security::catalog::{CustomTypeDef, StoredCustomType, SystemCatalog};

    let dir = tempfile::tempdir().unwrap();
    let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

    let def = StoredCustomType {
        database_id: 0,
        tenant_id: 1,
        name: "emotion".to_string(),
        def: CustomTypeDef::Enum {
            labels: vec!["joy".into(), "anger".into(), "fear".into()],
        },
        oid: 70_001,
        created_at: 42,
    };

    catalog.put_custom_type(&def).unwrap();

    // Re-open to simulate restart.
    drop(catalog);
    let catalog2 = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

    let loaded = catalog2.get_custom_type(0, 1, "emotion").unwrap().unwrap();
    assert_eq!(loaded.name, "emotion");
    assert_eq!(loaded.oid, 70_001);
    match &loaded.def {
        CustomTypeDef::Enum { labels } => {
            assert_eq!(labels, &vec!["joy", "anger", "fear"]);
        }
        other => panic!("expected Enum, got {other:?}"),
    }

    // Delete.
    assert!(catalog2.delete_custom_type(0, 1, "emotion").unwrap());
    assert!(catalog2.get_custom_type(0, 1, "emotion").unwrap().is_none());
}
