// SPDX-License-Identifier: BUSL-1.1

//! A `DECIMAL(p,s)` column enforces its declared precision and scale on every
//! engine, as PostgreSQL does. A value rounds to `s` fractional digits, half
//! away from zero. A value whose rounded integer part has more than `p - s`
//! digits is refused with SQLSTATE 22003. A plain `DECIMAL` keeps every digit
//! it is given. A typmod outside precision `1..=1000` and scale
//! `0..=precision` is refused at DDL with SQLSTATE 22023.

use crate::harness::TestServer;

/// One engine under test: the collection name, its key column, and the
/// `CREATE` statement with a `v` column of the given declared type.
struct Engine {
    name: String,
    key: &'static str,
    create: String,
}

/// Every engine that accepts a declared column, each with a `v` column of
/// `declared` type.
fn engines(suffix: &str, declared: &str) -> Vec<Engine> {
    let engine = |kind: &str, key: &'static str, columns: &str, engine_name: &str| {
        let name = format!("dec_{suffix}_{kind}");
        Engine {
            create: format!("CREATE COLLECTION {name} {columns} WITH (engine='{engine_name}')"),
            name,
            key,
        }
    };
    vec![
        engine(
            "strict",
            "id",
            &format!("(id TEXT PRIMARY KEY, v {declared})"),
            "document_strict",
        ),
        engine(
            "columnar",
            "id",
            &format!("COLUMNS (id TEXT, v {declared})"),
            "columnar",
        ),
        engine(
            "schemaless",
            "id",
            &format!("(id STRING PRIMARY KEY, v {declared})"),
            "document_schemaless",
        ),
        engine(
            "kv",
            "key",
            &format!("(key STRING PRIMARY KEY, v {declared})"),
            "kv",
        ),
    ]
}

/// The `v` value stored under key `'a'`.
async fn read_v(srv: &TestServer, engine: &Engine) -> Vec<Vec<String>> {
    let Engine { name, key, .. } = engine;
    srv.query_rows(&format!("SELECT v FROM {name} WHERE {key} = 'a'"))
        .await
        .unwrap()
}

/// A value with more integer digits than `DECIMAL(5,2)` holds is refused as
/// numeric value out of range, on INSERT, UPDATE and UPSERT.
#[tokio::test]
async fn decimal_past_declared_precision_is_22003() {
    let srv = TestServer::start().await;
    for engine in engines("range", "DECIMAL(5,2)") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.expect_error(
            &format!("INSERT INTO {name} ({key}, v) VALUES ('a', 123456.789)"),
            "SQLSTATE 22003",
        )
        .await;
        srv.exec(&format!("INSERT INTO {name} ({key}, v) VALUES ('a', 1.5)"))
            .await
            .unwrap();
        srv.expect_error(
            &format!("UPDATE {name} SET v = 123456.789 WHERE {key} = 'a'"),
            "SQLSTATE 22003",
        )
        .await;
        srv.expect_error(
            &format!("UPSERT INTO {name} ({key}, v) VALUES ('a', 123456.789)"),
            "SQLSTATE 22003",
        )
        .await;
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["1.50".to_string()]],
            "{name}"
        );
    }
}

/// A value with more fractional digits than the declared scale rounds half
/// away from zero, and reads back at the declared scale.
#[tokio::test]
async fn decimal_rounds_to_declared_scale() {
    let srv = TestServer::start().await;
    for engine in engines("round", "DECIMAL(5,2)") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!(
            "INSERT INTO {name} ({key}, v) VALUES ('a', 1.005)"
        ))
        .await
        .unwrap();
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["1.01".to_string()]],
            "{name}"
        );
        srv.exec(&format!("UPDATE {name} SET v = 2.675 WHERE {key} = 'a'"))
            .await
            .unwrap();
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["2.68".to_string()]],
            "{name}"
        );
    }
}

/// A plain `DECIMAL` has no digit limit and keeps every digit it is given.
#[tokio::test]
async fn plain_decimal_keeps_every_digit() {
    let srv = TestServer::start().await;
    let wide = "1234567890123456789.123456789";
    for engine in engines("plain", "DECIMAL") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!(
            "INSERT INTO {name} ({key}, v) VALUES ('a', {wide})"
        ))
        .await
        .unwrap();
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec![wide.to_string()]],
            "{name}"
        );
    }
}

/// The engines that store a row as a field map, so the Data Plane re-types a
/// computed value to the declared column: schemaless document and KV.
fn map_engines(suffix: &str, declared: &str) -> Vec<Engine> {
    engines(suffix, declared)
        .into_iter()
        .filter(|engine| engine.name.ends_with("_schemaless") || engine.name.ends_with("_kv"))
        .collect()
}

/// A computed assignment to `v` for `engine`'s row `'a'`. Schemaless runs it
/// as an `UPDATE`. KV refuses a computed `UPDATE` at plan time, so it runs as
/// the conflict branch of an `INSERT ... ON CONFLICT DO UPDATE`.
fn computed_set(engine: &Engine, expr: &str) -> String {
    let Engine { name, key, .. } = engine;
    if *key == "key" {
        format!(
            "INSERT INTO {name} ({key}, v) VALUES ('a', 0) \
             ON CONFLICT ({key}) DO UPDATE SET v = {expr}"
        )
    } else {
        format!("UPDATE {name} SET v = {expr} WHERE {key} = 'a'")
    }
}

/// A computed value past `DECIMAL(5,2)` is refused with 22003, on `UPDATE`
/// and on the conflict branch of an upsert, and the row keeps its value.
#[tokio::test]
async fn computed_decimal_past_declared_precision_is_22003() {
    let srv = TestServer::start().await;
    for engine in map_engines("computed_range", "DECIMAL(5,2)") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} ({key}, v) VALUES ('a', 1.5)"))
            .await
            .unwrap();
        srv.expect_error(&computed_set(&engine, "v * 1000"), "SQLSTATE 22003")
            .await;
        srv.expect_error(
            &format!(
                "INSERT INTO {name} ({key}, v) VALUES ('a', 0) \
                 ON CONFLICT ({key}) DO UPDATE SET v = v * 1000"
            ),
            "SQLSTATE 22003",
        )
        .await;
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["1.50".to_string()]],
            "{name}"
        );
    }
}

/// A computed value with more fractional digits than the declared scale
/// rounds to it.
#[tokio::test]
async fn computed_decimal_rounds_to_declared_scale() {
    let srv = TestServer::start().await;
    for engine in map_engines("computed_round", "DECIMAL(5,2)") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} ({key}, v) VALUES ('a', 1)"))
            .await
            .unwrap();
        srv.exec(&computed_set(&engine, "v / 3")).await.unwrap();
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["0.33".to_string()]],
            "{name}"
        );
    }
}

/// `INSERT ... SELECT` copies a value with more fractional digits than the
/// target's declared scale, and the target stores it rounded. KV refuses an
/// `INSERT ... SELECT` target, so this runs on schemaless.
#[tokio::test]
async fn insert_select_rounds_to_declared_scale() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dec_copy_src (id STRING PRIMARY KEY, v DECIMAL) \
         WITH (engine='document_schemaless')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dec_copy_src (id, v) VALUES ('a', 1.005)")
        .await
        .unwrap();
    for engine in map_engines("copy", "DECIMAL(5,2)")
        .into_iter()
        .filter(|engine| engine.key == "id")
    {
        let Engine { name, create, .. } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} SELECT * FROM dec_copy_src"))
            .await
            .unwrap();
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["1.01".to_string()]],
            "{name}"
        );
    }
    srv.exec("INSERT INTO dec_copy_src (id, v) VALUES ('b', 123456.789)")
        .await
        .unwrap();
    srv.exec(
        "CREATE COLLECTION dec_copy_narrow (id STRING PRIMARY KEY, v DECIMAL(5,2)) \
         WITH (engine='document_schemaless')",
    )
    .await
    .unwrap();
    srv.expect_error(
        "INSERT INTO dec_copy_narrow SELECT * FROM dec_copy_src",
        "SQLSTATE 22003",
    )
    .await;
}

/// A computed value past a declared `INT2` is refused with 22003, and the row
/// keeps its value.
#[tokio::test]
async fn computed_value_past_int2_is_22003() {
    let srv = TestServer::start().await;
    for engine in map_engines("computed_int2", "INT2") {
        let Engine { name, key, create } = &engine;
        srv.exec(create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} ({key}, v) VALUES ('a', 1)"))
            .await
            .unwrap();
        srv.expect_error(&computed_set(&engine, "v + 39999"), "SQLSTATE 22003")
            .await;
        assert_eq!(
            read_v(&srv, &engine).await,
            vec![vec!["1".to_string()]],
            "{name}"
        );
    }
}

/// A typmod past PostgreSQL's range, or past the 28 digits the engine stores
/// exactly, is refused at DDL on every engine.
#[tokio::test]
async fn invalid_typmod_is_refused_at_ddl() {
    let srv = TestServer::start().await;
    for (suffix, declared) in [
        ("p1001", "DECIMAL(1001,0)"),
        ("p0", "NUMERIC(0)"),
        ("s6", "DECIMAL(5,6)"),
        ("p29", "DECIMAL(29,2)"),
    ] {
        for engine in engines(suffix, declared) {
            srv.expect_error(&engine.create, "SQLSTATE 22023").await;
        }
    }
}
