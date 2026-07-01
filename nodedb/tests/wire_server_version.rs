// SPDX-License-Identifier: BUSL-1.1

//! Wire surfaces that announce a server version must source the value from
//! `crate::version::VERSION`, never an embedded literal. Format conventions
//! differ per protocol: pgwire uses `"NodeDB X"`, native uses `"NodeDB/X"`,
//! RESP uses bare `X` after `nodedb_version:`.

mod common;

use common::pgwire_harness::TestServer;
use nodedb::version::VERSION;

#[tokio::test]
async fn pgwire_show_server_version_tracks_workspace_version() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SHOW server_version").await.unwrap();
    assert_eq!(rows, vec![format!("NodeDB {VERSION}")]);
}

#[tokio::test]
async fn pgwire_version_function_returns_postgres_compatible_string() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SELECT version()").await.unwrap();
    assert_eq!(rows.len(), 1, "version() must return one row, got {rows:?}");
    assert!(rows[0].starts_with("PostgreSQL"), "got {rows:?}");
    assert!(
        rows[0].contains("15"),
        "must advertise PG major, got {rows:?}"
    );
}

#[tokio::test]
async fn pgwire_show_server_version_num_returns_pg_compat_number() {
    let srv = TestServer::start().await;
    let rows = srv.query_text("SHOW server_version_num").await.unwrap();
    assert_eq!(rows.len(), 1, "got {rows:?}");
    let n: i64 = rows[0]
        .trim()
        .parse()
        .expect("server_version_num must be numeric");
    assert!(
        n >= 100000,
        "must advertise a PG version number, got {rows:?}"
    );
}

#[tokio::test]
async fn pgwire_current_setting_server_version_num() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT current_setting('server_version_num')")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got {rows:?}");
    assert!(
        rows[0].trim().parse::<i64>().is_ok(),
        "must be numeric, got {rows:?}"
    );
}

#[tokio::test]
async fn pgwire_current_setting_server_version_is_nodedb() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT current_setting('server_version')")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("NodeDB"), "got {rows:?}");
}

#[tokio::test]
async fn pgwire_current_setting_unknown_missing_ok_true_is_null() {
    let srv = TestServer::start().await;
    // missing_ok = true → NULL (empty text over the wire), not an error.
    let rows = srv
        .query_text("SELECT current_setting('nodedb.does_not_exist', true)")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "got {rows:?}");
}

/// No file under `src/control/server/` may embed digits directly inside a
/// `"NodeDB ..."`, `"NodeDB/..."`, or `nodedb_version:...` literal — every
/// wire-surface version must format `crate::version::VERSION` in.
#[test]
fn no_hardcoded_version_literal_in_server_wire_surfaces() {
    use std::path::PathBuf;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("control")
        .join("server");

    let patterns: &[&str] = &[
        r#""NodeDB "#,
        r#""NodeDB/"#,
        r#""nodedb_version:"#,
        r#"nodedb_version:"#,
    ];

    let mut offenders: Vec<String> = Vec::new();
    walk_rs(&root, &mut |path, contents| {
        for (lineno, line) in contents.lines().enumerate() {
            for pat in patterns {
                if let Some(idx) = line.find(pat) {
                    let after = &line[idx + pat.len()..];
                    if after.starts_with(|c: char| c.is_ascii_digit()) {
                        offenders.push(format!(
                            "{}:{}: {}",
                            path.display(),
                            lineno + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
    });

    assert!(
        offenders.is_empty(),
        "wire-surface version literal must source from `crate::version::VERSION`:\n  {}",
        offenders.join("\n  ")
    );
}

fn walk_rs(dir: &std::path::Path, f: &mut impl FnMut(&std::path::Path, &str)) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_rs(&path, f);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let contents = std::fs::read_to_string(&path).unwrap();
            f(&path, &contents);
        }
    }
}
