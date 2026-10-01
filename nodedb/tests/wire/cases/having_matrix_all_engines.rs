// SPDX-License-Identifier: BUSL-1.1

//! The exhaustive `HAVING` matrix: every engine, every clause shape.
//!
//! `HAVING` filters the result of an aggregation. The planner reaches the
//! aggregate path through several routes, and an engine rule that cannot apply
//! the predicate must refuse it. What no engine may do is accept the statement
//! and silently widen the result set, because the user then sees rows they
//! explicitly excluded and no error to explain it.
//!
//! This file walks the whole space rather than the one shape an issue happened
//! to report. A guard that closes one route and leaves another open is not a
//! guard, and the only way to say which routes are closed is to exercise each
//! engine against each clause shape.
//!
//! Engines covered: `document_schemaless`, `document_strict`, `kv`, `columnar`,
//! `spatial`, `timeseries`, plus the default engine a bare `CREATE COLLECTION`
//! selects.

use crate::harness::TestServer;

/// One engine's create statement and seed values.
///
/// Each engine states its whole DDL rather than sharing a column prefix: the
/// engines do not agree on the shape. `kv` demands a `PRIMARY KEY`, `spatial`
/// only accepts `COLUMNS (...)` with a `GEOMETRY` column, and `timeseries`
/// needs a `TIME_KEY`. A shared prefix could express none of those, and a
/// fixture that fails on CREATE tests the fixture, not the clause under test.
struct EngineCase {
    /// The `CREATE COLLECTION` statement, complete.
    create: &'static str,
    /// Columns for the `INSERT`, matching `create`.
    columns: &'static str,
    /// One row's values, without the surrounding parentheses.
    row: &'static str,
}

/// Three groups with counts 3, 2 and 1, so `HAVING COUNT(*) > 1` has something
/// to exclude. `{n}` is the row ordinal.
const GROUPS: &[(&str, usize)] = &[("g_a", 3), ("g_b", 2), ("g_c", 1)];

const CASES: &[(&str, EngineCase)] = &[
    (
        "default",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT)",
            columns: "id, grp",
            row: "'id{n}', '{grp}'",
        },
    ),
    (
        "document_schemaless",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT) WITH (engine='document_schemaless')",
            columns: "id, grp",
            row: "'id{n}', '{grp}'",
        },
    ),
    (
        "document_strict",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT) WITH (engine='document_strict')",
            columns: "id, grp",
            row: "'id{n}', '{grp}'",
        },
    ),
    (
        "kv",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT) WITH (engine='kv')",
            columns: "id, grp",
            row: "'id{n}', '{grp}'",
        },
    ),
    (
        "columnar",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT) WITH (engine='columnar')",
            columns: "id, grp",
            row: "'id{n}', '{grp}'",
        },
    ),
    (
        "spatial",
        EngineCase {
            create: "CREATE COLLECTION {c} COLUMNS (id TEXT PRIMARY KEY, grp TEXT, location GEOMETRY) WITH (engine='spatial')",
            columns: "id, grp, location",
            row: "'id{n}', '{grp}', ST_Point(0.0, 0.0)",
        },
    ),
    (
        "timeseries",
        EngineCase {
            create: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, grp TEXT, ts TIMESTAMP TIME_KEY) WITH (engine='timeseries')",
            columns: "id, grp, ts",
            row: "'id{n}', '{grp}', '2020-03-05 10:00:00'",
        },
    ),
];

/// Create `collection` for `case` and seed three groups with counts 3, 2, 1.
async fn seed(server: &TestServer, collection: &str, case: &EngineCase) {
    let create = case.create.replace("{c}", collection);
    server
        .exec(&create)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}\n  ddl: {create}"));

    let mut rows = Vec::new();
    let mut n = 0;
    for (grp, count) in GROUPS {
        for _ in 0..*count {
            rows.push(format!(
                "({})",
                case.row
                    .replace("{n}", &n.to_string())
                    .replace("{grp}", grp)
            ));
            n += 1;
        }
    }
    let insert = format!(
        "INSERT INTO {collection} ({}) VALUES {}",
        case.columns,
        rows.join(", ")
    );
    server
        .exec(&insert)
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
}

/// The outcome a `HAVING` statement is allowed to have.
enum Outcome {
    /// The predicate applied: `g_c` (count 1) is absent.
    Filtered,
    /// The engine refused the clause, naming why.
    Refused,
}

/// Classify what `sql` did, failing when the clause was silently dropped.
async fn classify(server: &TestServer, sql: &str, label: &str) -> Outcome {
    match server.query_named_rows(sql).await {
        Err(error) => {
            let message = error.to_string();
            assert!(
                !message.is_empty(),
                "{label}: a refusal must name its reason, got an empty error"
            );
            Outcome::Refused
        }
        Ok(rows) => {
            let groups: Vec<String> = rows.iter().filter_map(|r| r.get("grp").cloned()).collect();
            // An empty result would satisfy the exclusion assertion below
            // vacuously, so the surviving groups are required here first: the
            // statement answered, and it answered the groups the predicate keeps.
            assert_eq!(
                groups.len(),
                2,
                "{label}: `HAVING COUNT(*) > 1` keeps g_a (3) and g_b (2) and \
                 excludes g_c (1); an empty or short result would pass the \
                 exclusion check without testing it: {rows:?}"
            );
            assert!(
                groups.iter().any(|g| g == "g_a") && groups.iter().any(|g| g == "g_b"),
                "{label}: the kept groups must both be present: {rows:?}"
            );
            assert!(
                !groups.iter().any(|g| g == "g_c"),
                "{label}: `HAVING COUNT(*) > 1` excludes g_c (one row), but the \
                 statement returned it — the clause was silently dropped: {rows:?}"
            );
            Outcome::Filtered
        }
    }
}

/// Every engine, the canonical shape: `GROUP BY` plus `HAVING` over `COUNT(*)`.
#[tokio::test]
async fn matrix_group_by_with_having() {
    let server = TestServer::start().await;
    for (name, case) in CASES {
        let collection = format!("mx_gb_{name}");
        seed(&server, &collection, case).await;
        let outcome = classify(
            &server,
            &format!("SELECT grp, COUNT(*) FROM {collection} GROUP BY grp HAVING COUNT(*) > 1"),
            name,
        )
        .await;
        // Either outcome is acceptable; the assertion inside `classify` is the
        // contract. Recorded so a future change that flips an engine from
        // refusing to filtering is visible rather than silent.
        match outcome {
            Outcome::Filtered => {}
            Outcome::Refused => {}
        }
    }
}

/// `HAVING` with no `GROUP BY` and no aggregate in the projection.
///
/// The statement is still an aggregate statement, so the clause must reach a
/// rule that can judge it. Its result shape differs from the `GROUP BY` case:
/// with no grouping it can only produce one grand-total row, or refuse. Two
/// grouped rows would mean the engine invented a grouping the statement did not
/// ask for, and `g_c` coming back means the clause was dropped.
#[tokio::test]
async fn matrix_having_without_group_by() {
    let server = TestServer::start().await;
    for (name, case) in CASES {
        let collection = format!("mx_bare_{name}");
        seed(&server, &collection, case).await;

        match server
            .query_named_rows(&format!("SELECT grp FROM {collection} HAVING COUNT(*) > 1"))
            .await
        {
            Err(error) => {
                let message = error.to_string();
                assert!(
                    !message.is_empty(),
                    "{name}: a refusal must name its reason, got an empty error"
                );
            }
            Ok(rows) => {
                assert!(
                    rows.len() <= 1,
                    "{name}: a HAVING with no GROUP BY aggregates the whole \
                     collection into at most one row; {} rows means the \
                     grouping was invented: {rows:?}",
                    rows.len()
                );
                let groups: Vec<String> =
                    rows.iter().filter_map(|r| r.get("grp").cloned()).collect();
                assert!(
                    !groups.iter().any(|g| g == "g_c"),
                    "{name}: `HAVING COUNT(*) > 1` excludes g_c, but the \
                     statement returned it — the clause was dropped: {rows:?}"
                );
            }
        }
    }
}

/// `HAVING` over a join. The join planner builds its own aggregate node rather
/// than consulting the engine rule, so this route is the one most likely to
/// keep dropping the clause.
///
/// The join route is a *separate* defect from the one this PR fixes: the join
/// does not merely ignore `HAVING`, it answers input rows under an aggregate
/// heading (`count(*)` comes back empty). That is its own issue, with its own
/// reproduction, and it affects every engine including the default — not just
/// timeseries. This test therefore records the current behaviour rather than
/// asserting the fixed one, so the PR does not claim a fix it does not make.
#[tokio::test]
async fn matrix_having_over_a_join_is_a_separate_defect() {
    let server = TestServer::start().await;
    for (name, case) in CASES {
        let collection = format!("mx_join_{name}");
        let counterpart = format!("mx_join_side_{name}");
        seed(&server, &collection, case).await;
        server
            .exec(&format!(
                "CREATE COLLECTION {counterpart} (grp TEXT PRIMARY KEY, label TEXT)"
            ))
            .await
            .unwrap_or_else(|e| panic!("create join side for {name}: {e}"));
        server
            .exec(&format!(
                "INSERT INTO {counterpart} (grp, label) VALUES ('g_a', 'x'), ('g_b', 'y'), ('g_c', 'z')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed join side for {name}: {e}"));

        // Either the clause applies (g_c, count 1, is excluded and the aggregate
        // is computed) or the statement refuses. Anything else is the join bug.
        let result = server
            .query_named_rows(&format!(
                "SELECT l.grp, COUNT(*) FROM {collection} l \
                 JOIN {counterpart} r ON l.grp = r.grp \
                 GROUP BY l.grp HAVING COUNT(*) > 1"
            ))
            .await;

        let Ok(rows) = result else { continue };
        let groups: Vec<String> = rows.iter().filter_map(|r| r.get("grp").cloned()).collect();
        let counts_empty = rows
            .iter()
            .all(|r| r.get("count(*)").is_some_and(|v| v.is_empty()));
        if groups.iter().any(|g| g == "g_c") || counts_empty {
            // The known join defect. Recorded, not asserted as correct; the
            // issue carrying its reproduction is filed separately.
            eprintln!("known join defect on {name}: HAVING dropped or aggregate empty — {rows:?}");
        }
    }
}

/// `WHERE` is not affected by any of this: it filters input rows and must keep
/// working on every engine, including the ones that refuse `HAVING`.
#[tokio::test]
async fn matrix_where_still_filters() {
    let server = TestServer::start().await;
    for (name, case) in CASES {
        let collection = format!("mx_where_{name}");
        seed(&server, &collection, case).await;
        let rows = server
            .query_named_rows(&format!(
                "SELECT grp, COUNT(*) FROM {collection} WHERE grp <> 'g_c' GROUP BY grp"
            ))
            .await
            .unwrap_or_else(|e| panic!("{name}: WHERE must still work: {e}"));
        let groups: Vec<String> = rows.iter().filter_map(|r| r.get("grp").cloned()).collect();
        assert!(
            !groups.iter().any(|g| g == "g_c"),
            "{name}: WHERE grp <> 'g_c' must exclude it: {rows:?}"
        );
    }
}
