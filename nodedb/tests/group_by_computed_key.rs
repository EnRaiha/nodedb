// SPDX-License-Identifier: BUSL-1.1

//! Regression guard: a computed GROUP BY key (e.g. `GROUP BY UPPER(label)`)
//! must group by the computed value AND emit that value as an output column,
//! not silently degenerate to a single global aggregate with the key column
//! dropped.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn setup() -> TestServer {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION grp \
         COLUMNS (id TEXT PRIMARY KEY, label TEXT, score INTEGER) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE grp");
    // 'alpha' and 'ALPHA' collapse to one group under UPPER(); 'beta' is a second.
    srv.exec("INSERT INTO grp (id,label,score) VALUES ('1','alpha',7)")
        .await
        .expect("i1");
    srv.exec("INSERT INTO grp (id,label,score) VALUES ('2','ALPHA',3)")
        .await
        .expect("i2");
    srv.exec("INSERT INTO grp (id,label,score) VALUES ('3','beta',5)")
        .await
        .expect("i3");
    srv
}

#[tokio::test]
async fn group_by_computed_key_serializes_value() {
    let srv = setup().await;
    let rows = srv
        .client
        .simple_query("SELECT UPPER(label) AS u, SUM(score) FROM grp GROUP BY UPPER(label)")
        .await
        .expect("computed GROUP BY key must plan and execute");

    let mut got = std::collections::HashMap::new();
    for msg in &rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
            assert_eq!(
                names.len(),
                2,
                "expected [u, sum(score)] columns, got {names:?}"
            );
            let key = row
                .get(0)
                .unwrap_or_else(|| panic!("computed GROUP BY key cell EMPTY; cols={names:?}"));
            let agg = row
                .get(1)
                .unwrap_or_else(|| panic!("SUM cell EMPTY; cols={names:?}"));
            let agg: f64 = agg.parse().unwrap_or_else(|e| panic!("SUM `{agg}`: {e}"));
            got.insert(key.to_string(), agg);
        }
    }
    // UPPER() collapses alpha+ALPHA -> "ALPHA" (7+3=10); beta -> "BETA" (5).
    assert_eq!(
        got.get("ALPHA"),
        Some(&10.0),
        "ALPHA group sum; got {got:?}"
    );
    assert_eq!(got.get("BETA"), Some(&5.0), "BETA group sum; got {got:?}");
    assert_eq!(got.len(), 2, "exactly two groups; got {got:?}");
}

/// The same computed key referenced through its SELECT-list alias
/// (`GROUP BY u`) rather than repeated inline (`GROUP BY UPPER(label)`).
/// Postgres resolves an output-column alias in GROUP BY; failing to resolve it
/// must not silently degenerate into a single global aggregate, which reports
/// one row of totals for what the client asked to be per-group.
#[tokio::test]
async fn group_by_select_list_alias_groups_by_computed_value() {
    let srv = setup().await;
    let rows = srv
        .client
        .simple_query("SELECT UPPER(label) AS u, SUM(score) FROM grp GROUP BY u")
        .await
        .expect("GROUP BY over a SELECT-list alias must plan and execute");

    let mut got = std::collections::HashMap::new();
    for msg in &rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
            let key = row
                .get(0)
                .unwrap_or_else(|| panic!("alias GROUP BY key cell EMPTY; cols={names:?}"));
            let agg = row
                .get(1)
                .unwrap_or_else(|| panic!("SUM cell EMPTY; cols={names:?}"));
            let agg: f64 = agg.parse().unwrap_or_else(|e| panic!("SUM `{agg}`: {e}"));
            got.insert(key.to_string(), agg);
        }
    }

    assert_eq!(
        got.len(),
        2,
        "alias GROUP BY must produce the same two groups as the inline form; \
         got {got:?} (a single row means every row collapsed into one global \
         aggregate)"
    );
    assert_eq!(
        got.get("ALPHA"),
        Some(&10.0),
        "ALPHA group sum; got {got:?}"
    );
    assert_eq!(got.get("BETA"), Some(&5.0), "BETA group sum; got {got:?}");
}
