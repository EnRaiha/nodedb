// SPDX-License-Identifier: BUSL-1.1

//! Regression guard for the GROUP BY output-shape defects: (1) a plain-column
//! select item's `AS` alias must be honoured as the result column name, (2)
//! result columns must follow SELECT-list order (not group-keys-first), even
//! when an aggregate is the leading select item, and (3) an unaliased
//! aggregate must serialize its computed value, not an empty cell.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn setup() -> TestServer {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION grp \
         COLUMNS (id TEXT PRIMARY KEY, label TEXT, score FLOAT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE grp");
    srv.exec("INSERT INTO grp (id,label,score) VALUES ('r1','alpha',7)")
        .await
        .expect("INSERT r1");
    srv.exec("INSERT INTO grp (id,label,score) VALUES ('r2','beta',3)")
        .await
        .expect("INSERT r2");
    srv
}

/// Symptom (3): `SELECT label, SUM(score) FROM grp GROUP BY label` — the
/// unaliased `SUM(score)` cell must carry the value, not be empty.
#[tokio::test]
async fn unaliased_aggregate_serializes_value() {
    let srv = setup().await;
    let rows = srv
        .client
        .simple_query("SELECT label, SUM(score) FROM grp GROUP BY label")
        .await
        .expect("GROUP BY with unaliased aggregate must plan and execute");

    let mut got = std::collections::HashMap::new();
    for msg in &rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
            let label = row
                .get(0)
                .unwrap_or_else(|| panic!("label cell null; cols={names:?}"));
            let agg = row
                .get(1)
                .unwrap_or_else(|| panic!("unaliased SUM(score) cell EMPTY; cols={names:?}"));
            let agg: f64 = agg
                .parse()
                .unwrap_or_else(|e| panic!("SUM cell `{agg}` not numeric: {e}"));
            got.insert(label.to_string(), agg);
        }
    }
    assert_eq!(got.get("alpha"), Some(&7.0), "alpha SUM(score) must be 7");
    assert_eq!(got.get("beta"), Some(&3.0), "beta SUM(score) must be 3");
}

/// Symptoms (1) + (2): `SELECT SUM(score) AS sum_score, label AS lbl FROM grp
/// GROUP BY label` — result columns must be `[sum_score, lbl]`, in SELECT-list
/// order (aggregate first), with the plain-column alias `lbl` honoured.
#[tokio::test]
async fn group_by_honours_select_list_alias_and_order() {
    let srv = setup().await;
    let rows = srv
        .client
        .simple_query("SELECT SUM(score) AS sum_score, label AS lbl FROM grp GROUP BY label")
        .await
        .expect("GROUP BY with aliased agg + group key must plan and execute");

    let mut seen = 0;
    for msg in &rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
            assert_eq!(
                names,
                vec!["sum_score", "lbl"],
                "columns must be SELECT-list aliases in SELECT-list order (agg first)"
            );
            let sum = row.get("sum_score").expect("sum_score cell non-null");
            let lbl = row.get("lbl").expect("lbl cell non-null");
            let sum: f64 = sum.parse().expect("sum_score numeric");
            match lbl {
                "alpha" => assert_eq!(sum, 7.0, "alpha sum"),
                "beta" => assert_eq!(sum, 3.0, "beta sum"),
                other => panic!("unexpected label {other}"),
            }
            seen += 1;
        }
    }
    assert_eq!(seen, 2, "two groups expected");
}
