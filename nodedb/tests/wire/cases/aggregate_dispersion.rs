// SPDX-License-Identifier: BUSL-1.1

//! `stddev` / `variance` with PostgreSQL semantics.
//!
//! `stddev`, `stddev_samp`, `variance` and `var_samp` are sample forms.
//! `stddev_pop` and `var_pop` are population forms. `ts_stddev` is the
//! population standard deviation. A sample form over one value is NULL; a
//! population form over one value is 0.

use crate::harness::TestServer;

/// Values 2, 4, 4, 4, 5, 5, 7, 9: mean 5, squared deviations total 32.
const VALUES: [i64; 8] = [2, 4, 4, 4, 5, 5, 7, 9];

fn close(cell: &str, want: f64) -> bool {
    cell.parse::<f64>()
        .is_ok_and(|got| (got - want).abs() < 1e-9)
}

async fn check(server: &TestServer, c: &str) {
    let sample: f64 = 32.0 / 7.0;
    let rows = server
        .query_rows(&format!(
            "SELECT stddev(v), stddev_samp(v), stddev_pop(v), \
             variance(v), var_samp(v), var_pop(v) FROM {c}"
        ))
        .await
        .unwrap_or_else(|e| panic!("{c}: dispersion: {e}"));
    let row = &rows[0];
    let want = [sample.sqrt(), sample.sqrt(), 2.0, sample, sample, 4.0];
    for (i, w) in want.iter().enumerate() {
        assert!(close(&row[i], *w), "{c}: column {i}: {row:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispersion_over_documents() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION disp_doc TYPE DOCUMENT SCHEMALESS")
        .await
        .unwrap();
    for (i, v) in VALUES.iter().enumerate() {
        server
            .exec(&format!("INSERT INTO disp_doc (id, v) VALUES ('{i}', {v})"))
            .await
            .unwrap();
    }
    check(&server, "disp_doc").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispersion_over_timeseries() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION disp_ts (ts BIGINT TIME_KEY, v BIGINT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    for (i, v) in VALUES.iter().enumerate() {
        let ts = (i as i64 + 1) * 1000;
        server
            .exec(&format!("INSERT INTO disp_ts (ts, v) VALUES ({ts}, {v})"))
            .await
            .unwrap();
    }
    check(&server, "disp_ts").await;
    let rows = server
        .query_rows("SELECT ts_stddev(v) FROM disp_ts")
        .await
        .unwrap();
    assert!(close(&rows[0][0], 2.0), "ts_stddev is population: {rows:?}");
}

/// One value: a sample form is NULL, a population form is 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispersion_over_one_value() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION disp_one TYPE DOCUMENT SCHEMALESS")
        .await
        .unwrap();
    server
        .exec("INSERT INTO disp_one (id, v) VALUES ('1', 3)")
        .await
        .unwrap();
    let messages = server
        .client
        .simple_query("SELECT stddev(v), variance(v), stddev_pop(v), var_pop(v) FROM disp_one")
        .await
        .unwrap();
    let row = messages
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("one row");
    assert_eq!(row.get(0), None, "stddev of one value is NULL");
    assert_eq!(row.get(1), None, "variance of one value is NULL");
    assert!(row.get(2).is_some_and(|c| close(c, 0.0)), "stddev_pop is 0");
    assert!(row.get(3).is_some_and(|c| close(c, 0.0)), "var_pop is 0");
}
