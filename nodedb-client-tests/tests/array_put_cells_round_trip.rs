// SPDX-License-Identifier: BUSL-1.1

//! `NativeClient::array_put_cells` loads cells through `ArrayPutBatch`
//! requests and the server's array bulk-load core. The pgwire client
//! answers the call with the trait's unsupported error.

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::protocol::ArrayCellWrite;

const SIDE: i64 = 64;

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

async fn create_tile(client: &NativeClient, name: &str) {
    client
        .execute_sql(
            &format!(
                "CREATE ARRAY {name} DIMS (y INT64 [0..63], x INT64 [0..63]) \
                 ATTRS (v FLOAT64) TILE_EXTENTS (16, 16)"
            ),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
}

fn value_of(y: i64, x: i64) -> f64 {
    (y * SIDE + x) as f64 + 0.25
}

fn cells() -> Vec<ArrayCellWrite> {
    (0..SIDE)
        .flat_map(|y| {
            (0..SIDE).map(move |x| ArrayCellWrite {
                coords: vec![Value::Integer(y), Value::Integer(x)],
                attrs: vec![Value::Float(value_of(y, x))],
            })
        })
        .collect()
}

fn number(value: &Value) -> f64 {
    match value {
        Value::Float(f) => *f,
        Value::Integer(i) => *i as f64,
        Value::String(s) => s.parse().unwrap_or_else(|_| panic!("not a number: {s}")),
        other => panic!("not a number: {other:?}"),
    }
}

/// Every cell of `name`, keyed by `(y, x)`, read by column name.
async fn read_cells(client: &NativeClient, name: &str) -> HashMap<(i64, i64), f64> {
    let result = client
        .execute_sql(
            &format!(
                "SELECT * FROM ARRAY_SLICE('{name}', '{{\"y\":[0,63],\"x\":[0,63]}}', '*', 100000)"
            ),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("ARRAY_SLICE {name}: {e}"));
    // An empty result carries no columns.
    if result.rows.is_empty() {
        return HashMap::new();
    }
    let column = |wanted: &str| {
        result
            .columns
            .iter()
            .position(|c| c == wanted)
            .unwrap_or_else(|| panic!("column {wanted} in {:?}", result.columns))
    };
    let (y, x, v) = (column("y"), column("x"), column("v"));
    result
        .rows
        .iter()
        .map(|row| {
            (
                (number(&row[y]) as i64, number(&row[x]) as i64),
                number(&row[v]),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_array_put_cells_loads_every_cell() {
    let server = TestServer::start().await;
    let client = native(&server);
    create_tile(&client, "native_tile").await;

    let loaded = client
        .array_put_cells("native_tile", cells())
        .await
        .expect("array_put_cells");
    assert_eq!(loaded, (SIDE * SIDE) as u64);

    let read = read_cells(&client, "native_tile").await;
    assert_eq!(read.len(), (SIDE * SIDE) as usize);
    for y in 0..SIDE {
        for x in 0..SIDE {
            assert_eq!(read.get(&(y, x)), Some(&value_of(y, x)), "cell ({y}, {x})");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_of_the_wrong_type_stops_the_load_with_the_committed_count() {
    let server = TestServer::start().await;
    let client = native(&server);
    create_tile(&client, "native_bad").await;

    let mut batch = cells();
    batch[100].attrs[0] = Value::String("not a float".into());
    let message = client
        .array_put_cells("native_bad", batch)
        .await
        .expect_err("a string in a FLOAT64 attr is refused")
        .to_string();
    assert!(message.contains("batch row 101"), "{message}");
    let head = message
        .rfind(" rows committed)")
        .map(|end| &message[..end])
        .unwrap_or_else(|| panic!("no committed count in {message}"));
    let committed: usize = head[head.rfind('(').expect("open paren") + 1..]
        .parse()
        .unwrap_or_else(|_| panic!("committed count in {message}"));
    assert!(committed < 100, "{message}");
    assert_eq!(
        read_cells(&client, "native_bad").await.len(),
        committed,
        "exactly the committed cells are in the array"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pgwire_client_answers_unsupported() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");
    let err = remote
        .array_put_cells("anything", cells())
        .await
        .expect_err("the pgwire client has no array_put_cells");
    assert!(err.to_string().contains("array_put_cells"), "{err}");
}
