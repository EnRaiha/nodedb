// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM STDIN` loads a scene through the array bulk-load core.
//!
//! The scene is 256 x 256 cells of three bands, `b_k = (y*256+x)*k + 0.5`.
//! Its CSV is several 4 MiB chunks, so every case past the first chunk runs
//! the chunker. ARRAY_SLICE rows are read by column name: `y, x, b1, b2, b3`.
//!
//! The wide scene's CSV passes the server's whole array memory budget, so
//! its cases prove the load streams. Outside a block a bad row stops the
//! load and the chunks before it stay committed. Inside a block it rolls
//! back whole.

use std::collections::HashMap;

use bytes::Bytes;
use futures::SinkExt;

use crate::harness::TestServer;

const SIDE: u64 = 256;

const CREATE_SCENE: &str = "CREATE ARRAY {name} \
     DIMS (y INT64 [0..255], x INT64 [0..255]) \
     ATTRS (b1 FLOAT64, b2 FLOAT64, b3 FLOAT64) \
     TILE_EXTENTS (64, 64)";

async fn create_scene(server: &TestServer, name: &str) {
    server
        .exec(&CREATE_SCENE.replace("{name}", name))
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
}

fn band(y: u64, x: u64, k: u64) -> f64 {
    ((y * SIDE + x) * k) as f64 + 0.5
}

/// The CSV of `rows` scene cells in row-major order, one line per cell.
fn scene_csv(rows: u64) -> String {
    let mut csv = String::with_capacity(rows as usize * 40);
    for i in 0..rows {
        let (y, x) = (i / SIDE, i % SIDE);
        csv.push_str(&format!(
            "{y},{x},{},{},{}\n",
            band(y, x, 1),
            band(y, x, 2),
            band(y, x, 3)
        ));
    }
    csv
}

/// A refused COPY: its SQLSTATE and message.
#[derive(Debug)]
struct Refusal {
    code: String,
    message: String,
}

fn refusal(e: tokio_postgres::Error) -> Refusal {
    match e.as_db_error() {
        Some(db) => Refusal {
            code: db.code().code().to_string(),
            message: db.message().to_string(),
        },
        None => Refusal {
            code: String::new(),
            message: e.to_string(),
        },
    }
}

/// Stream `data` into `sql` (a `COPY ... FROM STDIN`) and return the row
/// count of its `COPY <n>` tag.
async fn copy_in(server: &TestServer, sql: &str, data: String) -> Result<u64, Refusal> {
    let sink = server
        .client
        .copy_in::<_, Bytes>(sql)
        .await
        .map_err(refusal)?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(data))
        .await
        .map_err(refusal)?;
    sink.as_mut().finish().await.map_err(refusal)
}

/// Every cell of `name`, keyed by `(y, x)`, with its three bands.
async fn read_scene(server: &TestServer, name: &str) -> HashMap<(u64, u64), [f64; 3]> {
    let rows = server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{name}', '{{\"y\":[0,255],\"x\":[0,255]}}', '*', 100000)"
        ))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_SLICE {name}: {e}"));
    let num = |row: &HashMap<String, String>, column: &str| -> f64 {
        let text = row
            .get(column)
            .unwrap_or_else(|| panic!("column {column} in {row:?}"));
        text.parse()
            .unwrap_or_else(|_| panic!("{column} = '{text}' is not a number"))
    };
    rows.iter()
        .map(|row| {
            (
                (num(row, "y") as u64, num(row, "x") as u64),
                [num(row, "b1"), num(row, "b2"), num(row, "b3")],
            )
        })
        .collect()
}

const COPY_CSV: &str = "COPY {name} FROM STDIN WITH (FORMAT csv)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_loads_a_multi_chunk_scene() {
    let server = TestServer::start().await;
    create_scene(&server, "scene").await;

    let rows = SIDE * SIDE;
    let loaded = copy_in(
        &server,
        &COPY_CSV.replace("{name}", "scene"),
        scene_csv(rows),
    )
    .await
    .expect("COPY scene FROM STDIN");
    assert_eq!(loaded, 65_536, "the tag is COPY 65536");

    let scene = read_scene(&server, "scene").await;
    assert_eq!(scene.len(), rows as usize);
    for y in 0..SIDE {
        for x in 0..SIDE {
            let want = [band(y, x, 1), band(y, x, 2), band(y, x, 3)];
            assert_eq!(scene.get(&(y, x)), Some(&want), "cell ({y}, {x})");
        }
    }
}

/// The `N` of a refusal's `(N rows committed)`.
fn committed_count(err: &Refusal) -> u64 {
    let head = err
        .message
        .strip_suffix(" rows committed)")
        .unwrap_or_else(|| panic!("no committed count in {err:?}"));
    let digits = &head[head.rfind('(').unwrap_or_else(|| panic!("{err:?}")) + 1..];
    digits
        .parse()
        .unwrap_or_else(|_| panic!("committed count '{digits}' in {err:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_row_stops_the_load_and_reports_the_committed_count() {
    let server = TestServer::start().await;
    create_scene(&server, "scene_bad").await;

    let mut lines: Vec<String> = scene_csv(2_000).lines().map(str::to_string).collect();
    lines[999] = "3,231,1.5,not-a-number,2.5".to_string();
    let err = copy_in(
        &server,
        &COPY_CSV.replace("{name}", "scene_bad"),
        lines.join("\n"),
    )
    .await
    .expect_err("a row with an unparsable value is refused");
    assert_eq!(err.code, "22P02", "{err:?}");
    assert!(err.message.contains("row 1000"), "{err:?}");
    let committed = committed_count(&err);
    assert!(committed < 999, "{err:?}");
    assert_eq!(
        read_scene(&server, "scene_bad").await.len() as u64,
        committed,
        "exactly the committed rows are in the array"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_coord_keeps_the_last_value() {
    let server = TestServer::start().await;
    create_scene(&server, "scene_dup").await;

    let data = "0,0,1,1,1\n0,1,2,2,2\n0,0,9,9,9\n".to_string();
    let loaded = copy_in(&server, &COPY_CSV.replace("{name}", "scene_dup"), data)
        .await
        .expect("COPY with a repeated coord");
    assert_eq!(loaded, 3);

    let scene = read_scene(&server, "scene_dup").await;
    assert_eq!(scene.len(), 2);
    assert_eq!(
        scene.get(&(0, 0)),
        Some(&[9.0, 9.0, 9.0]),
        "last write wins"
    );
    assert_eq!(scene.get(&(0, 1)), Some(&[2.0, 2.0, 2.0]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_copy_leaves_no_cell() {
    let server = TestServer::start().await;
    create_scene(&server, "scene_txn").await;

    server.exec("BEGIN").await.expect("BEGIN");
    let loaded = copy_in(
        &server,
        &COPY_CSV.replace("{name}", "scene_txn"),
        scene_csv(5_000),
    )
    .await
    .expect("COPY inside a transaction block");
    assert_eq!(loaded, 5_000);
    server.exec("ROLLBACK").await.expect("ROLLBACK");

    assert!(
        read_scene(&server, "scene_txn").await.is_empty(),
        "ROLLBACK discards every staged chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loaded_scene_survives_restart() {
    let server = TestServer::start().await;
    create_scene(&server, "scene_restart").await;
    let rows = 4 * SIDE;
    copy_in(
        &server,
        &COPY_CSV.replace("{name}", "scene_restart"),
        scene_csv(rows),
    )
    .await
    .expect("COPY before restart");

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let scene = read_scene(&server, "scene_restart").await;
    assert_eq!(
        scene.len(),
        rows as usize,
        "every cell survives the restart"
    );
    for i in 0..rows {
        let (y, x) = (i / SIDE, i % SIDE);
        let want = [band(y, x, 1), band(y, x, 2), band(y, x, 3)];
        assert_eq!(scene.get(&(y, x)), Some(&want), "cell ({y}, {x})");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_header_that_does_not_name_the_schema_is_refused_with_42703() {
    let server = TestServer::start().await;
    create_scene(&server, "scene_header").await;

    let data = format!("y,x,b1,b3,b2\n{}", scene_csv(4));
    let err = copy_in(
        &server,
        "COPY scene_header FROM STDIN WITH (FORMAT csv, HEADER)",
        data,
    )
    .await
    .expect_err("a header out of schema order is refused");
    assert_eq!(err.code, "42703", "{err:?}");

    let ok = copy_in(
        &server,
        "COPY scene_header FROM STDIN WITH (FORMAT csv, HEADER)",
        format!("y,x,b1,b2,b3\n{}", scene_csv(4)),
    )
    .await
    .expect("a header in schema order loads");
    assert_eq!(ok, 4);
}

/// The wide scene: 512 x 512 cells of six bands, written with nine
/// decimals, so its CSV passes the test server's whole array memory budget.
const WIDE_SIDE: u64 = 512;

const CREATE_WIDE: &str = "CREATE ARRAY {name} \
     DIMS (y INT64 [0..511], x INT64 [0..511]) \
     ATTRS (b1 FLOAT64, b2 FLOAT64, b3 FLOAT64, b4 FLOAT64, b5 FLOAT64, b6 FLOAT64) \
     TILE_EXTENTS (64, 64)";

/// The test server's whole array memory budget: 2% of its 1 GiB limit.
const ARRAY_BUDGET_BYTES: usize = (1 << 30) / 50;

fn wide_band(i: u64, k: u64) -> String {
    format!("{:.9}", (i * k) as f64 + 0.123_456_789)
}

/// The CSV of the first `rows` wide-scene cells in row-major order.
fn wide_csv(rows: u64) -> String {
    let mut csv = String::with_capacity(rows as usize * 110);
    for i in 0..rows {
        csv.push_str(&format!("{},{}", i / WIDE_SIDE, i % WIDE_SIDE));
        for k in 1..=6 {
            csv.push(',');
            csv.push_str(&wide_band(i, k));
        }
        csv.push('\n');
    }
    csv
}

/// Stream `data` into `sql` in messages of `piece` bytes, which split
/// records, and return the row count of its `COPY <n>` tag.
async fn copy_in_pieces(
    server: &TestServer,
    sql: &str,
    data: &str,
    piece: usize,
) -> Result<u64, Refusal> {
    let sink = server
        .client
        .copy_in::<_, Bytes>(sql)
        .await
        .map_err(refusal)?;
    let mut sink = Box::pin(sink);
    for part in data.as_bytes().chunks(piece) {
        sink.as_mut()
            .send(Bytes::copy_from_slice(part))
            .await
            .map_err(refusal)?;
    }
    sink.as_mut().finish().await.map_err(refusal)
}

/// How many cells `name` holds.
async fn cell_count(server: &TestServer, name: &str) -> u64 {
    let rows = server
        .query_named_rows(&format!("SELECT * FROM ARRAY_AGG('{name}', 'b1', 'count')"))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_AGG {name}: {e}"));
    let Some(text) = rows.first().and_then(|row| row.get("result")) else {
        return 0;
    };
    text.parse::<f64>()
        .unwrap_or_else(|_| panic!("count '{text}' is not a number")) as u64
}

/// The wide-scene rows of `name` in `y` from `y0` to the last row.
async fn wide_rows_from(server: &TestServer, name: &str, y0: u64) -> Vec<HashMap<String, String>> {
    server
        .query_named_rows(&format!(
            "SELECT * FROM ARRAY_SLICE('{name}', '{{\"y\":[{y0},511],\"x\":[0,511]}}', '*', 100000)"
        ))
        .await
        .unwrap_or_else(|e| panic!("ARRAY_SLICE {name}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_past_the_memory_budget_streams_outside_a_block() {
    let server = TestServer::start().await;
    server
        .exec(&CREATE_WIDE.replace("{name}", "wide"))
        .await
        .expect("CREATE ARRAY wide");

    let rows = WIDE_SIDE * WIDE_SIDE;
    let csv = wide_csv(rows);
    assert!(
        csv.len() > ARRAY_BUDGET_BYTES,
        "the input ({} bytes) passes the whole array budget",
        csv.len()
    );
    let loaded = copy_in_pieces(
        &server,
        "COPY wide FROM STDIN WITH (FORMAT csv)",
        &csv,
        1_000_003,
    )
    .await
    .expect("COPY wide FROM STDIN outside a block");
    assert_eq!(loaded, rows);
    assert_eq!(cell_count(&server, "wide").await, rows);

    let tail = wide_rows_from(&server, "wide", 504).await;
    assert_eq!(tail.len() as u64, 8 * WIDE_SIDE);
    for row in &tail {
        let coord = |name: &str| row[name].parse::<f64>().expect("coordinate") as u64;
        let (y, x) = (coord("y"), coord("x"));
        let i = y * WIDE_SIDE + x;
        for k in 1..=6u64 {
            let got: f64 = row[&format!("b{k}")].parse().expect("band");
            let want: f64 = wide_band(i, k).parse().expect("want");
            assert_eq!(got, want, "cell ({y}, {x}) b{k}");
        }
    }
}

/// The first 150,001 wide-scene rows with row 150,000 unparsable. The rows
/// before it span several chunks.
fn wide_csv_bad_at_150000() -> String {
    let mut lines: Vec<String> = wide_csv(150_001).lines().map(str::to_string).collect();
    lines[149_999] = "292,495,1,2,x,4,5,6".to_string();
    lines.join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_row_past_the_first_chunks_keeps_them_committed() {
    let server = TestServer::start().await;
    server
        .exec(&CREATE_WIDE.replace("{name}", "wide_bad"))
        .await
        .expect("CREATE ARRAY wide_bad");

    let err = copy_in_pieces(
        &server,
        "COPY wide_bad FROM STDIN WITH (FORMAT csv)",
        &wide_csv_bad_at_150000(),
        1_000_003,
    )
    .await
    .expect_err("row 150000 is refused");
    assert_eq!(err.code, "22P02", "{err:?}");
    assert!(err.message.contains("row 150000"), "{err:?}");
    let committed = committed_count(&err);
    assert!(
        committed > 0 && committed < 150_000,
        "the chunks before the bad row committed: {err:?}"
    );
    assert_eq!(
        cell_count(&server, "wide_bad").await,
        committed,
        "exactly the committed rows are in the array"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_row_inside_a_block_rolls_back_every_chunk() {
    let server = TestServer::start().await;
    server
        .exec(&CREATE_WIDE.replace("{name}", "wide_txn"))
        .await
        .expect("CREATE ARRAY wide_txn");

    server.exec("BEGIN").await.expect("BEGIN");
    let err = copy_in_pieces(
        &server,
        "COPY wide_txn FROM STDIN WITH (FORMAT csv)",
        &wide_csv_bad_at_150000(),
        1_000_003,
    )
    .await
    .expect_err("row 150000 is refused");
    assert_eq!(err.code, "22P02", "{err:?}");
    assert!(err.message.contains("(0 rows committed)"), "{err:?}");
    server.exec("ROLLBACK").await.expect("ROLLBACK");

    assert!(
        wide_rows_from(&server, "wide_txn", 0).await.is_empty(),
        "the block's staged chunks roll back"
    );
}
