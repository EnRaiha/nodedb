// SPDX-License-Identifier: BUSL-1.1

//! Narrow array attr types over pgwire.
//!
//! `UINT8` and `INT16` read as `int2`, `UINT16` and `INT32` as `int4`,
//! `UINT32` as `int8`, and `FLOAT32` as `float4`. A value past the declared
//! width is refused with SQLSTATE 22003 on INSERT and on COPY. Values and
//! NULLs read back the same before and after a flush.

use bytes::Bytes;
use futures::SinkExt;

use crate::harness::TestServer;

const INT2: u32 = 21;
const INT4: u32 = 23;
const INT8: u32 = 20;
const FLOAT4: u32 = 700;

const CREATE_SCENE: &str = "CREATE ARRAY {name} DIMS (y INT64 [0..15]) \
     ATTRS (q UINT8, t INT16, b1 UINT16, n INT32, big UINT32, ndvi FLOAT32, w INT64) \
     TILE_EXTENTS (16)";

async fn create_scene(srv: &TestServer, name: &str) {
    srv.exec(&CREATE_SCENE.replace("{name}", name))
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {name}: {e}"));
}

/// Ten cells, enough for the structural tile codec. Odd rows hold NULL in
/// every attr but `w`.
async fn load_scene(srv: &TestServer, name: &str) {
    for y in 0..10i64 {
        let sql = if y % 2 == 0 {
            format!(
                "INSERT INTO ARRAY {name} COORDS ({y}) VALUES \
                 (255, -32768, 65535, 2147483647, 4294967295, 0.1, {y})"
            )
        } else {
            format!(
                "INSERT INTO ARRAY {name} COORDS ({y}) VALUES \
                 (NULL, NULL, NULL, NULL, NULL, NULL, {y})"
            )
        };
        srv.exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert y={y}: {e}"));
    }
}

/// Describe the slice, check each column's OID, then read every cell typed
/// by that OID.
async fn check_scene(srv: &TestServer, name: &str) {
    let stmt = srv
        .client
        .prepare(&format!(
            "SELECT * FROM ARRAY_SLICE('{name}', '{{y: [0, 15]}}')"
        ))
        .await
        .expect("prepare slice");
    let described: Vec<(String, u32)> = stmt
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_().oid()))
        .collect();
    assert_eq!(
        described,
        [
            ("y".to_string(), INT8),
            ("q".to_string(), INT2),
            ("t".to_string(), INT2),
            ("b1".to_string(), INT4),
            ("n".to_string(), INT4),
            ("big".to_string(), INT8),
            ("ndvi".to_string(), FLOAT4),
            ("w".to_string(), INT8),
        ]
    );
    let rows = srv.client.query(&stmt, &[]).await.expect("execute slice");
    assert_eq!(rows.len(), 10);
    for row in &rows {
        let y: i64 = row.get("y");
        let w: i64 = row.get("w");
        assert_eq!(w, y);
        let q: Option<i16> = row.get("q");
        let t: Option<i16> = row.get("t");
        let b1: Option<i32> = row.get("b1");
        let n: Option<i32> = row.get("n");
        let big: Option<i64> = row.get("big");
        let ndvi: Option<f32> = row.get("ndvi");
        if y % 2 == 0 {
            assert_eq!(q, Some(255));
            assert_eq!(t, Some(-32768));
            assert_eq!(b1, Some(65535));
            assert_eq!(n, Some(i32::MAX));
            assert_eq!(big, Some(4_294_967_295));
            assert_eq!(ndvi, Some(0.1));
        } else {
            assert_eq!(
                (q, t, b1, n, big, ndvi),
                (None, None, None, None, None, None),
                "y={y} holds NULLs"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn narrow_attrs_read_at_their_wire_widths_before_and_after_flush() {
    let srv = TestServer::start().await;
    create_scene(&srv, "narrow").await;
    load_scene(&srv, "narrow").await;
    check_scene(&srv, "narrow").await;
    srv.exec("SELECT ARRAY_FLUSH('narrow')")
        .await
        .expect("flush");
    check_scene(&srv, "narrow").await;
}

/// `0.1` stored as `FLOAT32` reads back as the text `0.1`, not the widened
/// `0.10000000149011612`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float32_point_one_reads_as_point_one() {
    let srv = TestServer::start().await;
    create_scene(&srv, "f32").await;
    load_scene(&srv, "f32").await;
    let rows = srv
        .query_named_rows("SELECT * FROM ARRAY_SLICE('f32', '{y: [0, 0]}')")
        .await
        .expect("slice");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ndvi").map(String::as_str), Some("0.1"));
}

/// `0.1` stored as `FLOAT32` reads back as the double `0.1` over the native
/// protocol, the same value pgwire and JSON carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float32_point_one_reads_as_point_one_over_native() {
    use nodedb_test_support::native_harness::{do_handshake, send_sql};
    use nodedb_types::protocol::HelloFrame;
    use nodedb_types::protocol::opcodes::ResponseStatus;

    let srv = TestServer::start().await;
    create_scene(&srv, "f32n").await;
    load_scene(&srv, "f32n").await;
    let addr = std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), srv.native_port);
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let resp = send_sql(
        &mut stream,
        1,
        "SELECT * FROM ARRAY_SLICE('f32n', '{y: [0, 0]}')",
    )
    .await;
    assert_eq!(resp.status, ResponseStatus::Ok, "{:?}", resp.error);
    let columns = resp.columns.expect("columns");
    let ndvi = columns
        .iter()
        .position(|c| c == "ndvi")
        .expect("ndvi column");
    let rows = resp.rows.expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][ndvi], nodedb_types::Value::Float(0.1));
}

/// Every value one past a declared width is refused with 22003. A float
/// into an integer attr is refused too, as another error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_past_the_declared_width_are_22003() {
    let srv = TestServer::start().await;
    create_scene(&srv, "range").await;
    let row = |q: &str, t: &str, b1: &str, n: &str, big: &str, ndvi: &str| {
        format!("INSERT INTO ARRAY range COORDS (0) VALUES ({q}, {t}, {b1}, {n}, {big}, {ndvi}, 0)")
    };
    let refused = [
        row("256", "0", "0", "0", "0", "0.0"),
        row("-1", "0", "0", "0", "0", "0.0"),
        row("0", "32768", "0", "0", "0", "0.0"),
        row("0", "-32769", "0", "0", "0", "0.0"),
        row("0", "0", "65536", "0", "0", "0.0"),
        row("0", "0", "-1", "0", "0", "0.0"),
        row("0", "0", "0", "2147483648", "0", "0.0"),
        row("0", "0", "0", "0", "4294967296", "0.0"),
        row("0", "0", "0", "0", "-1", "0.0"),
        row("0", "0", "0", "0", "0", F32_PAST_MAX),
    ];
    for sql in &refused {
        let err = srv.exec(sql).await.expect_err(sql);
        assert!(err.contains("SQLSTATE 22003"), "{sql}: {err}");
    }
    let err = srv
        .exec(&row("1.5", "0", "0", "0", "0", "0.0"))
        .await
        .expect_err("float into UINT8");
    assert!(!err.contains("SQLSTATE 22003"), "{err}");
    srv.exec(&row(
        "255",
        "32767",
        "65535",
        "-2147483648",
        "4294967295",
        F32_NEAR_MAX,
    ))
    .await
    .expect("every bound fits");
}

/// 1e39, past `f32::MAX`. The array literal grammar has no exponent form.
const F32_PAST_MAX: &str = "1000000000000000000000000000000000000000.0";
/// 3.4e38, inside `f32::MAX`.
const F32_NEAR_MAX: &str = "340000000000000000000000000000000000000.0";

/// Stream `data` into `COPY <name> FROM STDIN` as CSV and return the row
/// count of its `COPY <n>` tag.
async fn copy_csv(srv: &TestServer, name: &str, data: &str) -> Result<u64, tokio_postgres::Error> {
    let sink = srv
        .client
        .copy_in::<_, Bytes>(&format!("COPY {name} FROM STDIN WITH (FORMAT csv)"))
        .await?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::copy_from_slice(data.as_bytes()))
        .await?;
    sink.as_mut().finish().await
}

/// COPY narrows by declared width and refuses a row past it with 22003.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_refuses_a_value_past_the_width() {
    let srv = TestServer::start().await;
    create_scene(&srv, "copyn").await;
    let loaded = copy_csv(
        &srv,
        "copyn",
        "0,255,-32768,65535,7,4294967295,0.5,1\n1,,,,,,,2\n",
    )
    .await
    .expect("in-range rows load");
    assert_eq!(loaded, 2);
    let err = copy_csv(&srv, "copyn", "2,256,0,0,0,0,0,3\n")
        .await
        .expect_err("256 is past UINT8");
    let code = err.as_db_error().map(|db| db.code().code().to_string());
    assert_eq!(code.as_deref(), Some("22003"), "{err:?}");
}

async fn sum_text(srv: &TestServer, attr: &str) -> String {
    let rows = srv
        .query_named_rows(&format!("SELECT * FROM ARRAY_AGG('sums', '{attr}', 'sum')"))
        .await
        .expect("sum");
    rows.first()
        .and_then(|r| r.get("result").cloned())
        .unwrap_or_default()
}

/// An `INT64` SUM keeps every unit past 2^53, and the SUM of a `UINT16`
/// attr is an exact integer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integer_sums_are_exact() {
    let srv = TestServer::start().await;
    srv.exec("CREATE ARRAY sums DIMS (k INT64 [0..3]) ATTRS (v INT64, u UINT16) TILE_EXTENTS (4)")
        .await
        .expect("create");
    srv.exec("INSERT INTO ARRAY sums COORDS (0) VALUES (9007199254740992, 65535)")
        .await
        .expect("insert 2^53");
    srv.exec("INSERT INTO ARRAY sums COORDS (1) VALUES (1, 65535)")
        .await
        .expect("insert 1");
    assert_eq!(sum_text(&srv, "v").await, "9007199254740993");
    assert_eq!(sum_text(&srv, "u").await, "131070");
}
