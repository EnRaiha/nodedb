// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for GROUP BY ROLLUP, CUBE, and GROUPING SETS.
//!
//! Each test creates a small `orders` collection, inserts fixture data,
//! then queries with one of the grouping-set constructs and verifies
//! the expected row shapes are present.

use crate::harness::TestServer;

// ── Fixture ──────────────────────────────────────────────────────────────────

async fn create_orders(server: &TestServer) {
    server
        .exec("CREATE COLLECTION orders TYPE DOCUMENT SCHEMALESS")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO orders (id, region, country, sales) VALUES \
             ('1', 'AMER', 'US',  100), \
             ('2', 'AMER', 'US',  200), \
             ('3', 'AMER', 'CA',  150), \
             ('4', 'EMEA', 'DE',  300), \
             ('5', 'EMEA', 'FR',  250)",
        )
        .await
        .unwrap();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `GROUP BY ROLLUP (region, country)` should produce 4 row shapes:
/// - (region, country)  — grouped by both
/// - (region, NULL)     — regional subtotal
/// - (NULL, NULL)       — grand total
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollup_two_cols_produces_correct_shapes() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, country, SUM(sales) AS total \
             FROM orders \
             GROUP BY ROLLUP (region, country)",
        )
        .await
        .unwrap();

    // ROLLUP(region, country) = GROUPING SETS ((region, country), (region), ()).
    // The fixture has 4 distinct (region, country) combinations:
    //   (AMER, US) [100+200=300], (AMER, CA) [150],
    //   (EMEA, DE) [300], (EMEA, FR) [250]
    // Plus 2 regional subtotals (AMER, EMEA) plus 1 grand total = 7 rows.
    assert_eq!(
        rows.len(),
        7,
        "expected exactly 7 ROLLUP rows (4 grouped + 2 regional subtotals + 1 grand total): {:?}",
        rows
    );

    // Grand total row: region = NULL, country = NULL.
    let grand_total_rows: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .collect();
    assert_eq!(
        grand_total_rows.len(),
        1,
        "expected exactly 1 grand-total row (region=NULL, country=NULL): {:?}",
        rows
    );

    // Grand total should be 1000.
    let grand = grand_total_rows[0]
        .get(2)
        .unwrap_or(&String::new())
        .parse::<f64>()
        .unwrap_or(0.0);
    assert!(
        (grand - 1000.0).abs() < 0.01,
        "grand total SUM(sales) should be 1000, got {grand}"
    );

    // Regional subtotal rows: country = NULL, region = non-empty.
    let regional_rows: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| !s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .collect();
    assert_eq!(
        regional_rows.len(),
        2,
        "expected 2 regional subtotal rows (AMER, EMEA): {:?}",
        rows
    );
}

/// `GROUP BY CUBE (region, country)` should produce all 4 subset shapes:
/// (region, country), (region), (country), ()
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cube_two_cols_produces_all_subsets() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, country, SUM(sales) AS total \
             FROM orders \
             GROUP BY CUBE (region, country)",
        )
        .await
        .unwrap();

    // CUBE(region, country) = GROUPING SETS ((region,country), (region), (country), ()).
    // Set 1 (region,country): 4 unique combos
    // Set 2 (region only):    2 rows (AMER, EMEA)
    // Set 3 (country only):   4 rows (US, CA, DE, FR)
    // Set 4 ():               1 grand total
    // Total = 11 rows.
    assert_eq!(
        rows.len(),
        11,
        "expected 11 CUBE rows (4 + 2 + 4 + 1), got {}: {:?}",
        rows.len(),
        rows
    );

    // There must be exactly one grand-total row (both NULL).
    let grand_total = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .count();
    assert_eq!(
        grand_total, 1,
        "CUBE must produce exactly 1 grand-total row"
    );

    // country-only rows: region = NULL, country != NULL.
    let country_only = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| !s.is_empty()).unwrap_or(false)
        })
        .count();
    assert!(
        country_only >= 4,
        "CUBE must produce country-only subtotal rows (at least 4), got {country_only}"
    );
}

/// `GROUP BY GROUPING SETS ((region, country), (region), ())` should produce
/// exactly those three row-shape categories.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouping_sets_explicit_shapes() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, country, SUM(sales) AS total \
             FROM orders \
             GROUP BY GROUPING SETS ((region, country), (region), ())",
        )
        .await
        .unwrap();

    // Set 1: (region, country) — 5 rows (US×2 merged, CA, DE, FR) but US has 2 rows
    // so should be 4 unique combinations: US→300, CA→150, DE→300, FR→250
    // Set 2: (region) — 2 rows: AMER→450, EMEA→550
    // Set 3: () — 1 row: grand total 1000
    // Total = 7
    assert_eq!(
        rows.len(),
        7,
        "expected 7 rows for explicit GROUPING SETS, got {}: {:?}",
        rows.len(),
        rows
    );

    // Grand-total row (both NULL).
    let grand = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .count();
    assert_eq!(grand, 1, "expected 1 grand-total row");

    // Regional rows (country NULL, region non-empty).
    let regional = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| !s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .count();
    assert_eq!(regional, 2, "expected 2 regional subtotals");

    // Fully-grouped rows (both non-empty).
    let detailed = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| !s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| !s.is_empty()).unwrap_or(false)
        })
        .count();
    assert_eq!(detailed, 4, "expected 4 (region, country) rows");
}

/// `GROUPING(col)` returns 0 for real values and 1 for NULL-filled positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouping_function_returns_correct_indicator() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, country, SUM(sales) AS total, \
                    GROUPING(region) AS gr_r, GROUPING(country) AS gr_c \
             FROM orders \
             GROUP BY ROLLUP (region, country)",
        )
        .await
        .unwrap();

    // Grand-total row: both GROUPING() values should be 1.
    let grand: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| s.is_empty()).unwrap_or(false)
        })
        .collect();
    assert_eq!(grand.len(), 1, "expected 1 grand-total row");

    let gr_r = grand[0].get(3).map(|s| s.as_str()).unwrap_or("");
    let gr_c = grand[0].get(4).map(|s| s.as_str()).unwrap_or("");
    assert_eq!(
        gr_r, "1",
        "GROUPING(region) on grand-total row should be 1, got {gr_r:?}"
    );
    assert_eq!(
        gr_c, "1",
        "GROUPING(country) on grand-total row should be 1, got {gr_c:?}"
    );

    // Fully-grouped rows: both GROUPING() values should be 0.
    let detailed: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.first().map(|s| !s.is_empty()).unwrap_or(false)
                && r.get(1).map(|s| !s.is_empty()).unwrap_or(false)
        })
        .collect();
    assert!(!detailed.is_empty(), "expected some fully-grouped rows");
    for row in &detailed {
        let gr_r = row.get(3).map(|s| s.as_str()).unwrap_or("");
        let gr_c = row.get(4).map(|s| s.as_str()).unwrap_or("");
        assert_eq!(
            gr_r, "0",
            "GROUPING(region) on detail row should be 0, got {gr_r:?}: {row:?}"
        );
        assert_eq!(
            gr_c, "0",
            "GROUPING(country) on detail row should be 0, got {gr_c:?}: {row:?}"
        );
    }
}

/// Mixed: `GROUP BY region, ROLLUP (country)` — plain key always present,
/// ROLLUP adds (country) and () set variants.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_plain_and_rollup() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, country, SUM(sales) AS total \
             FROM orders \
             GROUP BY region, ROLLUP (country)",
        )
        .await
        .unwrap();

    // ROLLUP(country) with plain region produces:
    // Set 1: (region, country) — 4 combos: AMER/US, AMER/CA, EMEA/DE, EMEA/FR
    // Set 2: (region) — 2 rows: AMER, EMEA (country=NULL)
    // Total = 6
    assert_eq!(
        rows.len(),
        6,
        "expected 6 rows for mixed plain+rollup, got {}: {:?}",
        rows.len(),
        rows
    );

    // region is NEVER NULL (it's a plain key, always present).
    for row in &rows {
        let region = row.first().map(|s| s.as_str()).unwrap_or("");
        assert!(
            !region.is_empty(),
            "region must never be NULL in mixed plain+rollup, but got: {row:?}"
        );
    }

    // ROLLUP suffix: rows with country=NULL represent regional subtotals.
    let regional = rows
        .iter()
        .filter(|r| r.get(1).map(|s| s.is_empty()).unwrap_or(false))
        .count();
    assert_eq!(
        regional, 2,
        "expected 2 regional subtotal rows (AMER, EMEA)"
    );
}

/// Empty grouping set `()` must produce exactly one grand-total row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_grouping_set_produces_grand_total() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT SUM(sales) AS grand_total \
             FROM orders \
             GROUP BY GROUPING SETS (())",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "GROUPING SETS (()) must produce exactly 1 row, got {}: {:?}",
        rows.len(),
        rows
    );

    let total = rows[0]
        .first()
        .unwrap_or(&String::new())
        .parse::<f64>()
        .unwrap_or(0.0);
    assert!(
        (total - 1000.0).abs() < 0.01,
        "grand total should be 1000, got {total}"
    );
}

/// SUM / COUNT / AVG must compute correctly within each grouping set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregates_compute_correctly_per_set() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, SUM(sales) AS total, COUNT(*) AS cnt \
             FROM orders \
             GROUP BY ROLLUP (region)",
        )
        .await
        .unwrap();

    // ROLLUP(region): (region) + () = 2 + 1 = 3 rows.
    assert_eq!(
        rows.len(),
        3,
        "expected 3 rows for ROLLUP(region), got {}: {:?}",
        rows.len(),
        rows
    );

    // AMER: 100 + 200 + 150 = 450, count = 3.
    let amer: Vec<_> = rows
        .iter()
        .filter(|r| r.first().map(|s| s.as_str()) == Some("AMER"))
        .collect();
    assert_eq!(amer.len(), 1, "expected exactly 1 AMER row");
    let amer_sum = amer[0]
        .get(1)
        .unwrap_or(&String::new())
        .parse::<f64>()
        .unwrap_or(-1.0);
    let amer_cnt = amer[0]
        .get(2)
        .unwrap_or(&String::new())
        .parse::<i64>()
        .unwrap_or(-1);
    assert!(
        (amer_sum - 450.0).abs() < 0.01,
        "AMER SUM should be 450, got {amer_sum}"
    );
    assert_eq!(amer_cnt, 3, "AMER COUNT should be 3, got {amer_cnt}");

    // EMEA: 300 + 250 = 550, count = 2.
    let emea: Vec<_> = rows
        .iter()
        .filter(|r| r.first().map(|s| s.as_str()) == Some("EMEA"))
        .collect();
    assert_eq!(emea.len(), 1, "expected exactly 1 EMEA row");
    let emea_sum = emea[0]
        .get(1)
        .unwrap_or(&String::new())
        .parse::<f64>()
        .unwrap_or(-1.0);
    assert!(
        (emea_sum - 550.0).abs() < 0.01,
        "EMEA SUM should be 550, got {emea_sum}"
    );

    // Grand total: 1000, count = 5.
    let grand: Vec<_> = rows
        .iter()
        .filter(|r| r.first().map(|s| s.is_empty()).unwrap_or(false))
        .collect();
    assert_eq!(grand.len(), 1, "expected 1 grand-total row");
    let grand_sum = grand[0]
        .get(1)
        .unwrap_or(&String::new())
        .parse::<f64>()
        .unwrap_or(-1.0);
    let grand_cnt = grand[0]
        .get(2)
        .unwrap_or(&String::new())
        .parse::<i64>()
        .unwrap_or(-1);
    assert!(
        (grand_sum - 1000.0).abs() < 0.01,
        "grand SUM should be 1000, got {grand_sum}"
    );
    assert_eq!(grand_cnt, 5, "grand COUNT should be 5, got {grand_cnt}");
}

/// ORDER BY on grouped columns must work: NULLs sort after non-NULLs with ASC
/// (NodeDB's default NULL handling for ASC).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_on_rollup_result() {
    let server = TestServer::start().await;
    create_orders(&server).await;

    let rows = server
        .query_rows(
            "SELECT region, SUM(sales) AS total \
             FROM orders \
             GROUP BY ROLLUP (region) \
             ORDER BY region ASC",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 3, "expected 3 rows");

    // The first two rows should have non-NULL region (AMER, EMEA alphabetically).
    // The last row should be the grand-total (NULL region).
    let last = rows.last().unwrap();
    assert!(
        last.first().map(|s| s.is_empty()).unwrap_or(false),
        "last row with ORDER BY region ASC should be the grand-total (NULL), got: {last:?}"
    );
}
