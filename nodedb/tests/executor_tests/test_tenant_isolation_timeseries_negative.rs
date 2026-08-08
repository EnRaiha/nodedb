// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Timeseries engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B ingesting data into the same collection name as Tenant A
//! cannot contaminate Tenant A's scan results.  After Tenant B's ingest, Tenant A
//! must see the same row count as before.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use crate::helpers::*;

/// Tenant B ingesting timeseries data must not add rows to Tenant A's scan.
#[test]
fn timeseries_cross_tenant_ingest_does_not_contaminate_scan() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A ingests 3 rows.
    let ilp_a = "metrics,host=srv01 cpu=0.10 1000000000\n\
                 metrics,host=srv01 cpu=0.20 2000000000\n\
                 metrics,host=srv01 cpu=0.30 3000000000\n";
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".into(),
            payload: ilp_a.as_bytes().to_vec(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Establish baseline: Tenant A sees the 3 rows.
    let resp_baseline = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "metrics".into(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 100,
            filters: vec![],
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            rls_filters: vec![],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            computed_columns: vec![],
        }),
    );
    assert_eq!(resp_baseline.status, Status::Ok);
    let json_baseline = payload_json(&resp_baseline.payload);
    let baseline_count = serde_json::from_str::<serde_json::Value>(&json_baseline)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(
        baseline_count >= 1,
        "Tenant A must have rows before B's ingest"
    );

    // Tenant B ingests 10 rows into the same collection name.
    let mut ilp_b = String::new();
    for i in 0..10u32 {
        ilp_b.push_str(&format!(
            "metrics,host=b_srv{i} cpu=0.99 {}000000000\n",
            i + 10
        ));
    }
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".into(),
            payload: ilp_b.into_bytes(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Tenant A's scan must return the same count as baseline.
    let resp_after = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "metrics".into(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 100,
            filters: vec![],
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            rls_filters: vec![],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            computed_columns: vec![],
        }),
    );
    assert_eq!(resp_after.status, Status::Ok);
    let json_after = payload_json(&resp_after.payload);
    let after_count = serde_json::from_str::<serde_json::Value>(&json_after)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    assert_eq!(
        after_count, baseline_count,
        "Tenant B's timeseries ingest must not add rows to Tenant A's scan \
         (baseline={baseline_count}, after={after_count})"
    );

    // Tenant B's host tags must not appear in Tenant A's results.
    assert!(
        !json_after.contains("b_srv"),
        "Tenant B's host tags must NOT appear in Tenant A's timeseries scan; got: {json_after}"
    );
}
