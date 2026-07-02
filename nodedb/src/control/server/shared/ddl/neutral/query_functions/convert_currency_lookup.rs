// SPDX-License-Identifier: BUSL-1.1

//! `SELECT CONVERT_CURRENCY_LOOKUP(amount, from, to, rate_table, as_of, key_column, rate_column, time_column, precision)`
//!
//! Convenience overload: performs TEMPORAL_LOOKUP internally to find the rate,
//! then applies the arithmetic conversion.

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{clean_arg, err, extract_function_args, json_to_decimal, single_result};

/// Convenience currency conversion with rate table lookup.
///
/// Performs a temporal lookup on `rate_table` to find the latest rate for the
/// currency pair where `time_column <= as_of`, then returns `ROUND(amount * rate, precision)`.
pub async fn convert_currency_lookup(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "CONVERT_CURRENCY_LOOKUP")?;
    if args.len() < 9 {
        return Err(err(
            "42601",
            "CONVERT_CURRENCY_LOOKUP requires (amount, from, to, rate_table, as_of, key_column, rate_column, time_column, precision)",
        ));
    }

    let amount_str = clean_arg(args[0]);
    let from_ccy = clean_arg(args[1]);
    let to_ccy = clean_arg(args[2]);
    let rate_table = clean_arg(args[3]);
    let as_of = clean_arg(args[4]);
    let key_column = clean_arg(args[5]);
    let rate_column = clean_arg(args[6]);
    let time_column = clean_arg(args[7]);
    let precision: u32 = clean_arg(args[8])
        .parse()
        .map_err(|_| err("22023", "precision must be an integer"))?;
    if precision > 38 {
        return Err(err(
            "22023",
            &format!("precision must be <= 38, got {precision}"),
        ));
    }

    let amount: rust_decimal::Decimal = amount_str
        .parse()
        .map_err(|_| err("22023", &format!("cannot parse amount '{amount_str}'")))?;

    // Build the composite key: "{from}/{to}" for the rate table lookup.
    let key_value = format!("{from_ccy}/{to_ccy}");

    // Scan rate table to find latest rate where key_column == key_value AND time_column <= as_of.
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &rate_table);
    let scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: rate_table.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });

    let scan_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        scan_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("rate table scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = sonic_rs::from_str(&payload_json)
        .map_err(|e| err("22P02", &format!("invalid JSON in rate table scan: {e}")))?;

    // Find latest row where key matches and time <= as_of.
    let mut best_rate: Option<rust_decimal::Decimal> = None;
    let mut best_time = String::new();

    for doc in &docs {
        let obj = match doc.as_object() {
            Some(o) => o,
            None => continue,
        };
        let key_val = obj.get(&key_column).and_then(|v| v.as_str()).unwrap_or("");
        if key_val != key_value {
            continue;
        }
        let time_val = obj.get(&time_column).and_then(|v| v.as_str()).unwrap_or("");
        if time_val.is_empty() || time_val > as_of.as_str() {
            continue;
        }
        if time_val > best_time.as_str() {
            best_time = time_val.to_string();
            best_rate = obj.get(&rate_column).and_then(json_to_decimal);
        }
    }

    let Some(rate) = best_rate else {
        return Err(err(
            "02000",
            &format!("no exchange rate found for {from_ccy}/{to_ccy} as of {as_of}"),
        ));
    };

    // Compute: ROUND(amount * rate, precision, HALF_EVEN).
    let converted = amount * rate;
    let rounded = converted.round_dp_with_strategy(
        precision,
        rust_decimal::RoundingStrategy::MidpointNearestEven,
    );

    Ok(single_result(&rounded.to_string()))
}
