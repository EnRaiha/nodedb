// SPDX-License-Identifier: BUSL-1.1

//! The sorted-index read functions: `RANK`, `TOPK`, `RANGE`, `SORTED_COUNT`.
//!
//! Each names an index and returns keys, ranks, or counts drawn from the
//! collection it was built over, so each resolves that collection and gates on
//! it before a plan is built (see [`super::gate`]).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::super::result::{DdlError, DdlResult};
use super::dispatch::{SortedIndexTarget, dispatch_and_respond_json, dispatch_and_respond_rows};
use super::gate::gate_read;
use super::parse::{ddl_err, parse_function_args, parse_score_arg, unquote};

/// What each function delivers instead of row bodies, for the refusal message.
const RANK_WHAT: &str = "RANK(), which returns a position in the sorted index rather than rows";
const TOPK_WHAT: &str = "TOPK(), which returns the sorted index's ranked keys rather than rows";
const RANGE_WHAT: &str = "RANGE(), which returns the sorted index's ranked keys rather than rows";
const COUNT_WHAT: &str =
    "SORTED_COUNT(), which returns a count over the sorted index rather than rows";

/// Handle `SELECT RANK(index_name, 'key_value')`
pub async fn select_rank(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "RANK requires 2 arguments: (index_name, key_value)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let key_value = unquote(&args[1]);

    let collection = gate_read(state, identity, database_id, &index_name, RANK_WHAT)?;

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexRank {
        index_name,
        primary_key: key_value.into_bytes(),
    });

    dispatch_and_respond_json(
        state,
        &SortedIndexTarget {
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
        },
        plan,
        "rank",
    )
    .await
}

/// Handle `SELECT * FROM TOPK(index_name, k)` or `SELECT TOPK(index_name, k)`
pub async fn select_topk(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "TOPK requires 2 arguments: (index_name, k)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let k: u32 = args[1].trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("TOPK: k must be a positive integer, got '{}'", args[1]),
        )
    })?;

    let collection = gate_read(state, identity, database_id, &index_name, TOPK_WHAT)?;

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK { index_name, k });

    dispatch_and_respond_rows(
        state,
        &SortedIndexTarget {
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
        },
        plan,
    )
    .await
}

/// Handle `SELECT * FROM RANGE(index_name, score_min, score_max)`
pub async fn select_range(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "RANGE requires 3 arguments: (index_name, score_min, score_max)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();
    let score_min = parse_score_arg(&args[1]);
    let score_max = parse_score_arg(&args[2]);

    let collection = gate_read(state, identity, database_id, &index_name, RANGE_WHAT)?;

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexRange {
        index_name,
        score_min,
        score_max,
    });

    dispatch_and_respond_rows(
        state,
        &SortedIndexTarget {
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
        },
        plan,
    )
    .await
}

/// Handle `SELECT SORTED_COUNT(index_name)`
pub async fn select_sorted_count(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.is_empty() {
        return Err(ddl_err(
            "42601",
            "SORTED_COUNT requires 1 argument: (index_name)",
        ));
    }

    let index_name = unquote(&args[0]).to_lowercase();

    let collection = gate_read(state, identity, database_id, &index_name, COUNT_WHAT)?;

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexCount { index_name });

    dispatch_and_respond_json(
        state,
        &SortedIndexTarget {
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
        },
        plan,
        "sorted_count",
    )
    .await
}
