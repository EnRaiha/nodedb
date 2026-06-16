// SPDX-License-Identifier: BUSL-1.1

//! Shared streaming-eligibility predicate for lazy query sinks.
//!
//! The pgwire fast path ([`maybe_stream_select`]), the native protocol, and
//! the HTTP-NDJSON route all stream an autocommit, multi-row, unordered SELECT
//! straight to the client instead of materializing it first. This module
//! centralizes the *plan-shape* half of the eligibility check so every sink
//! agrees on what is streamable:
//!
//! - the plan is exactly `Query(Exchange(ExchangeOp{ child, Gather{
//!   as_aggregate: false }}))`, and
//! - `child.is_streamable_unordered_scan()` holds.
//!
//! The caller is responsible for the remaining, sink-specific gates:
//!
//! - **single task** — `tasks.len() == 1` (a multi-task statement, e.g. a
//!   multi-statement simple query, is not streamed),
//! - **no post-set-op** — `post_set_op == PostSetOp::None` (UNION / INTERSECT /
//!   EXCEPT need the full sets), and
//! - **autocommit** — for stateful protocols (pgwire, native), the connection
//!   must not be inside a `BEGIN..COMMIT` block. HTTP is stateless and has no
//!   transaction to check.
//!
//! [`maybe_stream_select`]: crate::control::server::pgwire
//!   ::handler::routing::streaming

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

/// If `plan` is a streamable `Exchange{Gather{as_aggregate:false}}` over an
/// unordered scan, return a clone of the gather *child* plan together with the
/// global take-N to apply while streaming. Otherwise return `None`.
///
/// This is the plan-shape half of the eligibility predicate shared by every
/// lazy query sink — the caller must still apply the single-task, no-set-op,
/// and (for stateful protocols) autocommit gates before streaming.
pub(crate) fn streamable_gather_child(plan: &PhysicalPlan) -> Option<(PhysicalPlan, usize)> {
    let PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
        child,
        mode: ExchangeMode::Gather {
            as_aggregate: false,
        },
    })) = plan
    else {
        return None;
    };

    if !child.is_streamable_unordered_scan() {
        return None;
    }

    let limit = child.streamable_scan_limit();
    Some(((**child).clone(), limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, ExchangeMode, ExchangeOp, QueryOp};

    fn unordered_scan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "docs".into(),
            filters: Vec::new(),
            limit: 1234,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        })
    }

    fn gather(child: PhysicalPlan, as_aggregate: bool) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(child),
            mode: ExchangeMode::Gather { as_aggregate },
        }))
    }

    #[test]
    fn streamable_gather_over_scan_yields_child_and_limit() {
        let plan = gather(unordered_scan(), false);
        let (child, limit) = streamable_gather_child(&plan).expect("eligible");
        assert!(child.is_streamable_unordered_scan());
        assert_eq!(limit, 1234);
    }

    #[test]
    fn aggregate_gather_is_not_streamable() {
        let plan = gather(unordered_scan(), true);
        assert!(streamable_gather_child(&plan).is_none());
    }

    #[test]
    fn bare_scan_without_exchange_is_not_streamable() {
        assert!(streamable_gather_child(&unordered_scan()).is_none());
    }
}
