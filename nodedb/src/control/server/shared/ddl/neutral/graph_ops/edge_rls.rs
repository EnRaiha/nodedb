// SPDX-License-Identifier: BUSL-1.1

//! Row-level-security resolution for the hand-built edge-write plans.
//!
//! `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` construct their `GraphOp` directly
//! and hand it to a trusted internal dispatch, which runs no injection pass of
//! its own. Without this step the graph DSL would be the one write surface a
//! `FOR WRITE` policy does not reach, while the same edge written through the
//! native protocol — which does inject — is governed.
//!
//! The verdict belongs to the injection pass, never to this module: an insert
//! carries its `PROPERTIES` image and is admitted or rejected there, and a
//! delete has its compiled predicate written into the plan's write-gate slot
//! for the Data Plane to decide against the edge's stored properties.

use nodedb_physical::physical_plan::GraphOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::read_gate::CollectionReadGate;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Resolve the collection's write policy against a hand-built edge write.
///
/// Takes and returns the op by value so the caller keeps a `GraphOp` for the
/// staging / single-home / Calvin branches without having to unwrap a plan
/// through a partial match.
pub(super) fn resolve_edge_write_rls(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    op: GraphOp,
) -> Result<GraphOp, DdlError> {
    let mut plan = PhysicalPlan::Graph(op);
    CollectionReadGate::for_request(state, identity, database_id).inject_rls(&mut plan)?;
    match plan {
        PhysicalPlan::Graph(op) => Ok(op),
        other => Err(ddl_err(
            "XX000",
            format!("edge write plan changed shape during RLS resolution: {other:?}"),
        )),
    }
}
