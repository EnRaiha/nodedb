// SPDX-License-Identifier: BUSL-1.1

//! Universal node-level "fan to all local cores and merge" primitive.
//!
//! `execute_plan_all_local_cores` is the canonical way to execute a
//! [`nodedb_physical::physical_plan::PhysicalPlan`] on THIS node and obtain a
//! single merged payload in exactly the same shape a single core's handler
//! produces. It is called:
//!
//! - by the remote `ExecuteRequest` receiver (`exec_receiver/executor.rs`) so
//!   that an inbound plan from another node is transparently fanned across all
//!   local cores before the merged result is returned,
//! - by the local BSP scatter path (`bsp_pagerank/scatter.rs`) so the
//!   coordinator's own node is treated identically to every remote node.
//!
//! At 1 core/node the fan is over a single core and every path is
//! behaviour-identical to the prior single-core dispatch.
//!
//! `dispatch` holds the top-level plan-shaped routing plus the two generic
//! gather variants (row-array and single-blob). `fanout` holds the shared
//! per-core dispatch primitive used by every single-blob merge path.
//! `snapshot`, `bsp`, and `wcc` each hold one field-concatenation merge for a
//! single-blob `PhysicalPlan` variant.

mod bsp;
mod dispatch;
mod fanout;
mod snapshot;
mod wcc;

pub use dispatch::NodeLevelResult;
pub(crate) use dispatch::execute_plan_all_local_cores;
