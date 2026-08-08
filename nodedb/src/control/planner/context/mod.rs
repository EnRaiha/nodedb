// SPDX-License-Identifier: BUSL-1.1

//! Query planning context for the Control Plane.
//!
//! Uses nodedb-sql for SQL parsing and planning. The DataFusion session
//! is retained only for the function body validator (CREATE FUNCTION)
//! and procedural executor (PL/pgSQL expression evaluation).

mod catalog_inputs;
pub mod query;
pub mod security;
pub mod system_security;

pub use query::{PlanSqlWithRlsParams, QueryContext, SYSTEM_FUNCTION_NAMES};
pub use security::PlanSecurityContext;
pub use system_security::SystemPlanSecurity;
