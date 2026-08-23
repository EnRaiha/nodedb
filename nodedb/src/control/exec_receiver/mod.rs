// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.

pub mod executor;
mod plan_decode;
mod request_validation;
mod support;

pub use executor::LocalPlanExecutor;
