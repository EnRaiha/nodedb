// SPDX-License-Identifier: BUSL-1.1

//! Translate gateway errors into listener-specific error shapes.
//!
//! Every listener calls `gateway.execute(plan)` and gets `Result<_, Error>`.
//! One module per protocol surface owns that surface's mapping, so a change
//! to its SQLSTATE / HTTP / RESP / native codes is a one-file edit.

mod gateway_map;
mod http;
mod native;
mod pgwire;
mod remote_code;
mod resp;
#[cfg(test)]
mod test_fixtures;

pub use gateway_map::GatewayErrorMap;
