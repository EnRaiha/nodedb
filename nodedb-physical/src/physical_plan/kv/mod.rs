// SPDX-License-Identifier: Apache-2.0

//! KV engine operations dispatched to the Data Plane.

pub mod collection;
pub mod op;

pub use op::KvOp;
