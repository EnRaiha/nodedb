// SPDX-License-Identifier: Apache-2.0

//! CRDT engine operations dispatched to the Data Plane.

pub mod collection;
pub mod op;

pub use op::CrdtOp;
