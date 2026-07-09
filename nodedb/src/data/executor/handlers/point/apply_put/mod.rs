// SPDX-License-Identifier: BUSL-1.1

//! Apply-a-PointPut family: core transaction helper, index side-effects
//! (spatial/vector), and UNIQUE-constraint check.

pub(in crate::data::executor::handlers::point) mod core;
pub(in crate::data::executor::handlers::point) mod index;
pub(in crate::data::executor::handlers::point) mod types;
pub(in crate::data::executor) mod unique;

pub(in crate::data::executor) use index::{VectorIndexDelta, VectorIndexPutParams};
pub(in crate::data::executor) use types::{PointPutOutcome, PointPutParams, map_enforcement_error};
