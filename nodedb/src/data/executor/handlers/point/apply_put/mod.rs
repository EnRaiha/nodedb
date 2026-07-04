// SPDX-License-Identifier: BUSL-1.1

//! Apply-a-PointPut family: core transaction helper, index side-effects
//! (spatial/vector), and UNIQUE-constraint check.

pub(in crate::data::executor::handlers::point) mod core;
pub(in crate::data::executor::handlers::point) mod index;
pub(in crate::data::executor::handlers::point) mod unique;

pub(in crate::data::executor) use core::{PointPutOutcome, PointPutParams, map_enforcement_error};
