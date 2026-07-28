// SPDX-License-Identifier: Apache-2.0

//! Geospatial SQL function evaluation, driven by a shared catalog.

pub mod accessors;
pub mod catalog;
pub mod constructors;
pub mod dispatch;
pub mod helpers;
pub mod indexing;
pub mod measures;
pub mod predicates;

pub use catalog::{GEO_FUNCTIONS, GeoArgShape, GeoFunctionSpec, GeoReturn, returns_geometry};
pub use dispatch::eval_geo_function;
pub use helpers::geometry_from_text;
