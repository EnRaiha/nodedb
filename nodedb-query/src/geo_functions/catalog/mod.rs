// SPDX-License-Identifier: Apache-2.0

pub mod lookup;
pub mod spec;
pub mod table;

pub use lookup::{lookup, returns_geometry};
pub use spec::{GeoArgShape, GeoFunctionSpec, GeoReturn, MAX_VARIADIC};
pub use table::GEO_FUNCTIONS;
