// SPDX-License-Identifier: Apache-2.0

//! Euclidean measures for geometries in a projected SRID, in metres.

pub mod area;
pub mod buffer;
pub mod distance;

pub use area::{planar_area, planar_ring_area};
pub use buffer::planar_buffer;
pub use distance::{planar_distance, planar_dwithin, planar_length, planar_perimeter};
