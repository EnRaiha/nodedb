// SPDX-License-Identifier: Apache-2.0

//! Predicates derived directly from the primitive contains/intersects tests.

use nodedb_types::geometry::Geometry;

use super::contains::st_contains;
use super::intersects::st_intersects;

/// ST_Within(a, b) — A is fully within B. Equivalent to ST_Contains(b, a).
pub fn st_within(a: &Geometry, b: &Geometry) -> bool {
    st_contains(b, a)
}

/// ST_Disjoint(a, b) — no shared space. Inverse of ST_Intersects.
pub fn st_disjoint(a: &Geometry, b: &Geometry) -> bool {
    !st_intersects(a, b)
}
