// SPDX-License-Identifier: Apache-2.0

//! Spatial engine primitives shared by Origin, Lite, and WASM: R*-tree with
//! bulk load and nearest-neighbor / range queries, geohash and H3 hex
//! indexes, OGC predicates (ST_Contains, ST_Intersects, ST_Within,
//! ST_DWithin, ST_Distance, etc.), WKB / WKT / GeoJSON interchange, and
//! hybrid spatial-vector composition.
//!
//! Spatial collections are picked per-collection via `WITH (engine='spatial')`
//! and use the columnar storage core; this crate provides the index
//! structures and predicate evaluators only.

pub mod geo_meta;
pub mod geohash;
pub mod geohash_index;
pub mod h3;
pub mod hybrid;
pub mod measures;
pub mod operations;
pub mod persist;
pub mod predicates;
pub mod rtree;
pub mod spatial_join;
pub mod validate;
pub mod wkb;
pub mod wkt;

pub use geohash::{geohash_decode, geohash_encode, geohash_neighbors};
pub use geohash_index::GeohashIndex;
pub use h3::{
    h3_encode, h3_encode_string, h3_is_valid, h3_neighbors, h3_parent, h3_resolution,
    h3_to_boundary, h3_to_center,
};
pub use hybrid::{SpatialPreFilterResult, bitmap_contains, ids_to_bitmap, spatial_prefilter};
pub use measures::{st_area, st_centroid};
pub use operations::{st_buffer, st_envelope, st_union};
pub use persist::{
    RTreeCheckpointError, SpatialIndexMeta, SpatialIndexType, deserialize_meta, meta_storage_key,
    rtree_storage_key, serialize_meta,
};
pub use predicates::{
    st_contains, st_disjoint, st_distance, st_dwithin, st_intersection, st_intersects, st_within,
};
pub use rtree::{RTree, RTreeEntry};
pub use validate::{is_valid, validate_geometry};
pub use wkb::{geometry_from_wkb, geometry_to_wkb};
pub use wkt::{geometry_from_wkt, geometry_to_wkt};
