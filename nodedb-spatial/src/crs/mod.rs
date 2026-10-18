// SPDX-License-Identifier: Apache-2.0

//! Coordinate reference system transforms for the supported SRIDs:
//! WGS 84, UTM 47N / 48N, and the GDM2000 Malaysian RSO grids.

pub mod ellipsoid;
pub mod error;
pub mod hom;
pub mod params;
pub mod tm;
pub mod transform;

pub use error::{CrsError, DomainReason};
pub use hom::{HomParams, HomVariant, HotineObliqueMercator};
pub use tm::{
    TAU_NEWTON_MAX_ITERS, TAU_TOLERANCE, TM_MAX_LON_OFFSET_DEG, TmParams, TransverseMercator,
};
pub use transform::{Projection, Transformer, transform_geometry};
