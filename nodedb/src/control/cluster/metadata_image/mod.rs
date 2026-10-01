// SPDX-License-Identifier: BUSL-1.1

//! The metadata Raft group 0 snapshot image: its format, capture, and
//! install.
//!
//! - [`format`]: the image and its codec.
//! - [`capture`]: capture from a live node between apply batches, or from
//!   catalogs on disk.
//! - [`install`]: the durable write, the offline install into a data
//!   directory, and the live install.
//! - [`reload`]: rebuild of the registries the catalog feeds.
//! - [`reconcile`]: the Data Plane effects of the entries an install skips.
//! - [`inventory`]: the catalog objects the reload and reconcile compare.

pub mod capture;
pub mod format;
pub mod install;
mod inventory;
mod reconcile;
pub mod reload;

pub use capture::{MetadataImageCapture, capture_metadata_image};
pub use format::{MetadataImage, decode_metadata_image, encode_metadata_image};
pub use install::{
    EpochPolicy, install_metadata_image, install_metadata_image_offline, merge_routing,
    write_metadata_image,
};
pub use reload::RaftOwnedState;
