// SPDX-License-Identifier: BUSL-1.1

mod merge;
mod staged;

pub(in crate::data::executor) use merge::IndexOverlayMergeParams;
pub use staged::{CollectionOverlay, MAX_TXN_OVERLAY_BYTES, Staged, TxnOverlay};
