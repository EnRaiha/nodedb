// SPDX-License-Identifier: BUSL-1.1

//! The wire shape of one resolved update arm.
//!
//! A MERGE or `UPDATE ... FROM` runs in two passes: the Data Plane matches rows
//! (RESOLVE) and the Control Plane re-issues concrete work (APPLY). The rows
//! travel between them as this tuple, and it is spelled in three places — the
//! Control-Plane decoder, the Data-Plane MERGE encoder, and the Data-Plane
//! update-from-join encoder. Naming it once keeps those three from drifting into
//! three subtly different tuples, which is a class of bug no compiler catches
//! when every element happens to be a `Vec<u8>`.

/// `(document_id, surrogate, pre_image, post_image)` for one matched row.
///
/// Both images travel, not just the post-image. A materialized sum folds a
/// signed delta from the pair, and a rewritten join key moves value between TWO
/// target rows — neither is derivable from the post-image alone.
pub type ResolvedUpdateRowWire = (String, Option<u32>, Vec<u8>, Vec<u8>);
