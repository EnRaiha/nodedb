// SPDX-License-Identifier: BUSL-1.1

//! Shared pgwire projection helpers.
//!
//! SELECT-read response shaping and column projection live entirely in the
//! protocol-neutral `response_shape::compose` core, which every SELECT-read
//! producer calls directly (encoding via `handler::shape_encode`). This module
//! retains only the `ProjectionItem` / `parse_select_projection` re-exports the
//! dispatch entry point uses to derive the projection list from the SQL.

pub(super) use crate::control::server::response_shape::project::{
    ProjectionItem, parse_select_projection,
};
