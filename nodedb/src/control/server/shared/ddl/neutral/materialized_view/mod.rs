// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral materialized view DDL — CREATE / DROP / REFRESH / SHOW.

pub mod create;
pub mod drop;
pub mod refresh;
pub mod show;
pub mod streaming_parse;

pub use create::create_materialized_view;
pub use drop::{drop_materialized_view, materialized_view_exists};
pub use refresh::refresh_materialized_view;
pub use show::show_materialized_views;
