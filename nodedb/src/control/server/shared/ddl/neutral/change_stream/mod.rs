// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral change stream DDL — CREATE / DROP / ALTER / SHOW.

pub mod alter;
pub mod create;
pub mod drop;
pub mod show;

pub use alter::alter_change_stream;
pub use create::create_change_stream;
pub use drop::{change_stream_exists, drop_change_stream};
pub use show::show_change_streams;
