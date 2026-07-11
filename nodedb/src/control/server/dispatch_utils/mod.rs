// SPDX-License-Identifier: BUSL-1.1

//! Shared dispatch utilities used by both the pgwire and native endpoints.

mod change_events;
mod collect;
mod dispatch;
mod types;

pub(crate) use collect::{DispatchCollectError, collect_bounded_response};
pub(crate) use dispatch::{dispatch_autocommit_write, dispatch_write_to_data_plane};
pub use dispatch::{
    dispatch_to_data_plane, dispatch_to_data_plane_with_source, dispatch_to_data_plane_with_txn,
};
pub(crate) use types::{AutocommitWrite, WriteDispatch};
