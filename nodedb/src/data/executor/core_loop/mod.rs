// SPDX-License-Identifier: BUSL-1.1

mod accessors;
mod bitemporal_time;
mod decode_stored;
pub(in crate::data::executor) mod deferred;
mod event_emit;
mod graph_partition;
pub(in crate::data::executor) mod maintenance;
mod open;
pub(in crate::data::executor) mod pressure;
pub(in crate::data::executor) mod priority_queues;
mod response;
mod state;
#[cfg(test)]
pub(crate) mod tests;
mod tick;

pub use state::CoreLoop;
