// SPDX-License-Identifier: BUSL-1.1

//! Environment variable overrides for `ServerConfig`, split by concern.
//! `dispatch::apply_env_overrides` is the public entry point; every other
//! submodule here handles one section of the override surface.

mod checkpoint;
mod cluster;
mod dispatch;
mod helpers;
mod host_ports;
mod memory_size;
mod numeric;
mod timeseries;
mod tls;
mod wal;

pub use cluster::parse_seed_nodes;
pub use dispatch::apply_env_overrides;
pub use memory_size::parse_memory_size;
