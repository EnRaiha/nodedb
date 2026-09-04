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

/// Serialize tests that read/write process env vars. Parallel cargo test
/// threads otherwise race on `std::env` (e.g. a strict-numeric test setting
/// NODEDB_DATA_PLANE_CORES while a host_ports test reads it).
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, OnceLock};

    pub(crate) fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }
}
