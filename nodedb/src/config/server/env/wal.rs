// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_WAL_DIRECT_IO` / `NODEDB_WAL_WRITE_BUFFER_SIZE` overrides.

use super::helpers::apply_bool_env;
use super::memory_size::parse_memory_size;
use crate::config::server::ServerConfig;

pub(super) fn apply_wal_tuning(config: &mut ServerConfig) {
    // On by default. Env-reachable because the one case that legitimately
    // needs it off — a data directory on a filesystem without direct-I/O
    // support, such as a harness tempdir on tmpfs — is a property of the
    // deployment, not of the config file shipped with it.
    apply_bool_env("NODEDB_WAL_DIRECT_IO", &mut config.tuning.wal.direct_io);

    if let Ok(val) = std::env::var("NODEDB_WAL_WRITE_BUFFER_SIZE") {
        match parse_memory_size(&val) {
            Ok(size) if size >= 64 * 1024 => {
                tracing::info!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = size,
                    "environment variable override applied"
                );
                config.tuning.wal.write_buffer_size = size;
            }
            Ok(size) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = size,
                    "ignoring value below minimum 64KiB, using config value"
                );
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_WAL_WRITE_BUFFER_SIZE",
                    value = %val,
                    "ignoring malformed environment variable, using config value"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::apply_env_overrides;
    use super::*;

    /// Direct I/O is the shipped default, and only an explicit opt-out turns
    /// it off — an absent or malformed env var must never be read as one.
    #[test]
    fn env_wal_direct_io_override() {
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert!(cfg.tuning.wal.direct_io, "default must be direct I/O");

        unsafe { std::env::set_var("NODEDB_WAL_DIRECT_IO", "false") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert!(!cfg.tuning.wal.direct_io);

        unsafe { std::env::set_var("NODEDB_WAL_DIRECT_IO", "nonsense") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert!(
            cfg.tuning.wal.direct_io,
            "a malformed value must not silently disable direct I/O"
        );

        unsafe { std::env::remove_var("NODEDB_WAL_DIRECT_IO") };
    }
}
