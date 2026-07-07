// SPDX-License-Identifier: BUSL-1.1

//! Surrogate WAL replay and superuser credential bootstrap.

use std::sync::Arc;

use tracing::{info, warn};

use crate::ServerConfig;
use crate::control::state::SharedState;
use nodedb_wal::WalRecord;

/// Replay surrogate WAL records into the surrogate registry.
///
/// Exits the process if replay fails — a partially-recovered surrogate
/// registry is not safe to continue with.
pub fn replay_surrogate_wal(shared: &Arc<SharedState>, wal_records: &Arc<[WalRecord]>) {
    {
        let catalog = shared.credentials.catalog();
        match crate::wal::replay::replay_surrogate_records(
            wal_records,
            catalog,
            &shared.surrogate_registry,
        ) {
            Ok(stats) => {
                if stats.allocs > 0 || stats.binds > 0 {
                    info!(
                        allocs = stats.allocs,
                        binds = stats.binds,
                        "WAL surrogate replay complete"
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "StartupError: surrogate WAL replay failed — refusing to start \
                     with a partially-recovered surrogate registry"
                );
                std::process::exit(1);
            }
        }
    }
}

/// Bootstrap the superuser credential from config / data dir, or warn about trust mode.
pub fn bootstrap_superuser(shared: &Arc<SharedState>, config: &ServerConfig) -> anyhow::Result<()> {
    let auth_mode = config.auth.mode.clone();
    match config
        .auth
        .resolve_superuser_password(&config.server.data_dir)
    {
        Ok(Some(password)) => {
            shared
                .credentials
                .bootstrap_superuser(&config.auth.superuser_name, &password)?;
            info!(
                user = config.auth.superuser_name,
                mode = ?auth_mode,
                "superuser bootstrapped"
            );
        }
        Ok(None) => {
            // Trust mode — no credentials needed, but operators must opt in explicitly.
            warn!("╔══════════════════════════════════════════════════════════════╗");
            warn!("║  WARNING: NodeDB is running in TRUST mode.                  ║");
            warn!("║  ALL connections are accepted WITHOUT credentials.           ║");
            warn!("║  This is UNSAFE for any environment beyond local dev/CI.    ║");
            warn!("║  Set auth.mode = \"password\" (or \"certificate\") to require   ║");
            warn!("║  credentials. Trust mode must be an explicit operator       ║");
            warn!("║  opt-in — it is never the NodeDB default.                   ║");
            warn!("╚══════════════════════════════════════════════════════════════╝");
        }
        Err(e) => {
            return Err(e.into());
        }
    }
    Ok(())
}

/// Print the startup banner and, if in trust mode, the trust-mode warning box.
pub fn print_startup_banner(config: &ServerConfig, cluster_mode_str: &str) {
    eprintln!(
        "{}",
        crate::version::format_banner(
            config.server.ports.pgwire,
            config.server.ports.http,
            config.server.ports.native,
            cluster_mode_str,
            &config.server.data_dir.display().to_string(),
            &format!("{:?}", config.auth.mode),
        )
    );
    if config.auth.mode == crate::config::auth::AuthMode::Trust {
        eprintln!("  ╔══════════════════════════════════════════════════════════════╗");
        eprintln!("  ║  WARNING: TRUST MODE — connections accepted without         ║");
        eprintln!("  ║  credentials. Unsafe outside local dev / CI.                ║");
        eprintln!("  ║  Set auth.mode = \"password\" to require credentials.         ║");
        eprintln!("  ╚══════════════════════════════════════════════════════════════╝");
        eprintln!();
    }
}
