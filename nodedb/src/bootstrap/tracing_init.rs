// SPDX-License-Identifier: BUSL-1.1

//! Tracing subscriber initialisation (format + filter).

use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::ServerConfig;

/// Initialise the global tracing subscriber based on the config log format.
///
/// Uses `RUST_LOG` env var for the filter if set; otherwise defaults to `warn`
/// for a clean startup. Must be called after config is loaded and before any
/// `tracing::info!` / `tracing::warn!` calls are expected to emit.
pub fn init_tracing(config: &ServerConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if config.server.log_format == crate::config::LogFormat::Json {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .json()
                    .flatten_event(true)
                    .with_filter(filter),
            )
            .with(breadcrumb_layer())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .with(breadcrumb_layer())
            .init();
    }
}

/// The flight recorder that turns events the server already emits into the
/// breadcrumb trail attached to every failure report.
///
/// Deliberately unfiltered by the log-level filter above: a trail is only
/// useful if it exists when the log level is `warn`, and it is a bounded
/// in-memory ring that never touches disk until something fails. The target
/// filter is a prefix match, so one entry covers every crate in this
/// workspace (`nodedb`, `nodedb_wal`, `nodedb_sql`, …) while keeping a
/// dependency's chatter from evicting the events that explain our own failure.
/// Crumbs from every layer merge into one ring, so a report filed inside the
/// WAL still carries the query that led to the bad read.
fn breadcrumb_layer() -> faultbox::BreadcrumbLayer {
    faultbox::BreadcrumbLayer::new().only_targets(["nodedb"])
}
