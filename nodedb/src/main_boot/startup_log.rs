// SPDX-License-Identifier: BUSL-1.1

//! Config-source + boot-banner logging, emitted right after the root
//! tracing span is entered.

use std::path::PathBuf;

use nodedb::ServerConfig;
use tracing::info;

/// Log where the config came from (default / CLI arg / `NODEDB_CONFIG`),
/// then emit the structured "nodedb starting" boot-banner event. Returns
/// the cluster-mode label (`"cluster"` or `"single-node"`) for reuse by
/// later boot steps. Pure relocation of what used to be inline in
/// `main()` right after the root span guard was entered.
pub(crate) fn log_boot_banner(
    config_path: &Option<PathBuf>,
    config: &ServerConfig,
) -> &'static str {
    match config_path {
        None => info!("no config file provided, using defaults"),
        Some(path)
            if std::env::var("NODEDB_CONFIG").is_ok() && std::env::args().nth(1).is_none() =>
        {
            info!(
                path = %path.display(),
                "config file loaded from NODEDB_CONFIG"
            );
        }
        Some(_) => {}
    }

    let cluster_mode_str = if config.cluster.is_some() {
        "cluster"
    } else {
        "single-node"
    };
    info!(
        target: "boot",
        version = nodedb::version::VERSION,
        git_commit = nodedb::version::GIT_COMMIT,
        build_date = nodedb::version::BUILD_DATE,
        build_profile = nodedb::version::BUILD_PROFILE,
        rust_version = nodedb::version::RUST_VERSION,
        wire_format_version = nodedb::version::WIRE_FORMAT_VERSION,
        features = nodedb::version::features_str(),
        host = %nodedb::version::hostname(),
        pid = std::process::id(),
        pgwire_port = config.server.ports.pgwire,
        http_port = config.server.ports.http,
        native_port = config.server.ports.native,
        cluster_mode = cluster_mode_str,
        cores = config.server.data_plane_cores,
        memory_limit = config.server.memory_limit,
        "nodedb starting",
    );

    cluster_mode_str
}
