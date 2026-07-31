// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder ownership.
//!
//! This binary is the host for the `faultbox` recorder shared by every layer of
//! the stack: the WAL files corruption, invariant, and durability reports at
//! the sites that detect them, and this module is the one place that decides
//! where those reports land, what redacts them, and whether native crashes are
//! captured. Libraries never call `faultbox::init` — the first call wins, so a
//! library that made this choice would silently make it for the whole process.
//!
//! There must be exactly one `faultbox` package in the dependency graph. Every
//! crate here depends on it by published version through the workspace table,
//! so Cargo unifies them; two copies would give two independent sets of
//! process-wide state, and every report filed through the uninitialized one
//! would vanish with no error.

use std::path::{Path, PathBuf};

use crate::ServerConfig;

/// Subdirectory of the data directory that holds report groups.
const REPORTS_SUBDIR: &str = "diagnostics";

/// Serve the out-of-process crash monitor loop when this process was spawned as
/// one, and report whether it did.
///
/// The monitor is a re-exec of this same binary, so `main` has to identify
/// itself before doing anything else — before argument parsing, before config
/// loading, before the allocator arenas come up. Without this call a spawned
/// monitor would run the server again from the top and spawn another monitor,
/// exponentially. It is mandatory whenever the handler can be armed, and cheap
/// (an environment-variable check) when it is not.
#[must_use]
pub fn run_crash_monitor_if_env() -> bool {
    faultbox::run_crash_monitor_if_env()
}

/// Initialize the recorder for this process. Call once, after the config is
/// loaded and before the tracing subscriber is built.
///
/// Reports go under the server's own data directory rather than a fixed system
/// path: that is the location the operator already provisioned for this
/// instance, it survives restarts, and two instances on one host do not write
/// into each other's reports.
pub fn init(config: &ServerConfig) {
    let reports_dir = reports_dir(&config.server.data_dir);
    let armed = config.server.native_crash_dumps;

    faultbox::init(
        faultbox::Config::new("nodedb", crate::version::VERSION, reports_dir)
            // Reports are meant to be submittable, so paths, credential-shaped
            // values, and addresses are masked before anything reaches disk.
            .redactor(Box::new(faultbox::BasicRedactor::new()))
            .features(build_features())
            // Off unless the operator asked for it: a minidump is the whole address
            // space of a database process and cannot be redacted.
            .install_native_crash_handler(armed),
    );
}

/// The report directory for a given data directory.
fn reports_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(REPORTS_SUBDIR)
}

/// Compile-time features recorded on every report, so a report read months
/// later says which build produced it.
fn build_features() -> Vec<&'static str> {
    let mut features = Vec::new();
    if cfg!(feature = "failpoints") {
        features.push("failpoints");
    }
    if cfg!(target_os = "linux") {
        features.push("linux");
    }
    features
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_live_under_the_configured_data_dir() {
        let dir = reports_dir(Path::new("/srv/nodedb/data"));
        assert_eq!(dir, Path::new("/srv/nodedb/data/diagnostics"));
    }

    #[test]
    fn default_config_does_not_arm_native_crash_capture() {
        let config = ServerConfig::default();
        assert!(
            !config.server.native_crash_dumps,
            "minidumps must be opt-in: they are an unredactable copy of the process's memory"
        );
    }
}
