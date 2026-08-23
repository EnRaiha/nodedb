// SPDX-License-Identifier: BUSL-1.1

//! NodeDB version and compatibility information.
//!
//! Follows semver: MAJOR.MINOR.PATCH.
//! - MAJOR changes MAY break wire/disk compatibility.
//! - MINOR changes MUST be backward compatible within the same MAJOR.

/// Current NodeDB version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Short git commit hash baked in at build time.
pub const GIT_COMMIT: &str = env!("NODEDB_GIT_COMMIT");

/// Build date in `YYYY-MM-DD` format.
pub const BUILD_DATE: &str = env!("NODEDB_BUILD_DATE");

/// Build profile: `"debug"` or `"release"`.
pub const BUILD_PROFILE: &str = env!("NODEDB_BUILD_PROFILE");

/// Rust toolchain version string (e.g. `"rustc 1.94.0 (…)"`).
pub const RUST_VERSION: &str = env!("NODEDB_RUST_VERSION");

/// Wire format version. Re-exported from `nodedb_types::wire_version`,
/// which is the single source of truth shared with `nodedb-cluster`
/// and any other crate that stamps or interprets the value.
///
/// Readers accept wire_version in
/// `[MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION]`
/// (see [`check_wire_compatibility`]).
pub use nodedb_types::wire_version::{MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION};

/// Returns a comma-separated list of always-on observability capabilities
/// compiled into this build.
pub fn features_str() -> &'static str {
    "promql,otel,grafana,kafka"
}

/// Returns the hostname of the current machine.
///
/// Uses `libc::gethostname`. Returns `"unknown"` on any error (buffer too small,
/// non-UTF-8, or syscall failure). Never panics.
pub fn hostname() -> String {
    // POSIX guarantees HOST_NAME_MAX + 1 bytes is sufficient (typically 256).
    // We use 256 to be safe across all platforms.
    let mut buf = vec![0u8; 256];
    let result = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if result != 0 {
        return "unknown".to_owned();
    }
    // Find the null terminator.
    let nul_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..nul_pos].to_vec()).unwrap_or_else(|_| "unknown".to_owned())
}

/// Returns the startup banner string.
///
/// All 11 fields are embedded so callers can assert or display a single value.
/// Intended for both `eprintln!` at server boot and unit tests.
pub fn format_banner(
    pgwire_port: u16,
    http_port: u16,
    native_port: u16,
    cluster_mode: &str,
    data_dir: &str,
    auth_mode: &str,
) -> String {
    let pid = std::process::id();
    let host = hostname();
    let mut out = String::with_capacity(512);
    out.push('\n');
    out.push_str(&format!("  NodeDB v{VERSION}\n"));
    out.push_str("  ─────────────────────────────────────\n");
    out.push_str(&format!("  git_commit          : {GIT_COMMIT}\n"));
    out.push_str(&format!("  build_date          : {BUILD_DATE}\n"));
    out.push_str(&format!("  build_profile       : {BUILD_PROFILE}\n"));
    out.push_str(&format!("  rust_version        : {RUST_VERSION}\n"));
    out.push_str(&format!("  wire_format_version : {WIRE_FORMAT_VERSION}\n"));
    out.push_str("  ─────────────────────────────────────\n");
    out.push_str(&format!("  hostname            : {host}\n"));
    out.push_str(&format!("  pid                 : {pid}\n"));
    out.push_str(&format!("  pgwire_port         : {pgwire_port}\n"));
    out.push_str(&format!("  http_port           : {http_port}\n"));
    out.push_str(&format!("  native_port         : {native_port}\n"));
    out.push_str(&format!("  cluster_mode        : {cluster_mode}\n"));
    out.push_str(&format!("  data_dir            : {data_dir}\n"));
    out.push_str(&format!("  auth_mode           : {auth_mode}\n"));
    out.push_str("\n  Press Ctrl+C to stop.\n");
    out
}

/// Check if a remote node's wire version is compatible.
///
/// Returns `Ok(())` if compatible, `Err(reason)` if not.
pub fn check_wire_compatibility(remote_version: u16) -> crate::Result<()> {
    if remote_version > WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is newer than local {WIRE_FORMAT_VERSION}; upgrade this node"
            ),
        });
    }
    if remote_version < MIN_WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is too old (minimum {MIN_WIRE_FORMAT_VERSION}); upgrade the remote node"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_version_compatible() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn newer_version_rejected() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION + 1).is_err());
    }

    /// With the window open (MIN < WIRE), a peer at the floor is accepted.
    #[test]
    fn older_in_window_accepted() {
        if WIRE_FORMAT_VERSION > MIN_WIRE_FORMAT_VERSION {
            assert!(check_wire_compatibility(MIN_WIRE_FORMAT_VERSION).is_ok());
        }
    }

    /// A peer below the floor is rejected.
    #[test]
    fn older_than_floor_rejected() {
        assert!(check_wire_compatibility(0).is_err());
    }

    #[test]
    fn version_string_valid() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn git_commit_env_present() {
        let commit = env!("NODEDB_GIT_COMMIT");
        assert!(!commit.is_empty(), "NODEDB_GIT_COMMIT must not be empty");
    }

    #[test]
    fn build_date_env_format() {
        let date = env!("NODEDB_BUILD_DATE");
        assert_eq!(
            date.len(),
            10,
            "NODEDB_BUILD_DATE must be YYYY-MM-DD (10 chars)"
        );
        let bytes = date.as_bytes();
        assert_eq!(bytes[4], b'-', "position 4 must be '-'");
        assert_eq!(bytes[7], b'-', "position 7 must be '-'");
        for (i, &b) in bytes.iter().enumerate() {
            if i == 4 || i == 7 {
                continue;
            }
            assert!(
                b.is_ascii_digit(),
                "position {i} must be a digit, got '{}'",
                b as char
            );
        }
    }

    #[test]
    fn features_str_contains_observability_capabilities() {
        let s = features_str();
        assert!(s.contains("promql"), "features_str must include promql");
        assert!(s.contains("otel"), "features_str must include otel");
        assert!(s.contains("kafka"), "features_str must include kafka");
    }

    #[test]
    fn hostname_is_non_empty() {
        let h = hostname();
        assert!(
            !h.is_empty(),
            "hostname() must never return an empty string"
        );
    }

    #[test]
    fn build_profile_is_debug_or_release() {
        let p = BUILD_PROFILE;
        assert!(
            p == "debug" || p == "release",
            "BUILD_PROFILE must be 'debug' or 'release', got '{p}'"
        );
    }

    #[test]
    fn rust_version_format() {
        let v = RUST_VERSION;
        assert!(
            v.starts_with("rustc "),
            "RUST_VERSION must start with 'rustc ', got '{v}'"
        );
    }

    #[test]
    fn format_banner_contains_all_required_fields() {
        let banner = format_banner(5432, 8080, 7700, "single-node", "/data/nodedb", "password");

        // version
        assert!(banner.contains(VERSION), "missing version");
        // git_commit
        assert!(banner.contains(GIT_COMMIT), "missing git_commit");
        // build_date
        assert!(banner.contains(BUILD_DATE), "missing build_date");
        // build_profile
        assert!(banner.contains(BUILD_PROFILE), "missing build_profile");
        // rust_version — first token "rustc" must appear
        assert!(banner.contains("rustc"), "missing rust_version");
        // wire_format_version
        assert!(
            banner.contains(&WIRE_FORMAT_VERSION.to_string()),
            "missing wire_format_version"
        );
        // ports
        assert!(banner.contains("5432"), "missing pgwire_port");
        assert!(banner.contains("8080"), "missing http_port");
        assert!(banner.contains("7700"), "missing native_port");
        // cluster_mode
        assert!(banner.contains("single-node"), "missing cluster_mode");
        // pid
        assert!(
            banner.contains(&std::process::id().to_string()),
            "missing pid"
        );
        // hostname
        assert!(!hostname().is_empty(), "hostname must not be empty");
        assert!(banner.contains(&hostname()), "missing hostname");
        // data_dir
        assert!(banner.contains("/data/nodedb"), "missing data_dir");
        // auth_mode
        assert!(banner.contains("password"), "missing auth_mode");
    }

    #[test]
    fn tracing_span_u64_field_captured() {
        // Validates that entering a span with a u64 field compiles and does
        // not panic — the same pattern used for `node_id` in main.rs.
        //
        // `entered()` consumes the span, so we use `enter()` (borrows) here
        // to retain the span handle for the subsequent `record` call.
        let span = tracing::info_span!("test_service", node_id = 0u64);
        let _guard = span.enter();
        // Record an updated value — mirrors the late-record call for cluster mode.
        span.record("node_id", 42u64);
        // If we reach here without panic the pattern is valid.
    }
}
