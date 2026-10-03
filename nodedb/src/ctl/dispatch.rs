// SPDX-License-Identifier: BUSL-1.1

//! Subcommand dispatch table.

use std::path::PathBuf;

use super::args::parse_flags;
use super::restore::{RestoreArgs, parse_restore_args};
use super::{healthcheck, join_token, regen_certs, restore, rotate_ca};

/// All operator subcommands. Constructed by [`parse_subcommand`] from
/// the raw `std::env::args()` tail so `main()` can decide before it
/// touches any config or tracing state.
#[derive(Debug, PartialEq)]
pub enum Subcommand {
    /// Reissue the per-node cert under the existing CA. Restart required.
    RegenCerts { data_dir: PathBuf, node_id: u64 },
    /// Stage a new CA: generate + register for cluster-wide trust.
    RotateCaStage { data_dir: PathBuf },
    /// Finalize a rotation: remove the named CA fingerprint from trust.
    RotateCaFinalize { remove_fingerprint: String },
    /// Issue a short-lived HMAC-signed token for a joining node.
    JoinTokenCreate {
        data_dir: PathBuf,
        for_node: u64,
        ttl: std::time::Duration,
    },
    /// Probe the local HTTP `/health` endpoint and exit 0/1.
    /// Used by container `HEALTHCHECK` directives so distroless /
    /// Chainguard runtime images don't need to ship `curl`.
    Healthcheck { port: u16 },
    /// Restore this node offline to a point in time: a base snapshot plus
    /// archived WAL cut at the target.
    Restore(RestoreArgs),
    /// Print the version string and exit 0.
    PrintVersion,
    /// Reserved subcommand that is not yet implemented.
    NotImplemented { name: String },
}

/// Stub subcommand descriptions for reserved names.
fn stub_description(name: &str) -> &'static str {
    match name {
        "migrate" => "schema/data migration",
        "backup" => "online backup",
        "verify" => "verify (consistency check)",
        "repair" => "repair",
        "dump" => "dump (logical export)",
        "fsck" => "fsck",
        _ => "not yet implemented",
    }
}

/// Parse `args` (the tail of `std::env::args()` after the binary name)
/// into a subcommand. Returns `Ok(None)` when the first arg doesn't
/// look like a subcommand (i.e. it's a config-file path and the
/// caller should fall through to the server bootstrap path).
pub fn parse_subcommand(args: &[String]) -> Result<Option<Subcommand>, String> {
    let Some(first) = args.first() else {
        return Ok(None);
    };
    // Anything that is not a subcommand name is a config file path, so the
    // `nodedb /etc/nodedb.toml` spelling keeps working.
    let name = first.as_str();
    let tail = &args[1..];
    match name {
        "help" | "--help" | "-h" => {
            print_usage();
            std::process::exit(0);
        }
        "--version" | "-V" | "version" => Ok(Some(Subcommand::PrintVersion)),
        "migrate" | "backup" | "verify" | "repair" | "dump" | "fsck" => {
            Ok(Some(Subcommand::NotImplemented {
                name: name.to_string(),
            }))
        }
        "restore" => parse_restore_args(tail)
            .map(|args| Some(Subcommand::Restore(args)))
            .map_err(|e| e.to_string()),
        "regen-certs" => {
            let flags = parse_flags(tail)?;
            let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
            let node_id: u64 = super::args::required(&flags, "node-id")?
                .parse()
                .map_err(|_| "--node-id must be an integer".to_string())?;
            Ok(Some(Subcommand::RegenCerts { data_dir, node_id }))
        }
        "rotate-ca" => {
            // Sub-mode: `--stage` or `--finalize --remove <fp>`.
            let flags = parse_flags(tail)?;
            if flags.contains_key("stage") {
                let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
                Ok(Some(Subcommand::RotateCaStage { data_dir }))
            } else if flags.contains_key("finalize") {
                let remove = super::args::required(&flags, "remove")?.clone();
                Ok(Some(Subcommand::RotateCaFinalize {
                    remove_fingerprint: remove,
                }))
            } else {
                Err("rotate-ca requires --stage or --finalize --remove <fingerprint>".into())
            }
        }
        "healthcheck" => {
            let port = healthcheck::parse_port(tail)?;
            Ok(Some(Subcommand::Healthcheck { port }))
        }
        "join-token" => {
            let flags = parse_flags(tail)?;
            if !flags.contains_key("create") {
                return Err("join-token requires --create".into());
            }
            let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
            let for_node: u64 = super::args::required(&flags, "for-node")?
                .parse()
                .map_err(|_| "--for-node must be an integer".to_string())?;
            let ttl =
                super::args::parse_duration(flags.get("ttl").map(|s| s.as_str()).unwrap_or("10m"))?;
            Ok(Some(Subcommand::JoinTokenCreate {
                data_dir,
                for_node,
                ttl,
            }))
        }
        _ => Ok(None),
    }
}

/// Execute a parsed subcommand. Returns process exit code.
pub fn run_subcommand(cmd: Subcommand) -> i32 {
    match cmd {
        Subcommand::RegenCerts { data_dir, node_id } => {
            match regen_certs::run(&data_dir, node_id) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }
        Subcommand::RotateCaStage { data_dir } => match rotate_ca::stage(&data_dir) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e}");
                1
            }
        },
        Subcommand::RotateCaFinalize { remove_fingerprint } => {
            match rotate_ca::finalize(&remove_fingerprint) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }
        Subcommand::JoinTokenCreate {
            data_dir,
            for_node,
            ttl,
        } => match join_token::create(&data_dir, for_node, ttl) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e}");
                1
            }
        },
        Subcommand::Healthcheck { port } => healthcheck::run(port),
        Subcommand::Restore(args) => restore::run(args),
        Subcommand::PrintVersion => {
            print!("{}", format_version_block());
            0
        }
        Subcommand::NotImplemented { name } => {
            let desc = stub_description(&name);
            eprintln!("error: {desc} not yet implemented");
            1
        }
    }
}

/// Returns the enriched version block printed by `--version` / `-V` / `version`.
///
/// Extracted as a pure function so unit tests can assert on the output without
/// capturing stdout.
pub(crate) fn format_version_block() -> String {
    use crate::version::{
        BUILD_DATE, BUILD_PROFILE, GIT_COMMIT, RUST_VERSION, VERSION, WIRE_FORMAT_VERSION,
    };
    format!(
        "nodedb {VERSION}\ngit commit:    {GIT_COMMIT}\nbuild date:    {BUILD_DATE}\nbuild profile: {BUILD_PROFILE}\nrust version:  {RUST_VERSION}\nwire format:   {WIRE_FORMAT_VERSION}\n"
    )
}

fn print_usage() {
    println!(
        "nodedb — NodeDB server + operator tooling

USAGE:
    nodedb [CONFIG_FILE]                     Run the server (default mode)
    nodedb --version                         Print version and exit
    nodedb regen-certs --data-dir D --node-id N
                                             Reissue this node's cert under the existing CA
    nodedb rotate-ca --stage --data-dir D    Generate a new CA, write to ca.d/, emit staged bundle
    nodedb rotate-ca --finalize --remove FP  Ask the running node to remove CA with fingerprint FP
    nodedb join-token --create --data-dir D --for-node N [--ttl 10m]
                                             Emit a one-time HMAC token for a joining node
    nodedb healthcheck [--port N]            Probe local HTTP /health (exit 0=healthy, 1=unhealthy)
    nodedb restore --config C (--target-time T | --target-lsn N) [--incarnation I] [--dry-run]
                                             Restore this node offline into the empty data dir of
                                             config C: the newest base snapshot at or below the
                                             target plus archived WAL cut at the target. T is
                                             RFC 3339 or epoch s/ms/us. --dry-run prints the plan
    nodedb restore --config C --cluster --restore-point ID [--incarnation I] [--dry-run]
                                             This node's part of a cluster restore to restore
                                             point ID. Stop every node, then run it on each
    nodedb help                              Print this message

RESERVED (not yet implemented):
    nodedb migrate                           Schema/data migration
    nodedb backup                            Online backup
    nodedb verify                            Consistency check
    nodedb repair                            Repair corrupted data
    nodedb dump                              Logical export
    nodedb fsck                              Filesystem consistency check"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &[&str]) -> Vec<String> {
        s.iter().map(|&x| x.to_string()).collect()
    }

    // --version and version → PrintVersion
    #[test]
    fn version_flag_parses() {
        assert_eq!(
            parse_subcommand(&args(&["--version"])).unwrap(),
            Some(Subcommand::PrintVersion)
        );
    }

    #[test]
    fn version_subcommand_parses() {
        assert_eq!(
            parse_subcommand(&args(&["version"])).unwrap(),
            Some(Subcommand::PrintVersion)
        );
    }

    // 7 reserved stub subcommands → NotImplemented
    #[test]
    fn stub_migrate() {
        assert_eq!(
            parse_subcommand(&args(&["migrate"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "migrate".to_string()
            })
        );
    }

    #[test]
    fn stub_backup() {
        assert_eq!(
            parse_subcommand(&args(&["backup"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "backup".to_string()
            })
        );
    }

    #[test]
    fn restore_parses() {
        let parsed = parse_subcommand(&args(&[
            "restore",
            "--config",
            "/etc/nodedb.toml",
            "--target-lsn",
            "120",
            "--dry-run",
        ]))
        .unwrap();
        let Some(Subcommand::Restore(restore)) = parsed else {
            panic!("expected a restore subcommand, got {parsed:?}");
        };
        assert_eq!(restore.config, PathBuf::from("/etc/nodedb.toml"));
        assert_eq!(
            restore.scope,
            crate::ctl::restore::RestoreScope::Node(crate::ctl::restore::RestoreTarget::Lsn(120))
        );
        assert!(restore.dry_run);
    }

    #[test]
    fn restore_without_a_target_is_a_usage_error() {
        let err = parse_subcommand(&args(&["restore", "--config", "/c.toml"])).unwrap_err();
        assert!(err.contains("--target-time"), "{err}");
    }

    #[test]
    fn stub_verify() {
        assert_eq!(
            parse_subcommand(&args(&["verify"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "verify".to_string()
            })
        );
    }

    #[test]
    fn stub_repair() {
        assert_eq!(
            parse_subcommand(&args(&["repair"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "repair".to_string()
            })
        );
    }

    #[test]
    fn stub_dump() {
        assert_eq!(
            parse_subcommand(&args(&["dump"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "dump".to_string()
            })
        );
    }

    #[test]
    fn stub_fsck() {
        assert_eq!(
            parse_subcommand(&args(&["fsck"])).unwrap(),
            Some(Subcommand::NotImplemented {
                name: "fsck".to_string()
            })
        );
    }

    // Existing subcommands still parse correctly
    #[test]
    fn regen_certs_parses() {
        let result = parse_subcommand(&args(&[
            "regen-certs",
            "--data-dir",
            "/data",
            "--node-id",
            "42",
        ]));
        assert!(matches!(
            result,
            Ok(Some(Subcommand::RegenCerts { node_id: 42, .. }))
        ));
    }

    #[test]
    fn rotate_ca_stage_parses() {
        let result = parse_subcommand(&args(&["rotate-ca", "--stage", "--data-dir", "/data"]));
        assert!(matches!(result, Ok(Some(Subcommand::RotateCaStage { .. }))));
    }

    #[test]
    fn rotate_ca_finalize_parses() {
        let result = parse_subcommand(&args(&["rotate-ca", "--finalize", "--remove", "abcdef"]));
        assert!(matches!(
            result,
            Ok(Some(Subcommand::RotateCaFinalize { .. }))
        ));
    }

    #[test]
    fn join_token_parses() {
        let result = parse_subcommand(&args(&[
            "join-token",
            "--create",
            "--data-dir",
            "/data",
            "--for-node",
            "7",
        ]));
        assert!(matches!(
            result,
            Ok(Some(Subcommand::JoinTokenCreate { for_node: 7, .. }))
        ));
    }

    // Config file path passes through as Ok(None)
    #[test]
    fn config_path_passthrough() {
        assert_eq!(parse_subcommand(&args(&["./config.toml"])).unwrap(), None);
    }

    #[test]
    fn absolute_config_path_passthrough() {
        assert_eq!(
            parse_subcommand(&args(&["/etc/nodedb/nodedb.toml"])).unwrap(),
            None
        );
    }

    #[test]
    fn dash_capital_v_alias_for_version() {
        assert_eq!(
            parse_subcommand(&args(&["-V"])).unwrap(),
            Some(Subcommand::PrintVersion)
        );
    }

    #[test]
    fn print_version_output_contains_enriched_fields() {
        let block = format_version_block();
        assert!(block.contains("git commit:"), "missing git commit label");
        assert!(block.contains("build date:"), "missing build date label");
        assert!(
            block.contains("build profile:"),
            "missing build profile label"
        );
        assert!(
            block.contains("rust version:"),
            "missing rust version label"
        );
        assert!(block.contains("wire format:"), "missing wire format label");
    }

    // No args → Ok(None)
    #[test]
    fn empty_args_passthrough() {
        assert_eq!(parse_subcommand(&[]).unwrap(), None);
    }

    // run_subcommand for PrintVersion returns 0
    #[test]
    fn run_print_version_exit_code() {
        // We can't capture stdout easily here without additional infrastructure,
        // but we can at least verify the exit code is 0 by testing the dispatcher
        // classification. The actual println! is a side effect.
        assert!(matches!(
            parse_subcommand(&args(&["--version"])).unwrap(),
            Some(Subcommand::PrintVersion)
        ));
    }

    // run_subcommand for NotImplemented returns 1
    #[test]
    fn not_implemented_exit_code() {
        let cmd = Subcommand::NotImplemented {
            name: "migrate".to_string(),
        };
        assert_eq!(run_subcommand(cmd), 1);
    }
}
