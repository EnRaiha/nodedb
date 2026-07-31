// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_NODE_ID` / `NODEDB_SEED_NODES` overrides — both are no-ops (with a
//! warning) unless a `[cluster]` section is already present in config.

use std::net::SocketAddr;

use crate::config::server::ServerConfig;

pub(super) fn apply_cluster_overrides(config: &mut ServerConfig) {
    if let Ok(val) = std::env::var("NODEDB_NODE_ID") {
        match val.trim().parse::<u64>() {
            Ok(node_id) => {
                if let Some(cluster) = config.cluster.as_mut() {
                    tracing::info!(
                        env_var = "NODEDB_NODE_ID",
                        value = node_id,
                        "environment variable override applied"
                    );
                    cluster.node_id = node_id;
                } else {
                    tracing::warn!(
                        env_var = "NODEDB_NODE_ID",
                        value = node_id,
                        "NODEDB_NODE_ID is set but no [cluster] section is present in config; \
                         ignoring (add a [cluster] section to enable cluster mode)"
                    );
                }
            }
            Err(_) => {
                tracing::warn!(
                    env_var = "NODEDB_NODE_ID",
                    value = %val,
                    "ignoring malformed environment variable (expected u64), using config value"
                );
            }
        }
    }

    if let Ok(val) = std::env::var("NODEDB_SEED_NODES") {
        match parse_seed_nodes(&val) {
            Ok(addrs) => {
                if let Some(cluster) = config.cluster.as_mut() {
                    tracing::info!(
                        env_var = "NODEDB_SEED_NODES",
                        value = %val,
                        count = addrs.len(),
                        "environment variable override applied"
                    );
                    cluster.seed_nodes = addrs;
                } else {
                    tracing::warn!(
                        env_var = "NODEDB_SEED_NODES",
                        value = %val,
                        "NODEDB_SEED_NODES is set but no [cluster] section is present in config; \
                         ignoring (add a [cluster] section to enable cluster mode)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    env_var = "NODEDB_SEED_NODES",
                    value = %val,
                    error = %e,
                    "ignoring malformed environment variable, using config value"
                );
            }
        }
    }
}

/// Parse a comma-separated list of `SocketAddr` strings.
///
/// Returns `Ok(Vec<SocketAddr>)` if every entry parses successfully.
/// Returns `Err(bad_entry)` with the first entry that fails to parse,
/// so callers can log it and skip the entire override.
pub fn parse_seed_nodes(s: &str) -> crate::Result<Vec<SocketAddr>> {
    let mut addrs = Vec::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        match entry.parse::<SocketAddr>() {
            Ok(addr) => addrs.push(addr),
            Err(_) => {
                return Err(crate::Error::Config {
                    detail: format!("invalid socket address: '{entry}'"),
                });
            }
        }
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::apply_env_overrides;
    use super::*;
    use crate::config::server::ClusterSettings;

    fn make_cluster(node_id: u64) -> ClusterSettings {
        ClusterSettings {
            node_id,
            listen: "0.0.0.0:9400".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:9400".parse().unwrap()],
            num_groups: 4,
            replication_factor: 3,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: false,
            log_compaction_threshold: None,
        }
    }

    #[test]
    fn env_cluster_overrides() {
        // Always start clean.
        unsafe {
            std::env::remove_var("NODEDB_NODE_ID");
            std::env::remove_var("NODEDB_SEED_NODES");
        }

        // ── NODEDB_NODE_ID: valid value with cluster present → overrides node_id ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "42") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(1)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.cluster.as_ref().unwrap().node_id,
            42,
            "NODEDB_NODE_ID=42 should override node_id"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_NODE_ID: cluster absent → config.cluster stays None ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "99") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg);
        assert!(
            cfg.cluster.is_none(),
            "NODEDB_NODE_ID with no [cluster] section must not create cluster"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_NODE_ID: malformed value → node_id unchanged ──

        unsafe { std::env::set_var("NODEDB_NODE_ID", "not_a_number") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(7)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        assert_eq!(
            cfg.cluster.as_ref().unwrap().node_id,
            7,
            "malformed NODEDB_NODE_ID must leave node_id unchanged"
        );
        unsafe { std::env::remove_var("NODEDB_NODE_ID") };

        // ── NODEDB_SEED_NODES: valid addresses with cluster present → overrides seed_nodes ──

        unsafe { std::env::set_var("NODEDB_SEED_NODES", "10.0.0.1:9400,10.0.0.2:9400") };
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(1)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        let seeds = &cfg.cluster.as_ref().unwrap().seed_nodes;
        assert_eq!(seeds.len(), 2, "two seed addresses should be applied");
        assert_eq!(seeds[0].to_string(), "10.0.0.1:9400");
        assert_eq!(seeds[1].to_string(), "10.0.0.2:9400");
        unsafe { std::env::remove_var("NODEDB_SEED_NODES") };

        // ── NODEDB_SEED_NODES: malformed entry → seed_nodes unchanged (no partial apply) ──

        unsafe { std::env::set_var("NODEDB_SEED_NODES", "10.0.0.1:9400,garbage") };
        let existing_seed: SocketAddr = "192.168.1.1:9400".parse().unwrap();
        let mut cfg = ServerConfig {
            cluster: Some(ClusterSettings {
                seed_nodes: vec![existing_seed],
                ..make_cluster(1)
            }),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg);
        let seeds = &cfg.cluster.as_ref().unwrap().seed_nodes;
        assert_eq!(
            seeds.len(),
            1,
            "malformed NODEDB_SEED_NODES must not partially apply"
        );
        assert_eq!(seeds[0], existing_seed);
        unsafe { std::env::remove_var("NODEDB_SEED_NODES") };
    }
}
