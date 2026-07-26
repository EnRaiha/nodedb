// SPDX-License-Identifier: BUSL-1.1

//! Per-protocol port configuration.

use serde::{Deserialize, Serialize};

/// Default sync WebSocket port.
///
/// Exported because the sync listener's own `SyncListenerConfig::default()`
/// must agree with the config default — one constant, not two literals that
/// can drift apart.
pub const DEFAULT_SYNC_PORT: u16 = 9090;

/// Port configuration for all protocol listeners.
///
/// Always-on protocols have a default port. Optional protocols (RESP, ILP)
/// are disabled by default — set a port to enable them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortsConfig {
    /// Native MessagePack protocol port. Default: 6433.
    #[serde(default = "default_native_port")]
    pub native: u16,
    /// PostgreSQL wire protocol port. Default: 6432.
    #[serde(default = "default_pgwire_port")]
    pub pgwire: u16,
    /// HTTP API port (REST, SSE, WebSocket). Default: 6480.
    #[serde(default = "default_http_port")]
    pub http: u16,
    /// Sync WebSocket port (NodeDB-Lite clients). Default: 9090.
    #[serde(default = "default_sync_port")]
    pub sync: u16,
    /// RESP (Redis-compatible) port. Disabled by default. Set to enable.
    #[serde(default)]
    pub resp: Option<u16>,
    /// ILP (InfluxDB Line Protocol) port. Disabled by default. Set to enable.
    #[serde(default)]
    pub ilp: Option<u16>,
}

impl Default for PortsConfig {
    fn default() -> Self {
        Self {
            native: default_native_port(),
            pgwire: default_pgwire_port(),
            http: default_http_port(),
            sync: default_sync_port(),
            resp: None,
            ilp: None,
        }
    }
}

fn default_native_port() -> u16 {
    6433
}
fn default_pgwire_port() -> u16 {
    6432
}
fn default_http_port() -> u16 {
    6480
}
fn default_sync_port() -> u16 {
    DEFAULT_SYNC_PORT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let p = PortsConfig::default();
        assert_eq!(p.native, 6433);
        assert_eq!(p.pgwire, 6432);
        assert_eq!(p.http, 6480);
        assert_eq!(p.sync, 9090);
        assert!(p.resp.is_none());
        assert!(p.ilp.is_none());
    }

    #[test]
    fn unknown_port_field_rejected() {
        let raw = "native = 1234\nhonk = 9999\n";
        let result: Result<PortsConfig, _> = toml::from_str(raw);
        assert!(result.is_err());
    }
}
