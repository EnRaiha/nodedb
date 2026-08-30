//! `NativeConnection` struct definition and construction (plain TCP / TLS).

use std::sync::atomic::AtomicU64;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::Limits;
use tokio::net::TcpStream;

use super::stream::ConnStream;
use super::tls::{TlsConfig, build_tls_client_config};

/// A single connection to a NodeDB server using the native binary protocol.
pub struct NativeConnection {
    pub(super) stream: ConnStream,
    pub(super) seq: AtomicU64,
    pub(super) authenticated: bool,
    /// Protocol version negotiated during the handshake (0 = handshake not performed).
    pub proto_version: u16,
    /// Capability bits advertised by the server in `HelloAckFrame`.
    pub capabilities: u64,
    /// Human-readable server version string from `HelloAckFrame`.
    pub server_version: String,
    /// Per-operation limits from `HelloAckFrame`.
    pub limits: Limits,
}

impl NativeConnection {
    /// Connect to a NodeDB server at the given address (plain TCP).
    pub async fn connect(addr: &str) -> NodeDbResult<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;
        let mut conn = Self {
            stream: ConnStream::Plain(stream),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Connect to a NodeDB server with TLS.
    pub async fn connect_tls(addr: &str, tls: &TlsConfig) -> NodeDbResult<Self> {
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;

        let config = build_tls_client_config(tls)?;
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

        let server_name = tls
            .server_name
            .as_deref()
            .unwrap_or_else(|| addr.split(':').next().unwrap_or("localhost"));

        let sni = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| {
                NodeDbError::sync_connection_failed(format!(
                    "invalid server name '{server_name}': {e}"
                ))
            })?;

        let tls_stream = connector.connect(sni, tcp).await.map_err(|e| {
            NodeDbError::sync_connection_failed(format!("TLS handshake failed: {e}"))
        })?;

        let mut conn = Self {
            stream: ConnStream::Tls(Box::new(tls_stream)),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Whether this connection has been authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }
}
