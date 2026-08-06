// SPDX-License-Identifier: BUSL-1.1

//! [`TransportSecurity`] — what a connection actually negotiated, captured at
//! accept and carried to the post-authentication point that evaluates policy.
//!
//! Every listener erases its stream behind `AsyncRead + AsyncWrite` (or hands
//! it to a protocol crate) shortly after the handshake, so the handshake facts
//! have to be read out while the `rustls` connection is still reachable and
//! then travelled with the session. This type is that value: `Copy`, plane-free,
//! and small enough to live on a session struct or in a request extension.

use super::version::TlsVersion;

/// The transport security of one accepted connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSecurity {
    /// No TLS: the peer is talking to us in the clear.
    Cleartext,
    /// TLS whose negotiated version is on the policy's version ladder.
    Tls(TlsVersion),
    /// TLS was negotiated, but the version is not one the policy can rank —
    /// an unrecognised ordinal, or a handshake state that never agreed on a
    /// version. Enforcement treats it as unassessable and refuses rather than
    /// assuming it clears the minimum.
    TlsUnidentified,
}

impl TransportSecurity {
    /// Read the transport facts out of a live `rustls` connection.
    ///
    /// Called once per connection, immediately after the acceptor's handshake
    /// future resolves, while the `ServerConnection` is still in hand.
    pub fn from_rustls(connection: &tokio_rustls::rustls::CommonState) -> Self {
        match connection.protocol_version() {
            Some(version) => match TlsVersion::from_negotiated(version) {
                Some(version) => Self::Tls(version),
                None => Self::TlsUnidentified,
            },
            None => Self::TlsUnidentified,
        }
    }

    /// Whether the connection is encrypted at all.
    pub fn is_encrypted(self) -> bool {
        !matches!(self, Self::Cleartext)
    }

    /// The negotiated version, when it is one the policy can rank.
    pub fn version(self) -> Option<TlsVersion> {
        match self {
            Self::Tls(version) => Some(version),
            Self::Cleartext | Self::TlsUnidentified => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleartext_is_not_encrypted_and_has_no_version() {
        assert!(!TransportSecurity::Cleartext.is_encrypted());
        assert_eq!(TransportSecurity::Cleartext.version(), None);
    }

    #[test]
    fn tls_reports_its_negotiated_version() {
        let transport = TransportSecurity::Tls(TlsVersion::Tls1_3);
        assert!(transport.is_encrypted());
        assert_eq!(transport.version(), Some(TlsVersion::Tls1_3));
    }

    /// An unidentified TLS connection is still encrypted — it just cannot be
    /// ranked, which is a refusal rather than a pass.
    #[test]
    fn unidentified_tls_is_encrypted_but_unrankable() {
        assert!(TransportSecurity::TlsUnidentified.is_encrypted());
        assert_eq!(TransportSecurity::TlsUnidentified.version(), None);
    }
}
