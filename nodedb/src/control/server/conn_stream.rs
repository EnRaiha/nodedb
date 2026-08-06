// SPDX-License-Identifier: BUSL-1.1

//! Polymorphic TCP/TLS connection stream for server sessions.
//!
//! Wraps plain TCP and TLS-encrypted streams behind a single type
//! implementing `AsyncRead + AsyncWrite`.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::control::security::tls_policy::TransportSecurity;

/// A connection stream — either plain TCP or TLS-wrapped.
pub enum ConnStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl ConnStream {
    pub fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }

    pub fn tls(stream: tokio_rustls::server::TlsStream<TcpStream>) -> Self {
        Self::Tls(Box::new(stream))
    }

    /// What this connection negotiated, for the TLS policy.
    ///
    /// Read once per connection right after accept, before the stream is
    /// erased behind `AsyncRead + AsyncWrite` (or moved into a `BufReader`)
    /// and the `rustls` session becomes unreachable. The listeners keep the
    /// returned value on their session state and hand it to
    /// [`check_transport_security`](crate::control::server::session_auth::check_transport_security)
    /// once the identity is known.
    pub fn transport_security(&self) -> TransportSecurity {
        match self {
            ConnStream::Plain(_) => TransportSecurity::Cleartext,
            ConnStream::Tls(stream) => {
                let (_, session) = stream.get_ref();
                TransportSecurity::from_rustls(session)
            }
        }
    }
}

impl AsyncRead for ConnStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}
