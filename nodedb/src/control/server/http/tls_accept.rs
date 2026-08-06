// SPDX-License-Identifier: BUSL-1.1

//! HTTPS acceptor that captures the negotiated TLS version per connection.
//!
//! `axum-server` terminates TLS inside its acceptor and then serves the
//! connection through a `hyper` service, so by the time a route runs the
//! `rustls` session is gone. Its [`Accept`] hook hands back *both* the
//! accepted stream and the service that will serve it, which is the one place
//! the handshake facts can be read and attached: this acceptor reads the
//! negotiated version off the stream and wraps the connection's service so
//! every request on it carries a [`TransportSecurity`] extension.

use std::future::Future;
use std::io;
use std::pin::Pin;

use axum::Extension;
use axum::middleware::AddExtension;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tower::Layer;

use crate::control::security::tls_policy::TransportSecurity;

/// A rustls acceptor that stamps each connection's transport security onto
/// every request served over it.
#[derive(Clone)]
pub(crate) struct TransportCapturingAcceptor {
    inner: RustlsAcceptor,
}

impl TransportCapturingAcceptor {
    pub(crate) fn new(config: RustlsConfig) -> Self {
        Self {
            inner: RustlsAcceptor::new(config),
        }
    }
}

impl<S> Accept<TcpStream, S> for TransportCapturingAcceptor
where
    S: Send + 'static,
{
    type Stream = TlsStream<TcpStream>;
    type Service = AddExtension<S, TransportSecurity>;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let handshake = self.inner.accept(stream, service);
        Box::pin(async move {
            let (stream, service) = handshake.await?;
            let transport = {
                let (_, session) = stream.get_ref();
                TransportSecurity::from_rustls(session)
            };
            Ok((stream, Extension(transport).layer(service)))
        })
    }
}
