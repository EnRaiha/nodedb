// SPDX-License-Identifier: BUSL-1.1

//! Joiner-side fetch: connect to a seed's bootstrap listener, present
//! a token, receive the creds bundle.

use std::net::SocketAddr;
use std::time::Duration;

use quinn::Endpoint;
use rustls::pki_types::CertificateDer;

use super::protocol::{
    BootstrapCredsRequest, BootstrapCredsResponse, DELIVERY_ACK, MAX_FRAME_BYTES, decode_response,
    encode_request,
};

/// Errors from [`fetch_creds`].
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("bootstrap client config: {0}")]
    ClientConfig(String),
    #[error("bootstrap client bind: {0}")]
    Bind(String),
    #[error("bootstrap connect {addr}: {detail}")]
    Connect { addr: SocketAddr, detail: String },
    #[error("bootstrap issuer identity mismatch: {0}")]
    IssuerIdentity(String),
    #[error("bootstrap stream open: {0}")]
    Stream(String),
    #[error("bootstrap io: {0}")]
    Io(String),
    #[error("bootstrap encode/decode: {0}")]
    Codec(String),
    #[error("bootstrap rejected: {0}")]
    Rejected(String),
    #[error("bootstrap frame too large: {0}")]
    FrameTooLarge(usize),
}

/// Connect to the bootstrap listener at `seed` and fetch a cred
/// bundle for `node_id` using `token_hex`. Returns the decoded
/// response; callers still inspect `ok` and extract the DER fields.
pub async fn fetch_creds(
    seed: SocketAddr,
    token_hex: &str,
    node_id: u64,
    timeout: Duration,
) -> Result<BootstrapCredsResponse, FetchError> {
    // Mirror `spawn_listener` — make sure rustls has a default
    // CryptoProvider registered before we build the client config.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let ca_der = crate::auth::join_token::bootstrap_ca_cert(token_hex)
        .map_err(|e| FetchError::ClientConfig(format!("token bootstrap CA: {e}")))?;
    let expected_issuer_spki = crate::auth::join_token::bootstrap_issuer_spki(token_hex)
        .map_err(|e| FetchError::ClientConfig(format!("token bootstrap issuer: {e}")))?;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(ca_der))
        .map_err(|e| FetchError::ClientConfig(format!("add token bootstrap CA: {e}")))?;
    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"nexar/1".to_vec()];
    let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
        .map_err(|e| FetchError::ClientConfig(e.to_string()))?;
    let client_config = quinn::ClientConfig::new(std::sync::Arc::new(quic_config));

    let bind_addr: SocketAddr = if seed.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 any")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 any")
    };
    let mut endpoint = Endpoint::client(bind_addr).map_err(|e| FetchError::Bind(e.to_string()))?;
    endpoint.set_default_client_config(client_config);

    let fut = async {
        let connecting = endpoint
            .connect(seed, crate::transport::config::SNI_HOSTNAME)
            .map_err(|e| FetchError::Connect {
                addr: seed,
                detail: e.to_string(),
            })?;
        let conn = connecting.await.map_err(|e| FetchError::Connect {
            addr: seed,
            detail: e.to_string(),
        })?;
        let peer_identity = conn
            .peer_identity()
            .and_then(|identity| identity.downcast::<Vec<CertificateDer<'static>>>().ok())
            .and_then(|chain| chain.first().cloned())
            .ok_or_else(|| {
                FetchError::IssuerIdentity("peer supplied no leaf certificate".into())
            })?;
        let actual_issuer_spki = crate::transport::peer_identity_verifier::spki_pin_from_cert_der(
            peer_identity.as_ref(),
        )
        .map_err(|e| FetchError::IssuerIdentity(format!("invalid peer leaf: {e}")))?;
        if actual_issuer_spki != expected_issuer_spki {
            return Err(FetchError::IssuerIdentity(
                "peer leaf SPKI does not match the join token".into(),
            ));
        }
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| FetchError::Stream(e.to_string()))?;

        let req = BootstrapCredsRequest {
            token_hex: token_hex.to_string(),
            node_id,
        };
        let body = encode_request(&req).map_err(|e| FetchError::Codec(e.to_string()))?;
        write_frame(&mut send, &body).await?;

        let resp_bytes = read_frame(&mut recv).await?;
        let resp: BootstrapCredsResponse =
            decode_response(&resp_bytes).map_err(|e| FetchError::Codec(e.to_string()))?;
        if resp.ok {
            // The token is durably consumed before the server exposes these
            // bytes. Once a complete success response is decoded, an ACK
            // transport failure must not discard the only usable bundle and
            // strand the joiner. The server retains the bounded enrollment
            // authorization when this best-effort ACK is absent.
            if let Err(error) = write_frame(&mut send, DELIVERY_ACK).await {
                tracing::warn!(%error, "bootstrap delivery ACK write failed after bundle receipt");
            } else if let Err(error) = send.finish() {
                tracing::warn!(%error, "bootstrap delivery ACK finish failed after bundle receipt");
            }
        } else {
            let _ = send.finish();
        }
        Ok::<_, FetchError>(resp)
    };

    let resp = tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| FetchError::Connect {
            addr: seed,
            detail: format!("timed out after {timeout:?}"),
        })??;

    endpoint.close(0u32.into(), b"");
    endpoint.wait_idle().await;

    if !resp.ok {
        return Err(FetchError::Rejected(resp.error.clone()));
    }
    Ok(resp)
}

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Vec<u8>, FetchError> {
    let mut hdr = [0u8; 4];
    recv.read_exact(&mut hdr)
        .await
        .map_err(|e| FetchError::Io(format!("read length prefix: {e}")))?;
    let len = u32::from_be_bytes(hdr) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FetchError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| FetchError::Io(format!("read frame body: {e}")))?;
    Ok(buf)
}

async fn write_frame(send: &mut quinn::SendStream, bytes: &[u8]) -> Result<(), FetchError> {
    let len = u32::try_from(bytes.len()).map_err(|_| FetchError::FrameTooLarge(bytes.len()))?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| FetchError::Io(format!("write length prefix: {e}")))?;
    send.write_all(bytes)
        .await
        .map_err(|e| FetchError::Io(format!("write frame body: {e}")))?;
    Ok(())
}
