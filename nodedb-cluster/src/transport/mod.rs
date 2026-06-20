// SPDX-License-Identifier: BUSL-1.1

pub mod auth_context;
pub mod client;
pub mod config;
pub mod credentials;
pub mod peer_identity_store;
pub mod peer_identity_verifier;
pub mod pinned_verifier;
pub mod rpc_handler;
pub mod server;

pub use auth_context::AuthContext;

pub use client::{NexarTransport, ShufflePushStream, TransportPeerSnapshot};
pub use config::{
    TlsCredentials, ca_fingerprint, ca_fingerprint_hex, generate_node_credentials,
    generate_node_credentials_multi_san, issue_leaf_for_sans, load_crls_from_pem,
    make_raft_client_config_mtls, make_raft_server_config_mtls,
};
pub use pinned_verifier::{PinnedClientVerifier, PinnedServerVerifier};

/// Re-exported PKI types used in the public shape of [`TlsCredentials`].
///
/// Consumers constructing `TlsCredentials` from PEM files or network bytes
/// do not need their own `rustls` dependency.
pub mod pki_types {
    pub use rustls::pki_types::{
        CertificateDer, CertificateRevocationListDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime,
    };
}
pub use credentials::{TransportCredentials, insecure_transport_count};
pub use peer_identity_store::{NoopIdentityStore, PeerIdentityStore};
pub use peer_identity_verifier::{
    IDENTITY_MISMATCH_QUIC_ERROR, VerifyMethod, VerifyOutcome, spki_pin_from_cert_der,
};
pub use rpc_handler::RaftRpcHandler;
