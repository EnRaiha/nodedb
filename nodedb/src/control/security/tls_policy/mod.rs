// SPDX-License-Identifier: BUSL-1.1

//! TLS enforcement: what the transport actually negotiated, what the operator
//! requires of it, and the refusal when the two disagree.

pub mod config;
pub mod policy;
pub mod transport;
pub mod version;

pub use config::TlsPolicyConfig;
pub use policy::{TlsPolicy, TlsRefusal};
pub use transport::TransportSecurity;
pub use version::TlsVersion;
