// SPDX-License-Identifier: Apache-2.0

//! Single TCP connection to a NodeDB server over the native binary protocol.
//!
//! Handles MessagePack framing, request/response correlation via sequence
//! numbers, authentication, and optional TLS encryption.

mod handshake;
mod query;
mod response;
mod send;
mod state;
mod stream;
mod tls;

pub(crate) use response::check_error;
pub use state::NativeConnection;
pub use tls::TlsConfig;
