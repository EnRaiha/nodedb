// SPDX-License-Identifier: BUSL-1.1

//! Strong, collision-free identifiers for server connections.

use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::str::FromStr;

/// An opaque process-local connection identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionId(NonZeroU64);

/// Failure to construct or parse a [`ConnectionId`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionIdError {
    #[error("connection id must not be zero")]
    Zero,
    #[error("connection id must be an unsigned decimal integer")]
    Invalid,
}

impl ConnectionId {
    /// Construct a connection identifier, rejecting the reserved zero value.
    pub fn new(value: u64) -> Result<Self, ConnectionIdError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ConnectionIdError::Zero)
    }

    /// Return the non-zero numeric identifier.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl FromStr for ConnectionId {
    type Err = ConnectionIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ConnectionIdError::Invalid);
        }
        value
            .parse::<u64>()
            .map_err(|_| ConnectionIdError::Invalid)
            .and_then(Self::new)
    }
}

/// Immutable network endpoints captured at accept time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionMetadata {
    pub peer_addr: SocketAddr,
    pub local_addr: SocketAddr,
}

/// Key for session state.
///
/// Address-keyed sessions remain only for legacy callers. Typed connection
/// registrations are never conflated with an address or each other.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SessionId {
    Connection(ConnectionId),
    LegacySocket(SocketAddr),
}

impl From<&SessionId> for SessionId {
    fn from(value: &SessionId) -> Self {
        *value
    }
}

impl From<ConnectionId> for SessionId {
    fn from(value: ConnectionId) -> Self {
        Self::Connection(value)
    }
}

impl From<&ConnectionId> for SessionId {
    fn from(value: &ConnectionId) -> Self {
        Self::Connection(*value)
    }
}

impl From<SocketAddr> for SessionId {
    fn from(value: SocketAddr) -> Self {
        Self::LegacySocket(value)
    }
}

impl From<&SocketAddr> for SessionId {
    fn from(value: &SocketAddr) -> Self {
        Self::LegacySocket(*value)
    }
}

/// Failure to register a typed connection session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionRegistrationError {
    #[error("connection id {0} is already registered")]
    Duplicate(ConnectionId),
}
