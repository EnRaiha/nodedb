// SPDX-License-Identifier: BUSL-1.1

//! Immutable identity captured for one accepted pgwire connection.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::control::server::shared::session::ConnectionId;

/// Immutable pgwire connection identity. Addresses are metadata only; all
/// session state is keyed by `id`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PgConnectionContext {
    pub(crate) id: ConnectionId,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) local_addr: SocketAddr,
}

/// Failure to allocate a non-wrapping connection identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ConnectionAllocationError {
    #[error("pgwire connection identifier space exhausted")]
    Exhausted,
}

/// Process-local allocator that never wraps into a previously issued ID.
pub(crate) struct ConnectionIdAllocator {
    next: AtomicU64,
}

impl ConnectionIdAllocator {
    pub(crate) const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    pub(crate) fn allocate(&self) -> Result<ConnectionId, ConnectionAllocationError> {
        let id = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ConnectionAllocationError::Exhausted)?;
        ConnectionId::new(id).map_err(|_| ConnectionAllocationError::Exhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_nonzero_and_unique() {
        let allocator = ConnectionIdAllocator::new();
        assert_eq!(allocator.allocate().unwrap().get(), 1);
        assert_eq!(allocator.allocate().unwrap().get(), 2);
    }

    #[test]
    fn allocation_refuses_to_wrap() {
        let allocator = ConnectionIdAllocator {
            next: AtomicU64::new(u64::MAX),
        };
        assert_eq!(
            allocator.allocate(),
            Err(ConnectionAllocationError::Exhausted)
        );
    }
}
