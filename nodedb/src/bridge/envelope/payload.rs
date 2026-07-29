// SPDX-License-Identifier: BUSL-1.1

//! Response payload bytes carried across the SPSC bridge.

use std::ops::Deref;
use std::sync::Arc;

/// Response payload: heap-allocated bytes behind an `Arc<[u8]>`.
///
/// The `Deref<Target=[u8]>` impl provides transparent byte access.
/// Slab-backed zero-copy transport is defined in `super::slab` and will be
/// wired in once the Data Plane slab pool is integrated.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Heap-allocated payload.
    Heap(Arc<[u8]>),
}

impl Payload {
    /// Create a heap-backed payload from a Vec.
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self::Heap(Arc::from(v.into_boxed_slice()))
    }

    /// Create an empty payload.
    pub fn empty() -> Self {
        Self::Heap(Arc::from([].as_slice()))
    }

    /// Get the payload bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Heap(a) => a,
        }
    }

    /// Whether this payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Convert to Vec<u8>.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deref for Payload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<Arc<[u8]>> for Payload {
    fn from(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }
}
