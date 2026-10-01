// SPDX-License-Identifier: BUSL-1.1

//! Content ids of snapshot chunks.
//!
//! A chunk's id is HMAC-SHA256 of its plaintext under a subkey of the WAL
//! key. Equal content gets one id under one key, so a later base finds and
//! reuses it. Without the key, an id reveals nothing about the content, and
//! two clusters with different keys never share an id.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroizing;

/// HKDF domain of the chunk id subkey.
const CHUNK_ID_DOMAIN: &[u8] = b"nodedb-snapshot-chunk-id-v1";

/// Hex digits in a chunk id.
const CHUNK_ID_LEN: usize = 64;

/// Computes chunk ids under one WAL key.
pub struct ChunkKeyer {
    subkey: Zeroizing<[u8; 32]>,
}

impl ChunkKeyer {
    pub fn new(key: &nodedb_wal::crypto::WalEncryptionKey) -> crate::Result<Self> {
        Ok(Self {
            subkey: Zeroizing::new(key.derive_subkey(CHUNK_ID_DOMAIN)?),
        })
    }

    /// The lowercase hex id of `plaintext`.
    pub fn id(&self, plaintext: &[u8]) -> crate::Result<String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.subkey.as_slice()).map_err(|e| {
            crate::Error::Internal {
                detail: format!("snapshot chunk id key: {e}"),
            }
        })?;
        mac.update(plaintext);
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

/// Whether `id` has the shape of a chunk id: 64 lowercase hex digits.
pub fn is_chunk_id(id: &str) -> bool {
    id.len() == CHUNK_ID_LEN && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::crypto::WalEncryptionKey;

    fn keyer(byte: u8) -> ChunkKeyer {
        ChunkKeyer::new(&WalEncryptionKey::from_bytes(&[byte; 32]).unwrap()).unwrap()
    }

    #[test]
    fn equal_content_gets_one_id_under_one_key() {
        let a = keyer(1);
        let id = a.id(b"chunk").unwrap();
        assert!(is_chunk_id(&id), "{id}");
        assert_eq!(id, a.id(b"chunk").unwrap());
        assert_ne!(id, a.id(b"other").unwrap());
    }

    #[test]
    fn another_key_gets_another_id() {
        assert_ne!(
            keyer(1).id(b"chunk").unwrap(),
            keyer(2).id(b"chunk").unwrap()
        );
    }

    #[test]
    fn the_id_is_not_a_plain_digest() {
        use sha2::Digest;
        let plain = hex::encode(Sha256::digest(b"chunk"));
        assert_ne!(keyer(1).id(b"chunk").unwrap(), plain);
    }

    #[test]
    fn malformed_ids_are_refused() {
        assert!(!is_chunk_id("../manifest"));
        assert!(!is_chunk_id(&"A".repeat(64)));
        assert!(!is_chunk_id(&"a".repeat(63)));
    }
}
