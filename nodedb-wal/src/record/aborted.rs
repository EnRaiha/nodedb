// SPDX-License-Identifier: Apache-2.0

//! Write-abort payload: names a previously appended record that must never be
//! replayed.
//!
//! A forward write record is appended before the executing engine has decided
//! whether to accept the write. When the verdict is a refusal, the record is
//! already in the log and replay would resurrect a write the server told the
//! client it refused. The abort record names that record's LSN so the replay
//! pre-pass can drop it.
//!
//! Payload layout (fixed 8 bytes, little-endian):
//!
//! ```text
//! ┌────────────────┐
//! │ aborted_lsn u64│
//! └────────────────┘
//! ```

use crate::error::{Result, WalError};

/// Size of a write-abort payload on disk.
pub const WRITE_ABORTED_PAYLOAD_SIZE: usize = 8;

/// Names the LSN of a forward write record that must not be replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteAbortedPayload {
    /// LSN of the record this abort invalidates.
    pub aborted_lsn: u64,
}

impl WriteAbortedPayload {
    pub const fn new(aborted_lsn: u64) -> Self {
        Self { aborted_lsn }
    }

    pub fn to_bytes(&self) -> [u8; WRITE_ABORTED_PAYLOAD_SIZE] {
        self.aborted_lsn.to_le_bytes()
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != WRITE_ABORTED_PAYLOAD_SIZE {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "WriteAborted payload must be {WRITE_ABORTED_PAYLOAD_SIZE} bytes, got {}",
                    buf.len()
                ),
            });
        }
        let aborted_lsn = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        Ok(Self { aborted_lsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_aborted_roundtrip() {
        let payload = WriteAbortedPayload::new(9_876_543_210);
        let bytes = payload.to_bytes();
        assert_eq!(WriteAbortedPayload::from_bytes(&bytes).unwrap(), payload);
    }

    #[test]
    fn write_aborted_wrong_size_rejected() {
        assert!(WriteAbortedPayload::from_bytes(&[0u8; 7]).is_err());
        assert!(WriteAbortedPayload::from_bytes(&[0u8; 9]).is_err());
    }
}
