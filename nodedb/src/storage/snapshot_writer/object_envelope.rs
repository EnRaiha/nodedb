// SPDX-License-Identifier: BUSL-1.1

//! Authenticated framing for individual object-store snapshot objects.

use nodedb_wal::crypto::{AUTH_TAG_SIZE, SEGMENT_ENVELOPE_PREAMBLE_SIZE, WalEncryptionKey};

use crate::storage::segment::{
    SegmentFooter, decrypt_untrusted_segment_bytes, encrypt_untrusted_segment_bytes,
};
use crate::types::Lsn;

/// Hard pre-fetch ceiling for every untrusted snapshot object, including AEAD
/// envelope framing. This is deliberately lower than the generic segment
/// limit because object-store reads are fully buffered.
pub(super) const MAX_SNAPSHOT_OBJECT_BYTES: u64 = 256 * 1024 * 1024;

pub(super) const SNAPSHOT_MANIFEST_KIND: u8 = 0;
pub(super) const SNAPSHOT_CORE_KIND: u8 = 1;

const SNAPSHOT_CONTEXT_MAGIC: [u8; 4] = *b"SNCT";
const SNAPSHOT_CONTEXT_VERSION: u8 = 1;
const SNAPSHOT_CONTEXT_FIXED_BYTES: usize = 4 + 1 + 1 + 2 + 8;

fn snapshot_context(prefix: &str, kind: u8, core_id: Option<usize>) -> crate::Result<Vec<u8>> {
    let core_id = match (kind, core_id) {
        (SNAPSHOT_MANIFEST_KIND, None) => u64::MAX,
        (SNAPSHOT_CORE_KIND, Some(core_id)) => {
            u64::try_from(core_id).map_err(|_| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: "core ID does not fit snapshot context".into(),
            })?
        }
        _ => {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: "invalid snapshot object context".into(),
            });
        }
    };
    let prefix_len = u16::try_from(prefix.len()).map_err(|_| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: "snapshot prefix is too long for object context".into(),
    })?;
    let capacity = SNAPSHOT_CONTEXT_FIXED_BYTES
        .checked_add(prefix.len())
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot object context length overflow".into(),
        })?;
    let mut context = Vec::with_capacity(capacity);
    context.extend_from_slice(&SNAPSHOT_CONTEXT_MAGIC);
    context.push(SNAPSHOT_CONTEXT_VERSION);
    context.push(kind);
    context.extend_from_slice(&prefix_len.to_le_bytes());
    context.extend_from_slice(&core_id.to_le_bytes());
    context.extend_from_slice(prefix.as_bytes());
    Ok(context)
}

pub(super) fn encrypt_snapshot_object(
    bytes: &[u8],
    prefix: &str,
    kind: u8,
    core_id: Option<usize>,
    node_name: &str,
    watermark: u64,
    key: &WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let context = snapshot_context(prefix, kind, core_id)?;
    let payload_len =
        context
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: "snapshot object context length overflow".into(),
            })?;
    let envelope_len = payload_len
        .checked_add(SegmentFooter::size())
        .and_then(|size| size.checked_add(SEGMENT_ENVELOPE_PREAMBLE_SIZE))
        .and_then(|size| size.checked_add(AUTH_TAG_SIZE))
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot envelope length overflow".into(),
        })?;
    check_snapshot_object_size(
        u64::try_from(envelope_len).map_err(|_| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot envelope length does not fit object metadata".into(),
        })?,
        "snapshot object",
    )?;

    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&context);
    payload.extend_from_slice(bytes);
    let lsn = Lsn::new(watermark);
    let footer = SegmentFooter::new(node_name, crc32c::crc32c(&payload), lsn, lsn);
    encrypt_untrusted_segment_bytes(&payload, &footer, key)
}

pub(super) fn decrypt_snapshot_object(
    raw: &[u8],
    prefix: &str,
    kind: u8,
    core_id: Option<usize>,
    key: &WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let expected_context = snapshot_context(prefix, kind, core_id)?;
    let payload = decrypt_untrusted_segment_bytes(raw, key)?;
    let content = payload
        .strip_prefix(expected_context.as_slice())
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot object context does not match requested object".into(),
        })?;
    Ok(content.to_vec())
}

pub(super) fn check_snapshot_object_size(size: u64, object_name: &str) -> crate::Result<()> {
    if size > MAX_SNAPSHOT_OBJECT_BYTES {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("{object_name} exceeds snapshot object resource limit: {size} bytes"),
        });
    }
    Ok(())
}
