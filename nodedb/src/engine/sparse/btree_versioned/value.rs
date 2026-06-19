// SPDX-License-Identifier: BUSL-1.1

//! Versioned payload format: `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body...]`.

pub const TAG_LIVE: u8 = 0x00;
pub const TAG_TOMBSTONE: u8 = 0xFF;
pub const TAG_GDPR_ERASED: u8 = 0xFE;

/// Encode a versioned value payload.
pub fn encode_value(tag: u8, valid_from_ms: i64, valid_until_ms: i64, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 16 + body.len());
    buf.push(tag);
    buf.extend_from_slice(&valid_from_ms.to_le_bytes());
    buf.extend_from_slice(&valid_until_ms.to_le_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Decoded view over a versioned value. `body` is empty for tombstone /
/// GDPR-erased entries.
#[derive(Debug, Clone)]
pub struct DecodedValue<'a> {
    pub tag: u8,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: &'a [u8],
}

impl DecodedValue<'_> {
    pub fn is_live(&self) -> bool {
        self.tag == TAG_LIVE
    }
}

pub fn decode_value(bytes: &[u8]) -> crate::Result<DecodedValue<'_>> {
    if bytes.len() < 17 {
        return Err(crate::Error::Serialization {
            format: "versioned-doc".into(),
            detail: format!("value too short: {} bytes", bytes.len()),
        });
    }
    let tag = bytes[0];
    let vf = i64::from_le_bytes(
        bytes[1..9]
            .try_into()
            .expect("length checked above — 8 bytes"),
    );
    let vu = i64::from_le_bytes(
        bytes[9..17]
            .try_into()
            .expect("length checked above — 8 bytes"),
    );
    Ok(DecodedValue {
        tag,
        valid_from_ms: vf,
        valid_until_ms: vu,
        body: &bytes[17..],
    })
}

/// Arguments for `SparseEngine::versioned_put`. Packed to keep the
/// signature under clippy's 7-arg limit without an `#[allow]`.
pub struct VersionedPut<'a> {
    pub database_id: u64,
    pub tenant: u64,
    pub coll: &'a str,
    pub doc_id: &'a str,
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: &'a [u8],
}
