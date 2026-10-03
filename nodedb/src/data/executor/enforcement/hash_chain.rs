// SPDX-License-Identifier: BUSL-1.1

//! SHA-256 hash chain over the rows of a `HASH_CHAIN` collection.
//!
//! Each INSERT stores `link = SHA-256(previous link || position || contents)`
//! in [`CHAIN_HASH_FIELD`] and its 1-based install-order position in
//! [`CHAIN_SEQ_FIELD`]. The contents are the canonical form of the row as
//! stored: decoded under the collection's storage mode, without the chain
//! fields, encoded as MessagePack. No storage key enters a link, so a chain
//! verifies after a restore or clone re-mints the row surrogates.

use sha2::{Digest, Sha256};

pub use crate::types::hash_chain::{
    CHAIN_HASH_FIELD, CHAIN_SEQ_FIELD, ChainHead, GENESIS_HASH, is_chain_field,
};

use super::super::doc_format;

/// Compute the link at position `seq`.
///
/// `hash = SHA-256(previous_hash || seq || row_contents)`, with the hash and
/// the contents length-prefixed and `seq` as 8 little-endian bytes, so no two
/// input splits produce the same byte stream.
pub fn compute_chain_hash(previous_hash: &str, seq: u64, row_contents: &[u8]) -> String {
    let mut hasher = Sha256::new();

    let prev_bytes = previous_hash.as_bytes();
    hasher.update((prev_bytes.len() as u32).to_le_bytes());
    hasher.update(prev_bytes);

    hasher.update(seq.to_le_bytes());

    hasher.update((row_contents.len() as u32).to_le_bytes());
    hasher.update(row_contents);

    hex::encode(hasher.finalize())
}

/// The canonical contents of a decoded stored row: every field except the
/// chain fields, as MessagePack.
pub fn canonical_contents(view: &serde_json::Value) -> Vec<u8> {
    match view {
        serde_json::Value::Object(obj) => {
            let mut fields = obj.clone();
            fields.retain(|name, _| !is_chain_field(name));
            doc_format::encode_to_msgpack(&serde_json::Value::Object(fields))
        }
        other => doc_format::encode_to_msgpack(other),
    }
}

/// The link a decoded stored row carries. `None` when either chain field is
/// absent or null.
pub fn stored_link(view: &serde_json::Value) -> Option<ChainHead> {
    let hash = view.get(CHAIN_HASH_FIELD)?.as_str()?.to_string();
    let seq = view.get(CHAIN_SEQ_FIELD)?.as_u64()?;
    Some(ChainHead { seq, hash })
}

/// `doc` with `link` written into its chain fields, as MessagePack.
///
/// The single body encoding for a link. The first install and every later
/// re-apply of the same row call it, so both store identical bytes.
pub fn link_body(doc: &serde_json::Value, link: &ChainHead) -> crate::Result<Vec<u8>> {
    let serde_json::Value::Object(obj) = doc else {
        return Err(crate::Error::BadRequest {
            detail: "a hash-chained row must be a document object".to_string(),
        });
    };
    let mut fields = obj.clone();
    fields.insert(
        CHAIN_HASH_FIELD.to_string(),
        serde_json::Value::String(link.hash.clone()),
    );
    fields.insert(
        CHAIN_SEQ_FIELD.to_string(),
        serde_json::Value::Number(link.seq.into()),
    );
    Ok(doc_format::encode_to_msgpack(&serde_json::Value::Object(
        fields,
    )))
}

/// Refuse a submitted document that sets a chain field.
///
/// The chain writes both fields. A null value is an unset strict column and
/// is accepted.
pub fn refuse_supplied_link(collection: &str, doc: &serde_json::Value) -> crate::Result<()> {
    let supplied = [CHAIN_HASH_FIELD, CHAIN_SEQ_FIELD]
        .into_iter()
        .find(|field| doc.get(field).is_some_and(|value| !value.is_null()));
    match supplied {
        Some(field) => Err(crate::Error::RejectedConstraint {
            collection: collection.to_string(),
            constraint: "HASH_CHAIN".to_string(),
            detail: format!("'{field}' is a hash-chain system column and is written by the chain"),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_chain() {
        let h1 = compute_chain_hash(GENESIS_HASH, 1, b"hello");
        assert_eq!(h1.len(), 64); // SHA-256 hex = 64 chars
        assert_ne!(h1, GENESIS_HASH);
    }

    #[test]
    fn chain_is_deterministic() {
        let h1 = compute_chain_hash(GENESIS_HASH, 1, b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, 1, b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_content_different_hash() {
        let h1 = compute_chain_hash(GENESIS_HASH, 1, b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, 1, b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn different_position_different_hash() {
        let h1 = compute_chain_hash(GENESIS_HASH, 1, b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, 2, b"hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn length_prefix_prevents_collision() {
        let h1 = compute_chain_hash("ab", 1, b"cd");
        let h2 = compute_chain_hash("abc", 1, b"d");
        assert_ne!(h1, h2);
    }

    /// The contents a link covers are the same with and without the chain
    /// fields, so the insert and the verifier hash identical bytes.
    #[test]
    fn canonical_contents_ignore_the_chain_fields() {
        let doc = serde_json::json!({"amount": 10, "memo": "a"});
        let link = ChainHead {
            seq: 3,
            hash: "ab".repeat(32),
        };
        let linked =
            doc_format::decode_document(&link_body(&doc, &link).expect("link")).expect("decode");
        assert_eq!(canonical_contents(&linked), canonical_contents(&doc));
        assert_eq!(stored_link(&linked), Some(link));
    }

    #[test]
    fn a_supplied_chain_field_is_refused_and_a_null_one_is_not() {
        let supplied = serde_json::json!({"amount": 1, "_chain_seq": 9});
        assert!(matches!(
            refuse_supplied_link("ledger", &supplied),
            Err(crate::Error::RejectedConstraint { .. })
        ));
        let unset = serde_json::json!({"amount": 1, "_chain_hash": null});
        assert!(refuse_supplied_link("ledger", &unset).is_ok());
    }
}
