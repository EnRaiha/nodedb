// SPDX-License-Identifier: BUSL-1.1

//! Canonical rows of the document and graph-edge sections.
//!
//! A document row is keyed by the identity RESTORE binds it under: the backup's
//! primary-key bind of its surrogate, else the identity INSERT derives from its
//! body. A bitemporal row emits one canonical row per version.

use std::collections::BTreeMap;

use nodedb_types::backup_envelope::VerifiedPart;
use nodedb_types::{RowIdentity, StorageKey, Value};

use crate::Error;
use crate::data::executor::strict_format::{binary_tuple_to_msgpack, undecodable_strict_row};
use crate::engine::graph::edge_store::{
    EdgeValuePayload, is_gdpr_erasure, is_tombstone, parse_versioned_edge_key,
};
use crate::engine::sparse::btree_versioned::{TAG_LIVE, decode_value};
use crate::types::hash_chain::is_chain_field;

use super::canonical::{Row, RowHasher, key_hash};
use super::walk::{DocShape, Walk, malformed};

/// A document's storage key as the snapshot carries it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RowKey<'a> {
    Surrogate(StorageKey),
    /// A key that is no storage key. It is its own identity.
    Other(&'a str),
}

impl<'a> RowKey<'a> {
    fn parse(text: &'a str) -> Self {
        StorageKey::parse(text).map_or(Self::Other(text), Self::Surrogate)
    }
}

/// A stored body decoded to MessagePack and to a native value.
struct Body {
    msgpack: Vec<u8>,
    value: Option<Value>,
}

impl Walk<'_> {
    pub(super) fn documents(
        &self,
        documents: &[(String, Vec<u8>)],
        documents_versioned: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        // A collection with no catalog row decodes as schemaless, keyed by `id`.
        let unknown = DocShape::default();
        for (key, body) in documents {
            let (stored, rest) = self.document_key(key)?;
            let bare = self.bare(stored);
            let shape = self.shapes.get(&bare).unwrap_or(&unknown);
            let row_key = RowKey::parse(rest);
            let body = decode_body(shape, &bare, row_key, body)?;
            let identity = self.identity(&bare, shape, row_key, Some(body.msgpack.as_slice()));
            let mut hasher = RowHasher::new(VerifiedPart::Documents, &[identity.as_slice()]);
            hash_body(&mut hasher, shape, body)?;
            sink(row(bare, &identity, hasher));
        }

        // Every version of a row, in system-time order: the identity comes from
        // the first live version, as RESTORE derives it.
        type Versions<'v> = Vec<(i64, &'v [u8])>;
        let mut versioned: BTreeMap<(&str, RowKey<'_>), Versions<'_>> = BTreeMap::new();
        for (key, value) in documents_versioned {
            let (stored, rest) = self.document_key(key)?;
            let (text, sys) = rest.split_once('\x00').ok_or_else(|| malformed(key))?;
            let sys = sys.parse::<i64>().map_err(|_| malformed(key))?;
            versioned
                .entry((stored, RowKey::parse(text)))
                .or_default()
                .push((sys, value.as_slice()));
        }
        for ((stored, row_key), mut versions) in versioned {
            versions.sort_by_key(|(sys, _)| *sys);
            let bare = self.bare(stored);
            let shape = self.shapes.get(&bare).unwrap_or(&unknown);
            let mut decoded = Vec::with_capacity(versions.len());
            for (_, raw) in versions {
                let version = decode_value(raw)?;
                let body = if version.tag == TAG_LIVE {
                    Some(decode_body(shape, &bare, row_key, version.body)?)
                } else {
                    None
                };
                decoded.push((version, body));
            }
            let first_live = decoded
                .iter()
                .find_map(|(_, body)| body.as_ref().map(|b| b.msgpack.as_slice()));
            let identity = self.identity(&bare, shape, row_key, first_live);
            for (version, body) in decoded {
                let mut hasher = RowHasher::new(VerifiedPart::Documents, &[identity.as_slice()]);
                hasher.bytes(b't', &[version.tag]);
                hasher.int(b'f', version.valid_from_ms);
                hasher.int(b'u', version.valid_until_ms);
                match body {
                    Some(body) => hash_body(&mut hasher, shape, body)?,
                    None => hasher.bytes(b'R', version.body),
                }
                sink(row(bare.clone(), &identity, hasher));
            }
        }
        Ok(())
    }

    pub(super) fn edges(
        &self,
        edges: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (key, value) in edges {
            let (stored, src, label, dst, _system_from) =
                parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
            let endpoints: [&[u8]; 3] = [src.as_bytes(), label.as_bytes(), dst.as_bytes()];
            let mut hasher = RowHasher::new(VerifiedPart::Edges, &endpoints);
            if is_tombstone(value) {
                hasher.bytes(b't', &[]);
            } else if is_gdpr_erasure(value) {
                hasher.bytes(b'g', value);
            } else {
                hasher.bytes(b'p', &EdgeValuePayload::decode(value)?.properties);
            }
            sink(Row {
                collection: self.bare(stored),
                part: VerifiedPart::Edges,
                key: Some(key_hash(VerifiedPart::Edges, &endpoints)),
                hash: hasher.finish(),
                expire_at_ms: 0,
            });
        }
        Ok(())
    }

    /// Split `"{db}:{tid}:{collection}:{rest}"` into the stored collection and
    /// the rest, checking the tenant.
    fn document_key<'k>(&self, key: &'k str) -> Result<(&'k str, &'k str), Error> {
        let (stored, rest) = self
            .scoped_rest(key)?
            .split_once(':')
            .ok_or_else(|| malformed(key))?;
        if stored.is_empty() {
            return Err(malformed(key));
        }
        Ok((stored, rest))
    }

    /// The identity RESTORE binds the row under.
    fn identity(
        &self,
        bare: &str,
        shape: &DocShape,
        key: RowKey<'_>,
        body: Option<&[u8]>,
    ) -> Vec<u8> {
        let key = match key {
            RowKey::Surrogate(key) => key,
            RowKey::Other(text) => return text.as_bytes().to_vec(),
        };
        if let Some(pk) = self.binds.pk(bare, key.surrogate().as_u32()) {
            return pk.to_vec();
        }
        let identity = match body {
            Some(body) => {
                RowIdentity::of_stored_row(body, shape.declared_primary_key.as_deref(), key)
            }
            None => key.to_identity(),
        };
        identity.as_str().as_bytes().to_vec()
    }
}

fn row(collection: String, identity: &[u8], hasher: RowHasher) -> Row {
    Row {
        collection,
        part: VerifiedPart::Documents,
        key: Some(key_hash(VerifiedPart::Documents, &[identity])),
        hash: hasher.finish(),
        expire_at_ms: 0,
    }
}

/// Decode a stored body: a strict row's Binary Tuple with the collection's
/// schema, a schemaless row as it is.
fn decode_body(
    shape: &DocShape,
    collection: &str,
    key: RowKey<'_>,
    body: &[u8],
) -> Result<Body, Error> {
    let msgpack = match &shape.strict {
        Some(schema) => binary_tuple_to_msgpack(body, schema).ok_or_else(|| {
            let identity = match key {
                RowKey::Surrogate(key) => key.to_identity().as_str().to_string(),
                RowKey::Other(text) => text.to_string(),
            };
            undecodable_strict_row(collection, &identity)
        })?,
        None => body.to_vec(),
    };
    let value = nodedb_types::value_from_msgpack(&msgpack).ok();
    Ok(Body { msgpack, value })
}

/// Hash a decoded body. A hash-chained row drops its chain fields: the restore
/// relinks the chain under the destination's storage keys. A body that is no
/// MessagePack value hashes as its bytes.
fn hash_body(hasher: &mut RowHasher, shape: &DocShape, body: Body) -> Result<(), Error> {
    match body.value {
        Some(Value::Object(mut fields)) => {
            if shape.hash_chain {
                fields.retain(|name, _| !is_chain_field(name));
            }
            hasher.value(&Value::Object(fields))
        }
        Some(value) => hasher.value(&value),
        None => {
            hasher.bytes(b'R', &body.msgpack);
            Ok(())
        }
    }
}
