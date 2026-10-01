// SPDX-License-Identifier: BUSL-1.1

//! Canonical rows of the array section.
//!
//! One row per cell version, keyed by its coordinate and system time. The
//! restore keeps both, and assigns the cell's surrogate anew, so the
//! surrogate stays out of the hash.

use nodedb_types::backup_envelope::VerifiedPart;

use crate::Error;
use crate::engine::array::export::ArrayCellVersion;
use crate::types::ArrayCellsBlob;

use super::canonical::{Row, RowHasher, key_hash};
use super::walk::Walk;

const LIVE: u8 = b'l';
const TOMBSTONE: u8 = b't';
const ERASED: u8 = b'e';

fn encode_error(array: &str, e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup verification: array '{array}': {e}"),
    }
}

impl Walk<'_> {
    pub(super) fn arrays(
        &self,
        blobs: &[ArrayCellsBlob],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for blob in blobs {
            let versions: Vec<ArrayCellVersion> =
                zerompk::from_msgpack(&blob.cells).map_err(|e| encode_error(&blob.array, e))?;
            for version in versions {
                sink(array_row(&blob.array, &version)?);
            }
        }
        Ok(())
    }
}

fn array_row(array: &str, version: &ArrayCellVersion) -> Result<Row, Error> {
    let coord = zerompk::to_msgpack_vec(&version.coord).map_err(|e| encode_error(array, e))?;
    let system = version.system_from_ms.to_le_bytes();
    let key: [&[u8]; 2] = [&coord, &system];
    let mut hasher = RowHasher::new(VerifiedPart::Array, &key);
    match &version.payload {
        Some(payload) => {
            hasher.bytes(b'r', &[LIVE]);
            hasher.int(b'f', payload.valid_from_ms);
            hasher.int(b'u', payload.valid_until_ms);
            let attrs =
                zerompk::to_msgpack_vec(&payload.attrs).map_err(|e| encode_error(array, e))?;
            hasher.bytes(b'a', &attrs);
        }
        None if version.erased => hasher.bytes(b'r', &[ERASED]),
        None => hasher.bytes(b'r', &[TOMBSTONE]),
    }
    Ok(Row {
        collection: array.to_string(),
        part: VerifiedPart::Array,
        key: Some(key_hash(VerifiedPart::Array, &key)),
        hash: hasher.finish(),
        expire_at_ms: 0,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_array::tile::cell_payload::CellPayload;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{OPEN_UPPER, Surrogate};

    use super::*;

    fn live(x: i64, v: i64, surrogate: u32) -> ArrayCellVersion {
        ArrayCellVersion {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: 10,
            payload: Some(CellPayload {
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                attrs: vec![CellValue::Int64(v)],
                surrogate: Surrogate::new(surrogate),
            }),
            erased: false,
        }
    }

    fn row(version: &ArrayCellVersion) -> Row {
        array_row("grid", version).expect("row")
    }

    /// A restored cell under another surrogate hashes the same. A changed
    /// value, system time or row kind does not.
    #[test]
    fn the_digest_ignores_surrogates_only() {
        let source = row(&live(1, 7, 3));
        assert_eq!(source.hash, row(&live(1, 7, 99)).hash);
        assert_eq!(source.key, row(&live(1, 7, 99)).key);
        assert_ne!(source.hash, row(&live(1, 8, 3)).hash);
        let later = ArrayCellVersion {
            system_from_ms: 11,
            ..live(1, 7, 3)
        };
        assert_ne!(source.key, row(&later).key);
        let tombstone = ArrayCellVersion {
            payload: None,
            ..live(1, 7, 3)
        };
        let erased = ArrayCellVersion {
            erased: true,
            ..tombstone.clone()
        };
        assert_ne!(row(&tombstone).hash, row(&erased).hash);
        assert_ne!(row(&tombstone).hash, source.hash);
    }
}
