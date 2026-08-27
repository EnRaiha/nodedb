// SPDX-License-Identifier: BUSL-1.1

//! Reshape the array coordinator's per-shard cell payload.
//!
//! The coordinator partitions a write into buckets of separately-encoded
//! cells (`Vec<raw-cell-bytes>`); `ArrayOp::Put` / `ArrayOp::Delete` carry one
//! flat msgpack array instead. Every consumer of a bucket reshapes it here.

use zerompk::{FromMessagePackOwned, ToMessagePack};

/// Decode a coordinator bucket (`Vec<raw-cell-bytes>`) and re-encode it as the
/// flat `Vec<T>` msgpack array the Data Plane array handlers decode. `what`
/// names the payload in any error the reshape reports.
pub fn flatten_blob_vec<T>(blob_vec_msgpack: &[u8], what: &str) -> crate::Result<Vec<u8>>
where
    T: FromMessagePackOwned + ToMessagePack,
{
    let blobs: Vec<Vec<u8>> =
        zerompk::from_msgpack(blob_vec_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("{what}: bucket decode: {e}"),
        })?;
    let cells: Vec<T> = blobs
        .iter()
        .map(|blob| {
            zerompk::from_msgpack::<T>(blob).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("{what}: cell decode: {e}"),
            })
        })
        .collect::<crate::Result<Vec<T>>>()?;
    zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{what}: flat re-encode: {e}"),
    })
}
