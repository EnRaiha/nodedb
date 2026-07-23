// SPDX-License-Identifier: Apache-2.0

//! Wire result for a bounded, non-mutating CRDT delta preview.

/// Typed result returned by a Data-Plane CRDT preview.
///
/// `post_image_msgpack` encodes `Option<Value>` with zerompk. `None` represents
/// a target row absent after a valid delete; it is distinct from `Some(Null)`.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CrdtPreviewResult {
    /// Canonical zerompk encoding of the validated target post-image.
    pub post_image_msgpack: Vec<u8>,
    /// Number of newly imported operations represented by this delta.
    pub imported_ops: u64,
    /// Domain-bound current frontier digest used to fence the subsequent apply.
    pub frontier_digest: [u8; 32],
}
