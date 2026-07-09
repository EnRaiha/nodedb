// SPDX-License-Identifier: Apache-2.0

//! Calvin-specific identity types carried by `MetaOp` Calvin variants.

/// Identity of a single key read by a passive Calvin participant.
///
/// Used as the map key in `MetaOp::CalvinExecuteActive::injected_reads` so
/// active participants can look up which value belongs to which key.
///
/// `String` is used for `collection` rather than `Arc<str>` because
/// `zerompk` derives work directly with `String`; callers may intern
/// the string downstream if needed.
///
/// `BTreeMap` key: `Ord` is derived lexicographically — collection first,
/// surrogate second. This is the determinism contract: all replicas must
/// iterate `injected_reads` in the same order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PassiveReadKeyId {
    /// Collection the key belongs to.
    pub collection: String,
    /// Global surrogate for the row.
    pub surrogate: u32,
}
