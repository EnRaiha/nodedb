// SPDX-License-Identifier: BUSL-1.1

//! Plane-neutral row identity.
//!
//! [`KeyRepr`] is the single representation of a row's identity within a
//! collection, shared by both planes: the Data-Plane per-core write-version
//! index keys committed writes by it, and the Control-Plane transaction
//! read-set keys observed reads by it. Because read keys and write keys must
//! compare in one namespace, the type lives here — a pure `Send + Sync` value
//! type with no plane affiliation — rather than inside either plane's module
//! tree.

/// Identity of a row within a collection.
///
/// The engine that owns the row chooses the representation:
/// - `Surrogate` for the cross-engine `u32` surrogate (schemaless + strict
///   document rows, and vector-by-document upserts keyed on the owning doc).
/// - `KvKey` for the raw Key-Value engine key bytes.
/// - `Edge` for a graph edge, whose identity is the `(src, label, dst)` tuple
///   rather than a surrogate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyRepr {
    /// Cross-engine `u32` surrogate identity.
    Surrogate(u32),
    /// Raw Key-Value engine key bytes.
    KvKey(Box<[u8]>),
    /// Graph edge identity: `(source node, edge label, destination node)`.
    Edge {
        src: Box<str>,
        label: Box<str>,
        dst: Box<str>,
    },
}
