// SPDX-License-Identifier: BUSL-1.1

//! `DocumentEngine` — MessagePack document storage with secondary indexing.
//!
//! Documents stored as MessagePack blobs keyed by `(collection, doc_id)`;
//! declared index paths are extracted on write into redb B-Trees keyed
//! `(collection, path, value) -> doc_id`.

pub mod batch;
pub mod delete;
pub mod get;
pub mod put;
pub mod versioned;

pub use batch::DocumentEngine;
