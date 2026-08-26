// SPDX-License-Identifier: BUSL-1.1

//! The decided row set a governed predicate write resolved to.

use nodedb_physical::physical_plan::{DocumentResolvedMutation, KvResolvedMutation};
use nodedb_types::Value;

/// Concrete rows a governed predicate `UPDATE`/`DELETE` resolved to, after the
/// Data Plane decided the write policy against each one's exact image.
///
/// Native `nodedb_types::Value` throughout. `Value::from(serde_json::Value)`
/// is documented-lossy for `Bytes`, `Uuid`, `Ulid`, `Regex`, `DateTime`,
/// `NaiveDateTime`, `Duration`, `Range` and `Record`, so a policy decided
/// against JSON-roundtripped rows would be decided against a value the
/// collection never holds.
pub enum ResolvedRows {
    /// `(primary key, full post-image)` for every row the policy admitted.
    Update(Vec<(Value, Vec<Value>)>),
    /// Primary key of every row the policy admitted for removal.
    Delete(Vec<Value>),
    /// KV: every mutation this resolved write applies, plus the exact response
    /// payload to hand back once they all apply cleanly.
    ///
    /// A KV write is not row-set shaped: one statement can put, delete, and
    /// move TTL across two collections. The mutation list is the decided form,
    /// and the payload travels with it because a `CAS` that did not match owes
    /// the caller a reply while writing nothing at all.
    Kv {
        mutations: Vec<KvResolvedMutation>,
        response_payload: Vec<u8>,
    },
    /// Document: every row mutation this resolved write applies, plus the exact
    /// response payload to hand back once they all apply cleanly.
    ///
    /// One shape for all five governed deferred document writes. A point op
    /// resolves to one mutation, a bulk op to N in the same vector. The payload
    /// travels with them because a `RETURNING` projection is decided against
    /// the images the resolve read, not against state that has since moved on.
    Document {
        mutations: Vec<DocumentResolvedMutation>,
        response_payload: Vec<u8>,
    },
}
