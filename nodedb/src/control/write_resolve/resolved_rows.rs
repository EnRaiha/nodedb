// SPDX-License-Identifier: BUSL-1.1

//! The decided row set a governed predicate write resolved to.

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
}
