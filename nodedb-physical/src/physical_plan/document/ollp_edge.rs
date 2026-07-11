// SPDX-License-Identifier: Apache-2.0

//! OLLP predicted-edge tuple carried in `BulkDelete` / `BulkUpdate` plans.

/// One predicted implicit graph edge carried in an OLLP `BulkDelete` plan.
///
/// The Control Plane recon scan surfaces, for every matched edge document
/// (a schemaless doc carrying `_from`/`_to`), the tuple
/// `(surrogate, _from, _to, _type)`. The data plane recomputes the SAME tuple
/// from the actual stored docs at execution time and compares the sorted sets;
/// any divergence (a matched doc's `_from`/`_to`/`_type` concurrently changed,
/// or an edge appeared/disappeared among the matched docs between recon and
/// execution) yields `OllpRetryRequired` before any write — closing the
/// recon→execute content TOCTOU that the surrogate-set check alone misses.
///
/// `label` is the raw `_type` exactly as stored (or `None` when absent); the
/// default-label substitution is NOT applied here so both sides compare the
/// raw stored value.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct OllpPredictedEdge {
    // Field order is the canonical sort order (derived `Ord`): both the
    // control-plane injector and the data-plane verifier sort by
    // `(surrogate, from, to, label)` via `sort_unstable`, so the two sides
    // produce identical orderings and the set comparison is well-defined.
    pub surrogate: u32,
    pub from: String,
    pub to: String,
    pub label: Option<String>,
}
