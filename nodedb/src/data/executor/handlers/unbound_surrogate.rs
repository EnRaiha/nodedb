// SPDX-License-Identifier: BUSL-1.1

//! The refusal of a write that carries no surrogate.
//!
//! Every Control Plane writer binds a row's surrogate before the write
//! reaches the Data Plane, and every WAL record carries it. `Surrogate::ZERO`
//! names no bound row, so a write that carries it is refused instead of
//! stored under a key no lookup or delete can reach.

use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;

/// The refusal of a `engine` write into `collection` that carries
/// `Surrogate::ZERO`. `None` when `surrogate` is bound.
pub(in crate::data::executor) fn refuse_unbound(
    engine: &str,
    collection: &str,
    surrogate: Surrogate,
) -> Option<ErrorCode> {
    (surrogate == Surrogate::ZERO).then(|| ErrorCode::RejectedPrevalidation {
        reason: format!(
            "{engine} write into '{collection}' carries no surrogate (Surrogate::ZERO); every \
             row must be bound before it is stored"
        ),
    })
}
