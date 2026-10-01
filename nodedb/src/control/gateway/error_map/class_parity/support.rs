// SPDX-License-Identifier: BUSL-1.1

//! Helpers shared by the class-parity suites.

/// A named encoder that turns an `Error` into its node-hop wire form.
pub(super) type HopEncoder = (
    &'static str,
    fn(crate::Error) -> nodedb_cluster::rpc_codec::TypedClusterError,
);

/// The SQLSTATE class: the first two characters.
pub(super) fn class(state: &str) -> &str {
    state.get(..2).unwrap_or(state)
}
