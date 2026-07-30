// SPDX-License-Identifier: BUSL-1.1

//! What a durable sync dispatch produced, beyond the ack itself.

/// The result of dispatching one sync write durably.
///
/// `payload` is the apply-payload the caller decodes for the gate verdict. The
/// counts beside it describe what the delta actually carried, which the verdict
/// alone cannot say: an ack reports the server's decision, and two deltas with
/// opposite consequences for the client's data can produce the same one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncDispatchOutcome {
    /// zerompk-encoded `SyncAckResult` bytes produced by the apply.
    pub payload: Vec<u8>,
    /// Operations the delta encoded that the target document already knew, and
    /// the CRDT merge therefore discarded. Zero for non-CRDT sync writes, which
    /// have no merge step to trim anything.
    pub trimmed_ops: u64,
}

impl SyncDispatchOutcome {
    /// A dispatch whose path performs no CRDT merge, so nothing was trimmed.
    pub fn untrimmed(payload: Vec<u8>) -> Self {
        Self {
            payload,
            trimmed_ops: 0,
        }
    }
}
