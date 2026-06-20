// SPDX-License-Identifier: BUSL-1.1

//! Cross-node streaming shuffle (E1): receiver registry, per-part inbox +
//! build barrier, the cluster-hook adapter, and the producer send helper.

pub mod inbox;
pub mod producer;
pub mod receiver;

pub use inbox::{ShuffleInbox, ShuffleKey, ShuffleReceiverRegistry};
pub use producer::send_shuffle_push;
pub use receiver::{DEFAULT_SHUFFLE_INBOX_CAPACITY, RegistryShuffleReceiver};
