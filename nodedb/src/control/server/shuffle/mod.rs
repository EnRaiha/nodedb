// SPDX-License-Identifier: BUSL-1.1

//! Cross-node shuffle: receiver registry, per-part inbox + build barrier, the
//! cluster-hook adapters (receiver E3b + producer E4a), the one-shot producer
//! send helper, and the produce-side hash-partition fan-out sink.

pub mod aggregator_hook;
pub mod consumer_hook;
pub mod fanout;
pub mod frame_explode;
pub mod inbox;
pub mod producer;
pub mod producer_hook;
pub mod receiver;

pub use aggregator_hook::RegistryShuffleAggregator;
pub use consumer_hook::RegistryShuffleConsumer;
pub use fanout::{ShuffleFanoutSink, ShuffleFanoutSinkParams};
pub use inbox::{ShuffleInbox, ShuffleKey, ShuffleReceiverRegistry};
pub use producer::send_shuffle_push;
pub use producer_hook::RegistryShuffleProducer;
pub use receiver::RegistryShuffleReceiver;
