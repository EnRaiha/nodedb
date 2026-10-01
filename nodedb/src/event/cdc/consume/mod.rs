// SPDX-License-Identifier: BUSL-1.1

pub mod error;
pub mod local;
pub mod params;
pub mod remote;
pub mod sink;

pub use error::ConsumeError;
pub use local::{
    consume_local, consume_local_with_offsets, consume_stream, validate_consume_identity,
};
pub use params::{ConsumeParams, ConsumeResult, batch_tails};
pub use remote::{RemoteConsumeReply, consume_remote, decode_remote_committed_offsets};
pub use sink::consume_for_sink;
