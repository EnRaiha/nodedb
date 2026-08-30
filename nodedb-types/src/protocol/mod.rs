// SPDX-License-Identifier: Apache-2.0

pub mod auth;
pub mod batch;
pub mod frames;
pub mod handshake;
pub mod opcodes;
pub mod request_fields;
pub mod text_fields;

pub use auth::{AuthMethod, AuthResponse};
pub use batch::{BatchDocument, BatchVector};
pub use frames::{ErrorPayload, NativeRequest, NativeResponse};
pub use handshake::{
    CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_MSGPACK, CAP_SPATIAL, CAP_STREAMING,
    CAP_TIMESERIES, DEFAULT_NATIVE_PORT, FRAME_HEADER_LEN, HELLO_ACK_MAGIC, HELLO_ERROR_MAGIC,
    HELLO_ERROR_MAGIC_U32, HELLO_MAGIC, HelloAckFrame, HelloErrorCode, HelloErrorFrame, HelloFrame,
    Limits, MAX_FRAME_SIZE, PROTO_VERSION, PROTO_VERSION_MAX, PROTO_VERSION_MIN,
};
pub use opcodes::{OpCode, ResponseStatus, UnknownOpCode};
pub use request_fields::RequestFields;
pub use text_fields::TextFields;
