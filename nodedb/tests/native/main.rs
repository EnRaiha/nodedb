// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for the native MessagePack protocol.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Cases here drive the native wire protocol directly — handshake
//! frames, opcodes, and response status — rather than SQL over pgwire.

mod cases;
