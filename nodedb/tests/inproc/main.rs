// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for in-process integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Every case here links the `nodedb` library and drives it in
//! process — building `SharedState` directly, or reaching into engine
//! internals a wire client cannot observe. Cases that only need SQL over a
//! socket belong in the `wire` target, which links no server code at all.

mod cases;
