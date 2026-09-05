// SPDX-License-Identifier: Apache-2.0

//! Grouped test target for the vector-engine integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Held as loose files under `tests/`, these cases cost a separate
//! compile and link step each — nine of them, all pulling in the same
//! `nodedb-vector` closure — for one crate's tests. Grouped, they link once.
//!
//! Every case keeps its own module, so a test's path stays
//! `cases::<file>::<test>` and names never collide across files.

mod cases;
mod support;
