// SPDX-License-Identifier: Apache-2.0

//! Grouped test target for the WAL integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Held as loose files under `tests/`, these cases cost a separate
//! compile and link step each — sixteen of them, all pulling in the same
//! `nodedb-wal` closure — for one crate's tests. Grouped, they link once.
//!
//! Every case keeps its own module, so a test's path stays
//! `cases::<file>::<test>` and names never collide across files. Cases that
//! only apply to some build configurations carry their `cfg` on the `mod`
//! declaration in `cases/mod.rs`.

mod cases;
