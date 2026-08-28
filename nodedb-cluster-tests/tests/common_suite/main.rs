// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target for cluster tests that only need the shared `common`
//! harness (`TestClusterNode` / `cluster_harness` over pgwire). Compiles to
//! ONE test binary rather than one per file. `common` is declared at the
//! crate root, not in `cases/mod.rs`, because every case addresses it via
//! the absolute path `crate::common::...`.

#[path = "../common/mod.rs"]
mod common;

mod cases;
