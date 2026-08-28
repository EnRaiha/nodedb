// SPDX-License-Identifier: BUSL-1.1

//! Grouped test target wrapping the `sql_cluster_cross_node_dml` case suite
//! (cross-node CREATE/INSERT/SELECT over 3 pgwire clients).
//!
//! `common` is declared at the crate root, not in `cases/mod.rs`, because
//! every file under `sql_cluster_cross_node_dml_tests/` addresses it via the
//! absolute path `crate::common::...`.

#[path = "../common/mod.rs"]
mod common;

mod cases;
